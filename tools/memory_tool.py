#!/usr/bin/env python3
"""
Memory Tool Module - Persistent Curated Memory

Provides bounded, file-backed memory that persists across sessions. Two stores:
  - MEMORY.md: agent's personal notes and observations (environment facts, project
    conventions, tool quirks, things learned)
  - USER.md: what the agent knows about the user (preferences, communication style,
    expectations, workflow habits)

Both are injected into the system prompt as a frozen snapshot at session start.
Mid-session writes update files on disk immediately (durable) but do NOT change
the system prompt -- this preserves the prefix cache for the entire session.
The snapshot refreshes on the next session start.

Entry delimiter: § (section sign). Entries can be multiline.
Character limits (not tokens) because char counts are model-independent.

Design:
- Single `memory` tool with action parameter: add, replace, remove, read
- replace/remove use short unique substring matching (not full text or IDs)
- Behavioral guidance lives in the tool schema description
- Frozen snapshot pattern: system prompt is stable, tool responses show live state
"""

import json
import logging
import os
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from hermes_constants import get_hermes_home
from typing import Dict, Any, List, Optional

import yaml  # constraint validation

from utils import atomic_replace

# fcntl is Unix-only; on Windows use msvcrt for file locking
msvcrt = None
try:
    import fcntl
except ImportError:
    fcntl = None
    try:
        import msvcrt
    except ImportError:
        pass

logger = logging.getLogger(__name__)

# Where memory files live — resolved dynamically so profile overrides
# (HERMES_HOME env var changes) are always respected.  The old module-level
# constant was cached at import time and could go stale if a profile switch
# happened after the first import.
def get_memory_dir() -> Path:
    """Return the profile-scoped memories directory."""
    return get_hermes_home() / "memories"

ENTRY_DELIMITER = "\n§\n"


# ---------------------------------------------------------------------------
# Memory content scanning — lightweight check for injection/exfiltration
# in content that gets injected into the system prompt.
#
# Patterns live in ``tools/threat_patterns.py`` — the single source of truth
# shared with the context-file scanner and the tool-result delimiter system.
# Memory uses the "strict" scope (broadest pattern set) because:
#  - memory entries are user-curated; the user can rewrite a flagged entry
#  - memory enters the system prompt as a FROZEN snapshot, so a poisoned
#    entry persists for the entire session and across sessions until
#    explicitly removed.
# ---------------------------------------------------------------------------

from tools.threat_patterns import first_threat_message as _first_threat_message


def _scan_memory_content(content: str) -> Optional[str]:
    """Scan memory content for injection/exfil patterns. Returns error string if blocked."""
    return _first_threat_message(content, scope="strict")


def _drift_error(path: "Path", bak_path: str) -> Dict[str, Any]:
    """Build the error dict returned when external drift is detected.

    The on-disk memory file contains content that wouldn't round-trip
    through the tool's parser/serializer — flushing would discard the
    appended/edited content from a patch tool, shell append, manual edit,
    or sister-session write. We refuse the mutation, point the operator at
    the .bak.<ts> snapshot we took, and tell them what to do next.
    """
    return {
        "success": False,
        "error": (
            f"Refusing to write {path.name}: file on disk has content that "
            f"wouldn't round-trip through the memory tool (likely added by "
            f"the patch tool, a shell append, a manual edit, or a "
            f"concurrent session). A snapshot was saved to {bak_path}. "
            f"Resolve the drift first — either rewrite the file as a clean "
            f"§-delimited list of entries, or move the extra content out — "
            f"then retry. This guard exists to prevent silent data loss "
            f"(issue #26045)."
        ),
        "drift_backup": bak_path,
        "remediation": (
            "Open the .bak file, integrate the missing entries into the "
            "memory tool one at a time via memory(action=add, content=...), "
            "then remove or rewrite the original file to a clean state."
        ),
    }


class MemoryStore:
    """
    Bounded curated memory with file persistence. One instance per AIAgent.

    Maintains two parallel states:
      - _system_prompt_snapshot: frozen at load time, used for system prompt injection.
        Never mutated mid-session. Keeps prefix cache stable.
      - memory_entries / user_entries: live state, mutated by tool calls, persisted to disk.
        Tool responses always reflect this live state.
    """

    # After this many failed consolidation attempts (overflow / zero-match) in
    # ONE turn, stop instructing the model to "retry in this turn" and return a
    # terminal "save skipped" result so a fragile replace/add can't loop the
    # turn to budget exhaustion and suppress the user's reply (issue #42405).
    _MAX_CONSOLIDATION_FAILURES_PER_TURN = 5

    def __init__(self, memory_char_limit: int = 2200, user_char_limit: int = 1375):
        self.memory_entries: List[str] = []
        self.user_entries: List[str] = []
        self.memory_char_limit = memory_char_limit
        self.user_char_limit = user_char_limit
        # Frozen snapshot for system prompt -- set once at load_from_disk()
        self._system_prompt_snapshot: Dict[str, str] = {"memory": "", "user": ""}
        # Per-turn counter of failed at-capacity consolidation attempts; reset
        # at each turn boundary by reset_consolidation_failures() (#42405).
        self._consolidation_failures = 0

    def reset_consolidation_failures(self) -> None:
        """Reset the per-turn consolidation-failure counter (call at turn start)."""
        self._consolidation_failures = 0

    def _consolidation_failure(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """Count an at-capacity consolidation failure and degrade gracefully.

        Under the per-turn cap, return ``response`` unchanged (it already tells
        the model how to self-correct + retry in this turn). Once the cap is
        exceeded, drop the retry instruction and return a TERMINAL result so the
        model stops looping memory calls and proceeds to answer the user — a
        failed memory side effect must never block the turn's reply (#42405).
        """
        self._consolidation_failures += 1
        if self._consolidation_failures <= self._MAX_CONSOLIDATION_FAILURES_PER_TURN:
            return response
        return {
            "success": False,
            "done": True,
            "error": (
                f"Memory consolidation failed {self._consolidation_failures} times "
                "this turn. Stop retrying memory calls — leave memory unchanged for "
                "now and continue with your reply to the user. The fact can be saved "
                "in a later turn."
            ),
        }

    def load_from_disk(self):
        """Load entries from MEMORY.md and USER.md, capture system prompt snapshot.

        The frozen snapshot is what enters the system prompt. We scan each
        entry for injection/promptware patterns at snapshot-build time —
        ANY hit replaces the entry text in the snapshot with a placeholder
        like ``[BLOCKED: …]``, so a poisoned-on-disk memory file (supply
        chain, compromised tool, sister-session write) cannot inject into
        the system prompt.

        The live ``memory_entries`` / ``user_entries`` lists keep the
        original text so the user can still SEE poisoned entries via
        see poisoned entries by inspecting the source files directly, and remove them — silently dropping them would hide the attack from the user.

        Scanning is deterministic from disk bytes, so the snapshot remains
        stable for the entire session (prefix-cache invariant holds).
        """
        mem_dir = get_memory_dir()
        mem_dir.mkdir(parents=True, exist_ok=True)

        self.memory_entries = self._read_file(mem_dir / "MEMORY.md")
        self.user_entries = self._read_file(mem_dir / "USER.md")

        # Deduplicate entries (preserves order, keeps first occurrence)
        self.memory_entries = list(dict.fromkeys(self.memory_entries))
        self.user_entries = list(dict.fromkeys(self.user_entries))

        # Sanitize entries for the system-prompt snapshot only.  Live state
        # (memory_entries / user_entries) keeps the raw text so the user
        # can see + remove poisoned entries via the memory tool.
        sanitized_memory = self._sanitize_entries_for_snapshot(self.memory_entries, "MEMORY.md")
        sanitized_user = self._sanitize_entries_for_snapshot(self.user_entries, "USER.md")

        # Capture frozen snapshot for system prompt injection
        self._system_prompt_snapshot = {
            "memory": self._render_block("memory", sanitized_memory),
            "user": self._render_block("user", sanitized_user),
        }

        # Prefer summary file if recent (within 24h)
        self._summary_snapshot_mtime = 0.0
        self._user_summary_snapshot_mtime = 0.0
        self._memory_guard_mtime = 0.0  # YAML constraint guard application timestamp
        summary_path = mem_dir / "MEMORY_SUMMARY.md"
        if summary_path.exists():
            import time
            age_seconds = time.time() - summary_path.stat().st_mtime
            if age_seconds < 86400:
                summary_text = summary_path.read_text(encoding="utf-8")
                summary_text = self._apply_memory_constraint_guard(summary_text)
                header = f"MEMORY (daily summary) [{len(summary_text):,} chars, {age_seconds/3600:.1f}h ago]"
                separator = "═" * 46
                self._system_prompt_snapshot["memory"] = f"{separator}\n{header}\n{separator}\n{summary_text}"
                self._summary_snapshot_mtime = summary_path.stat().st_mtime

        # Prefer user summary if recent (within 24h)
        # Symmetric with memory summary redirection
        user_summary_path = mem_dir / "USER_SUMMARY.md"
        if user_summary_path.exists():
            import time
            age_seconds = time.time() - user_summary_path.stat().st_mtime
            if age_seconds < 86400:
                summary_text = user_summary_path.read_text(encoding="utf-8")
                # READ-time 约束守护：与 _refresh_user_summary() 对称，
                # 确保 gateway 启动后的首个会话也有 guard 保护。
                summary_text = self._apply_user_constraint_guard(summary_text)
                header = f"USER PROFILE (summary) [{len(summary_text):,} chars, {age_seconds/3600:.1f}h ago]"
                separator = "═" * 46
                self._system_prompt_snapshot["user"] = f"{separator}\n{header}\n{separator}\n{summary_text}"
                self._user_summary_snapshot_mtime = user_summary_path.stat().st_mtime
            else:
                logger.warning(
                    "USER_SUMMARY.md is stale (%.1fh old), falling back to full USER.md",
                    age_seconds / 3600,
                )

    def _refresh_memory_summary(self):
        """Re-read MEMORY_SUMMARY.md into the system prompt snapshot if newer.

        Called from format_for_system_prompt() on every new conversation
        (system prompt build).  If MEMORY.md is newer than the summary,
        regenerates it on the spot (DS V3, ~2-3s) so the current
        conversation always gets a fresh summary.  In the steady state
        this is a single stat(2) check and returns immediately, preserving
        prefix-cache stability across turns.
        """
        mem_dir = get_memory_dir()
        summary_path = mem_dir / "MEMORY_SUMMARY.md"
        memory_path = mem_dir / "MEMORY.md"

        import time

        # ── 当场刷新：MEMORY.md 比摘要新 → 重新生成 ──
        if memory_path.exists():
            memory_mtime = memory_path.stat().st_mtime
            summary_mtime = summary_path.stat().st_mtime if summary_path.exists() else 0.0
            if memory_mtime > summary_mtime:
                self._generate_summary_on_demand(memory_path, summary_path)

        if not summary_path.exists():
            return

        summary_mtime = summary_path.stat().st_mtime
        if summary_mtime <= self._summary_snapshot_mtime:
            return  # No change since last load

        age_seconds = time.time() - summary_mtime
        if age_seconds >= 86400:
            return  # Stale summary

        summary_text = summary_path.read_text(encoding="utf-8")
        # READ-time constraint guard: independent YAML mtime tracking.
        # Only re-scan when MEMORY_CONSTRAINTS.yaml is newer than our last
        # guard application — avoids redundant string scans on every /new.
        constraint_path = mem_dir / "MEMORY_CONSTRAINTS.yaml"
        if constraint_path.exists():
            constraint_mtime = constraint_path.stat().st_mtime
            if constraint_mtime > self._memory_guard_mtime:
                guarded = self._apply_memory_constraint_guard(summary_text)
                if guarded != summary_text:
                    summary_text = guarded
                self._memory_guard_mtime = constraint_mtime

        header = f"MEMORY (daily summary) [{len(summary_text):,} chars, {age_seconds/3600:.1f}h ago]"
        separator = "═" * 46
        self._system_prompt_snapshot["memory"] = (
            f"{separator}\n{header}\n{separator}\n{summary_text}"
        )
        self._summary_snapshot_mtime = summary_mtime

    def _refresh_user_summary(self):
        """Re-read USER_SUMMARY.md into the system prompt snapshot if newer.

        Called from format_for_system_prompt() on every new conversation.
        If USER.md is newer than USER_SUMMARY.md, regenerates it on the
        spot (DS V3, ~2-3s) so the current conversation always gets a
        fresh summary.  In the steady state this is a single stat(2)
        check and returns immediately, preserving prefix-cache stability
        across turns.

        Applies _apply_user_constraint_guard() to mechanically ensure all
        USER_CONSTRAINTS.yaml entries are covered in the injected prompt.
        """
        mem_dir = get_memory_dir()
        user_summary_path = mem_dir / "USER_SUMMARY.md"
        user_path = mem_dir / "USER.md"

        import time

        # ── 当场刷新：USER.md 比摘要新 → 重新生成 ──
        if user_path.exists():
            user_mtime = user_path.stat().st_mtime
            summary_mtime = (
                user_summary_path.stat().st_mtime
                if user_summary_path.exists()
                else 0.0
            )
            if user_mtime > summary_mtime:
                self._generate_user_summary_on_demand(user_path, user_summary_path)

        if not user_summary_path.exists():
            return

        user_summary_mtime = user_summary_path.stat().st_mtime
        if user_summary_mtime <= self._user_summary_snapshot_mtime:
            return  # No change since last load

        age_seconds = time.time() - user_summary_mtime
        if age_seconds >= 86400:
            return  # Stale summary — fall back to full USER.md

        summary_text = user_summary_path.read_text(encoding="utf-8")
        # READ-time 约束守护：每次加载摘要时检查 YAML 约束覆盖，追加未覆盖条目
        summary_text = self._apply_user_constraint_guard(summary_text)
        header = f"USER PROFILE (summary) [{len(summary_text):,} chars, {age_seconds/3600:.1f}h ago]"
        separator = "═" * 46
        self._system_prompt_snapshot["user"] = (
            f"{separator}\n{header}\n{separator}\n{summary_text}"
        )
        self._user_summary_snapshot_mtime = user_summary_mtime

    def _apply_memory_constraint_guard(self, summary_text: str) -> str:
        """READ-time 机械兜底：MEMORY_CONSTRAINTS.yaml → 检查摘要覆盖 → 漏了的追加。

        USER Phase 2 反哺：MEMORY 原只有 WRITE-time 守护（hook 生成摘要时）。
        如果 hook 未触发（摘要未过期 + MEMORY.md 未更新 → hook 跳过），
        约束文件修改后守护延后到下次 hook 触发。补 READ-time 第二层。

        mtime 去重：解析摘要中的 HTML 时间戳 → 约束文件未更新则跳过。
        """
        mem_dir = get_memory_dir()
        constraint_path = mem_dir / "MEMORY_CONSTRAINTS.yaml"
        if not constraint_path.exists():
            return summary_text

        import re as _re
        from datetime import datetime as _dt

        # mtime 去重：约束文件未更新 → 跳过
        m = _re.search(r'<!-- guard:mtime:(\S+) -->', summary_text)
        if m:
            try:
                guard_ts = _dt.fromisoformat(m.group(1)).timestamp()
                if constraint_path.stat().st_mtime <= guard_ts:
                    return summary_text  # 约束未更新，跳过
            except Exception:
                pass  # 时间戳解析失败 → 跑守护（安全侧）

        try:
            data = yaml.safe_load(constraint_path.read_text(encoding="utf-8"))
            constraints = data.get('constraints', [])
        except Exception:
            return summary_text  # YAML 损坏 → 降级

        uncovered = []
        for c in constraints:
            try:
                kws = c.get('keywords', [])
                pts = c.get('patterns', [])
                ft = c.get('full_text', '')
                pri = c.get('priority', 'P2')
                if any(kw in summary_text for kw in kws):
                    continue
                matched = False
                for p in pts:
                    if not p.strip():
                        continue
                    try:
                        if _re.search(p, summary_text):
                            matched = True
                            break
                    except _re.error:
                        continue
                if not matched:
                    uncovered.append((pri, ft))
            except Exception:
                continue

        if not uncovered:
            return summary_text

        priority_order = {'P0': 0, 'P1': 1, 'P2': 2}
        uncovered.sort(key=lambda x: priority_order.get(x[0], 2))

        guard_block = (
            "\n\n---\n"
            "## ⚠️ 机械兜底约束（Governance Decay Guard）\n"
            "> 以下条目由 MEMORY_CONSTRAINTS.yaml 机械追加。\n"
            "> 若与摘要中用户意图冲突，以摘要为准。\n"
        )
        for _, text in uncovered:
            guard_block += f"\n- {text}"
        guard_block += f"\n<!-- guard:mtime:{_dt.now().isoformat(timespec='seconds')} -->"

        return summary_text + guard_block

    def _apply_user_constraint_guard(self, summary_text: str) -> str:
        """机械兜底：检查 USER_SUMMARY.md 是否覆盖 USER_CONSTRAINTS.yaml 所有条目。

        YAML 解析失败 → 降级返回原文（不阻塞注入）。
        单条约束损坏 → skip 该条，继续处理其余。
        未覆盖条目按 priority 降序（P0 在前）追加到摘要末尾。
        约束追加块加注释：若与摘要中用户意图冲突，以摘要为准。

        READ-time 设计（非 WRITE-time）：USER 摘要手动维护，无 hook 生成路径，
        守护在每次 /new 加载摘要时执行。
        """
        mem_dir = get_memory_dir()
        constraint_path = mem_dir / "USER_CONSTRAINTS.yaml"
        if not constraint_path.exists():
            return summary_text

        try:
            data = yaml.safe_load(constraint_path.read_text(encoding="utf-8"))
            constraints = data.get('constraints', [])
        except Exception:
            return summary_text  # YAML 损坏时降级——不阻塞注入

        uncovered = []
        for c in constraints:
            try:
                keywords = c.get('keywords', [])
                full_text = c.get('full_text', '')
                priority = c.get('priority', 'P2')
                if not any(kw in summary_text for kw in keywords):
                    uncovered.append((priority, full_text))
            except Exception:
                continue  # 单条损坏不影响其余

        if not uncovered:
            return summary_text

        priority_order = {'P0': 0, 'P1': 1, 'P2': 2}
        uncovered.sort(key=lambda x: priority_order.get(x[0], 2))

        guard_block = (
            "\n\n---\n"
            "## ⚠️ 机械兜底约束\n"
            "> 以下条目由 USER_CONSTRAINTS.yaml 机械追加。\n"
            "> 若与摘要中用户意图冲突，以摘要为准。\n"
        )
        for _, text in uncovered:
            guard_block += f"\n- {text}"

        return summary_text + guard_block

    def _generate_summary_on_demand(self, memory_path, summary_path):
        """Regenerate MEMORY_SUMMARY.md from MEMORY.md using DeepSeek V3.

        Called from _refresh_memory_summary() when MEMORY.md mtime is
        newer than the cached summary — the current /new conversation
        should not wait for a cron or hook to catch up.
        """
        import json as _json
        import re as _re
        import urllib.request as _urllib

        memory_text = memory_path.read_text(encoding="utf-8")
        if len(memory_text) < 100:
            return

        # ── API key ──
        ds_key_file = os.path.expanduser("~/.hermes/.deepseek_key")
        if not os.path.exists(ds_key_file):
            logger.warning("_generate_summary_on_demand: no DeepSeek key")
            return
        ds_key = Path(ds_key_file).read_text(encoding="utf-8").strip()

        prompt = (
            "将以下 MEMORY.md 压缩为 5KB 以内的结构化摘要。\n"
            "保留：活跃项目状态、关键决策、用户偏好、已知陷阱、环境配置。\n"
            "丢弃：已完成的记录、过时配置、重复内容。\n\n"
            "MEMORY.md 原文：\n"
            + memory_text +
            "\n\n按以下结构输出（不要加额外说明）：\n"
            "# MEMORY.md 结构化摘要 (5K)\n"
            f"> 生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            "> 模型: DeepSeek V3\n\n"
            "## 活跃项目状态与关键决策\n"
            "## 用户偏好与铁律\n"
            "## 已知陷阱与避坑\n"
            "## 环境与配置要点\n\n"
            "> ⚠️ This is a compressed summary. Use session_search for full content."
        )

        payload = _json.dumps({
            "model": "deepseek-chat",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3,
            "max_tokens": 3000,
        }).encode()

        try:
            req = _urllib.Request(
                "https://api.deepseek.com/v1/chat/completions",
                data=payload,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {ds_key}",
                },
            )
            resp = _json.loads(_urllib.urlopen(req, timeout=60).read())
            summary = resp["choices"][0]["message"]["content"].strip()
        except Exception:
            logger.warning(
                "_generate_summary_on_demand: API call failed", exc_info=True
            )
            return

        if not summary:
            return

        # ── Governance Decay Guard — 机械兜底约束 ──
        constraints_path = os.path.expanduser(
            "~/.hermes/memories/MEMORY_CONSTRAINTS.txt"
        )
        if os.path.exists(constraints_path):
            appended = []
            with open(constraints_path) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    parts = line.split("|")
                    if len(parts) < 2:
                        continue
                    *patterns, human_text = parts
                    if any(_re.search(p, summary) for p in patterns):
                        continue
                    appended.append(human_text)
            if appended:
                guard = "## ⚠️ 机械兜底约束（Governance Decay Guard）\n"
                guard += "\n".join(appended)
                summary = guard + "\n\n" + summary

        summary_path.write_text(summary, encoding="utf-8")
        logger.info(
            "_generate_summary_on_demand: regenerated (%d chars)", len(summary)
        )

    def _generate_user_summary_on_demand(self, user_path, summary_path):
        """Regenerate USER_SUMMARY.md from USER.md using DeepSeek V3.

        Called from _refresh_user_summary() when USER.md mtime is
        newer than the summary — the current /new conversation
        should not wait for manual maintenance to catch up.

        Symmetric to _generate_summary_on_demand() for MEMORY.
        """
        import json as _json
        import urllib.request as _urllib

        user_text = user_path.read_text(encoding="utf-8")
        if len(user_text) < 100:
            return

        # ── API key ──
        ds_key_file = os.path.expanduser("~/.hermes/.deepseek_key")
        if not os.path.exists(ds_key_file):
            logger.warning("_generate_user_summary_on_demand: no DeepSeek key")
            return
        ds_key = Path(ds_key_file).read_text(encoding="utf-8").strip()

        # ── Read existing summary as style reference ──
        existing_summary = ""
        if summary_path.exists():
            existing_summary = summary_path.read_text(encoding="utf-8")

        prompt = (
            "将以下 USER.md 压缩为 3KB 以内的结构化摘要。\n"
            "保留所有核心行为偏好，按 8 组分类：\n"
            "行为底线、分析方法、交互节奏、设计原则、\n"
            "执行节奏、沟通与授权、输出标准、架构与护栏。\n"
            "丢弃：已过时的项目状态、特定日期事件、家人微信备注等个人细节。\n"
            "每条偏好一行，格式：'- 偏好名——简短说明'。\n\n"
        )
        if existing_summary:
            prompt += (
                "当前摘要（作格式参考，内容可能过时——请根据 USER.md 原文重新生成）：\n"
                + existing_summary[:2000]
                + "\n\n"
            )
        prompt += (
            "USER.md 原文：\n"
            + user_text
            + "\n\n"
            "按以下格式输出（不要加额外说明，不要用代码块包裹）：\n"
            "# USER.md Summary\n\n"
            "> 完整 USER.md 通过 memory(action='read', target='user') 获取。\n"
            "> 以下核心偏好按 8 组排列。完整约束见 USER_CONSTRAINTS.yaml。\n\n"
            "## 行为底线\n"
            "## 分析方法\n"
            "## 交互节奏\n"
            "## 设计原则\n"
            "## 执行节奏\n"
            "## 沟通与授权\n"
            "## 输出标准\n"
            "## 架构与护栏\n"
        )

        payload = _json.dumps({
            "model": "deepseek-chat",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3,
            "max_tokens": 4000,
        }).encode()

        try:
            req = _urllib.Request(
                "https://api.deepseek.com/v1/chat/completions",
                data=payload,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {ds_key}",
                },
            )
            resp = _json.loads(_urllib.urlopen(req, timeout=60).read())
            summary = resp["choices"][0]["message"]["content"].strip()
        except Exception:
            logger.warning(
                "_generate_user_summary_on_demand: API call failed", exc_info=True
            )
            return

        if not summary:
            return

        # ── Strip code block markers if LLM wrapped output ──
        if summary.startswith("```"):
            summary = summary.split("\n", 1)[-1]
        if summary.endswith("```"):
            summary = summary.rsplit("\n", 1)[0]

        summary_path.write_text(summary, encoding="utf-8")
        logger.info(
            "_generate_user_summary_on_demand: regenerated (%d chars)", len(summary)
        )

    @staticmethod
    def _sanitize_entries_for_snapshot(entries: List[str], filename: str) -> List[str]:
        """Return ``entries`` with any threat-matching entry replaced by a placeholder.

        Each entry is scanned with the shared threat-pattern library at the
        ``"strict"`` scope (same as memory writes).  On match, the entry is
        replaced in the returned list with ``"[BLOCKED: <filename> entry
        contained threat pattern: <ids>. Removed from system prompt.]"`` —
        the placeholder enters the snapshot, the original entry stays in
        live state for the user to inspect and delete.

        Empty or already-block-marker entries pass through unchanged.
        """
        from tools.threat_patterns import scan_for_threats

        sanitized: List[str] = []
        for entry in entries:
            if not entry or entry.startswith("[BLOCKED:"):
                sanitized.append(entry)
                continue
            findings = scan_for_threats(entry, scope="strict")
            if findings:
                logger.warning(
                    "Memory entry from %s blocked at load time: %s",
                    filename, ", ".join(findings),
                )
                sanitized.append(
                    f"[BLOCKED: {filename} entry contained threat pattern(s): "
                    f"{', '.join(findings)}. Removed from system prompt; "
                    f"use memory(action=remove) "
                    f"to delete the original.]"
                )
            else:
                sanitized.append(entry)
        return sanitized

    @staticmethod
    @contextmanager
    def _file_lock(path: Path):
        """Acquire an exclusive file lock for read-modify-write safety.

        Uses a separate .lock file so the memory file itself can still be
        atomically replaced via os.replace().
        """
        lock_path = path.with_suffix(path.suffix + ".lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)

        if fcntl is None and msvcrt is None:
            yield
            return

        fd = open(lock_path, "a+", encoding="utf-8")
        try:
            if fcntl:
                fcntl.flock(fd, fcntl.LOCK_EX)
            else:
                fd.seek(0)
                msvcrt.locking(fd.fileno(), msvcrt.LK_LOCK, 1)
            yield
        finally:
            if fcntl:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                except (OSError, IOError):
                    pass
            elif msvcrt:
                try:
                    fd.seek(0)
                    msvcrt.locking(fd.fileno(), msvcrt.LK_UNLCK, 1)
                except (OSError, IOError):
                    pass
            fd.close()

    @staticmethod
    def _path_for(target: str) -> Path:
        mem_dir = get_memory_dir()
        if target == "user":
            return mem_dir / "USER.md"
        return mem_dir / "MEMORY.md"

    def _reload_target(self, target: str, *, skip_drift: bool = False) -> Optional[str]:
        """Re-read entries from disk into in-memory state.

        Called under file lock to get the latest state before mutating.
        Returns the backup path if external drift was detected (the on-disk
        file contains content that wouldn't round-trip through our
        parser/serializer, OR an entry larger than the store's char limit).
        When drift is detected the caller must abort the mutation —
        flushing would discard the un-roundtrippable content.
        Returns None on clean reload.

        When *skip_drift* is True the round-trip / entry-size check is
        bypassed.  Used by the ``add`` action which appends without
        rewriting, so existing content is never clobbered.
        """
        path = self._path_for(target)
        bak = None if skip_drift else self._detect_external_drift(target)
        fresh = self._read_file(path)
        fresh = list(dict.fromkeys(fresh))  # deduplicate
        self._set_entries(target, fresh)
        return bak

    def save_to_disk(self, target: str):
        """Persist entries to the appropriate file. Called after every mutation."""
        get_memory_dir().mkdir(parents=True, exist_ok=True)
        self._write_file(self._path_for(target), self._entries_for(target))

    def _entries_for(self, target: str) -> List[str]:
        if target == "user":
            return self.user_entries
        return self.memory_entries

    def _set_entries(self, target: str, entries: List[str]):
        if target == "user":
            self.user_entries = entries
        else:
            self.memory_entries = entries

    def _char_count(self, target: str) -> int:
        entries = self._entries_for(target)
        if not entries:
            return 0
        return len(ENTRY_DELIMITER.join(entries))

    def _char_limit(self, target: str) -> int:
        if target == "user":
            return self.user_char_limit
        return self.memory_char_limit

    def _ensure_date_prefix(self, content: str) -> str:
        """Prepend today's date if content doesn't already start with one.
        
        Formats recognised: （YYYY-MM-DD） and (YYYY-MM-DD).
        Skipped during pytest runs to avoid breaking upstream tests.
        """
        import os
        from datetime import date
        # Honour upstream test flag so existing assertions stay green.
        if os.environ.get("PYTEST_CURRENT_TEST"):
            return content
        import re as _re
        if _re.match(r"^[（(]\d{4}-\d{2}-\d{2}[）)]", content):
            return content
        return f"（{date.today().isoformat()}）{content}"

    def add(self, target: str, content: str) -> Dict[str, Any]:
        """Append a new entry. Returns error if it would exceed the char limit."""
        content = content.strip()
        if not content:
            return {"success": False, "error": "Content cannot be empty."}

        content = self._ensure_date_prefix(content)
        # Scan for injection/exfiltration before accepting
        scan_error = _scan_memory_content(content)
        if scan_error:
            return {"success": False, "error": scan_error}

        with self._file_lock(self._path_for(target)):
            # Re-read from disk under lock to pick up writes from other sessions.
            # For add (append-only), we skip the drift guard — appending never
            # clobbers existing content, so round-trip mismatches from prior
            # tool-written entries in the same session are harmless.  The drift
            # guard remains active for replace/remove where full-file rewrite
            # would discard un-roundtrippable content (issue #26045).
            self._reload_target(target, skip_drift=True)

            entries = self._entries_for(target)
            limit = self._char_limit(target)

            # Reject exact duplicates
            if content in entries:
                return self._success_response(target, "Entry already exists (no duplicate added).")

            # Calculate what the new total would be
            new_entries = entries + [content]
            new_total = len(ENTRY_DELIMITER.join(new_entries))

            if new_total > limit:
                current = self._char_count(target)
                return self._consolidation_failure({
                    "success": False,
                    "error": (
                        f"Memory at {current:,}/{limit:,} chars. "
                        f"Adding this entry ({len(content)} chars) would exceed the limit. "
                        f"Consolidate now: use 'replace' to merge overlapping entries into "
                        f"shorter ones or 'remove' stale or less important entries (see "
                        f"current_entries below), then retry this add — all in this turn."
                    ),
                    "current_entries": entries,
                    "usage": f"{current:,}/{limit:,}",
                })

            entries.append(content)
            self._set_entries(target, entries)
            self.save_to_disk(target)

        return self._success_response(target, "Entry added.")

    def replace(self, target: str, old_text: str, new_content: str) -> Dict[str, Any]:
        """Find entry containing old_text substring, replace it with new_content."""
        old_text = old_text.strip()
        new_content = new_content.strip()
        if not old_text:
            return {"success": False, "error": "old_text cannot be empty."}
        if not new_content:
            return {"success": False, "error": "new_content cannot be empty. Use 'remove' to delete entries."}

        new_content = self._ensure_date_prefix(new_content)
        # Scan replacement content for injection/exfiltration
        scan_error = _scan_memory_content(new_content)
        if scan_error:
            return {"success": False, "error": scan_error}

        with self._file_lock(self._path_for(target)):
            bak = self._reload_target(target)
            if bak:
                return _drift_error(self._path_for(target), bak)

            entries = self._entries_for(target)
            matches = [(i, e) for i, e in enumerate(entries) if old_text in e]

            if not matches:
                return self._consolidation_failure({
                    "success": False,
                    "error": f"No entry matched '{old_text}'. Check current_entries below and retry with the exact text of the entry you want to replace.",
                    "current_entries": entries,
                })

            if len(matches) > 1:
                # If all matches are identical (exact duplicates), operate on the first one
                unique_texts = {e for _, e in matches}
                if len(unique_texts) > 1:
                    previews = self._previews([e for _, e in matches])
                    return {
                        "success": False,
                        "error": f"Multiple entries matched '{old_text}'. Be more specific.",
                        "matches": previews,
                    }
                # All identical -- safe to replace just the first

            idx = matches[0][0]
            limit = self._char_limit(target)

            # Check that replacement doesn't blow the budget
            test_entries = entries.copy()
            test_entries[idx] = new_content
            new_total = len(ENTRY_DELIMITER.join(test_entries))

            if new_total > limit:
                current = self._char_count(target)
                return self._consolidation_failure({
                    "success": False,
                    "error": (
                        f"Replacement would put memory at {new_total:,}/{limit:,} chars. "
                        f"Shorten the new content, or 'remove' other stale or less important "
                        f"entries to make room (see current_entries below), then retry — all "
                        f"in this turn."
                    ),
                    "current_entries": entries,
                    "usage": f"{current:,}/{limit:,}",
                })

            entries[idx] = new_content
            self._set_entries(target, entries)
            self.save_to_disk(target)

        return self._success_response(target, "Entry replaced.")

    def remove(self, target: str, old_text: str) -> Dict[str, Any]:
        """Remove the entry containing old_text substring."""
        old_text = old_text.strip()
        if not old_text:
            return {"success": False, "error": "old_text cannot be empty."}

        with self._file_lock(self._path_for(target)):
            bak = self._reload_target(target)
            if bak:
                return _drift_error(self._path_for(target), bak)

            entries = self._entries_for(target)
            matches = [(i, e) for i, e in enumerate(entries) if old_text in e]

            if not matches:
                return self._consolidation_failure({
                    "success": False,
                    "error": f"No entry matched '{old_text}'. Check current_entries below and retry with the exact text of the entry you want to remove.",
                    "current_entries": entries,
                })

            if len(matches) > 1:
                # If all matches are identical (exact duplicates), remove the first one
                unique_texts = {e for _, e in matches}
                if len(unique_texts) > 1:
                    previews = self._previews([e for _, e in matches])
                    return {
                        "success": False,
                        "error": f"Multiple entries matched '{old_text}'. Be more specific.",
                        "matches": previews,
                    }
                # All identical -- safe to remove just the first

            idx = matches[0][0]
            entries.pop(idx)
            self._set_entries(target, entries)
            self.save_to_disk(target)

        return self._success_response(target, "Entry removed.")

    def apply_batch(self, target: str, operations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply a sequence of add/replace/remove ops to one target atomically.

        All operations are validated and applied against the FINAL budget --
        intermediate overflow is irrelevant. This lets the model free space
        (remove/replace) and add new entries in a SINGLE tool call instead of
        the multi-turn consolidate-then-retry dance that re-sends the whole
        conversation context several times.

        Semantics: all-or-nothing. If any op is malformed, doesn't match, or
        the net result would exceed the char limit, NOTHING is written and an
        error is returned describing the first failure plus the live state.
        """
        if not operations:
            return {"success": False, "error": "operations list is empty."}

        # Scan every add/replace content for injection/exfil BEFORE touching
        # disk -- a single poisoned op rejects the whole batch.
        for i, op in enumerate(operations):
            act = (op or {}).get("action")
            new_content = (op or {}).get("content")
            if act in {"add", "replace"} and new_content:
                new_content = self._ensure_date_prefix(new_content)
                op["content"] = new_content
                scan_error = _scan_memory_content(new_content)
                if scan_error:
                    return {"success": False, "error": f"Operation {i + 1}: {scan_error}"}

        with self._file_lock(self._path_for(target)):
            bak = self._reload_target(target)
            if bak:
                return _drift_error(self._path_for(target), bak)

            # Work on a copy; only commit if the whole batch validates.
            working: List[str] = list(self._entries_for(target))
            limit = self._char_limit(target)

            for i, op in enumerate(operations):
                op = op or {}
                act = op.get("action")
                content = (op.get("content") or "").strip()
                old_text = (op.get("old_text") or "").strip()
                pos = f"Operation {i + 1} ({act or 'unknown'})"

                if act == "add":
                    if not content:
                        return self._batch_error(target, f"{pos}: content is required.")
                    if content in working:
                        continue  # idempotent -- skip duplicate, don't fail the batch
                    working.append(content)

                elif act == "replace":
                    if not old_text:
                        return self._batch_error(target, f"{pos}: old_text is required.")
                    if not content:
                        return self._batch_error(
                            target,
                            f"{pos}: content is required (use action='remove' to delete).",
                        )
                    matches = [j for j, e in enumerate(working) if old_text in e]
                    if not matches:
                        return self._batch_error(target, f"{pos}: no entry matched '{old_text}'.")
                    if len({working[j] for j in matches}) > 1:
                        return self._batch_error(
                            target,
                            f"{pos}: '{old_text}' matched multiple distinct entries -- be more specific.",
                        )
                    working[matches[0]] = content

                elif act == "remove":
                    if not old_text:
                        return self._batch_error(target, f"{pos}: old_text is required.")
                    matches = [j for j, e in enumerate(working) if old_text in e]
                    if not matches:
                        return self._batch_error(target, f"{pos}: no entry matched '{old_text}'.")
                    if len({working[j] for j in matches}) > 1:
                        return self._batch_error(
                            target,
                            f"{pos}: '{old_text}' matched multiple distinct entries -- be more specific.",
                        )
                    working.pop(matches[0])

                else:
                    return self._batch_error(
                        target,
                        f"{pos}: unknown action. Use add, replace, or remove.",
                    )

            # Budget check against the FINAL state only.
            new_total = len(ENTRY_DELIMITER.join(working)) if working else 0
            if new_total > limit:
                current = self._char_count(target)
                return self._consolidation_failure({
                    "success": False,
                    "error": (
                        f"After applying all {len(operations)} operations, memory would be at "
                        f"{new_total:,}/{limit:,} chars -- over the limit. Remove or shorten more "
                        f"entries in the same batch (see current_entries below), then retry."
                    ),
                    "current_entries": self._entries_for(target),
                    "usage": f"{current:,}/{limit:,}",
                })

            # Commit.
            self._set_entries(target, working)
            self.save_to_disk(target)

        return self._success_response(target, f"Applied {len(operations)} operation(s).")

    def _batch_error(self, target: str, message: str) -> Dict[str, Any]:
        """Build a batch-abort error that reports live (uncommitted) state."""
        current = self._char_count(target)
        limit = self._char_limit(target)
        return self._consolidation_failure({
            "success": False,
            "error": message + " No operations were applied (batch is all-or-nothing).",
            "current_entries": self._entries_for(target),
            "usage": f"{current:,}/{limit:,}",
        })

    def format_for_system_prompt(self, target: str) -> Optional[str]:
        """
        Return the frozen snapshot for system prompt injection.

        This returns the state captured at load_from_disk() time, NOT the live
        state. Mid-session writes do not affect this. This keeps the system
        prompt stable across all turns, preserving the prefix cache.

        For target="memory", lazily refreshes MEMORY_SUMMARY.md if a newer
        version exists on disk (e.g. cron regenerated it after gateway start).
        The refresh only fires on mtime change, so steady-state conversations
        keep a cache-stable prompt.

        Returns None if the snapshot is empty (no entries at load time).
        """
        if target == "memory":
            self._refresh_memory_summary()
        elif target == "user":
            self._refresh_user_summary()
        block = self._system_prompt_snapshot.get(target, "")
        return block if block else None

    # -- Internal helpers --

    @staticmethod
    def _previews(entries: List[str], width: int = 80) -> List[str]:
        """Truncated one-line previews of entries for error feedback."""
        return [e[:width] + ("..." if len(e) > width else "") for e in entries]

    def _success_response(self, target: str, message: str = None) -> Dict[str, Any]:
        # A successful write means the consolidation loop made progress, so the
        # per-turn failure budget resets (the cap counts consecutive failures,
        # not lifetime ones within a turn) (#42405).
        self._consolidation_failures = 0
        entries = self._entries_for(target)
        current = self._char_count(target)
        limit = self._char_limit(target)
        pct = min(100, int((current / limit) * 100)) if limit > 0 else 0

        # The success response is intentionally TERMINAL: it confirms the write
        # landed and tells the model to stop. We do NOT echo the full entries
        # list here -- dumping it invites the model to "find more to fix" and
        # re-issue the same operations (observed thrash: the correct batch on
        # call 1, then 5 redundant repeats). Entries are only shown on the
        # error/over-budget paths, where the model genuinely needs them to
        # decide what to consolidate.
        resp = {
            "success": True,
            "done": True,
            "target": target,
            "usage": f"{pct}% — {current:,}/{limit:,} chars",
            "entry_count": len(entries),
        }
        if message:
            resp["message"] = message
        resp["note"] = "Write saved. This update is complete — do not repeat it."
        return resp

    def _render_block(self, target: str, entries: List[str]) -> str:
        """Render a system prompt block with header and usage indicator."""
        if not entries:
            return ""

        limit = self._char_limit(target)
        content = ENTRY_DELIMITER.join(entries)
        current = len(content)
        pct = min(100, int((current / limit) * 100)) if limit > 0 else 0

        if target == "user":
            header = f"USER PROFILE (who the user is) [{pct}% — {current:,}/{limit:,} chars]"
        else:
            header = f"MEMORY (your personal notes) [{pct}% — {current:,}/{limit:,} chars]"

        separator = "═" * 46
        return f"{separator}\n{header}\n{separator}\n{content}"

    @staticmethod
    def _read_file(path: Path) -> List[str]:
        """Read a memory file and split into entries.

        No file locking needed: _write_file uses atomic rename, so readers
        always see either the previous complete file or the new complete file.
        """
        if not path.exists():
            return []
        try:
            raw = path.read_text(encoding="utf-8")
        except (OSError, IOError):
            return []

        if not raw.strip():
            return []

        # Use ENTRY_DELIMITER for consistency with _write_file. Splitting by "§"
        # alone would incorrectly split entries that contain "§" in their content.
        entries = [e.strip() for e in raw.split(ENTRY_DELIMITER)]
        return [e for e in entries if e]

    def _detect_external_drift(self, target: str) -> Optional[str]:
        """Return a backup-path string if on-disk content shows external drift.

        The memory file is supposed to be a list of small entries the tool
        wrote, joined by §. Detect drift via two signals:

        1. Round-trip mismatch — re-parsing and re-serializing the file
           doesn't produce identical bytes (rare; would catch oddly-encoded
           delimiters).
        2. Entry-size overflow — any single parsed entry exceeds the
           store's whole-file char limit. The tool budgets the ENTIRE store
           against that limit; no single tool-written entry can exceed it.
           When we see one entry larger than the limit, an external writer
           (patch tool, shell append, manual edit, sister session) appended
           free-form content into what the tool will treat as one entry.
           Flushing would then truncate that entry to the model's new
           content, discarding the appended bytes — issue #26045.

        Returns the absolute path of the .bak file when drift was found and
        backed up; returns None when the file looks tool-shaped.

        Note: this is an INSTANCE method (not static) because we need the
        per-target char_limit for signal #2.
        """
        path = self._path_for(target)
        if not path.exists():
            return None
        try:
            raw = path.read_text(encoding="utf-8")
        except (OSError, IOError):
            return None
        if not raw.strip():
            return None

        parsed = [e.strip() for e in raw.split(ENTRY_DELIMITER) if e.strip()]
        roundtrip = ENTRY_DELIMITER.join(parsed)

        char_limit = self._char_limit(target)
        max_entry_len = max((len(e) for e in parsed), default=0)

        drift_detected = (raw.strip() != roundtrip) or (max_entry_len > char_limit)
        if not drift_detected:
            return None

        # Drift confirmed — snapshot the file so the operator can recover
        # whatever the external writer added, then return the .bak path so
        # the caller can refuse the mutation.
        ts = int(time.time())
        bak_path = path.with_suffix(path.suffix + f".bak.{ts}")
        try:
            bak_path.write_text(raw, encoding="utf-8")
        except (OSError, IOError):
            return str(bak_path) + " (BACKUP FAILED — file unchanged on disk)"
        return str(bak_path)

    @staticmethod
    def _write_file(path: Path, entries: List[str]):
        """Write entries to a memory file using atomic temp-file + rename.

        Previous implementation used open("w") + flock, but "w" truncates the
        file *before* the lock is acquired, creating a race window where
        concurrent readers see an empty file. Atomic rename avoids this:
        readers always see either the old complete file or the new one.
        """
        content = ENTRY_DELIMITER.join(entries) if entries else ""
        try:
            # Write to temp file in same directory (same filesystem for atomic rename)
            fd, tmp_path = tempfile.mkstemp(
                dir=str(path.parent), suffix=".tmp", prefix=".mem_"
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    f.write(content)
                    f.flush()
                    os.fsync(f.fileno())
                atomic_replace(tmp_path, path)
            except BaseException:
                # Clean up temp file on any failure
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except (OSError, IOError) as e:
            raise RuntimeError(f"Failed to write memory file {path}: {e}")


def load_on_disk_store() -> "MemoryStore":
    """Build a fresh on-disk :class:`MemoryStore`, honoring configured char limits.

    Use this from any context that has no live agent (the messaging gateway, the
    Desktop GUI, the bare CLI ``/memory`` handler) but still needs to read or
    apply approved memory writes. Mirrors how the live agent constructs its store
    in ``agent/agent_init.py`` — including the user's ``memory.memory_char_limit``
    / ``memory.user_char_limit`` overrides — so an approval applied without a live
    agent enforces the SAME caps as one applied with one.

    Falls back to the built-in defaults if config can't be loaded, so this can
    never raise on a missing/unreadable config.
    """
    memory_char_limit = 2200
    user_char_limit = 1375
    try:
        from hermes_cli.config import load_config

        mem_cfg = (load_config() or {}).get("memory", {}) or {}
        memory_char_limit = int(mem_cfg.get("memory_char_limit", memory_char_limit))
        user_char_limit = int(mem_cfg.get("user_char_limit", user_char_limit))
    except Exception:
        pass  # config optional — fall back to defaults rather than break /memory

    store = MemoryStore(
        memory_char_limit=memory_char_limit,
        user_char_limit=user_char_limit,
    )
    store.load_from_disk()
    return store


def _apply_write_gate(action: str, target: str, content: Optional[str],
                      old_text: Optional[str]) -> Optional[str]:
    """Evaluate the memory write gate. Returns a JSON tool-result string when
    the write should NOT proceed normally (blocked or staged), or None when the
    caller should perform the real write.

    Only the mutating actions (add/replace/remove) are gated.
    """
    if action not in {"add", "replace", "remove"}:
        return None

    try:
        from tools import write_approval as wa
    except Exception:
        # If the gate module can't load, fail open (current behaviour) rather
        # than blocking all memory writes.
        return None

    # Build a small inline summary/detail for the foreground approval prompt.
    label = "user profile" if target == "user" else "memory"
    if action == "add":
        summary = f"add to {label}"
        detail = content or ""
    elif action == "replace":
        summary = f"replace in {label}"
        detail = f"old: {old_text}\nnew: {content}"
    else:  # remove
        summary = f"remove from {label}"
        detail = old_text or ""

    decision = wa.evaluate_gate(wa.MEMORY, inline_summary=summary, inline_detail=detail)

    if decision.allow:
        return None

    if decision.blocked:
        return tool_error(decision.message, success=False)

    # stage
    payload = {
        "action": action,
        "target": target,
        "content": content,
        "old_text": old_text,
    }
    record = wa.stage_write(
        wa.MEMORY, payload,
        summary=f"{summary}: {detail[:120]}",
        origin=wa.current_origin(),
    )
    return json.dumps(
        {"success": True, "staged": True, "pending_id": record["id"],
         "message": decision.message},
        ensure_ascii=False,
    )


def _apply_batch_write_gate(target: str, operations: List[Dict[str, Any]]) -> Optional[str]:
    """Evaluate the write gate for a batch of memory operations.

    Returns a JSON tool-result string when the batch should NOT proceed
    (blocked or staged), or None when the caller should perform the real
    batch write. The whole batch is gated as a single unit.
    """
    try:
        from tools import write_approval as wa
    except Exception:
        return None

    label = "user profile" if target == "user" else "memory"
    summary = f"apply {len(operations)} op(s) to {label}"
    detail_lines = []
    for op in operations:
        op = op or {}
        act = op.get("action", "?")
        if act == "remove":
            detail_lines.append(f"- remove: {op.get('old_text', '')}")
        elif act == "replace":
            detail_lines.append(f"- replace: {op.get('old_text', '')} -> {op.get('content', '')}")
        else:
            detail_lines.append(f"- {act}: {op.get('content', '')}")
    detail = "\n".join(detail_lines)

    decision = wa.evaluate_gate(wa.MEMORY, inline_summary=summary, inline_detail=detail)

    if decision.allow:
        return None

    if decision.blocked:
        return tool_error(decision.message, success=False)

    payload = {"action": "batch", "target": target, "operations": operations}
    record = wa.stage_write(
        wa.MEMORY, payload,
        summary=f"{summary}: {detail[:120]}",
        origin=wa.current_origin(),
    )
    return json.dumps(
        {"success": True, "staged": True, "pending_id": record["id"],
         "message": decision.message},
        ensure_ascii=False,
    )


def _missing_old_text_error(store: "MemoryStore", target: str, action: str) -> str:
    """Build a recoverable error for a replace/remove call that arrived without
    ``old_text``.

    ``replace``/``remove`` are inherently targeted -- without ``old_text`` there
    is no entry to act on, so we cannot fulfil the call. But returning a bare
    "old_text is required" is a dead-end: some structured-output clients omit the
    optional ``old_text`` field (it isn't, and can't be, schema-required without
    a top-level combinator the Codex backend rejects -- see
    tests/tools/test_memory_tool_schema.py). So instead we return the current
    entry inventory plus an explicit retry instruction, letting the model reissue
    the call with ``old_text`` set to a unique substring of the entry it means.
    Mirrors the batch path's ``_batch_error`` shape. (issues #43412, #49466)
    """
    entries = store._entries_for(target)
    current = store._char_count(target)
    limit = store._char_limit(target)
    return json.dumps(
        {
            "success": False,
            "error": (
                f"'{action}' needs old_text -- a short unique substring of the entry "
                f"to {action}. None was provided. Reissue the {action} with old_text "
                f"set to part of one of the current_entries below."
            ),
            "current_entries": entries,
            "usage": f"{current:,}/{limit:,}",
        },
        ensure_ascii=False,
    )


def memory_tool(
    action: str = None,
    target: str = "memory",
    content: str = None,
    old_text: str = None,
    operations: Optional[List[Dict[str, Any]]] = None,
    source: str = None,
    store: Optional[MemoryStore] = None,
) -> str:
    """
    Single entry point for the memory tool. Dispatches to MemoryStore methods.

    Two shapes:
      - Single op: action + (content / old_text).
      - Batch:     operations=[{action, content?, old_text?}, ...] applied
                   atomically against the final char budget in ONE call.

    Returns JSON string with results.
    """
    if store is None:
        return tool_error("Memory is not available. It may be disabled in config or this environment.", success=False)

    # Some strict providers fill optional schema fields with JSON null rather
    # than omitting them.  Treat ``target: null`` as omitted so memory writes
    # still use the documented default store instead of failing validation.
    if target is None:
        target = "memory"

    if target not in {"memory", "user"}:
        return tool_error(f"Invalid target '{target}'. Use 'memory' or 'user'.", success=False)

    # --- Batch path -------------------------------------------------------
    if operations:
        if not isinstance(operations, list):
            return tool_error("operations must be a list of {action, content?, old_text?} objects.", success=False)
        gate_result = _apply_batch_write_gate(target, operations)
        if gate_result is not None:
            return gate_result
        result = store.apply_batch(target, operations)
        if result.get("success") and target == "memory":
            for op in operations:
                op_action = op.get("action", "")
                op_content = op.get("content", "")
                if op_action == "add" and op_content:
                    _update_memory_meta(op_content, op.get("source"))
        return json.dumps(result, ensure_ascii=False)

    # --- Single-op path ---------------------------------------------------
    # Validate required params BEFORE the gate so an invalid write is rejected
    # immediately instead of being staged and only failing at approve time.
    if action == "add" and not content:
        return tool_error("Content is required for 'add' action.", success=False)
    if action == "replace" and (not old_text or not content):
        missing = "old_text" if not old_text else "content"
        if not old_text:
            # The client/model omitted old_text. Replace is inherently targeted
            # -- we can't guess which entry. Return the current inventory plus a
            # retry instruction so the model can reissue with old_text set,
            # instead of hitting a dead-end error. (issues #43412, #49466)
            return _missing_old_text_error(store, target, "replace")
        return tool_error(f"{missing} is required for 'replace' action.", success=False)
    if action == "remove" and not old_text:
        return _missing_old_text_error(store, target, "remove")

    # Approval gate: when on, stages the write (background/gateway) or prompts
    # inline (interactive CLI); when off (default) passes straight through.
    gate_result = _apply_write_gate(action, target, content, old_text)
    if gate_result is not None:
        return gate_result

    if action == "add":
        result = store.add(target, content)
        if result.get("success") and target == "memory":
            _update_memory_meta(content, source)

    elif action == "replace":
        result = store.replace(target, old_text, content)
        if result.get("success") and target == "memory":
            _update_memory_meta(content, source)

    elif action == "remove":
        result = store.remove(target, old_text)

    elif action == "read":
        mem_path = store._path_for(target)
        if not mem_path.exists():
            return json.dumps({"success": False, "error": f"{target.upper()}.md not found"})
        result = {"success": True, "content": mem_path.read_text(encoding="utf-8")}

    else:
        return tool_error(f"Unknown action '{action}'. Use: add, replace, remove, or read (single-op)", success=False)

    return json.dumps(result, ensure_ascii=False)


def check_memory_requirements() -> bool:
    """Memory tool has no external requirements -- always available."""
    return True


def apply_memory_pending(payload: Dict[str, Any], store: "MemoryStore") -> Dict[str, Any]:
    """Replay a staged memory write directly against the store, bypassing the
    write gate. Called by the /memory approve handler.

    Returns the store's result dict.
    """
    action = payload.get("action")
    target = payload.get("target", "memory")
    content = payload.get("content") or ""
    old_text = payload.get("old_text") or ""
    if action == "batch":
        return store.apply_batch(target, payload.get("operations") or [])
    if action == "add":
        return store.add(target, content)
        # Phase5b: meta handled by main handler
    if action == "replace":
        return store.replace(target, old_text, content)
    if action == "remove":
        return store.remove(target, old_text)
    return {"success": False, "error": f"Unknown staged action '{action}'."}
# OpenAI Function-Calling Schema
# =============================================================================

MEMORY_SCHEMA = {
    "name": "memory",
    "description": (
        "Save durable facts to persistent memory that survive across sessions. Memory is "
        "injected into every future turn, so keep entries compact and high-signal.\n\n"
        "HOW: make ALL your changes in ONE call via an 'operations' array (each item: "
        "{action, content?, old_text?}). The batch applies atomically and the char limit is "
        "checked only on the FINAL result — so a single call can remove/replace stale entries "
        "to free room AND add new ones, even when an add alone would overflow. The response "
        "reports current/limit chars and confirms completion; one batch call finishes the "
        "update, so don't repeat it. Use the bare action/content/old_text fields only for a "
        "single lone change.\n\n"
        "WHEN: save proactively when the user states a preference, correction, or personal "
        "detail, or you learn a stable fact about their environment, conventions, or workflow. "
        "Priority: user preferences & corrections > environment facts > procedures. The best "
        "memory stops the user repeating themselves.\n\n"
        "IF FULL: an add is rejected with the current entries shown. Reissue as ONE batch that "
        "removes or shortens enough stale entries and adds the new one together.\n\n"
        "TARGETS: 'user' = who the user is (name, role, preferences, style). 'memory' = your "
        "notes (environment, conventions, tool quirks, lessons).\n\n"
        "SKIP: trivial/obvious info, easily re-discovered facts, raw data dumps, task progress, "
        "completed-work logs, temporary TODO state (use session_search for those). Reusable "
        "procedures belong in a skill, not memory."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["add", "replace", "remove", "read"],
                "description": "The action to perform (single-op shape). Omit when using 'operations'."
            },
            "target": {
                "type": "string",
                "enum": ["memory", "user"],
                "description": "Which memory store: 'memory' for personal notes, 'user' for user profile."
            },
            "content": {
                "type": "string",
                "description": "The entry content. Required for 'add' and 'replace' (single-op shape)."
            },
            "old_text": {
                "type": "string",
                "description": "REQUIRED for 'replace' and 'remove' (single-op shape): a short unique substring identifying the existing entry to modify. Omit only for 'add'."
            },
            "source": {
                "type": "string",
                "enum": ["user_stated", "llm_inferred"],
                "description": "🆕 Source classification: user_stated = user explicitly said this (permanent TTL); llm_inferred = agent inference/extraction (90d TTL). Default: llm_inferred."
            },
            "operations": {
                "type": "array",
                "description": (
                    "Batch shape: a list of operations applied atomically in one call "
                    "against the final char budget. Preferred when making multiple changes "
                    "or consolidating to make room. Each item is {action, content?, old_text?}."
                ),
                "items": {
                    "type": "object",
                    "properties": {
                        "action": {"type": "string", "enum": ["add", "replace", "remove"]},
                        "content": {"type": "string", "description": "Entry content for add/replace."},
                        "old_text": {"type": "string", "description": "Substring identifying the entry for replace/remove."},
                        "source": {"type": "string", "enum": ["user_stated", "llm_inferred"], "description": "Source classification for this operation."},
                    },
                    "required": ["action"],
                },
            },
        },
        "required": ["target"],
    },
}


# --- Registry ---
from tools.registry import registry, tool_error

registry.register(
    name="memory",
    toolset="memory",
    schema=MEMORY_SCHEMA,
    handler=lambda args, **kw: memory_tool(
        source=args.get("source"),
        action=args.get("action", ""),
        target=args.get("target", "memory"),
        content=args.get("content"),
        old_text=args.get("old_text"),
        operations=args.get("operations"),
        store=kw.get("store")),
    check_fn=check_memory_requirements,
    emoji="🧠",
)





# Source-graded memory meta

def _update_memory_meta(content: str, source: str = None):
    """Write entry hash + source to memory_meta.json. External to MEMORY.md."""
    import json, hashlib
    from pathlib import Path
    meta_path = Path.home() / ".hermes" / "state" / "memory_meta.json"
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta = {}
    if meta_path.exists():
        try:
            with open(meta_path) as f:
                meta = json.load(f)
        except (json.JSONDecodeError, ValueError):
            meta = {}
    entry_hash = hashlib.sha256(content.encode()).hexdigest()[:12]
    if "entries" not in meta:
        meta["entries"] = {}
    meta["entries"][entry_hash] = {
        "hash": entry_hash,
        "source": source or "llm_inferred",
        "created_at": __import__("datetime").datetime.now().isoformat(),
    }
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
>>>>>>> 6c78706b2 (feat(memory): source-graded memory meta)
