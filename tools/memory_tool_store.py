"""MemoryStore — bounded, file-backed curated memory (MEMORY.md / USER.md).
Entries are joined by ``ENTRY_DELIMITER``; budgets are in chars (model-independent).
Module state that tests monkeypatch (``get_memory_dir``, ``fcntl``/``msvcrt``) stays
in ``tools.memory_tool`` and is read lazily."""

import logging
import os
import re
import time
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from utils import atomic_write_text
from tools.threat_patterns import first_threat_message as _first_threat_message
# 渐进式披露：本地定制移植所需依赖
import os
import yaml
from hermes_constants import get_hermes_home

logger = logging.getLogger("tools.memory_tool")

# Block header prefixes rendered by _render_block; agent/conversation_compression.py
# matches them to detect a leftover block for an emptied target — keep in lockstep.
MEMORY_BLOCK_HEADERS = {
    "memory": "MEMORY (your personal notes)", "user": "USER PROFILE (who the user is)"}

ENTRY_DELIMITER = "\n§\n"


def _scan_memory_content(content: str) -> Optional[str]:
    """Error string if *content* matches injection/exfil patterns. Strict scope:
    memory enters the system prompt, so a poisoned entry persists across sessions."""
    return _first_threat_message(content, scope="strict")


# Why the last refusal/failure in this context happened, for the shared metric only (a closed name from
# shared_metrics_contract.MEMORY_OP_FAILURE_CLASSES); never part of the result the model sees.
FAILURE_CLASS: ContextVar[str] = ContextVar("memory_failure_class", default="other")


def _error(message: str, failure_class: str, **extra) -> dict[str, Any]:
    FAILURE_CLASS.set(failure_class)
    return {"success": False, "error": message, **extra}


def _store_target(filename: str) -> str:
    """The memory tool target that owns a store file, for hints that show the call to make."""
    return "user" if filename == "USER.md" else "memory"


def _drift_error(path: Path, bak_path: str) -> dict[str, Any]:
    """External drift: the file wouldn't round-trip, so flushing would discard content."""
    return _error((
        f"Refusing to write {path.name}: it holds an entry larger than the whole store's char limit, "
        f"i.e. free-form text written without § delimiters (the patch tool, a shell append, a manual "
        f"edit). Rewriting it would lose that text. A snapshot was saved to {bak_path}. Resolve the drift first — "
        f"either rewrite the file as a clean §-delimited list of entries, or move the extra "
        f"content out — then retry. This guard exists to prevent silent data loss (issue #26045)."
    ), "drift", drift_backup=bak_path, remediation=(
        "Open the .bak file, integrate the missing entries into the memory tool one at a time via "
        f"memory(action=add, target='{_store_target(path.name)}', content=...), then remove or rewrite the "
        "original file to a clean state."))


def _read_failed_error(path: Path) -> dict[str, Any]:
    """Existing-but-unreadable file: saving from an assumed-empty view would wipe it."""
    return _error(
        f"Refusing to write {path.name}: the file exists on disk but could not be read right now "
        f"(temporarily locked by another program, a permission change, invalid/corrupt text encoding, "
        f"or a filesystem error). Treating an unreadable file as empty and saving would wipe existing "
        f"memory, so the write is refused. Nothing was changed — retry in a moment.", "read_failed")


# Typography a model re-types differently from the stored entry: every quote/backtick -> "'",
# every dash/minus -> "-". Plus whitespace runs (re-wrapped lines, NBSP) -> one space.
_MATCH_FOLD = str.maketrans({**dict.fromkeys("\"`\u2018\u2019\u201a\u201b\u201c\u201d\u201e\u201f", "'"),
                             **dict.fromkeys("\u2010\u2011\u2012\u2013\u2014\u2015\u2212", "-")})


def _normalize_for_match(text: str) -> str:
    """Quote/dash/whitespace-folded form for tolerant old_text matching. Models re-emit an
    entry with straight quotes for curly ones, '-' for an em dash, unwrapped lines, or a
    newline over-escaped to a literal backslash-n, and a byte-exact substring test then
    reports "No entry matched" for an unambiguous target."""
    return " ".join(str(text).replace("\\n", " ").translate(_MATCH_FOLD).split())


def _substring_matches(entries: list[str], old_text: str) -> list[int]:
    """Indices of entries containing *old_text*: byte-exact first, else typography-folded."""
    if exact := [i for i, e in enumerate(entries) if old_text in e]:
        return exact
    needle = _normalize_for_match(old_text)
    if not any(c.isalnum() for c in needle):  # punctuation/space alone would select a whole entry
        return []
    return [i for i, e in enumerate(entries) if needle in _normalize_for_match(e)]


def _find_unique_match(entries: list[str], old_text: str) -> tuple[Optional[int], bool]:
    """``(index, ambiguous)`` for entries matching *old_text*. A whole-entry
    EXACT match (``old_text == entry``) takes absolute priority — substring
    matches are only considered when no entry equals *old_text*, so a short
    entry stays addressable even when its full text is contained inside a
    longer sibling entry (remove('test') vs '...tests pass...'). Exact-duplicate
    matches are safe (first wins); distinct matches → ``(None, True)``."""
    exact = [i for i, e in enumerate(entries) if e == old_text]
    matches = exact or _substring_matches(entries, old_text)
    if len({entries[i] for i in matches}) > 1:
        return None, True
    return (matches[0] if matches else None), False


def _no_match_error(entries: list[str], old_text: str, verb: str, **extra) -> dict[str, Any]:
    """Zero-match error that names the fix: the entries sharing the most words with
    *old_text* (a paraphrased or stale old_text usually still shares most of them), and a
    pointer when old_text spans several entries joined by the delimiter."""
    def _words(text: str) -> set:
        return set(re.findall(r"\w+", text.lower()))
    words = _words(old_text)
    scored = sorted(((len(words & _words(e)) / len(words), i) for i, e in enumerate(entries)) if words else [],
                    key=lambda p: (-p[0], p[1]))
    closest = [entries[i] for score, i in scored[:3] if score >= 0.3]
    hint = (" old_text must come from ONE entry ('§' separates entries); use one op per entry."
            if "§" in old_text else "")
    return _error(f"No entry matched '{old_text}'.{hint} Retry the {verb} with old_text copied verbatim "
                  f"from the intended entry" + (" (closest_entries below)." if closest else "."), "no_match",
                  **({"closest_entries": closest} if closest else {}), **extra)


def _over_limit(new_total: int, limit: int) -> str:
    return f"would put memory at {new_total:,}/{limit:,} chars: free at least {new_total - limit:,} chars"


# Patch content cannot be empty, and 'remove' would drop the sibling facts in the entry.
PATCH_DROP_HINT = "To drop text inside an entry, patch a longer span that contains it; 'remove' deletes the whole entry."


def _repeated_span_message(old_text: str, count: int) -> str:
    return (f"'{old_text}' appears {count} times in the matched entry, and patch edits one span. "
            f"Include more surrounding text so old_text occurs once.")


def _span_occurrences(text: str, span: str) -> int:
    """Occurrences of *span* in *text*, overlapping ones included: '00' occurs twice in '1000'."""
    return sum(text.startswith(span, i) for i in range(len(text) - len(span) + 1))


def _patch_entry(entry: str, old_text: str, new_text: str) -> tuple[Optional[str], str, str]:
    """``(patched entry, "", "none")``, or ``(None, error, failure class)`` when the span occurs more
    than once in the entry, the edit changes nothing, or the patched entry fails the threat scan.
    Single and batch patches both go through here, so they share one rule."""
    if (count := _span_occurrences(entry, old_text)) > 1:
        return None, _repeated_span_message(old_text, count), "ambiguous"
    patched = entry.replace(old_text, new_text, 1)
    if patched == entry:
        return None, f"Patch is a no-op: '{old_text}' -> '{new_text}' leaves the entry unchanged.", "invalid_args"
    if scan_error := _scan_memory_content(patched):
        return None, scan_error, "scan_blocked"
    return patched, "", "none"


def _pinned_index(entries: list[str], matched_entry: str) -> Optional[int]:
    """Index of the exact entry a staged write was reviewed against; None once it is gone (stale)."""
    return entries.index(matched_entry) if matched_entry in entries else None


def _stale_entry_message(entry: str) -> str:
    return (f"Entry changed since it was staged, so this write was not applied: '{entry}' is no longer "
            f"in memory as reviewed. Recreate the change against the current entry or reject it; the "
            f"pending record has been preserved.")


# Optional third value of an _apply closure: a dict merged into the success payload —
# e.g. the full text an entry-level replace overwrote, so a whole-entry write is never
# silent about the loss (#117952). Same convention as _error(**extra).


class MemoryStore:
    """Bounded curated memory with file persistence; one instance per AIAgent.
    ``_system_prompt_snapshot`` is frozen at load time (prefix-cache stable);
    ``memory_entries`` / ``user_entries`` are live state persisted to disk."""

    # Failed consolidation attempts (overflow / zero-match) allowed per turn before
    # a TERMINAL "save skipped" result, so a fragile replace/add can't loop the turn
    # to budget exhaustion and suppress the user's reply.
    # See #42405.
    _MAX_CONSOLIDATION_FAILURES_PER_TURN = 3

    def __init__(self, memory_char_limit: int = 2200, user_char_limit: int = 1375, *,
                 memory_enabled: bool = True, user_profile_enabled: bool = True):
        self.memory_entries: list[str] = []
        self.user_entries: list[str] = []
        self.memory_char_limit, self.user_char_limit = memory_char_limit, user_char_limit
        self.memory_enabled, self.user_profile_enabled = memory_enabled, user_profile_enabled
        self._system_prompt_snapshot: dict[str, str] = {"memory": "", "user": ""}
        self._consolidation_failures = 0  # per turn; reset by reset_consolidation_failures()
        # ── 渐进式披露 mtime 字段（移植自本地） ──
        self._summary_snapshot_mtime = 0.0
        self._user_summary_snapshot_mtime = 0.0
        self._memory_guard_mtime = 0.0
        # §5.4 成本监控：session 唯一标识
        import uuid as _uuid
        self._cost_sid = _uuid.uuid4().hex[:8]

    # Per-turn counter of failed at-capacity consolidation attempts; reset at each turn boundary by
    # reset_consolidation_failures() (#42405).
    def target_enabled(self, target: str) -> bool:
        return self.user_profile_enabled if target == "user" else self.memory_enabled

    def reset_consolidation_failures(self) -> None:
        """Call at turn start."""
        self._consolidation_failures = 0

    def _consolidation_failure(self, response: dict[str, Any]) -> dict[str, Any]:
        """Count a consolidation failure: under the per-turn cap return ``response``
        (it says how to retry); past it a TERMINAL result so the model stops looping.

        Once the cap is exceeded, drop the retry instruction and return a TERMINAL result so the model stops
        looping memory calls and proceeds to answer the user — a failed memory side effect must never block
        the turn's reply (#42405).
        """
        self._consolidation_failures += 1
        if self._consolidation_failures <= self._MAX_CONSOLIDATION_FAILURES_PER_TURN:
            return response
        FAILURE_CLASS.set("retry_cap")
        return {"success": False, "done": True, "error": (
            f"Memory consolidation failed {self._consolidation_failures} times this turn. Stop retrying "
            "memory calls — leave memory unchanged for now and continue with your reply to the user. "
            "The fact can be saved in a later turn.")}

    def load_from_disk(self):
        """Load MEMORY.md / USER.md and capture the frozen system-prompt snapshot.
        Threat hits are replaced by a ``[BLOCKED: …]`` placeholder in the SNAPSHOT only;
        live lists keep the raw text so the user can see and remove poisoned entries
        (dropping them silently would hide the attack)."""
        from tools.threat_patterns import scan_for_threats

        def _sanitize(entry, filename):
            # Strict scope, same as writes; empty / already-blocked entries pass through.
            findings = scan_for_threats(entry, scope="strict") if entry and not entry.startswith("[BLOCKED:") else None
            if not findings:
                return entry
            logger.warning("Memory entry from %s blocked at load time: %s", filename, ", ".join(findings))
            return (f"[BLOCKED: {filename} entry contained threat pattern(s): {', '.join(findings)}. "
                    f"Removed from system prompt; use memory(action=remove, target='{_store_target(filename)}') "
                    f"to delete the original.]")

        for target in ("memory", "user"):
            path = self._path_for(target)
            from hermes_constants import mkdir_under_hermes_home

            mkdir_under_hermes_home(path.parent)
            # Deduplicate (order-preserving, first occurrence wins).
            entries = list(dict.fromkeys(self._read_file(path)))
            self._set_entries(target, entries)
            # External writers (MCP bridges, hand edits) can exceed the cap; the limit only fires on
            # add/replace, so the oversized block would silently ride in the prompt while every later
            # add is refused with no visible cause (#10877). Warn; never truncate a user's memories.
            if (count := self._char_count(target)) > (limit := self._char_limit(target)):
                logger.warning("%s exceeds its char limit on load: %d/%d chars. Entries stay loaded; "
                               "further additions are blocked until it is back under the limit.",
                               path.name, count, limit)
            self._system_prompt_snapshot[target] = self._render_block(target, [_sanitize(e, path.name) for e in entries])
        # MEMORY_SUMMARY.md 优先注入：摘要存在且在 24h 内 → 用 ~5K 替代 ~100K 全量
        from tools import memory_tool
        mem_dir = memory_tool.get_memory_dir()
        summary_path = mem_dir / "MEMORY_SUMMARY.md"

        # ── 分层索引 v2 灰度开关 ──
        _flag_path = mem_dir.parent / "state" / "flags" / "lean_index_v2"
        _v2_path = mem_dir / "MEMORY_SUMMARY_V2.md"
        if _flag_path.exists() and _v2_path.exists():
            summary_path = _v2_path

        if summary_path.exists():
            import time
            age_seconds = time.time() - summary_path.stat().st_mtime
            # 2026-09-21 fix（同类同修）：原为 `if age_seconds < 86400`，超龄时**静默**
            # 注入全量 MEMORY.md（连告警都没有）。改为无条件注入摘要；超 24h 记告警
            # （每日 03:00 的 memory_summarize.py 本该让它保持新鲜）。
            if age_seconds >= 86400:
                logger.warning(
                    "MEMORY_SUMMARY.md is %.1fh old — injecting the summary; no full-memory "
                    "fallback (daily 03:00 cron should have refreshed it)", age_seconds / 3600,
                )
            summary_text = summary_path.read_text(encoding="utf-8")
            summary_text = self._apply_memory_constraint_guard(summary_text)
            header = f"MEMORY (daily summary) [{len(summary_text):,} chars, {age_seconds/3600:.1f}h ago]"
            _wm = self._memory_watermark()
            if _wm:
                header += " | " + _wm
            _src_hint = _memory_source_stats_hint()
            if _src_hint:
                header += " | " + _src_hint
            separator = "═" * 46
            self._system_prompt_snapshot["memory"] = f"{separator}\n{header}\n{separator}\n{summary_text}"
            self._summary_snapshot_mtime = summary_path.stat().st_mtime
            from tools.memory_tool import _cost_record
            _cost_record(self._cost_sid, "lean_chars", len(summary_text))

        # USER_SUMMARY.md 优先注入（与 MEMORY 摘要重定向对称设计，Phase 2 USER 渐进式披露）
        user_summary_path = mem_dir / "USER_SUMMARY.md"
        if user_summary_path.exists():
            import time
            age_seconds = time.time() - user_summary_path.stat().st_mtime
            # 2026-09-21 fix（B，同类同修）：原为 `if age_seconds < 86400` 才注入摘要，
            # 否则回退全量 USER.md。年龄不代表内容过期——USER.md 未变则摘要即最新版，
            # 且 _refresh_user_summary 已按 mtime 决定要不要重建。
            # 改为无条件注入；超 24h 仅记告警，供监控发现「该刷新时没刷新」。
            if age_seconds >= 86400:
                logger.warning(
                    "USER_SUMMARY.md is %.1fh old but USER.md is not newer — injecting the "
                    "summary; no full-profile fallback", age_seconds / 3600,
                )
            summary_text = user_summary_path.read_text(encoding="utf-8")
            summary_text = self._apply_user_constraint_guard(summary_text)
            header = f"USER PROFILE (summary) [{len(summary_text):,} chars, {age_seconds/3600:.1f}h ago]"
            separator = "═" * 46
            self._system_prompt_snapshot["user"] = f"{separator}\n{header}\n{separator}\n{summary_text}"
            self._user_summary_snapshot_mtime = user_summary_path.stat().st_mtime
            from tools.memory_tool import _cost_record
            _cost_record(self._cost_sid, "user_chars", len(summary_text))

    @staticmethod
    @contextmanager
    def _file_lock(path: Path):
        """Exclusive lock on a separate .lock file so the memory file itself can
        still be atomically replaced."""
        from tools import memory_tool as _mt  # fcntl/msvcrt live (and are patched) there
        fcntl, msvcrt = _mt.fcntl, _mt.msvcrt
        lock_path = path.with_suffix(path.suffix + ".lock")
        from hermes_constants import mkdir_under_hermes_home

        mkdir_under_hermes_home(lock_path.parent)
        if fcntl is None and msvcrt is None:
            yield
            return
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        raw_fd = os.open(lock_path, flags, 0o600)
        try:
            # The creation mode is filtered through the process umask and does
            # not repair a lock left loose by an older Hermes process. Tighten
            # the opened inode before acquiring the lock so both cases are
            # owner-only. Operating on the fd avoids a path-swap window.
            if hasattr(os, "fchmod"):
                os.fchmod(raw_fd, 0o600)
            fd = os.fdopen(raw_fd, "r+", encoding="utf-8")
        except Exception:
            os.close(raw_fd)
            raise
        with fd:
            def _flock(unlock: bool):
                if fcntl:
                    fcntl.flock(fd, fcntl.LOCK_UN if unlock else fcntl.LOCK_EX)
                else:
                    fd.seek(0)
                    msvcrt.locking(fd.fileno(), msvcrt.LK_UNLCK if unlock else msvcrt.LK_LOCK, 1)
            _flock(False)
            try:
                yield
            finally:
                with suppress(OSError):
                    _flock(True)

    @staticmethod
    def _path_for(target: str) -> Path:
        from tools import memory_tool  # get_memory_dir is monkeypatched there
        return memory_tool.get_memory_dir() / ("USER.md" if target == "user" else "MEMORY.md")

    def _entries_for(self, target: str) -> list[str]:
        return self.user_entries if target == "user" else self.memory_entries

    def _set_entries(self, target: str, entries: list[str]):
        setattr(self, "user_entries" if target == "user" else "memory_entries", entries)

    def _char_count(self, target: str) -> int:
        return len(ENTRY_DELIMITER.join(self._entries_for(target)))

    def _char_limit(self, target: str) -> int:
        return self.user_char_limit if target == "user" else self.memory_char_limit

    def _usage(self, target: str) -> str:
        return f"{self._char_count(target):,}/{self._char_limit(target):,}"

    def _usage_pct(self, target: str, current: int) -> str:
        limit = self._char_limit(target)
        return f"{min(100, int((current / limit) * 100)) if limit > 0 else 0}% — {current:,}/{limit:,} chars"

    def _memory_watermark(self) -> str:
        """CI 2.1-A（2026-09-15）：把 MEMORY 的预算水位显式写进注入头。

        理由：限额与每周整理都已存在，但模型在会话里看不到水位——只能等一次写入被拒
        （事后）或等 Auto-Dream 周日跑（系统被动）。此处只做**只读展示**，不改任何行为。
        口径用 _char_count（= 限额真正执法的那个数），非文件字节数。
        """
        try:
            cur = self._char_count("memory")
            if cur <= 0:
                # CI 3.2 #10 复核修复：_char_count 读的是**内存态** entries，而本函数在
                # 「构建系统提示」时被调用——那一刻条目往往还没载入 ⇒ 计数恒为 0 ⇒
                # 水位永远不渲染（线上 454 条含 daily summary 的提示里「文件水位」0 命中）。
                # 回退到磁盘，但**口径与限额执法完全同一套**：parse + ENTRY_DELIMITER.join。
                try:
                    raw = self._path_for("memory").read_text(encoding="utf-8")
                except OSError:
                    return ""
                # 2026-09-21 fix: 冷态必须与 load_from_disk 的去重口径对齐（那边是
                # list(dict.fromkeys(_read_file(...)))）。文件里残留的连续 "§" 行会被
                # 解析成若干内容为 "§" 的条目，不去重则冷态多算、热态少算（实测 Δ=20），
                # 水位随「是否已载入」漂移 → CI 2.1 夹具 W5 变红（2026-09-21 注册表巡检）。
                cur = len(ENTRY_DELIMITER.join(dict.fromkeys(self._parse_entries(raw))))
            limit = self._char_limit("memory")
            if cur <= 0 or limit <= 0:
                return ""
            pct = min(100, int(cur / limit * 100))
            flag = " ⚠️ 接近上限" if pct >= 85 else ""
            return f"文件水位 {cur:,}/{limit:,} = {pct}%{flag}"
        except Exception:  # 展示层永不阻断注入
            return ""

    def _failure_with_entries(self, target: str, message: str) -> dict[str, Any]:
        """Over-budget consolidation failure carrying the live entries so the model can consolidate."""
        return self._consolidation_failure(
            _error(message, "over_budget", current_entries=self._entries_for(target), usage=self._usage(target)))

    def _batch_failure(self, target: str, message: str, failure_class: str,
                       closest_entries: Optional[list[str]] = None) -> dict[str, Any]:
        """Batch-abort failure WITHOUT ``current_entries``: the store did not change and the
        caller already holds the inventory, so echoing it made each consolidation retry
        grow the context it was invoked to shrink (#97316). A zero-match op carries only
        its few ``closest_entries``."""
        return self._consolidation_failure(
            _error(message + " No operations were applied (batch is all-or-nothing).", failure_class,
                   usage=self._usage(target),
                   **({"closest_entries": closest_entries} if closest_entries else {})))

    def _mutate(self, target: str, mutate, *, skip_drift: bool = False) -> dict[str, Any]:
        """Lock, re-read from disk, run ``mutate(entries, limit)`` -> ``(new_entries, message)``
        or an error dict, then persist and return the success response. The reload aborts
        on an existing-but-unreadable file (even append-only ``add`` rewrites the whole
        file) and, unless *skip_drift*, on external drift (flushing would discard
        un-roundtrippable content). Drift check and parse use the SAME raw snapshot —
        a failed second read used to count as "no drift". The closure may return a
        third value, a dict merged into the success payload (``_error``'s ``**extra``
        convention) — e.g. the full text a replace overwrote (#117952). ANY dict the closure
        returns is passed through verbatim and nothing is persisted: error dicts, or the
        success payload of a read-only closure (``resolve_entry``, ``resolve_batch_entries``)."""
        path = self._path_for(target)
        with self._file_lock(path):
            raw, read_ok = self._read_raw_checked(path)
            if not read_ok:
                return _read_failed_error(path)
            bak = None if skip_drift else self._detect_external_drift(target, raw)
            self._set_entries(target, list(dict.fromkeys(self._parse_entries(raw))))
            if bak:
                return _drift_error(path, bak)
            result = mutate(self._entries_for(target), self._char_limit(target))
            if isinstance(result, dict):
                return result
            self._set_entries(target, result[0])
            from hermes_constants import mkdir_under_hermes_home

            mkdir_under_hermes_home(path.parent)
            self._write_file(path, result[0])
            extra_fields = result[2] if len(result) > 2 else {}
            return self._success_response(target, result[1], **extra_fields)

    def add(self, target: str, content: str) -> dict[str, Any]:
        """Append a new entry. Returns error if it would exceed the char limit."""
        content = content.strip()
        if not content:
            return _error("Content cannot be empty.", "missing_content")
        if scan_error := _scan_memory_content(content):
            return _error(scan_error, "scan_blocked")

        def _add(entries, limit):
            if content in entries:
                return self._success_response(target, "Entry already exists (no duplicate added).")
            if len(ENTRY_DELIMITER.join(entries + [content])) > limit:
                new_total = len(ENTRY_DELIMITER.join(entries + [content]))
                return self._failure_with_entries(target, (
                    f"Memory at {self._char_count(target):,}/{limit:,} chars; adding this entry ({len(content)} chars) "
                    f"would exceed the limit by {new_total - limit:,} chars. Retry as ONE 'operations' batch that "
                    f"removes or shortens (patch) stale entries from current_entries below to free at least "
                    f"{new_total - limit:,} chars AND adds this entry — the limit is checked only on the batch result."))
            return entries + [content], "Entry added."
        # Append-only: skip the drift guard (appending never clobbers foreign
        # content) but still refuse a failed read — add rewrites the WHOLE file.
        return self._mutate(target, _add, skip_drift=True)

    def replace(self, target: str, old_text: str, new_content: str,
                matched_entry: Optional[str] = None) -> dict[str, Any]:
        """Find the entry containing old_text (whole-entry exact match first) and
        replace the WHOLE entry with new_content — old_text only locates the entry;
        the matched span is not spliced into it."""
        new_content = new_content.strip()
        if not old_text.strip():
            return _error("old_text cannot be empty.", "missing_old_text")
        if not new_content:
            return _error("new_content cannot be empty. Use 'remove' to delete entries.", "missing_content")
        if scan_error := _scan_memory_content(new_content):
            return _error(scan_error, "scan_blocked")
        return self._edit(target, old_text.strip(), new_content, matched_entry)

    def patch(self, target: str, old_text: str, new_text: str,
              matched_entry: Optional[str] = None) -> dict[str, Any]:
        """Find the entry containing *old_text* and replace that one span with *new_text*;
        the rest of the entry is preserved verbatim. This is the partial-edit shape
        ``replace`` deliberately is not (replace overwrites the WHOLE entry). A span that
        occurs more than once in the entry is rejected rather than edited everywhere."""
        if not (old_text or "").strip():
            return _error("old_text cannot be empty.", "missing_old_text")
        if new_text is None or not str(new_text).strip():
            return _error(f"new_text cannot be empty. {PATCH_DROP_HINT}", "missing_content")
        # The span keeps its whitespace: it is part of the text being replaced ("tea " -> "coffee ").
        return self._patch(target, old_text, str(new_text), matched_entry)

    def _patch(self, target: str, old_text: str, new_text: str,
               matched_entry: Optional[str] = None) -> dict[str, Any]:
        """Locked find-and-splice inside the matched entry (#124582)."""
        def _apply(entries, limit):
            idx = self._locate(entries, old_text, "patch", matched_entry)
            if isinstance(idx, dict):
                return idx
            before = entries[idx]
            after, message, failure_class = _patch_entry(before, old_text, new_text)
            if after is None:
                return _error(message, failure_class)
            patched = entries[:idx] + [after] + entries[idx + 1:]
            new_total = len(ENTRY_DELIMITER.join(patched))
            if new_total > limit:
                return self._failure_with_entries(target, (
                    f"Patch would put memory at {new_total:,}/{limit:,} chars. Shorten the new "
                    f"text, or 'remove' other stale or less important entries to make room "
                    f"(see current_entries below), then retry — all in this turn."))
            return patched, "Entry patched.", {
                "patched_entry_before": before, "patched_entry_after": after}
        return self._mutate(target, _apply)

    def remove(self, target: str, old_text: str, matched_entry: Optional[str] = None) -> dict[str, Any]:
        """Remove the entry containing old_text substring."""
        if not old_text.strip():
            return _error("old_text cannot be empty.", "missing_old_text")
        return self._edit(target, old_text.strip(), None, matched_entry)

    def _locate(self, entries: list[str], old_text: str, verb: str, matched_entry: Optional[str] = None):
        """Index of the entry *old_text* selects, or the error dict the edit returns. A write
        staged for approval carries the FULL entry it was reviewed against (*matched_entry*):
        only that exact entry qualifies, so replay never hits a newer entry that still
        contains old_text."""
        if matched_entry is not None:
            idx = _pinned_index(entries, matched_entry)
            return idx if idx is not None else _error(_stale_entry_message(matched_entry), "stale_entry")
        idx, ambiguous = _find_unique_match(entries, old_text)
        if ambiguous:
            return _error(f"Multiple entries matched '{old_text}'. Be more specific.", "ambiguous",
                          matches=[entries[i][:80] + ("..." if len(entries[i]) > 80 else "")
                                   for i in _substring_matches(entries, old_text)])
        if idx is None:
            return self._consolidation_failure(_no_match_error(entries, old_text, verb, current_entries=entries))
        return idx

    def resolve_entry(self, target: str, old_text: str, verb: str) -> dict[str, Any]:
        """``{"success": True, "matched_entry": <full entry>}`` for the entry *old_text* selects
        now, read under the lock, or the error the direct edit would return."""
        def _resolve(entries, limit):
            idx = self._locate(entries, old_text.strip(), verb)
            return idx if isinstance(idx, dict) else {"success": True, "matched_entry": entries[idx]}
        return self._mutate(target, _resolve, skip_drift=True)

    def _edit(self, target: str, old_text: str, new_content: Optional[str],
              matched_entry: Optional[str] = None) -> dict[str, Any]:
        """Locked replace (``new_content`` set) or remove (None) of the entry matching *old_text*."""
        def _apply(entries, limit):
            idx = self._locate(entries, old_text, "replace" if new_content else "remove", matched_entry)
            if isinstance(idx, dict):
                return idx
            replaced = entries[:idx] + ([] if new_content is None else [new_content]) + entries[idx + 1:]
            if new_content is None:
                return replaced, "Entry removed.", {"removed_entry": entries[idx]}
            new_total = len(ENTRY_DELIMITER.join(replaced))
            if new_total > limit:
                return self._failure_with_entries(target, (
                    f"Replacement {_over_limit(new_total, limit)}. Shorten the new content, or retry as ONE "
                    f"'operations' batch with this replace plus removes/shortenings of other stale entries "
                    f"(see current_entries below) — the limit is checked only on the batch result."))
            return replaced, "Entry replaced.", {"replaced_entry": entries[idx]}
        return self._mutate(target, _apply)

    @staticmethod
    def _apply_batch_op(working: list[str], act: str, content: str, old_text: str, pos: str,
                        matched_entry: Optional[str] = None) -> tuple[Optional[str], Any, str, Optional[str]]:
        """Apply one batch op to *working*; return ``(error message, previous content, failure class,
        patched entry)`` — on a zero-match error the second value is that op's ``closest_entries``
        instead, and the fourth is the entry a patch produced (None for every other op).
        Previous content is captured before each replace/remove, under the store lock.
        It is published only after the entire batch has been validated and persisted.
        A staged op's *matched_entry* selects exactly that entry, as in ``_locate``.
        """
        if act == "add":
            if not content:
                return f"{pos}: content is required.", None, "missing_content", None
            if content not in working:  # idempotent -- skip duplicate, don't fail the batch
                working.append(content)
            return None, None, "none", None
        if act not in ("replace", "remove", "patch"):
            return f"{pos}: unknown action. Use add, patch, or remove.", None, "invalid_args", None
        if not old_text.strip():
            return f"{pos}: old_text is required.", None, "missing_old_text", None
        if act == "replace" and not content:
            return (f"{pos}: content is required — set this operation object's own 'content' to the COMPLETE "
                    f"new entry (or use action='remove' to delete)."), None, "missing_content", None
        if act == "patch" and not content.strip():
            return (f"{pos}: content is required — set this operation object's own 'content' to the new "
                    f"text for the old_text span. {PATCH_DROP_HINT}"), None, "missing_content", None
        if matched_entry is not None:
            idx = _pinned_index(working, matched_entry)
            if idx is None:
                return f"{pos}: {_stale_entry_message(matched_entry)}", None, "stale_entry", None
        else:
            idx, ambiguous = _find_unique_match(working, old_text)
            if ambiguous:
                return (f"{pos}: '{old_text}' matched multiple distinct entries -- be more specific.", None,
                        "ambiguous", None)
            if idx is None:
                miss = _no_match_error(working, old_text, act)
                return f"{pos}: {miss['error']}", miss.get("closest_entries"), "no_match", None
        previous_content = working[idx]
        if act != "patch":
            working[idx:idx + 1] = [content] if act == "replace" else []
            return None, previous_content, "none", None
        patched, message, failure_class = _patch_entry(previous_content, old_text, content)
        if patched is None:
            return f"{pos}: {message}", None, failure_class, None
        working[idx:idx + 1] = [patched]
        return None, previous_content, "none", patched

    def apply_batch(self, target: str, operations: list[dict[str, Any]]) -> dict[str, Any]:
        """Apply add/replace/remove ops atomically against the FINAL budget, so one call
        can free space and add entries. All-or-nothing: any malformed / unmatched op or
        an over-limit result writes NOTHING and returns the first failure. Aborts do not
        echo ``current_entries`` — the store is unchanged and the model already has it."""
        return self._batch(target, operations, commit=True)

    def resolve_batch_entries(self, target: str, operations: list[dict[str, Any]]) -> dict[str, Any]:
        """Dry-run ``apply_batch`` under the lock without persisting: the same content scan,
        op walk, empty-store and budget checks, so it fails exactly where the direct batch
        would; on success ``{"success": True, "matched_entries": [...]}`` — per op, the FULL
        entry its replace/remove selects now (None for add), in batch order."""
        return self._batch(target, operations, commit=False)

    def _batch(self, target: str, operations: list[dict[str, Any]], *, commit: bool) -> dict[str, Any]:
        from tools.memory_tool import _memory_target_error
        if target_error := _memory_target_error(self, target, operations):
            return target_error
        if not operations:
            return _error("operations list is empty.", "invalid_args")
        ops = [op or {} for op in operations]
        # Scan every add/replace content BEFORE touching disk -- one poisoned op rejects the batch.
        for i, op in enumerate(ops):
            scan_error = op.get("action") in {"add", "replace"} and op.get("content") and _scan_memory_content(op["content"])
            if scan_error:
                return _error(f"Operation {i + 1}: {scan_error}", "scan_blocked")

        def _apply(entries, limit):
            working = list(entries)  # only committed if the whole batch validates
            matched = []  # per op, the entry a replace/patch/remove selected (None for add)
            produced = {}  # 1-based op index -> the entry a patch produced
            for i, op in enumerate(ops):
                act = op.get("action")
                content, old_text = op.get("content") or op.get("new_text") or "", op.get("old_text") or ""
                if act != "patch":  # whole-entry actions; a patch span keeps its whitespace
                    content, old_text = content.strip(), old_text.strip()
                msg, previous_content, failure_class, after = self._apply_batch_op(
                    working, act, content, old_text, f"Operation {i + 1} ({act or 'unknown'})",
                    op.get("matched_entry"))
                if msg:  # a failed op's second value is its closest_entries (or None)
                    return self._batch_failure(target, msg, failure_class, closest_entries=previous_content)
                matched.append(previous_content)
                if after is not None:
                    produced[i + 1] = after
            if entries and not working:
                # #103419: a consolidation batch that removes the last entry would
                # commit an empty file as a normal successful write. Refuse; single
                # remove() is the deliberate-wipe path.
                label = self._path_for(target).name
                return self._batch_failure(target, (
                    f"Refusing to empty {label}: this batch would remove every entry from a "
                    f"previously non-empty store. Keep at least one entry — merge overlapping "
                    f"entries into a shorter one instead of removing the last one. To delete the "
                    f"final entry deliberately, use single remove() calls."), "would_empty")
            new_total = len(ENTRY_DELIMITER.join(working))  # budget check against the FINAL state only
            if new_total > limit:
                return self._batch_failure(target, (
                    f"Applying all {len(operations)} operations {_over_limit(new_total, limit)} more. Add "
                    f"removes or shortening patches of other stale entries to the same batch, then retry."),
                    "over_budget")
            if not commit:
                return {"success": True, "matched_entries": matched}
            # op index -> full entry text its replace/remove selected (#117952), 1-based to
            # match the "Operation N" error numbering the model sees for failed ops.
            replaced, removed, patched = {}, {}, {}
            for i, (op, previous_content) in enumerate(zip(ops, matched), 1):
                if previous_content is not None:
                    result_map = {"replace": replaced, "patch": patched, "remove": removed}.get(op.get("action"))
                    if result_map is not None:
                        result_map[i] = previous_content
            replaced_fields = {"replaced_entries": replaced} if replaced else {}
            if patched:
                replaced_fields["patched_entries"] = patched
                replaced_fields["patched_entries_after"] = produced
            if removed:
                replaced_fields["removed_entries"] = removed
            return working, f"Applied {len(operations)} operation(s).", replaced_fields
        return self._mutate(target, _apply, skip_drift=not commit)

    def format_for_system_prompt(self, target: str) -> Optional[str]:
        """
        Return the frozen snapshot for system prompt injection.

        This returns the state captured at load_from_disk() time, NOT the live
        state. Mid-session writes do not affect this. This keeps the system
        prompt stable across all turns, preserving the prefix cache.

        Returns None if the snapshot is empty (no entries at load time).

        For target="memory", lazily refreshes MEMORY_SUMMARY.md if a newer
        version exists on disk (e.g. cron regenerated it after gateway start).
        The refresh only fires on mtime change, so steady-state conversations
        keep a cache-stable prompt.
        """
        if target == "memory":
            self._refresh_memory_summary()
        elif target == "user":
            self._refresh_user_summary()
        block = self._system_prompt_snapshot.get(target, "")
        return block if block else None

    def _refresh_memory_summary(self):
        """Re-read MEMORY_SUMMARY.md into the system prompt snapshot if newer.

        Called from format_for_system_prompt() on every new conversation
        (system prompt build).  If MEMORY.md is newer than the summary,
        regenerates it on the spot (DS V3, ~2-3s) so the current
        conversation always gets a fresh summary.  In the steady state
        this is a single stat(2) check and returns immediately, preserving
        prefix-cache stability across turns.
        """
        from tools import memory_tool
        mem_dir = memory_tool.get_memory_dir()
        summary_path = mem_dir / "MEMORY_SUMMARY.md"

        # ── 分层索引 v2 灰度开关 ──
        _flag_path = mem_dir.parent / "state" / "flags" / "lean_index_v2"
        _v2_path = mem_dir / "MEMORY_SUMMARY_V2.md"
        if _flag_path.exists() and _v2_path.exists():
            summary_path = _v2_path

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
        # 2026-09-21 fix（同类同修）：原为 `return` ⇒ 超 24h 回退注入全量 MEMORY.md。
        # 与 USER 侧同一模式，一并改为照常注入 + 记告警（每日 03:00 cron 本该保鲜）。
        if age_seconds >= 86400:
            logger.warning(
                "MEMORY_SUMMARY.md is %.1fh old — injecting the summary; no full-memory "
                "fallback (daily 03:00 cron should have refreshed it)", age_seconds / 3600,
            )

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
        _wm = self._memory_watermark()
        if _wm:
            header += " | " + _wm
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
        from tools import memory_tool
        mem_dir = memory_tool.get_memory_dir()
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
            # 2026-09-21 fix（B）：旧行为是直接 return ⇒ 回退注入**全量 USER.md**。
            # 但摘要的内容新鲜度由上方「USER.md mtime > 摘要 mtime」比较决定——USER.md
            # 长期不变时摘要就是最新版，年龄本身不代表过期。硬回退实测让 35,346 字符
            # 全量画像顶替 4,216 字符摘要（连续 37h、单日 8 次告警）。改为照常注入 +
            # 留一条告警，供监控发现「该刷新时没刷新」。
            logger.warning(
                "USER_SUMMARY.md is %.1fh old but USER.md is not newer — injecting the "
                "summary; no full-profile fallback", age_seconds / 3600,
            )

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
        from tools import memory_tool
        mem_dir = memory_tool.get_memory_dir()
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
        from tools import memory_tool
        mem_dir = memory_tool.get_memory_dir()
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
            "将以下 MEMORY.md 压缩为 3-5K chars 的分层索引。\n"
            "MEMORY.md 原文：\n"
            + memory_text +
            "\n\n按以下格式输出（不要加额外说明）：\n"
            "> ⚠️ 以下为分层索引摘要。每条目含核心事实+元数据。\n"
            "> 需要完整上下文时，必须调用 memory(action='read') 获取原文。\n"
            "> ⛔ 违反本指令将被实时监控——多次违规累计后 session 将被自动终止。\n"
            "> 禁止基于摘要片段做出判断或声称——复述摘要也视为违反。\n\n"
            "## 索引\n"
            "| # | 类别 | 条目 | Token | 核心事实 | 关键词 |\n"
            "|:--|:--|:--|:--|:--|:--|\n"
            "(每行一条记忆类别。token 估算：~4 chars = 1 token。\n"
            "核心事实列每格 10-30 字——只写最关键的一句话事实，不可省略。\n"
            "活跃项目/30天新决策→正常列入。长期稳定偏好→压缩为一句核心事实。\n"
            "已完成项目→只列标题。纯操作日志/已解决技术债→不列入。)\n"
            "**总计**: N类 M条目 ~XK chars, ~Y tokens\n\n"
            "⚠️ 格式铁律：输出必须恰好是指定格式——4行头部→空行→\"## 索引\"→表格→\"**总计**\"——之后立即停止，不要任何额外内容、解释、注释。超过 6K chars 的输出将被截断丢弃。"
        )

        payload = _json.dumps({
            "model": "deepseek-v4-flash",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3,
            "max_tokens": 384000,
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
            # 2026-09-21 fix（C，同类同修）：与 USER 摘要生成同款超时问题，一并放宽到 600s。
            resp = _json.loads(_urllib.urlopen(req, timeout=600).read())
            summary = resp["choices"][0]["message"]["content"].strip()
            # Strip markdown code block wrappers（对称 USER 摘要生成行 691-695）
            if summary.startswith("```"):
                summary = summary.split("\n", 1)[-1]
            if summary.endswith("```"):
                summary = summary.rsplit("\n", 1)[0]
            summary = summary.strip()
        except Exception:
            logger.warning(
                "_generate_summary_on_demand: API call failed", exc_info=True
            )
            return

        if not summary:
            return

        # ── Governance Decay Guard — 机械兜底约束 ──
        constraints_path = os.path.expanduser(
            "~/.hermes/memories/MEMORY_CONSTRAINTS.yaml"
        )
        if os.path.exists(constraints_path):
            appended = []
            try:
                with open(constraints_path, encoding="utf-8") as f:
                    _constraints_data = yaml.safe_load(f)
                _constraints = (
                    _constraints_data.get("constraints", [])
                    if isinstance(_constraints_data, dict)
                    else []
                )
            except Exception:
                _constraints = []  # YAML 损坏 → 降级（不附加，不炸系统）
            for _c in _constraints:
                if not isinstance(_c, dict):
                    continue
                _ft = _c.get("full_text", "")
                if not _ft:
                    continue
                _hit = False
                for _p in _c.get("patterns", []):
                    if not isinstance(_p, str) or not _p.strip():
                        continue
                    try:
                        if _re.search(_p, summary):
                            _hit = True
                            break
                    except _re.error:
                        continue  # 单个坏 pattern → 跳过该条，不拖垮系统
                if not _hit:
                    appended.append(_ft)
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
            "# USER.md 摘要 — 渐进式披露\n\n"
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
            "model": "deepseek-v4-flash",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3,
            "max_tokens": 384000,
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
            # 2026-09-21 fix（C）：实测单次重建 125.7s（35K 字符输入），原 timeout=60
            # 靠「数据持续流动」侥幸不超时。放宽到 600s；**不限制 max_tokens**（保持 384000）。
            resp = _json.loads(_urllib.urlopen(req, timeout=600).read())
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

    def _success_response(self, target: str, message: str | None = None, **extra) -> dict[str, Any]:
        """TERMINAL and WITHOUT the entries list: echoing entries invites the model to
        "find more to fix" and re-issue the same ops. A successful write resets the
        per-turn failure budget. ``**extra`` mirrors ``_error``'s convention — e.g. the
        full text a replace overwrote (#117952), deliberately visible despite the
        no-entries rule: silent data loss is the failure this field exists to prevent."""
        # A successful write means the consolidation loop made progress, so the per-turn failure budget
        # resets (the cap counts consecutive failures, not lifetime ones within a turn) (#42405).
        self._consolidation_failures = 0
        return {"success": True, "done": True, "target": target,
                "usage": self._usage_pct(target, self._char_count(target)),
                "entry_count": len(self._entries_for(target)), **({"message": message} if message else {}),
                **extra,
                "note": "Write saved. This update is complete — do not repeat it."}

    def _render_block(self, target: str, entries: list[str]) -> str:
        """System prompt block: header + usage indicator + entries ("" when empty)."""
        if not entries:
            return ""
        content, sep = ENTRY_DELIMITER.join(entries), "═" * 46
        title = MEMORY_BLOCK_HEADERS["user" if target == "user" else "memory"]
        return f"{sep}\n{title} [{self._usage_pct(target, len(content))}]\n{sep}\n{content}"

    @staticmethod
    def _read_raw_checked(path: Path) -> tuple[str, bool]:
        """``(raw, read_ok)``; ``read_ok`` is False ONLY when the file EXISTS but can't be
        read. Decoding stays STRICT (``errors="replace"`` would hand callers a lossy view
        a save then persists); ``utf-8-sig`` strips a Notepad BOM off the first entry."""
        if not path.exists():
            return "", True
        try:
            # utf-8-sig strips a leading UTF-8 BOM (Notepad-edited memory files on Windows) and is
            # byte-identical to utf-8 otherwise. Plain utf-8 kept U+FEFF glued to the first entry,
            # corrupting matching/dedup for that entry forever (#10878 / PR #10888). Decode errors stay
            # STRICT on purpose: errors="replace" would hand read-modify-write callers a lossy view that a
            # subsequent save persists over the real bytes — the wipe class documented above. Undecodable
            # bytes must surface as read_ok=False.
            return path.read_text(encoding="utf-8-sig"), True
        except (OSError, UnicodeDecodeError):
            return "", False

    @staticmethod
    def _parse_entries(raw: str) -> list[str]:
        """Stripped, non-empty entries; splits on the FULL delimiter so a bare "§" survives."""
        return [e for e in (x.strip() for x in raw.split(ENTRY_DELIMITER)) if e]

    @staticmethod
    def _read_file(path: Path) -> list[str]:
        """Entries of a memory file ([] on any error). Read-only callers only; mutation
        paths use ``_read_raw_checked`` so they can refuse to overwrite an unreadable file."""
        return MemoryStore._parse_entries(MemoryStore._read_raw_checked(path)[0])

    @staticmethod
    def _write_file(path: Path, entries: list[str]):
        """Atomic temp-file + rename: readers never see a truncated file. Callers
        hold ``_file_lock`` (via ``_mutate``): a bare write from an earlier snapshot
        drops concurrent entries (#119668)."""
        try:
            atomic_write_text(path, ENTRY_DELIMITER.join(entries), tmp_prefix=".mem_")
        except OSError as e:
            raise RuntimeError(f"Failed to write memory file {path}: {e}")

    def _detect_external_drift(self, target: str, raw: str) -> Optional[str]:
        """``.bak.<ts>`` snapshot path if *raw* shows external drift, else None. The one
        signal is an entry over the whole-file limit: no tool-written entry can be, so it is
        free-form text appended without delimiters, and replacing that "entry" would discard
        it. Layout differences (blank lines around ``§``, empty entries, padding) are NOT
        drift: parsing only strips whitespace, so the flush loses nothing but whitespace, and
        refusing there blocked every replace/remove on a hand-formatted file for good (#107270)."""
        if max(map(len, self._parse_entries(raw)), default=0) <= self._char_limit(target):
            return None
        path = self._path_for(target)
        bak_path = path.with_suffix(path.suffix + f".bak.{int(time.time())}")
        try:
            bak_path.write_text(raw, encoding="utf-8")
        except OSError:
            return str(bak_path) + " (BACKUP FAILED — file unchanged on disk)"
        return str(bak_path)

def _memory_source_stats_hint() -> str:
    """2026-08-11 CI 白鳝方法论2: 统计 memory_meta.json 中 source 分布，返回注入提示。

    纯知情不改行为：让 Agent/用户看到摘要时知道哪些条目是 AI 推断。
    失败降级返回空串（不阻塞注入）。
    """
    try:
        import json as _json
        meta_path = get_hermes_home() / "state" / "memory_meta.json"
        if not meta_path.exists():
            return ""
        with open(meta_path, encoding="utf-8") as _f:
            meta = _json.load(_f)
        entries = meta.get("entries", {})
        if not entries:
            return ""
        user_cnt = sum(1 for v in entries.values() if v.get("source") == "user_stated")
        llm_cnt = sum(1 for v in entries.values() if v.get("source") == "llm_inferred")
        total = len(entries)
        return f"⚠️ {total}条记忆中 {llm_cnt}条为AI推断(llm_inferred)，{user_cnt}条为你陈述(user_stated)——引用推断条目需核实"
    except Exception:
        return ""
