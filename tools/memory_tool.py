#!/usr/bin/env python3
"""Memory Tool - persistent curated memory (MEMORY.md = agent notes, USER.md = user
profile). Both enter the system prompt as a FROZEN snapshot at session start;
mid-session writes hit disk but never change the prompt (prefix cache intact).
Single `memory` tool: add/patch/remove or a batch `operations` list."""

import copy
import hashlib
import json
import logging
import sys
import time
from contextvars import ContextVar
from pathlib import Path
from hermes_constants import get_hermes_home
from typing import Dict, Any, List, Optional, Tuple

from utils import is_truthy_value
from tools.registry import no_cache_check_fn

# fcntl is Unix-only; Windows uses msvcrt. MemoryStore reads both lazily from
# this module (tests patch ``memory_tool.fcntl``).
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

# One tool-definition pass must use ONE config decision for availability and the
# dynamic target schema: the check_fn result flows to the immediately following
# dynamic_schema_overrides call; ContextVar isolates concurrent profile builds.
_memory_surface_flags: ContextVar[Optional[tuple[bool, bool]]] = ContextVar("memory_surface_flags", default=None)


def get_memory_dir() -> Path:
    """Profile-scoped memories dir, resolved per call (HERMES_HOME may switch after import)."""
    return get_hermes_home() / "memories"


from tools.memory_tool_store import (
    ENTRY_DELIMITER, FAILURE_CLASS, MEMORY_BLOCK_HEADERS, PATCH_DROP_HINT, MemoryStore, _scan_memory_content)


def load_on_disk_store() -> MemoryStore:
    """Fresh on-disk MemoryStore with configured limits/flags for contexts with no live
    agent (gateway, Desktop, ``/memory``) so approvals enforce the SAME caps as
    ``agent_init``. Falls back to defaults if config can't load; never raises."""
    try:
        from hermes_cli.config import load_config
        config = load_config() or {}
        mem_cfg = get_builtin_memory_config(config)
        memory_enabled, user_profile_enabled = get_builtin_memory_store_flags(config)
        store = MemoryStore(int(mem_cfg.get("memory_char_limit", 2200)), int(mem_cfg.get("user_char_limit", 1375)),
                            memory_enabled=memory_enabled, user_profile_enabled=user_profile_enabled)
    except Exception:
        store = MemoryStore()  # config optional — fall back to defaults rather than break /memory
    store.load_from_disk()
    return store


def _pin_matched_entries(store: MemoryStore, payload: dict[str, Any]) -> Optional[str]:
    """Record on each staged replace/remove the FULL entry its old_text selects now. Approval
    then applies to exactly the entry the approver reviewed and refuses if it changed:
    re-running the old_text search at approve time could hit a newer entry that still
    contains it. Returns the JSON error when the search fails now, as the direct write would."""
    target = payload.get("target", "memory")
    if payload.get("action") == "batch":
        result = store.resolve_batch_entries(target, payload["operations"])
        if result.get("success"):
            payload["operations"] = [op if entry is None else {**op, "matched_entry": entry}
                                     for op, entry in zip(payload["operations"], result["matched_entries"])]
    elif payload.get("action") in _BG_DELETE_ACTIONS:
        result = store.resolve_entry(target, payload.get("old_text") or "", payload["action"])
        if result.get("success"):
            payload["matched_entry"] = result["matched_entry"]
    else:
        return None
    return None if result.get("success") else json.dumps(result, ensure_ascii=False)


def _gate_or_stage(store: MemoryStore, summary: str, detail: str, payload: dict[str, Any]) -> Optional[str]:
    """JSON tool-result string when the write must NOT proceed (blocked or staged
    for approval), None to proceed. Fails open if the gate module can't load."""
    try:
        from tools import write_approval as wa
    except Exception:
        return None
    decision = wa.evaluate_gate(wa.MEMORY, inline_summary=summary, inline_detail=detail)
    if decision.allow:
        return None
    if decision.blocked:
        FAILURE_CLASS.set("gate_refused")
        return tool_error(decision.message, success=False)
    if (unmatched := _pin_matched_entries(store, payload)) is not None:
        return unmatched
    FAILURE_CLASS.set("staged")
    record = wa.stage_write(wa.MEMORY, payload, summary=f"{summary}: {detail[:120]}", origin=wa.current_origin())
    return json.dumps({"success": True, "staged": True, "pending_id": record["id"], "message": decision.message},
                      ensure_ascii=False)


# action -> (store call, gate (summary, detail) text) for the live tool path and staged replay
# ('replace' only replays writes staged before it was removed from the tool).
_STORE_ACTIONS = {
    "add": (lambda store, target, content, old_text, entry=None: store.add(target, content),
            lambda label, content, old_text: (f"add to {label}", content or "")),
    "replace": (lambda store, target, content, old_text, entry=None: store.replace(target, old_text, content, entry),
                lambda label, content, old_text: (f"replace in {label}",
                                                  f"entry matching: {old_text}\nwhole entry becomes: {content}")),
    "remove": (lambda store, target, content, old_text, entry=None: store.remove(target, old_text, entry),
               lambda label, content, old_text: (f"remove from {label}", old_text or "")),
    "patch": (lambda store, target, content, old_text, entry=None: store.patch(target, old_text, content, entry),
              lambda label, content, old_text: (f"patch in {label}",
                                                f"entry matching: {old_text}\nspan '{old_text}' becomes: {content} "
                                                "(rest of the entry is kept)"))}


def _batch_op_line(op: dict[str, Any]) -> str:
    op = op or {}
    act, content, old = op.get("action", "?"), op.get("content") or op.get("new_text") or "", op.get("old_text", "")
    if act == "remove":
        return f"- remove: {old}"
    if act == "patch":
        return f"- patch span '{old}' -> '{content}' (rest of the entry is kept)"
    return f"- {act}: {content}"


def _apply_write_gate(store: MemoryStore, action: str, target: str, content: Optional[str],
                      old_text: Optional[str], operations: Optional[list[dict[str, Any]]] = None) -> Optional[str]:
    """Gate one mutating op, or (``operations`` set) a whole batch as a single unit."""
    label = "user profile" if target == "user" else "memory"
    if operations is not None:
        return _gate_or_stage(store, f"apply {len(operations)} op(s) to {label}",
                              "\n".join(_batch_op_line(op) for op in operations),
                              {"action": "batch", "target": target, "operations": operations})
    return _gate_or_stage(store, *_STORE_ACTIONS[action][1](label, content, old_text),
                          {"action": action, "target": target, "content": content, "old_text": old_text})


def _validate_single_op(store, action, target, content, old_text) -> Optional[str]:
    """Validate BEFORE the gate so an invalid write is rejected now, not at approve time.
    Missing ``old_text`` is recoverable (it can't be schema-required — needs a combinator
    the Codex backend rejects): return the inventory plus a retry instruction."""
    if action == "add" and not content:
        FAILURE_CLASS.set("missing_content")
        return tool_error("Content is required for 'add' action.", success=False)
    if action in ("remove", "patch") and not old_text:
        FAILURE_CLASS.set("missing_old_text")
        return json.dumps({
            "success": False,
            "error": (f"'{action}' needs old_text -- a short unique substring of the entry "
                      f"to {action}. None was provided. Reissue the {action} with old_text "
                      f"set to part of one of the current_entries below."),
            "current_entries": store._entries_for(target), "usage": store._usage(target)}, ensure_ascii=False)
    if action == "patch" and not content:
        FAILURE_CLASS.set("missing_content")
        return tool_error(f"content (the new text for the old_text span) is required for 'patch' action. "
                          f"{PATCH_DROP_HINT}", success=False)
    return None


# 'replace' is no longer offered to the model; it stays here so writes staged before its
# removal still pin, display and replay.
_BG_DELETE_ACTIONS = ("replace", "remove", "patch")
_REPLACE_REMOVED = ("'replace' is not available; use 'patch'. old_text is the exact span to change and "
                    "content is its new text; the rest of the entry is kept. To rewrite a whole entry, "
                    "set old_text to the whole entry. Nothing was written.")

def destructive_ops(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """The replace/remove ops of a staged memory payload, single-op or batch shape."""
    ops = (payload.get("operations") or []) if payload.get("action") == "batch" else [payload]
    return [op for op in ops if (op or {}).get("action") in _BG_DELETE_ACTIONS]


def _background_delete_gate(store, action, operations, target="memory", content=None,
                            old_text=None) -> Optional[str]:
    """Fail-closed operation gate for unattended background-review forks (#105921): ``add``
    stays available (it is all any review prompt asks for), while ``patch``/``remove`` —
    single or inside a batch — are never applied unattended. The op is staged in the pending
    store instead of merely denied: the fork's own review summary is never published back, so
    a plain denial would drop the consolidation request with no surfacing path at all. A
    staging failure fails closed to a plain denial."""
    from tools.skill_provenance import is_unattended_review

    if not is_unattended_review():
        return None
    payload = ({"action": "batch", "target": target, "operations": operations}
               if operations is not None else
               {"action": action, "target": target, "content": content, "old_text": old_text})
    if not destructive_ops(payload):
        return None
    detail = ("; ".join(_batch_op_line(op) for op in operations) if operations is not None
              else _batch_op_line({"action": action, "content": content, "old_text": old_text}))
    try:
        if (unmatched := _pin_matched_entries(store, payload)) is not None:
            return unmatched
        from tools import write_approval as wa
        FAILURE_CLASS.set("staged")
        record = wa.stage_write(
            wa.MEMORY, payload,
            summary=(f"background review consolidation ({'batch' if operations is not None else action} "
                     f"on {target}): {detail}")[:200],
            origin=wa.current_origin())
        return json.dumps({
            "success": True, "staged": True, "proposal_staged": True, "pending_id": record["id"],
            "message": ("Background review may not delete memory entries unattended. The proposed "
                        f"{'batch' if operations is not None else action} was staged for your approval — "
                        "review it with /memory pending (approve to apply, discard to drop)."),
        }, ensure_ascii=False)
    except Exception:
        logger.warning("Failed to stage background-review consolidation; denying", exc_info=True)
        FAILURE_CLASS.set("gate_refused")
        return tool_error(
            "Background review may not delete memory entries ('patch'/'remove', including in a "
            "batch); 'add' is still available.", success=False)


def _import_from_scripts(module_name: str):
    """从当前 profile 的 ``scripts/`` 导入模块，导入后立刻把该目录移出 ``sys.path``。

    本地定制模块（memory_routing / memory_meta_lock / memory_semantic_fallback）不在包里，
    只能按路径加载；但**永久** insert sys.path 会污染进程，测试的 home_io_guard 会把
    后续任何 realpath 扫描判成「测试读写真实 hermes home」而误杀（2026-10-19 实测）。
    """
    import importlib
    cached = sys.modules.get(module_name)
    if cached is not None:
        return cached
    scripts_dir = str(get_hermes_home() / "scripts")
    added = scripts_dir not in sys.path
    if added:
        sys.path.insert(0, scripts_dir)
    try:
        return importlib.import_module(module_name)
    finally:
        if added:
            try:
                sys.path.remove(scripts_dir)
            except ValueError:
                pass


def memory_tool(action: str | None = None, target: str = "memory", content: str | None = None, old_text: str | None = None,
                new_text: str | None = None, operations: Optional[list[dict[str, Any]]] = None,
                store: Optional[MemoryStore] = None, query: Optional[str] = None) -> str:
    """Tool entry point; returns a JSON string. Single op (action + content/old_text)
    or batch (``operations``, atomic against the final budget). ``new_text``
    aliases ``content``."""
    if store is None:
        return tool_error("Memory is not available. It may be disabled in config or this environment.", success=False)
    token = FAILURE_CLASS.set("other")
    try:
        outcome, result = _memory_tool(action, target, content, old_text, new_text, operations, store, query=query)
        failure_class = "none" if outcome == "success" else FAILURE_CLASS.get()
    finally:
        FAILURE_CLASS.reset(token)
    from hermes_cli.observability.shared_metrics_loop import record_builtin_memory_call
    record_builtin_memory_call(action, operations, outcome=outcome, failure_class=failure_class)
    return result


def _applied(result: dict[str, Any]) -> tuple[str, str]:
    return ("success" if result.get("success") else "failed"), json.dumps(result, ensure_ascii=False)


def _invalid(message: str) -> tuple[str, str]:
    FAILURE_CLASS.set("invalid_args")
    return "rejected", tool_error(message, success=False)


def _memory_tool(action, target, content, old_text, new_text, operations, store, query=None) -> tuple[str, str]:
    """``(outcome, result_json)``: ``rejected`` when refused or held before touching the store."""
    # An omitted optional string can arrive as "" (#90468): let the new_text alias fill it.
    if not content and new_text:
        content = new_text
    target_error = _memory_target_error(store, target, operations)
    if target_error is not None:
        return "rejected", json.dumps(target_error)
    # ── 本地定制（2026-08-02 / 2026-10-17）：read action + query 检索 ──────────
    # 上游无 read action；本分支支持全量读取与关键词检索（L1 词面 + L2 语义兜底）。
    if action == "read":
        mem_path = store._path_for(target)
        if not mem_path.exists():
            return _applied({"success": False, "error": f"{target.upper()}.md not found"})
        try:
            content = mem_path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            return _applied({"success": False, "target": target,
                             "error": f"{target.upper()}.md unreadable: {exc}"})
        if query and query.strip():
            import re as _re
            keywords = list(dict.fromkeys(
                k.strip().lower() for k in _re.split(r"[\s,，、;；.。:：]+", query) if k.strip()))
            entries = [e for e in content.split(ENTRY_DELIMITER) if e.strip()]
            if not keywords:
                result = {"success": True, "target": target, "query": query, "matched": 0, "content": ""}
            else:
                scored = []
                for e in entries:
                    low = e.lower()
                    hits = sum(1 for k in keywords if k in low)
                    if hits > 0:
                        scored.append((hits, e))
                if scored:
                    scored.sort(key=lambda x: -x[0])
                    matched = [e for _, e in scored[:5]]
                    result = {"success": True, "target": target, "query": query,
                              "matched": len(matched), "content": ENTRY_DELIMITER.join(matched)}
                else:
                    try:
                        sem = _semantic_fallback(query, content, target)
                    except Exception:
                        sem = None
                    if sem:
                        result = {"success": True, "target": target, "query": query, **sem}
                    else:
                        result = {"success": True, "target": target, "query": query, "matched": 0, "content": ""}
            content = result.get("content", "")
        else:
            result = {"success": True, "content": content}
        if target == "memory":
            _increment_read_counts(content)
        if hasattr(store, "_cost_sid"):
            _cost_incr(store._cost_sid, "read_count", 1)
            _cost_incr(store._cost_sid, "read_chars", len(content))
        return _applied(result)
    if operations:
        if not isinstance(operations, list):
            return _invalid("operations must be a list of {action, content?, old_text?} objects.")
        if any(isinstance(op, dict) and op.get("action") == "replace" for op in operations):
            return _invalid(_REPLACE_REMOVED)
        denied = _background_delete_gate(store, action, operations, target)
        if denied is not None:
            return "rejected", denied
        # Approval gate: stages (background/gateway) or prompts inline (CLI); off by default.
        gate_result = _apply_write_gate(store, "batch", target, None, None, operations)
        if gate_result is not None:
            return "rejected", gate_result
        batch_result = store.apply_batch(target, operations)
        _record_meta_for_operations(target, operations, batch_result)
        return _applied(batch_result)
    # Reject calls that provide neither action (single-op) nor operations
    # (batch).  Without this guard the dispatch falls through to the generic
    # "Unknown action 'None'" error, which gives the model no signal about
    # *why* the call was malformed and can trigger repeated retries. (#64291)
    if not action and not operations:
        return _invalid(
            "Missing required parameter: provide 'action' (add/patch/remove) "
            "or 'operations' (batch list). Got neither."
        )
    if action == "replace":
        return _invalid(_REPLACE_REMOVED)
    if action not in _STORE_ACTIONS:
        return _invalid(f"Unknown action '{action}'. Use: add, patch, remove")
    invalid = (_validate_single_op(store, action, target, content, old_text)
               or _background_delete_gate(store, action, None, target, content, old_text)
               or _apply_write_gate(store, action, target, content, old_text))
    if invalid is not None:
        return "rejected", invalid
    # ── 本地定制：写入分流（规则类→正常写；事件类→转存 kaah；unknown→写入+警告头）──
    if action == "add" and target == "memory" and content:
        try:
            check_route = _import_from_scripts("memory_routing").check_route
            route = check_route(content, target)
            if route.get("action") == "block":
                return _applied({
                    "success": False,
                    "status": "routed_to_kaah",
                    "message": "事件类记忆已自动转存 kaah 归档（不占常驻 MEMORY）",
                    "archive_path": route.get("archive_path", ""),
                })
            if route.get("action") == "route_only":
                content = content + f"（事件类，已自动转存 kaah: {route.get('archive_path', '')}）"
            elif route.get("action") == "write_with_warning":
                content = "⚠️未分类 " + content
        except Exception:
            pass  # 分流模块失败 → fail open，不阻断记忆写入
    result = _STORE_ACTIONS[action][0](store, target, content, old_text)
    if target == "memory" and isinstance(result, dict) and result.get("success"):
        if action in ("add", "replace"):
            _record_memory_meta(content)
        elif action == "remove":
            _prune_orphan_meta()
    return _applied(result)


def get_builtin_memory_config(config: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """Normalized ``memory`` config section ({} when missing/malformed → flags default to
    enabled). ``agent_init`` reads the same section so availability and store cannot diverge."""
    if config is None:
        try:
            from hermes_cli.config import load_config_readonly
            config = load_config_readonly()
        except Exception:
            logger.debug("Could not read memory config for availability", exc_info=True)
            return {}
    section = config.get("memory") if isinstance(config, dict) else None
    return section if isinstance(section, dict) else {}


def get_builtin_memory_store_flags(config: Optional[dict[str, Any]] = None) -> tuple[bool, bool]:
    """Return ``(memory_enabled, user_profile_enabled)`` from resolved config."""
    section = get_builtin_memory_config(config)
    return tuple(is_truthy_value(section.get(k), default=True) for k in ("memory_enabled", "user_profile_enabled"))


@no_cache_check_fn
def check_memory_requirements() -> bool:
    """Snapshot store flags and report whether the built-in tool is available."""
    _memory_surface_flags.set(None)
    flags = get_builtin_memory_store_flags()
    _memory_surface_flags.set(flags)
    return flags[0] or flags[1]


def _memory_target_error(
    store: MemoryStore, target: str | None, operations: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """Validate one explicit store before writing, staging, or replaying a batch."""
    if target is None:
        FAILURE_CLASS.set("invalid_args")
        return {"success": False, "error": (
            "Missing required target. Retry with target='memory' or target='user' at the top level. "
            "For a batch, remove target from every operation; use separate calls for different stores. "
            "Nothing was written or staged.")}
    if not isinstance(target, str) or target not in {"memory", "user"}:
        from tools.registry import _bound_error_text
        FAILURE_CLASS.set("invalid_args")
        return {"success": False,
                "error": _bound_error_text(f"Invalid memory target '{target}'. Use 'memory' or 'user'.")}
    if isinstance(operations, list):
        for index, op in enumerate(operations, 1):
            # A nested target equal to the call's (or null) is redundant, not wrong; only a
            # different one asks for two stores in one call, which a batch cannot do.
            if isinstance(op, dict) and op.get("target") not in (None, target):
                FAILURE_CLASS.set("invalid_args")
                return {"success": False, "error": (
                    f"Operation {index} names a different target than the one at the top level. "
                    "One call writes one store: retry with separate calls for different stores. "
                    "Nothing was written or staged.")}
    if store.target_enabled(target):
        return None
    label = "USER.md" if target == "user" else "MEMORY.md"
    FAILURE_CLASS.set("disabled")
    return {"success": False, "error": f"Built-in {label} writes are disabled in memory config.", "target": target}


def apply_memory_pending(payload: dict[str, Any], store: MemoryStore) -> dict[str, Any]:
    """Replay a staged write against the store, bypassing the gate (/memory approve). A
    replace/remove applies to exactly its pinned ``matched_entry`` or is refused; a record
    staged before pinning has no verifiable target, so it is refused rather than replayed by
    old_text (which could hit a newer entry the approver never saw)."""
    action, target = payload.get("action"), payload.get("target")
    target_error = _memory_target_error(store, target, payload.get("operations"))
    if target_error is not None:
        return target_error
    if any(not op.get("matched_entry") for op in destructive_ops(payload)):
        return {"success": False, "error": "This destructive pending write predates entry pinning and cannot be "
                                           "verified; nothing was applied. Reject it and recreate the change."}
    if action == "batch":
        return store.apply_batch(target, payload.get("operations") or [])
    if action not in _STORE_ACTIONS:
        return {"success": False, "error": f"Unknown staged action '{action}'."}
    return _STORE_ACTIONS[action][0](store, target, payload.get("content") or "", payload.get("old_text") or "",
                                     payload.get("matched_entry"))


MEMORY_SCHEMA = {
    "name": "memory",
    "description": (
        "Save durable facts to persistent memory that survive across sessions. Memory is "
        "injected into every future turn, so keep entries compact and high-signal.\n\n"
        "HOW: make ALL your changes to one target in ONE call via an 'operations' array (each "
        "item: {action, content?, old_text?}). The batch applies atomically and the char limit is "
        "checked only on the FINAL result — so a single call can remove/patch stale entries "
        "to free room AND add new ones, even when an add alone would overflow. The response "
        "reports current/limit chars and confirms completion; one batch call finishes the "
        "update, so don't repeat it. Use the bare action/content/old_text fields only for a "
        "single lone change.\n\n"
        "WHEN: only for facts that apply to EVERY session regardless of task: who the user "
        "is, stable environment facts, standing conventions with no task home. Anything "
        "learned while doing a task (procedures, pitfalls, and the user's preferences and "
        "corrections for that kind of work) belongs in the task's skill via skill_manage, "
        "where it loads only when relevant; memory is injected into every turn and must "
        "stay small.\n\n"
        "IF FULL: an add is rejected with the current entries shown. Reissue as ONE batch that "
        "removes or shortens enough stale entries and adds the new one together.\n\n"
        "TARGETS: 'user' = who the user is (name, role, preferences, style). 'memory' = your "
        "notes (environment, conventions, tool quirks, lessons).\n\n"
        "READ (action='read'): 获取 MEMORY/USER 原文。信息获取优先级：①先用注入的摘要 ②摘要不够时 "
        "调用 read 拉取原文（可传 query 关键词按条目拉取，省 token）③read 找不到才用 session_search。"
        "禁止跳过 read 直接用 session_search 替代结构化记忆。\n\n"
        "SKIP: trivial/obvious info, easily re-discovered facts, raw data dumps, task progress, "
        "completed-work logs, temporary TODO state (use session_search for those). Reusable "
        "procedures belong in a skill, not memory."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["add", "patch", "remove", "read"],
                "description": "The action to perform (single-op shape). Omit when using 'operations'. 'read' 获取 MEMORY/USER 原文（支持 query 按关键词拉条目）。"
            },
            "target": {
                "type": "string",
                "enum": ["memory", "user"],
                "description": "Which memory store: 'memory' for personal notes, 'user' for user profile."
            },
            "content": {
                "type": "string",
                "description": "Required for 'add' and 'patch'. For 'add', the new entry. For 'patch', the text that replaces old_text; the rest of the entry is kept. Alias: 'new_text'."
            },
            "old_text": {
                "type": "string",
                "description": "REQUIRED for 'patch' and 'remove': text from one entry. 'patch' replaces exactly this span (it must occur once in the entry); 'remove' deletes the whole entry. Omit for 'add'."
            },
            "new_text": {
                "type": "string",
                "description": "Alias for 'content'. If both are set, 'content' wins."
            },
            "query": {
                "type": "string",
                "description": "仅 read 用：空格分隔的关键词，按关键词匹配 MEMORY 条目只返回命中项（省 token）。省略则返回全量。"
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
                        "action": {"type": "string", "enum": ["add", "patch", "remove"]},
                        "content": {"type": "string", "description": "Required for add and patch, as 'content' above."},
                        "new_text": {"type": "string", "description": "Alias for 'content' in a batch op."},
                        "old_text": {"type": "string", "description": "As 'old_text' above."},
                    },
                    "required": ["action"],
                    "additionalProperties": False,
                },
            },
        },
        "required": ["target"],
    },
}


# Schema text when only one built-in store is enabled: (target description, TARGETS replacement).
_SINGLE_TARGET_TEXT = {
    ("memory",): ("The enabled built-in store: 'memory' for personal notes.",
                  "TARGET: only 'memory' is enabled for personal notes (environment, conventions, "
                  "tool quirks, lessons)."),
    ("user",): ("The enabled built-in store: 'user' for user profile.",
                "TARGET: only 'user' is enabled for user profile facts (name, role, preferences, style).")}


def _build_memory_schema_overrides() -> dict[str, Any]:
    """Narrow the advertised target surface using the availability snapshot."""
    flags = _memory_surface_flags.get() or get_builtin_memory_store_flags()
    _memory_surface_flags.set(None)
    targets = [t for t, on in zip(("memory", "user"), flags) if on]
    parameters = copy.deepcopy(MEMORY_SCHEMA["parameters"])
    target_schema, description = parameters["properties"]["target"], MEMORY_SCHEMA["description"]
    target_schema["enum"] = targets
    if narrowed := _SINGLE_TARGET_TEXT.get(tuple(targets)):
        target_schema["description"], replacement = narrowed
        description = description.replace(
            "TARGETS: 'user' = who the user is (name, role, preferences, style). 'memory' = your "
            "notes (environment, conventions, tool quirks, lessons).", replacement)
    return {"description": description, "parameters": parameters}


from tools.registry import registry, tool_error

registry.register(
    name="memory",
    toolset="memory",
    schema=MEMORY_SCHEMA,
    handler=lambda args, **kw: memory_tool(
        action=args.get("action", ""), target=args.get("target"), store=kw.get("store"),
        query=args.get("query"),
        **{k: args.get(k) for k in ("content", "old_text", "new_text", "operations")}),
    check_fn=check_memory_requirements,
    emoji="🧠",
    dynamic_schema_overrides=_build_memory_schema_overrides)


# ══════════════════════════════════════════════════════════════════
# 本地定制函数区（自 v0.21.5 移植；上游 v0.22.2 无同名实现）
# ══════════════════════════════════════════════════════════════════

# §5.4 成本监控 — per-session memory cost tracking
# （恢复自 pre-upgrade-20260731-backup，2026-08-01）
# ════════════════════════════════════════════
import os as _cost_os
import time as _cost_time
import json as _cost_json
import atexit as _cost_atexit

_COST_LOG = _cost_os.path.expanduser("~/.hermes/logs/memory_cost.jsonl")
_cost_data = {}  # session_id → {lean_chars, user_chars, read_count, read_chars, ts}


_PLUGIN_COMPAT_LAZY = {
    'atomic_write_text': ('utils', 'atomic_write_text'),
}


def __getattr__(name):  # PEP 562 — lazy so no import cycles
    target = _PLUGIN_COMPAT_LAZY.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib
    from hermes_cli.plugin_compat import warn_once
    warn_once(__name__, name, *target)
    return getattr(importlib.import_module(target[0]), target[1])


def _archive_expired_entry(entry_hash: str, entry_meta: dict, age_days=None) -> str:
    """Save expired entry content to archive/ and mark meta as archived.
    Does NOT modify MEMORY.md——cleanup is a separate step.
    Returns archive file path or empty string."""
    import json as _json, hashlib as _hl
    from pathlib import Path as _Path
    from datetime import datetime as _dt
    
    mem_path = get_hermes_home() / "memories" / "MEMORY.md"
    meta_path = get_hermes_home() / "state" / "memory_meta.json"
    archive_dir = get_hermes_home() / "memories" / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    if not mem_path.exists():
        return ""
    
    # Find matching entry in MEMORY.md by hash
    # 2026-10-17 审查修复：
    # ① 分隔符改走 ENTRY_DELIMITER —— 硬编码 "\n§\n" 在常量一旦变更后会把整篇当成
    #    一条 ⇒ 永远匹配不到（同类硬编码在本文件另有 _increment_read_counts）。
    # ② 找不到正文就**不写归档**：原实现照样写一条「(entry content not found in
    #    MEMORY.md — hash may be stale)」并标 archived ⇒ 假归档污染审计账，且
    #    该条记录从此不再参与 TTL，等于用一条假记录掩盖了真实问题。
    content = mem_path.read_text(encoding="utf-8")
    entries_list = [e.strip() for e in content.split(ENTRY_DELIMITER)]

    found = None
    for e in entries_list:
        if _hl.sha256(e.encode()).hexdigest()[:12] == entry_hash:
            found = e
            break
    if found is None:
        # 正文已不在 MEMORY.md（历史重写 / replace 所致）：**不伪造正文**，写一条
        # 墓碑记录——只有元信息 + 明确标记；报表侧用自算 mem_hashes 判 🪦，不会算成
        # 「正文已归档」。
        logger.warning("archive: hash %s has no body in MEMORY.md — writing tombstone record",
                       entry_hash)
        found = ("🪦 墓碑：(entry content not found in MEMORY.md — hash 已失效)。"
                 "该 hash 对应的正文已不在 MEMORY.md（历史重写 / 替换所致），"
                 "本条只保留元信息，无正文可归档。")
    
    # Write archive file — reached only when the entry was found above.
    ts = _dt.now().strftime("%Y%m%d_%H%M%S")
    archive_path = archive_dir / f"{entry_hash}_{ts}.md"
    archive_path.write_text(
        f"# Archived Entry\n"
        f"# Hash: {entry_hash}\n"
        f"# Archived at: {_dt.now().isoformat()}\n"
        f"# Source: {entry_meta.get('source', 'unknown')}\n"
        f"# TTL: {entry_meta.get('ttl', 'unknown')}\n"
        f"# Access count: {entry_meta.get('access_count', 0)}\n"
        f"# Age at eviction: {age_days if age_days is not None else entry_meta.get('age_at_eviction', 'N/A')}\n\n"
        f"{found}\n",
        encoding="utf-8"
    )
    archive_path.chmod(0o600)

    # Update meta —— 2026-10-17 审查：原实现读-改-写全程无锁，与持锁的
    # _update_memory_meta* 并发时会用旧快照整文件覆盖（丢 created_at、回滚 archived）。
    # 改走同一把锁 + 原子写；账本不可读时保留已写出的归档文件，不强行改写账本。
    try:
        with _meta_lock_ctx():
            meta = _load_meta_file(meta_path)
            if meta is None or entry_hash not in (meta.get("entries") or {}):
                logger.warning("archive %s written but meta ledger unavailable for %s",
                               archive_path, entry_hash)
                return str(archive_path)
            rec = meta["entries"][entry_hash]
            rec["archived"] = True
            rec["archived_at"] = _dt.now().isoformat()
            rec["archive_path"] = str(archive_path)
            _write_meta_atomic(meta_path, meta)
    except Exception:
        logger.debug("archive meta update failed", exc_info=True)

    return str(archive_path)


def _parse_ttl(ttl_str: str):
    """解析 TTL 字符串 -> 天数（None = permanent）"""
    import re as _re
    if ttl_str in ("permanent", None):
        return None
    match = _re.match(r'^(\d+)d$', str(ttl_str))
    if match:
        return int(match.group(1))
    return 14


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


def _calc_effective_ttl(entry_meta: dict):
    """频率阶梯 TTL"""
    count = entry_meta.get("access_count", 0)
    source = entry_meta.get("source", "llm_inferred")
    if source == "user_stated":
        return None
    base_ttl = 90
    if count == 0:
        return base_ttl
    elif count <= 3:
        return base_ttl * 2
    elif count <= 10:
        return base_ttl * 5
    else:
        return None


def _check_ttl_expiry() -> list:
    """Scan memory_meta.json for entries past their effective TTL.
    Returns list of (hash, meta_dict, age_days, effective_ttl_days)."""
    import json as _json
    from pathlib import Path as _Path
    from datetime import datetime as _dt
    
    meta_path = get_hermes_home() / "state" / "memory_meta.json"
    if not meta_path.exists():
        return []
    
    # 2026-10-17 审查：原实现裸 json.load —— meta 损坏时整个 TTL 任务崩溃。
    # 统一走 _load_meta_file（损坏会留档 .corrupt-* 并返回 None），本轮直接退出。
    meta = _load_meta_file(meta_path)
    if meta is None:
        return []

    entries = meta.get("entries", {})
    # 2026-10-17 审查修复：判据从「meta 里有这条记录」改成「MEMORY.md 里还有这条正文」。
    # 原实现遍历 meta entries 而非 MEMORY.md ⇒ 孤儿记录（replace/remove 留下的旧 hash）
    # 到期时被判 expired，进而写出「content not found」假归档并标 archived。
    live = _live_entry_hashes()
    verified = live is not None
    if not verified:
        # 读不到 MEMORY.md：不能按空集合判定（会把全部记录判成孤儿而静默跳过驱逐），
        # 也不该假装「到期 0 条」。照常产出候选，由调用方报「无法自算墓碑真值」并把
        # 每条计为归档失败（_archive_expired_entry 在文件缺失时返回空串）。
        logger.warning("ttl: MEMORY.md unavailable — entries are unverifiable this round")
        live = set()
    now = _dt.now()
    expired = []
    orphans = 0

    for h, e in entries.items():
        if e.get("archived"):
            continue
        if verified and h not in live:
            # 孤儿（正文已不在 MEMORY.md）：不再从这里 continue —— 让它照常走到期判定，
            # 由 _archive_expired_entry 写**墓碑记录**（元信息 + 明确标记，不伪造正文）。
            # 报表侧用自算 mem_hashes 判 🪦，不会算成「正文已归档」；归档后 meta 标
            # archived，不会再每轮重复。这里只计数用于日志。
            orphans += 1
        eff = _calc_effective_ttl(e)
        if eff is None:
            continue  # permanent
        
        last_str = e.get("last_accessed") or e.get("created_at")
        if not last_str:
            continue
        
        try:
            last_dt = _dt.fromisoformat(last_str)
        except (ValueError, TypeError):
            continue

        # 2026-09-21 fix: memory_meta.json 里两种时间戳格式并存——naive 本地（多数）
        # 与 aware "+08:00"（memory_citations.py 的 backfill 写入）。naive 与 aware 相减
        # 抛 TypeError，让整个 TTL 驱逐任务 exit 1（ttl-eviction-daily 每日失败）。
        # 统一折算成 naive 本地时间再比较。
        if last_dt.tzinfo is not None:
            last_dt = last_dt.astimezone().replace(tzinfo=None)

        age = (now - last_dt).days
        if age > eff:
            expired.append((h, e, age, eff))

    if orphans:
        logger.info("ttl: skipped %d orphan meta record(s) absent from MEMORY.md", orphans)
    return expired


def _cost_ensure_dir():
    try:
        _cost_os.makedirs(_cost_os.path.dirname(_COST_LOG), exist_ok=True)
    except Exception:
        pass


def _cost_flush():
    """Write accumulated cost data to JSONL. Called from atexit."""
    if not _cost_data:
        return
    _cost_ensure_dir()
    try:
        with open(_COST_LOG, "a") as f:
            for sid, data in _cost_data.items():
                f.write(_cost_json.dumps({**data, "session_id": sid}, ensure_ascii=False) + "\n")
    except Exception:
        pass
    _cost_data.clear()


def _cost_incr(session_id: str, key: str, delta: int = 1):
    # Write immediately (not atexit-only) so data survives across long-running gateway
    _cost_ensure_dir()
    try:
        ts = _cost_time.strftime("%Y-%m-%dT%H:%M:%S")
        with open(_COST_LOG, "a") as f:
            _cost_json.dump({"ts": ts, "session_id": session_id, key: delta}, f, ensure_ascii=False)
            f.write("\n")
    except Exception:
        pass


def _cost_record(session_id: str, key: str, value):
    """Write per-session total value to JSONL（每个 key 每 session 只写一次，值为总量）。
    与 _cost_incr 不同：_cost_incr 写增量（每调用写一次 delta），下游 sum() 合并。"""
    _cost_ensure_dir()
    try:
        ts = _cost_time.strftime("%Y-%m-%dT%H:%M:%S")
        with open(_COST_LOG, "a") as f:
            _cost_json.dump({"ts": ts, "session_id": session_id, key: value}, f, ensure_ascii=False)
            f.write("\n")
    except Exception:
        pass


def _increment_read_counts(content: str):
    """2026-08-02 恢复: 对 memory(read) 返回的全部条目递增 access_count。

    基于内容 hash 匹配 memory_meta.json 中的已有条目。8/2 渐进式披露补丁误删，
    导致 TTL 机制(access_count)空转。写入节流：每次 memory(read) 最多写一次
    memory_meta.json（>=1s 间隔）。恢复 7/31 版本实现。
    """
    import json as _json, hashlib as _hashlib, time as _time, os as _os
    from pathlib import Path as _Path

    meta_path = get_hermes_home() / "state" / "memory_meta.json"
    if not meta_path.exists():
        return

    _last_flush = getattr(_increment_read_counts, "_last_flush", 0.0)
    _dirty = getattr(_increment_read_counts, "_dirty_hashes", set())
    now = _time.time()

    # 2026-10-17 审查修复：分隔符走 ENTRY_DELIMITER（原硬编码 "\n§\n"）。
    entries = [e.strip() for e in content.split(ENTRY_DELIMITER) if e.strip()]
    for entry in entries:
        h = _hashlib.sha256(entry.encode()).hexdigest()[:12]
        _dirty.add(h)

    if now - _last_flush < 1.0:
        _increment_read_counts._dirty_hashes = _dirty
        return

    # 2026-10-17（① 同族）：读-改-写同样持锁 —— 原实现虽已 tmp+replace，但无锁 ⇒
    # 会覆盖并发写入方（add/batch）刚写入的条目（lost update）。
    try:
        with _meta_lock_ctx():
            meta = _load_meta_file(meta_path)
            if not meta:
                return          # 读失败/空档 ⇒ 放弃本次刷写，不覆盖（与原语义一致）
            if "entries" not in meta:
                meta["entries"] = {}
            now_iso = __import__("datetime").datetime.now().isoformat()
            for h in _dirty:
                if h in meta["entries"]:
                    entry = meta["entries"][h]
                    entry["access_count"] = entry.get("access_count", 0) + 1
                    entry["last_accessed"] = now_iso
            _write_meta_atomic(meta_path, meta)
    except Exception:
        logger.debug("read-count flush failed", exc_info=True)

    _increment_read_counts._last_flush = now
    _increment_read_counts._dirty_hashes = set()


def _live_entry_hashes():
    """MEMORY.md 当前真实条目的 hash 集合（口径与 meta 记账一致：strip 后 sha256[:12]）。

    返回 ``None`` 表示**不可判定**（文件不存在 / IO 错误）——调用方必须把它当成
    「不知道」，而不是「一条都没有」：当成空集会把全部 meta 判成孤儿（prune 会删光
    账本、TTL 会静默跳过全部驱逐）。只有读到文件且解析成功才返回真实集合。
    """
    mem_path = get_hermes_home() / "memories" / "MEMORY.md"
    try:
        content = mem_path.read_text(encoding="utf-8")
    except (FileNotFoundError, OSError):
        return None
    return {
        hashlib.sha256(e.strip().encode()).hexdigest()[:12]
        for e in content.split(ENTRY_DELIMITER) if e.strip()
    }


def _load_meta_file(meta_path):
    """读 memory_meta.json。

    2026-10-17 审查：损坏时不再返回 ``{}`` —— 调用方随后会拿空 dict 整体 os.replace
    写回，把**全部**既有 created_at/ttl/access_count/archived 记录一次性清空（静默数据
    丢失）。现在：文件不存在 ⇒ ``{}``（正常的首写）；解析失败 ⇒ 先把损坏文件留档
    ``.corrupt-<ts>`` 再返回 ``None``，调用方据此放弃本次写入（fail-closed）。
    """
    import json as _json
    from datetime import datetime as _dt
    from pathlib import Path as _Path
    try:
        with open(meta_path, encoding="utf-8") as f:
            d = _json.load(f)
        return d if isinstance(d, dict) else {}
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as exc:
        try:
            bak = _Path(str(meta_path) + ".corrupt-" + _dt.now().strftime("%Y%m%d_%H%M%S"))
            _Path(meta_path).replace(bak)
            logger.error("memory_meta.json unreadable (%s) — moved to %s; "
                         "skipping this write instead of wiping the ledger", exc, bak)
        except OSError:
            logger.error("memory_meta.json unreadable (%s) — refusing to overwrite it", exc)
        return None




def _merge_meta_entry(meta: dict, content: str, source: str = None) -> bool:
    """把一条 content 合并进 meta（原地改）。False = 被每日 permanent 上限拒绝。

    TTL 口径与 _update_memory_meta_unlocked 完全一致：user_stated→permanent，其余 90d。
    """
    import hashlib
    from datetime import datetime
    entry_hash = hashlib.sha256(content.encode()).hexdigest()[:12]
    meta.setdefault("entries", {})
    src = source if source else "llm_inferred"
    ttl = "permanent" if src == "user_stated" else "90d"
    if ttl == "permanent":
        today = datetime.now().strftime("%Y-%m-%d")
        existing_entry = meta["entries"].get(entry_hash, {})
        is_new_or_upgrade = (existing_entry.get("hash") != entry_hash
                             or existing_entry.get("ttl") != "permanent")
        if is_new_or_upgrade:
            perm_today = sum(1 for v in meta["entries"].values()
                             if v.get("ttl") == "permanent"
                             and v.get("created_at", "")[:10] == today)
            if perm_today >= 10:
                import sys
                print(f"[ALERT] Daily permanent write limit (10) reached. Entry {entry_hash} rejected.", file=sys.stderr)
                return False
    existing = meta["entries"].get(entry_hash, {})
    meta["entries"][entry_hash] = {
        "hash": entry_hash,
        "source": src,
        "ttl": ttl,
        "created_at": existing.get("created_at", datetime.now().isoformat()),
        "access_count": existing.get("access_count", 0),
        "last_accessed": existing.get("last_accessed"),
    }
    return True


def _meta_lock_ctx():
    """memory_meta.json 的 flock 侧车锁（fail-open：拿不到锁就降级为无锁写，绝不阻断记忆写入）。

    2026-10-17（用户批准 · ①）：原实现读-改-写全程无锁、且批量路径 N 条 = N 次
    「整文件读→整文件写」，并发/批量下互相覆盖。锁实现复用 scripts/memory_meta_lock.py
    （它按 HERMES_HOME 定位，沙箱可用）。
    """
    try:
        meta_lock = _import_from_scripts("memory_meta_lock").meta_lock
        return meta_lock(5.0)
    except Exception:
        import contextlib as _ctx
        logger.debug("memory meta lock unavailable, falling back to unlocked write", exc_info=True)
        return _ctx.nullcontext()




def _prune_orphan_meta() -> int:
    """删除 memory_meta.json 里已不在 MEMORY.md 的条目记录。

    2026-10-17 审查（memory-meta-batch-write-gap）根因：replace/remove 只写新条目的
    meta，**从不清理被替换/删除内容的旧 hash** ⇒ 孤儿记录留在 entries 里。后果：
    ① `_check_ttl_expiry` 遍历的是 meta entries 而非 MEMORY.md ⇒ 孤儿到期时被判
    expired，`_archive_expired_entry` 找不到正文仍写出一条
    「(entry content not found in MEMORY.md)」的**假归档**并标 archived；
    ② `_memory_source_stats_hint` 的 total/llm_inferred 计数被孤儿污染（虚高）。
    ③ 同一段内容先删后加回，会沿用旧 created_at ⇒ 刚写入就被判过期、静默丢失。

    只删**非 archived** 的孤儿；archived 是历史审计记录，保留。fail-open。
    """
    live = _live_entry_hashes()
    if live is None:
        return 0
    meta_path = get_hermes_home() / "state" / "memory_meta.json"
    if not meta_path.exists():
        return 0
    try:
        with _meta_lock_ctx():
            meta = _load_meta_file(meta_path)
            if meta is None:
                return 0
            if not isinstance(meta.get("entries"), dict):
                return 0
            stale = [h for h, e in meta["entries"].items()
                     if h not in live and not (isinstance(e, dict) and e.get("archived"))]
            if not stale:
                return 0
            for h in stale:
                del meta["entries"][h]
            _write_meta_atomic(meta_path, meta)
            logger.info("memory meta pruned %d orphan(s): %s", len(stale), ",".join(stale[:6]))
            return len(stale)
    except Exception:
        logger.debug("memory meta prune failed", exc_info=True)
        return 0


def _record_memory_meta(content) -> None:
    """登记一条已落盘的记忆条目到 memory_meta.json（fail-open，绝不阻断写入）。

    口径必须与**落盘**一致：store 落盘前 `.strip()`
    （memory_tool_store.py:380 单条 / :529 批量），故哈希前先 strip —— 否则带首尾空白的
    content 会键错位，条目在 TTL 执法眼里永远「无日期」。2026-10-17 二修。
    """
    if not isinstance(content, str) or not content.strip():
        return
    try:
        _update_memory_meta(content.strip())
    except Exception:
        logger.debug("memory meta update failed", exc_info=True)
    # replace 会从 MEMORY.md 里拿走旧正文，但旧 hash 的 meta 记录没有别的清理路径。
    # 必须在**每次**写后对账一次，否则 A2 场景（replace）仍留孤儿。
    _prune_orphan_meta()


def _record_meta_for_operations(target, operations, result) -> None:
    """批量写入后补记 meta：仅 target=='memory' 且整体 success（apply_batch 全或无语义）。

    2026-10-17（①）：改为一次加锁/一次读/一次原子写（原实现逐条调用 = N 倍读-改-写窗口）。
    """
    if target != "memory" or not isinstance(result, dict) or not result.get("success"):
        return
    contents = [op.get("content") or op.get("new_text")
                for op in (operations or [])
                if isinstance(op, dict) and op.get("action") in ("add", "replace")]
    if contents:
        _update_memory_meta_many(contents)
    # 2026-10-17 审查修复：replace/remove 会把旧内容从 MEMORY.md 里拿走，但旧 hash 的
    # meta 记录没有任何清理路径 ⇒ 孤儿。批量里可能只有 remove（contents 为空），
    # 所以 prune 必须独立于 contents 执行。
    _prune_orphan_meta()


def _semantic_fallback(query: str, full_content: str, target: str):
    """L2 语义兜底：memory(read) 词面 0 命中时调 Flash 编号检索（P048 v4）。

    fail-open：任何失败（import/API/解析）→ None → 调用方返回 matched:0，不阻断 read。
    """
    try:
        # 2026-10-17 审查：路径必须跟随当前 profile（原写死 Path.home()/.hermes，
        # 非默认 profile 下会加载默认 home 的脚本，异常又被吞 ⇒ L2 兜底静默失效）。
        semantic_match = _import_from_scripts("memory_semantic_fallback").semantic_match
        return semantic_match(query, full_content, target)
    except Exception:
        return None


def _update_memory_meta(content: str, source: str = None):
    """单条登记：加锁 + 原子写（2026-10-17 ①）。原实现整体下移为 _unlocked，行为不变。"""
    try:
        with _meta_lock_ctx():
            return _update_memory_meta_unlocked(content, source)
    except Exception:
        logger.debug("memory meta locked write failed, retrying unlocked", exc_info=True)
        try:
            return _update_memory_meta_unlocked(content, source)
        except Exception:
            logger.debug("memory meta update failed", exc_info=True)


def _update_memory_meta_many(contents, source: str = None) -> int:
    """批量登记：一次加锁、一次读、一次原子写（消除 N 倍读-改-写窗口）。返回成功条数。"""
    items = [c.strip() for c in (contents or []) if isinstance(c, str) and c.strip()]
    if not items:
        return 0
    meta_path = get_hermes_home() / "state" / "memory_meta.json"
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    try:
        with _meta_lock_ctx():
            meta = _load_meta_file(meta_path)
            if meta is None:          # 损坏/不可读 ⇒ fail-closed，绝不用空 dict 覆盖账本
                return n
            for c in items:
                if _merge_meta_entry(meta, c, source):
                    n += 1
            _write_meta_atomic(meta_path, meta)
    except Exception:
        logger.debug("memory meta batch update failed", exc_info=True)
    return n


def _update_memory_meta_unlocked(content: str, source: str = None):
    """2026-08-02 恢复: Write entry hash + source + TTL to memory_meta.json.

    Phase 3: Admission Rule — source-based TTL determination.
    - user_stated → permanent
    - llm_inferred → 90d
    - Daily permanent write limit: 10 (攻击面保护)
    8/2 渐进式披露补丁误删此函数，导致写 memory 不再记录 TTL/source 元数据。
    恢复 7/31 版本实现。
    """
    import json, hashlib
    from pathlib import Path
    from datetime import datetime

    meta_path = get_hermes_home() / "state" / "memory_meta.json"
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    # 2026-10-17 审查：原实现在 JSON 损坏时 meta = {} 后整体重写 ⇒ 全账清空。
    # 改用统一 loader：损坏会留档 .corrupt-* 并返回 None ⇒ 本次放弃写入（fail-closed）。
    meta = _load_meta_file(meta_path)
    if meta is None:
        return
    entry_hash = hashlib.sha256(content.encode()).hexdigest()[:12]
    if "entries" not in meta:
        meta["entries"] = {}

    # ── Phase 3: TTL 判定 ──
    src = source if source else "llm_inferred"
    if src == "user_stated":
        ttl = "permanent"
    else:
        ttl = "90d"

    # ── 攻击面保护：每日 permanent 写入上限 10 ──
    if ttl == "permanent":
        today = datetime.now().strftime("%Y-%m-%d")
        existing_entry = meta["entries"].get(entry_hash, {})
        is_new_or_upgrade = (existing_entry.get("hash") != entry_hash
                            or existing_entry.get("ttl") != "permanent")
        if is_new_or_upgrade:
            perm_today = sum(
                1 for v in meta["entries"].values()
                if v.get("ttl") == "permanent"
                and v.get("created_at", "")[:10] == today
            )
            if perm_today >= 10:
                import sys
                print(f"[ALERT] Daily permanent write limit (10) reached. Entry {entry_hash} rejected.", file=sys.stderr)
                return

    # Preserve existing access_count if updating
    existing = meta["entries"].get(entry_hash, {})
    meta["entries"][entry_hash] = {
        "hash": entry_hash,
        "source": src,
        "ttl": ttl,
        "created_at": existing.get("created_at", datetime.now().isoformat()),
        "access_count": existing.get("access_count", 0),
        "last_accessed": existing.get("last_accessed"),
    }
    _write_meta_atomic(meta_path, meta)


def _write_meta_atomic(meta_path, meta) -> None:
    """tmp + os.replace 原子替换（读-改-写窗口里崩溃不会留下半截 JSON）。"""
    import json as _json
    import os as _os
    tmp = str(meta_path) + ".tmp"
    with open(tmp, "w") as f:
        _json.dump(meta, f, indent=2, ensure_ascii=False)
        f.flush()
        _os.fsync(f.fileno())
    _os.replace(tmp, meta_path)






