"""回归测试：memory read query 参数转发 + P048 语义兜底（2026-08-07）。

三层防线：
1. 函数层行为测试：query 过滤 / OR 语义 / 无 query 全量 / L2 兜底调用。
2. 调用链守卫测试：AST 扫描 3 个调用点，断言 query 转发在位（机械防再丢参）。
3. P048（2026-08-07）：AND→OR+评分+Top-5；0 命中走 L2 Flash 兜底（fail-open）。
"""

import ast
import json
from pathlib import Path

import pytest

from tools.memory_tool import MemoryStore, memory_tool


@pytest.fixture()
def store(tmp_path, monkeypatch):
    """Create a MemoryStore with temp storage（与 test_memory_tool.py 同款）。"""
    monkeypatch.setattr("tools.memory_tool.get_memory_dir", lambda: tmp_path)
    s = MemoryStore(memory_char_limit=500, user_char_limit=300)
    s.load_from_disk()
    return s


# =========================================================================
# 函数层：query 过滤行为
# =========================================================================

class TestReadWithQuery:
    def test_query_filters_entries(self, store):
        store.add("memory", "alpha fact about prompt caching")
        store.add("memory", "beta fact about memory routing")
        result = json.loads(memory_tool(
            action="read", query="caching", target="memory", store=store))
        assert result["success"] is True
        assert result["matched"] == 1
        assert "caching" in result["content"]
        assert "beta" not in result["content"]

    def test_query_or_semantics(self, store):
        # P048：AND 全词 → OR+评分。两条目各中一词 → 都返回（行为变更 2026-08-07）
        store.add("memory", "alpha caching fact")
        store.add("memory", "beta routing fact")
        result = json.loads(memory_tool(
            action="read", query="caching routing", target="memory", store=store))
        assert result["success"] is True
        assert result["matched"] == 2
        assert "alpha caching fact" in result["content"]
        assert "beta routing fact" in result["content"]

    def test_or_ranking_top5(self, store):
        # Top-5 截断：6 条各中一词 → 返回 5 条（原文序前 5）
        for i in range(6):
            store.add("memory", f"entry{i} shared token")
        result = json.loads(memory_tool(
            action="read", query="shared", target="memory", store=store))
        assert result["matched"] == 5
        assert "entry0" in result["content"]
        assert "entry5" not in result["content"]

    def test_duplicate_keywords_dedup(self, store):
        # 重复关键词去重（P048 v2 审计发现 9）
        store.add("memory", "alpha caching fact")
        store.add("memory", "beta routing fact")
        r1 = json.loads(memory_tool(
            action="read", query="caching caching", target="memory", store=store))
        r2 = json.loads(memory_tool(
            action="read", query="caching", target="memory", store=store))
        assert r1["matched"] == r2["matched"] == 1

    def test_punctuation_split(self, store):
        # 标点切分（P048 v2 审计发现 3）："caching,routing" 按逗号分词
        store.add("memory", "alpha caching fact")
        store.add("memory", "beta routing fact")
        result = json.loads(memory_tool(
            action="read", query="caching,routing", target="memory", store=store))
        assert result["matched"] == 2

    def test_semantic_fallback_zero_hit(self, store, monkeypatch):
        # L2 兜底：L1 0 命中 → 调 semantic_match（P048 v4）
        store.add("memory", "alpha caching fact")
        calls = {}

        def fake_semantic(query, full_content, target):
            calls["hit"] = True
            assert target == "memory"
            return {"matched": 1, "content": "alpha caching fact"}

        monkeypatch.setattr("tools.memory_tool._semantic_fallback", fake_semantic)
        result = json.loads(memory_tool(
            action="read", query="zzz 不存在词", target="memory", store=store))
        assert calls.get("hit") is True
        assert result["matched"] == 1
        assert "alpha caching fact" in result["content"]

    def test_semantic_fallback_failopen(self, store, monkeypatch):
        # fail-open：兜底抛异常 → matched:0，不阻断（P048 v4）
        store.add("memory", "alpha caching fact")

        def boom(query, full_content, target):
            raise RuntimeError("API down")

        monkeypatch.setattr("tools.memory_tool._semantic_fallback", boom)
        result = json.loads(memory_tool(
            action="read", query="zzz 不存在词", target="memory", store=store))
        assert result["success"] is True
        assert result["matched"] == 0

    def test_l1_hit_skips_l2(self, store, monkeypatch):
        # 成本守卫：L1 命中 → L2 不被调用（P048 v2 审计 F1）
        store.add("memory", "alpha caching fact")
        called = []

        def should_not_call(query, full_content, target):
            called.append(True)
            return None

        monkeypatch.setattr("tools.memory_tool._semantic_fallback", should_not_call)
        result = json.loads(memory_tool(
            action="read", query="caching", target="memory", store=store))
        assert result["matched"] == 1
        assert called == []

    def test_without_query_returns_all(self, store):
        store.add("memory", "alpha fact")
        store.add("memory", "beta fact")
        result = json.loads(memory_tool(action="read", target="memory", store=store))
        assert result["success"] is True
        assert "alpha fact" in result["content"]
        assert "beta fact" in result["content"]


# =========================================================================
# 调用链守卫：3 个调用点必须转发 query（AST 静态检查）
# =========================================================================

REPO = Path(__file__).resolve().parents[2]  # <repo>/tests/tools/.. = repo root

CALL_SITES = {
    "agent/inline_tool_executors.py": "memory",   # 上游重构后 memory 内置工具走 INLINE_TOOL_EXECUTORS
    "tools/memory_tool.py": "memory",  # registry handler
}


class TestQueryForwardingGuard:
    @pytest.mark.parametrize("rel_path", list(CALL_SITES))
    def test_memory_call_forwards_query(self, rel_path):
        """每个调用点转发 query 到 memory_tool。

        上游 v2026.9.7 重构后有两类调用点：
        - 间接：agent/inline_tool_executors.py 的 _memory 经 _call_tool(..., arg_specs)
          传递参数——检查 arg_specs 是否含 ("query", "query")。
        - 直接：tools/memory_tool.py 的 registry handler——检查 memory_tool(query=...)。
        """
        src = (REPO / rel_path).read_text(encoding="utf-8")
        tree = ast.parse(src)

        # 间接调用模式：inline_tool_executors._memory 的 _call_tool arg_specs
        if rel_path == "agent/inline_tool_executors.py":
            # 找到 _memory 函数的 _call_tool(...) 调用，检查其 arg_specs 元组含 ("query", "query")
            memory_fn = next((n for n in ast.walk(tree)
                              if isinstance(n, ast.FunctionDef) and n.name == "_memory"), None)
            assert memory_fn is not None, "inline_tool_executors.py 未找到 _memory 函数"
            # 扫描 _memory 内的所有字符串常量，需含 "query"（arg_specs 里 ("query","query")）
            strs = {n.value for n in ast.walk(memory_fn) if isinstance(n, ast.Constant) and isinstance(n.value, str)}
            assert "query" in strs, "inline_tool_executors._memory 的 arg_specs 未转发 query"
            return

        # 直接调用模式：memory_tool(...) 调用点必须含 query= 关键字
        calls = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                if node.func.id in ("memory_tool", "_memory_tool"):
                    calls.append(node)
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr in ("memory_tool", "_memory_tool"):
                    calls.append(node)

        assert calls, f"{rel_path}: 未找到 memory_tool 调用点（可能被重构——需人工检查）"

        missing = []
        for call in calls:
            kw_names = {kw.arg for kw in call.keywords if kw.arg}
            if "query" not in kw_names:
                missing.append(call.lineno)
        assert not missing, (
            f"{rel_path}: {len(missing)} 个 memory_tool 调用点未转发 query（行 {missing}）"
            "——改函数签名必须同步全部调用点（Pitfall 69）"
        )
