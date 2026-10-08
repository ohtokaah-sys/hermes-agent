"""P048 L2 兜底模块测试：memory_semantic_fallback.py（2026-08-07）。

代码 review 发现 E1：模块 114 行核心逻辑（编号正则解析/索引边界/退避状态机/
头部过滤/20 上限）零测试——7 个旧测试全用 monkeypatch 绕过真实模块。
本文件直测模块本体，不 mock API（用 monkeypatch 替换 urlopen 返回固定响应）。
"""
import json
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path.home() / ".hermes" / "scripts"))
import memory_semantic_fallback as msf


@pytest.fixture(autouse=True)
def reset_backoff():
    """每个测试前重置退避状态（模块级 dict 是共享状态）。"""
    msf._FAIL_BACKOFF.update(consecutive=0, skip_until=0.0)


@pytest.fixture()
def mock_api(monkeypatch):
    """替换 urlopen 返回指定 content（模拟 Flash 响应）。"""
    responses = []

    def fake_urlopen(req, timeout=None):
        payload = json.loads(req.data.decode())
        text = responses.pop(0) if responses else "[]"
        return type("R", (), {"read": lambda self: json.dumps({
            "choices": [{"message": {"content": text}}],
            "usage": {},
        }).encode()})()

    monkeypatch.setattr(msf.urllib.request, "urlopen", fake_urlopen)
    return responses


FULL = "条目零\n§\n条目一 alpha\n§\n条目二 beta\n§\n条目三 gamma"
ENTRIES = ["条目零", "条目一 alpha", "条目二 beta", "条目三 gamma"]


class TestFilterEntries:
    def test_filters_auto_gen_header(self):
        # MEMORY.md 首段 "## Active Context (auto-generated)"（<120 字）被过滤
        content = "## Active Context (auto-generated)\n(no sessions)\n§\n真实条目"
        assert msf._filter_entries(content) == ["真实条目"]

    def test_keeps_long_header(self):
        # 超过 120 字的 ## 段是真实条目，不过滤
        content = "## " + "长" * 150 + "\n§\n条目"
        assert len(msf._filter_entries(content)) == 2


class TestSemanticMatch:
    def test_normal_array(self, mock_api):
        mock_api.append("[1,3]")
        r = msf.semantic_match("q", FULL, "memory")
        assert r == {"matched": 2, "content": "条目一 alpha\n§\n条目三 gamma"}

    def test_space_tolerance(self, mock_api):
        # 编号带空格容错（P048 review E1 指定场景）
        mock_api.append("[1, 2]")
        r = msf.semantic_match("q", FULL, "memory")
        assert r["matched"] == 2

    def test_markdown_fence(self, mock_api):
        # 模型输出带 markdown 围栏 → 只取第一个 [...] 数组
        mock_api.append('```json\n[0,2]\n```')
        r = msf.semantic_match("q", FULL, "memory")
        assert r["matched"] == 2

    def test_duplicate_indices(self, mock_api):
        mock_api.append("[1,1,1]")
        r = msf.semantic_match("q", FULL, "memory")
        assert r["matched"] == 1

    def test_out_of_range_filtered(self, mock_api):
        # 越界/负数编号过滤（P048 review E1）
        mock_api.append("[99,-1,0]")
        r = msf.semantic_match("q", FULL, "memory")
        assert r["matched"] == 1
        assert "条目零" in r["content"]

    def test_cap_at_max_entries(self, mock_api):
        # 上限 = _MAX_ENTRIES（2026-09-17 起由硬编码 20 改为可配，默认 40）
        n = msf._MAX_ENTRIES
        big = "\n§\n".join(f"条目{i}" for i in range(n + 10))
        mock_api.append("[" + ",".join(str(i) for i in range(n + 10)) + "]")
        r = msf.semantic_match("q", big, "memory")
        assert r["matched"] == n

    def test_empty_array_is_success(self, mock_api):
        # 合法空 [] → 返回 None 但不算失败（退避不累加）
        mock_api.append("[]")
        assert msf.semantic_match("q", FULL, "memory") is None
        assert msf._FAIL_BACKOFF["consecutive"] == 0

    def test_garbage_text_counts_backoff(self, mock_api):
        # 垃圾文本（无编号数组）→ 计入退避（P048 review B1）
        mock_api.append("完全不相关的话")
        assert msf.semantic_match("q", FULL, "memory") is None
        assert msf._FAIL_BACKOFF["consecutive"] == 1

    def test_backoff_after_3_fails(self, mock_api):
        # 3 次解析失败 → skip_until 置未来（P048 review B1）
        mock_api.append("垃圾1")
        mock_api.append("垃圾2")
        mock_api.append("垃圾3")
        for _ in range(3):
            msf.semantic_match("q", FULL, "memory")
        assert msf._FAIL_BACKOFF["skip_until"] > time.time()
        assert msf._FAIL_BACKOFF["consecutive"] == 0

    def test_skip_during_backoff(self, mock_api):
        # 退避期间直接返回 None，不调 API
        msf._FAIL_BACKOFF["skip_until"] = time.time() + 999
        mock_api.append("[0]")  # 若被调用会 pop 到
        assert msf.semantic_match("q", FULL, "memory") is None

    def test_target_validation(self, mock_api):
        # target 非 memory/user → None（SOUL 不在 memory_tool 体系）
        assert msf.semantic_match("q", FULL, "soul") is None
