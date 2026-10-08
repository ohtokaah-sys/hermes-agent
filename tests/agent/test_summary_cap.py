"""机械封顶（甲方案，ADR_20260914_上下文摘要机械封顶）回归测试。

零模型、零网络：只调用 ``ContextCompressor._cap_summary_body`` 的纯逻辑。

跑法::

    cd ~/.hermes/hermes-agent && python3 -m pytest tests/agent/test_summary_cap.py -q
"""

import json
import sys
import types

REPO = __file__.rsplit("/tests/", 1)[0]
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from agent import context_compressor as cc  # noqa: E402

BUDGET = int(8000 * cc._CAP_CHARS_PER_TOKEN)  # 13,600


def _stub(**kw):
    base = dict(
        summary_cap_tokens=8000,
        summary_cap_recent_reserve=800,
        summary_cap_dry_run=False,
        _session_id="PYTEST_CAP",
    )
    base.update(kw)
    return types.SimpleNamespace(**base)


def _cap(text, **kw):
    return cc.ContextCompressor._cap_summary_body(_stub(**kw), text, [])


def _guard_corpus():
    return (
        "## 当前任务\n处理上下文成本战役补丁落地。\n"
        "## 关键台账\n"
        "- 路径 /home/ohtok/kaah/分析/上下文成本战役_20260913.md\n"
        "- 决策：甲方案 mg8000 保留率 0.91，封顶 13,600 字符\n"
        "- 文件 ADR_20260914_摘要机械封顶_甲方案.md 已接受\n"
        "- 编号 4ad4bef0222d2013 五轮折叠基线\n"
        "- 未决：观察期 7 天\n"
        "## 填充\n"
    )


def _big_corpus(n_sec=30, n_item=6, pad=520):
    parts = ["## 当前任务\n处理上下文成本战役补丁落地。"]
    for s in range(n_sec):
        parts.append(f"## 标{s}")
        for i in range(n_item):
            parts.append(f"- round{s}_{i}: " + "填" * pad)
    return "\n\n".join(parts)


def _reserve_corpus():
    a = "\n\n".join("x" * 700 for _ in range(20))  # 20 条 × 700 = 14,000 > budget
    return "## A\n" + a + "\n\n## Z\n" + "z" * 700


def test_disabled_is_byte_identical():
    text = _big_corpus()
    assert _cap(text, summary_cap_tokens=0) == text


def test_budget_respected_and_marker_added():
    text = _big_corpus()
    out = _cap(text)
    assert len(out) <= BUDGET * 1.05
    assert cc._CAP_OMIT_MARK in out
    assert out.startswith("## 当前任务")


def test_guard_items_survive():
    out = _cap(_guard_corpus() + "填" * 20000)
    for token in (
        "上下文成本战役_20260913.md",
        "ADR_20260914_摘要机械封顶_甲方案.md",
        "4ad4bef0222d2013",
        "未决：观察期 7 天",
    ):
        assert token in out


def test_dry_run_does_not_change_summary(tmp_path, monkeypatch):
    target = tmp_path / "dryrun.jsonl"
    monkeypatch.setattr(cc, "_CAP_DRYRUN_PATH", str(target))
    text = _big_corpus()
    out = _cap(text, summary_cap_dry_run=True)
    assert out == text
    rows = [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert rows and rows[-1]["would_drop_chars"] > 0
    assert rows[-1]["budget_chars"] == BUDGET


def test_render_headers_unique_and_no_empty_section():
    out = _cap(_big_corpus())
    heads = [ln for ln in out.split("\n") if ln.startswith("## ")]
    assert len(heads) == len(set(heads))
    assert "\n\n\n" not in out


def test_idempotent():
    first = _cap(_big_corpus())
    assert _cap(first) == first


def test_last_section_reserve():
    text = _reserve_corpus()
    assert "z" * 700 in _cap(text, summary_cap_recent_reserve=800)
    assert "z" * 700 not in _cap(text, summary_cap_recent_reserve=0)
