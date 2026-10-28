"""微信补投聚合批次 / 控制消息 / 外部清空 / 风暴断路验收。"""
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from gateway.platforms import weixin as wx
from gateway.platforms.base import PlatformConfig, SendResult
from gateway.platforms.weixin import WeixinAdapter


def _adapter() -> WeixinAdapter:
    a = WeixinAdapter(
        PlatformConfig(enabled=True, token="test-token", extra={"account_id": "test-account"})
    )
    a._session = object()
    a._send_session = a._session
    a._token = "test-token"
    a._base_url = "https://weixin.example.com"
    a._token_store.get = lambda account_id, chat_id: "ctx-token"
    return a


@pytest.fixture
def q(tmp_path, monkeypatch):
    cron = tmp_path / "weixin_retry.jsonl"
    resp = tmp_path / "weixin_retry_response.jsonl"
    monkeypatch.setattr(wx, "_retry_path", str(cron))
    monkeypatch.setattr(wx, "_retry_response_path", str(resp))
    monkeypatch.setattr(wx, "get_hermes_home", lambda: str(tmp_path))
    monkeypatch.setattr(wx, "_RETRY_DRAIN_GAP_SECONDS", 0)
    (tmp_path / "state").mkdir()
    return SimpleNamespace(cron=cron, resp=resp)


def _write(path, items):
    path.write_text(
        "".join(json.dumps(i, ensure_ascii=False) + "\n" for i in items),
        encoding="utf-8",
    )


def _read(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _item(mid, text, ts, source="response", attempts=1):
    return {
        "id": mid,
        "ts": ts,
        "first_ts": ts,
        "to": "u1",
        "text": text,
        "source": source,
        "status": "active",
        "attempts": attempts,
        **({"no_dead_letter": True} if source == "response" else {}),
    }


@pytest.mark.asyncio
async def test_completion_failure_never_reenqueues(q, monkeypatch, caplog):
    """T1：收尾控制行失败时走 no_retry，绝不回填 response 桶。"""
    enqueue_spy = AsyncMock(return_value=True)
    monkeypatch.setattr(wx, "_retry_enqueue", enqueue_spy)

    async def fail_send_message(*args, **kwargs):
        raise RuntimeError("completion send failed")

    monkeypatch.setattr(wx, "_send_message", fail_send_message)
    a = _adapter()
    a._send_chunk_retries = 0
    a._send_chunk_retry_delay_seconds = 0

    result = await a.send(
        "u1",
        "[补投完成 · 迟到送达 1 条，失败 0 条（已保留，等下次入站）]",
        metadata={"source": "response", "_no_retry": True},
    )

    assert result.success is False
    assert enqueue_spy.await_count == 0 and enqueue_spy.call_count == 0, \
        "收尾控制行失败不得调用 _retry_enqueue"
    assert _read(q.resp) == [], "response 桶必须保持为空"
    assert any("control message dropped after send failure (no_retry)" in r.message
               for r in caplog.records)


@pytest.mark.asyncio
async def test_three_items_one_chat_one_aggregated_send(q):
    """T2：同会话 3 条只发一次聚合 markdown，且按 id 全部出队。"""
    now = time.time()
    _write(q.resp, [
        _item("r1", "第一条正文", now - 60),
        _item("r2", "第二条正文", now - 40),
        _item("r3", "第三条正文", now - 20),
    ])
    sent = []

    async def fake_send(self, chat_id, content, reply_to=None, metadata=None):
        sent.append((chat_id, content, metadata or {}))
        return SendResult(success=True)

    with patch.object(WeixinAdapter, "send", new=fake_send):
        await _adapter()._drain_retry_queue(wake_suspended=True)

    assert len(sent) == 1, f"同会话应只发一个聚合批次，实际 {len(sent)} 次"
    chat_id, text, metadata = sent[0]
    assert chat_id == "u1"
    assert "微信补投（3 条）" in text
    for body in ("第一条正文", "第二条正文", "第三条正文"):
        assert body in text
    assert metadata.get("_in_drain") is True
    assert _read(q.resp) == []


@pytest.mark.asyncio
async def test_external_clear_is_not_resurrected(q, monkeypatch, caplog):
    """T3：加载后队列被外部截空，失败条目的 keep 快照不得写回。"""
    _write(q.resp, [_item("r1", "等待补投", time.time() - 10)])
    saves = []
    original_save = wx._save_bucket

    def tracking_save(path, items):
        saves.append((path, list(items)))
        original_save(path, items)

    monkeypatch.setattr(wx, "_save_bucket", tracking_save)

    async def truncate_then_fail(self, chat_id, content, reply_to=None, metadata=None):
        q.resp.write_text("", encoding="utf-8")
        raise RuntimeError("batch send failed")

    with patch.object(WeixinAdapter, "send", new=truncate_then_fail):
        await _adapter()._drain_retry_queue(wake_suspended=True)

    assert q.resp.read_text(encoding="utf-8") == "", "外部清空后不得复活旧条目"
    assert not any(path == str(q.resp) for path, _ in saves), "清空后的桶不应再调用 _save_bucket"
    assert any("bucket modified externally; skip save" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_five_batch_failures_pause_chat(q, caplog):
    """T4：同会话 60s 内 5 次批次失败 → 暂停 300s，条目保留且不抛异常。"""
    _write(q.resp, [_item("r1", "失败但仍保留", time.time() - 10)])
    a = _adapter()
    batch_attempts = 0

    async def always_fail(self, chat_id, content, reply_to=None, metadata=None):
        nonlocal batch_attempts
        if (metadata or {}).get("_in_drain"):
            batch_attempts += 1
        raise RuntimeError("session not ready")

    with patch.object(WeixinAdapter, "send", new=always_fail):
        for _ in range(5):
            await a._drain_retry_queue(wake_suspended=True)

    assert batch_attempts == 5
    assert len(_read(q.resp)) == 1, "失败条目必须保留"
    assert a._retry_chat_paused_until.get("u1", 0) > time.time(), "第 5 次失败后应暂停该会话"
    assert any("retry drain paused for chat" in r.message for r in caplog.records)

    # 暂停期内再次 drain：不得再发批次、不得计失败。
    with patch.object(WeixinAdapter, "send", new=always_fail):
        await a._drain_retry_queue(wake_suspended=True)
    assert batch_attempts == 5, "暂停期内不应重试该会话"
    assert len(_read(q.resp)) == 1
