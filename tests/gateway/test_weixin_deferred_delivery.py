"""推迟投递（deferred）语义单测 —— 2026-09-15。

背景：夜间长时间无人对话 → weixin context_token 过期 → cron 冷推返回 ret=-2
（`prepare failed`）→ 消息被送入**补投队列**（drain 会补发，消息不丢）。
但 job 仍被记为 ``delivery_failed`` ⇒ 心跳哨兵对「实际已送达」的消息持续误报
⇒ **报警失真**（真故障被淹在假警报里）。

本组锁定改动的不变量：
  · 补投以 **active** 落盘 ⇒ 适配器传 deferred 信号 ⇒ cron 层【不】记失败；
  · ★ **反向（最关键）**：入队失败 / 转 suspended / 普通异常 ⇒ 行为**完全不变**，
    仍须记为投递失败 —— 没有这条等于只测了 happy path。

原则来源：``cron/delivery_queue._terminalize_wait_timeout``
—— "a deferral, not a failure: report success so the job is not recorded
``delivery_failed`` for a message the drain will still send."
"""
import hashlib
import json
import time
from unittest.mock import AsyncMock, patch

import pytest

from gateway.platforms import weixin as wx
from gateway.platforms.base import PlatformConfig, SendResult
from gateway.platforms.weixin import WeixinAdapter


def _make_adapter() -> WeixinAdapter:
    return WeixinAdapter(
        PlatformConfig(
            enabled=True,
            token="test-token",
            extra={"account_id": "test-account"},
        )
    )


def _connected_adapter() -> WeixinAdapter:
    """与 tests/gateway/test_weixin.py::TestWeixinChunkDelivery._connected_adapter 同构。"""
    adapter = _make_adapter()
    adapter._session = object()
    adapter._send_session = adapter._session
    adapter._token = "test-token"
    adapter._base_url = "https://weixin.example.com"
    adapter._token_store.get = lambda account_id, chat_id: "ctx-token"
    return adapter


@pytest.fixture
def retry_queue(tmp_path, monkeypatch):
    """把补投队列指到 tmp —— 不碰真实 ~/.hermes。"""
    p = tmp_path / "weixin_retry.jsonl"
    monkeypatch.setattr(wx, "_retry_path", str(p))
    return p


def _mid(to: str, text: str) -> str:
    return hashlib.sha1(f"{to}|{text}".encode()).hexdigest()[:16]


class TestRetryEnqueueReturnContract:
    """_retry_enqueue 必须能区分「必达的推迟」与「可能不达」。"""

    def test_active_enqueue_returns_true(self, retry_queue):
        assert wx._retry_enqueue("u1", "hello", source="cron") is True
        assert json.loads(retry_queue.read_text().strip())["status"] == "active"

    def test_suspended_returns_false(self, retry_queue):
        """attempts 超上限 → 转 suspended（有 24h 转死信风险，不算必达）。"""
        retry_queue.write_text(
            json.dumps({
                "id": _mid("u1", "hello"),
                "ts": time.time(),
                "to": "u1",
                "text": "hello",
                "attempts": wx._RETRY_MAX_ATTEMPTS + 1,
                "source": "cron",
                "status": "active",
            }) + "\n"
        )
        assert wx._retry_enqueue("u1", "hello", source="cron") is False
        assert json.loads(retry_queue.read_text().strip())["status"] == "suspended"

    def test_enqueue_failure_returns_false(self, retry_queue):
        """入队本身失败 ⇒ False（调用方须按【真失败】处理，不得记 ok）。"""
        retry_queue.mkdir()  # 路径变成目录 ⇒ 写入必失败
        assert wx._retry_enqueue("u1", "hello", source="cron") is False


class TestSendDeferredMapping:
    """send() 的异常映射：推迟 → deferred=True；真失败 → deferred=False。"""

    @pytest.mark.asyncio
    async def test_deferred_exception_maps_to_deferred_true(self):
        adapter = _connected_adapter()
        with patch.object(
            WeixinAdapter,
            "_send_text_chunk",
            new=AsyncMock(side_effect=wx._DeferredToRetryQueue("ret=-2 prepare failed")),
        ):
            res = await adapter.send("u1", "hello world")
        assert isinstance(res, SendResult)
        assert res.deferred is True
        assert res.success is False

    @pytest.mark.asyncio
    async def test_plain_exception_is_not_deferred(self):
        """★ 反向：真失败绝不能被升格为 deferred（防 except 分支顺序写错）。"""
        adapter = _connected_adapter()
        with patch.object(
            WeixinAdapter,
            "_send_text_chunk",
            new=AsyncMock(side_effect=RuntimeError("boom")),
        ):
            res = await adapter.send("u1", "hello world")
        assert res.deferred is False
        assert res.success is False


class TestSendResultInvariant:
    def test_deferred_defaults_false(self):
        """不能有「默认即推迟」—— 否则所有失败都会被静音。"""
        assert SendResult(success=False, error="x").deferred is False

    def test_deferred_marker_is_exception_subclass(self):
        assert issubclass(wx._DeferredToRetryQueue, Exception)
