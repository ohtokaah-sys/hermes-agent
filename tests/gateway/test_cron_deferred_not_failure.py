"""cron 投递层：deferred 不得被记成 delivery_failed（2026-10-28）。

生产事故（2026-10-27 19:00 · 日常定时推送任务）：
  weixin 会话过期 → 适配器把消息**落进持久补投队列**并返回 `SendResult(deferred=True)`
  → `gateway/delivery.py::_deliver_to_platform` 一律 `raise RuntimeError(error)` ⇒
  deferred 标记丢失 ⇒ `_live_send_text` 走 `except Exception` 分支：
    · target_errors 记 "live adapter send failed: …"
    · 再走 standalone 车道二次入队（同一内容进 cron / response 两个桶）
    · job 终态记 delivery_failed（消息 20:40 已补投送达，账单仍红）

本文件钉两层行为：
  A 推迟（deferred）⇒ live 车道返回 ok=True、**不**记任何 target/delivery error（不走 standalone）；
  B 反向对照：普通失败仍抛 + 记 target_error（没有被静默）。
第二条是防「为了消红把失败一律当推迟」——没有它等于只测了 happy path。
"""
import asyncio
import threading
from types import SimpleNamespace

import pytest

from cron.scheduler_delivery import _TargetDelivery, _live_send_text
from gateway.config import Platform
from gateway.delivery import DeliveryTransport
from gateway.platforms.base import SendResult


class _Adapter:
    """最小适配器：只实现 DeliveryTransport 会调用到的 send()。"""

    splits_long_messages = False

    def __init__(self, result):
        self._result = result

    async def send(self, chat_id, content, metadata=None):
        return self._result


def _target(loop, adapter) -> _TargetDelivery:
    return _TargetDelivery(
        job={"id": "job-deferred-test", "execution_id": "exec-deferred-test"},
        platform=Platform.WEIXIN,
        platform_name="weixin",
        chat_id="chat-1",
        thread_id=None,
        transport=DeliveryTransport(adapter, None, Platform.WEIXIN),
        pconfig=None,
        runtime_adapter=adapter,
        target_adapters={},
        config=SimpleNamespace(filter_silence_narration=True, platforms={}),
        loop=loop,
        notify_delivery=False,
        origin={},
        origin_target=False,
        origin_user_id=None,
        is_dm_target=False,
        mirror_text="",
        mirror_this_target=False,
        in_channel_surface=False,
        inchannel_continuable=False,
        opened_thread_id=None,
    )


@pytest.fixture
def gateway_loop():
    """生产形态：`_live_send_text` 把协程 schedule 到**另一个**循环上再阻塞等结果。

    所以这里必须起一个后台循环线程；在测试自己的循环里同步 future.result() 会自锁。
    """
    loop = asyncio.new_event_loop()
    th = threading.Thread(target=lambda: (asyncio.set_event_loop(loop), loop.run_forever()),
                          daemon=True)
    th.start()
    try:
        yield loop
    finally:
        loop.call_soon_threadsafe(loop.stop)
        th.join(5)
        loop.close()


def test_deferred_is_not_recorded_as_failure(gateway_loop):
    adapter = _Adapter(SendResult(success=False, deferred=True,
                                  error="消息已入补投队列（source=cron），下次入站补发"))
    t = _target(gateway_loop, adapter)
    target_errors: list = []
    delivery_errors: list = []
    unverified: list = []

    ok, timed_out, message_id = _live_send_text(
        t, "hello", None, {"job_id": "job-deferred-test"},
        target_errors=target_errors, delivery_errors=delivery_errors,
        unverified_targets=unverified)

    assert (ok, timed_out, message_id) == (True, False, None)
    assert target_errors == [], f"推迟不该记 target_error：{target_errors}"
    assert delivery_errors == [], f"推迟不该记 delivery_error：{delivery_errors}"
    assert unverified == []


def test_plain_failure_still_raises_and_records(gateway_loop):
    """★ 反向对照：普通失败必须照旧抛 + 记错，否则等于把所有失败静音。"""
    adapter = _Adapter(SendResult(success=False, error="boom"))
    t = _target(gateway_loop, adapter)
    target_errors: list = []

    with pytest.raises(RuntimeError):
        _live_send_text(
            t, "hello", None, {"job_id": "job-deferred-test"},
            target_errors=target_errors, delivery_errors=[],
            unverified_targets=[])

    assert any("live adapter send failed" in e for e in target_errors), target_errors
    assert t.live_error and "boom" in t.live_error
