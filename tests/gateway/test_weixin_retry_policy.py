"""补投策略单测 —— 2026-10-26（经 owner 授权，决策 1+3）。

背景：iLink 在用户再次入站前不会 prepare bot 主动发送。对策是「不丢 · 不空转 ·
下次入站按原序一次补投并标注延迟」。本文件锁定该策略的六条不变量：

  T1 response 同样受 3 次上限约束 → 转 suspended（原先豁免 ⇒ 永远占 active 位）
  T2 response suspended 超 24h **不**写死信（对话断链），长期保留
  T3 cron suspended 超 24h → 写死信归档
  T4 入站 drain：按 first_ts 顺序、延迟 >5min 带标注、首条带汇总、成功按 id 出队
  T5 非入站 drain **不**用 30 分钟兜底去叫醒 response（空转禁止）
  T6 补投路径失败不再入队（in_drain），避免「带前缀的新条目」造成重复投递

★ 每条都配了反向断言：写成 happy-path-only 等于没测。
"""
import asyncio
import json
import time

import pytest
from unittest.mock import AsyncMock, patch

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


def _mid(to: str, text: str) -> str:
    import hashlib
    return hashlib.sha1(f"{to}|{text}".encode()).hexdigest()[:16]


@pytest.fixture
def queues(tmp_path, monkeypatch):
    """队列与死信都指到 tmp —— 不碰真实 ~/.hermes。

    2026-10-26 分桶后两个桶都指到同一个文件：本文件测「策略」（上限/挂起/死信/标注），
    单文件语义足够；分桶与零丢失验收见 test_weixin_retry_buckets.py。
    """
    retry = tmp_path / "weixin_retry.jsonl"
    monkeypatch.setattr(wx, "_retry_path", str(retry))
    monkeypatch.setattr(wx, "_retry_response_path", str(retry))
    monkeypatch.setattr(wx, "get_hermes_home", lambda: str(tmp_path))
    (tmp_path / "state").mkdir()
    return retry, tmp_path / "state" / "weixin_retry_dead.jsonl"


def _write(queue_path, items):
    queue_path.write_text(
        "".join(json.dumps(i, ensure_ascii=False) + "\n" for i in items), encoding="utf-8"
    )


def _read(queue_path):
    if not queue_path.exists():
        return []
    return [json.loads(l) for l in queue_path.read_text().splitlines() if l.strip()]


class TestAttemptCap:
    def test_response_gets_suspended_after_cap(self, queues):
        """T1：response 不再豁免上限。"""
        q, _ = queues
        _write(q, [{
            "id": _mid("u1", "hi"), "ts": time.time(), "first_ts": time.time(),
            "to": "u1", "text": "hi", "source": "response", "status": "active",
            "attempts": wx._RETRY_MAX_ATTEMPTS, "no_dead_letter": True,
        }])
        assert wx._retry_enqueue("u1", "hi", source="response") is False
        assert _read(q)[0]["status"] == "suspended"

    def test_cron_still_suspends_after_cap(self, queues):
        """反向：cron 行为不变。"""
        q, _ = queues
        _write(q, [{
            "id": _mid("u1", "hi"), "ts": time.time(), "to": "u1", "text": "hi",
            "source": "cron", "status": "active", "attempts": wx._RETRY_MAX_ATTEMPTS,
        }])
        assert wx._retry_enqueue("u1", "hi", source="cron") is False
        assert _read(q)[0]["status"] == "suspended"


class TestDeadLetterPolicy:
    @pytest.mark.asyncio
    async def test_response_suspended_never_dead_lettered(self, queues):
        """T2：超 24h 的对话回复不进死信；入站唤醒后必须真的重投（不能只保留不投递）。"""
        q, dead = queues
        old = time.time() - (wx._RETRY_SUSPEND_TTL + 3600)
        _write(q, [{
            "id": "r1", "ts": old, "first_ts": old, "to": "u1", "text": "hi",
            "source": "response", "status": "suspended", "suspended_since": old,
            "attempts": 5, "no_dead_letter": True,
        }])
        # 非入站 drain：只保留、不投递（不空转）
        idle_send = AsyncMock(return_value=SendResult(success=True))
        with patch.object(WeixinAdapter, "send", new=idle_send):
            await _adapter()._drain_retry_queue(wake_suspended=False)
        assert not dead.exists(), "对话回复不得进死信"
        assert len(_read(q)) == 1, "非入站 drain 只能保留"
        assert idle_send.await_count == 0, "非入站 drain 不得尝试投递"
        assert idle_send.call_count == 0, "不得调用 send（漏写 await 时 await_count 仍是 0）"

        # 入站唤醒：必须真的重投并出队。原实现超 TTL 后无条件 keep+continue ⇒ 入站也叫不醒，
        # 条目永久占桶却永不送达（2026-10-26 审查 D0.4 命中；只保留不投递 = 回复丢失）。
        sent = []

        async def fake_send(self, chat_id, content, reply_to=None, metadata=None):
            sent.append(content)
            return SendResult(success=True)

        with patch.object(WeixinAdapter, "send", new=fake_send):
            await _adapter()._drain_retry_queue(wake_suspended=True)
        assert sent and sent[0].startswith("# 微信补投（1 条）"), \
            f"入站唤醒必须投递聚合批次，实际 {sent}"
        assert "hi" in sent[0], f"聚合批次必须包含原正文，实际 {sent}"
        assert not dead.exists(), "对话回复不得进死信"
        # 「按 id 出队」由 test_remove_by_id_keeps_siblings 单独钉住：
        # 只有一条记录时，清空整队与按 id 删除的结果相同，这个断言本身分辨不了。
        assert _read(q) == [], "补投成功后必须出队（按 id）"

    @pytest.mark.asyncio
    async def test_cron_suspended_dead_lettered(self, queues):
        """T3：cron 超 24h → 死信归档（内容完整保留），且归档前不该先真去投递。"""
        q, dead = queues
        old = time.time() - (wx._RETRY_SUSPEND_TTL + 3600)
        _write(q, [{
            "id": "c1", "ts": old, "to": "u1", "text": "cron payload",
            "source": "cron", "status": "suspended", "suspended_since": old, "attempts": 5,
        }])
        # 原用例不 mock send：_session=object 时一旦走到投递就 AttributeError/真 I/O（审查命中）
        send = AsyncMock(return_value=SendResult(success=True))
        with patch.object(WeixinAdapter, "send", new=send):
            await _adapter()._drain_retry_queue(wake_suspended=True)
        assert send.await_count == 0, "已超 TTL 的 cron 条目应直接归档，不该先投递"
        assert send.call_count == 0, "不得调用 send（漏写 await 时 await_count 仍是 0）"
        assert dead.exists()
        assert "cron payload" in dead.read_text()
        assert _read(q) == []


@pytest.mark.asyncio
async def test_remove_by_id_keeps_siblings(queues):
    """按 id 出队：只删命中的那条，兄弟条目必须原样留下（防「清空整队」式实现）。"""
    q, _ = queues
    _write(q, [
        {"id": "x1", "ts": time.time(), "to": "u", "text": "one", "attempts": 1,
         "source": "cron", "status": "active"},
        {"id": "x2", "ts": time.time(), "to": "u", "text": "two", "attempts": 1,
         "source": "cron", "status": "active"},
    ])
    wx._retry_remove_by_id("x1")
    left = {i.get("id") for i in _read(q)}
    assert left == {"x2"}, f"按 id 删除后应只剩 x2，实际 {left}"


class TestInboundDrain:
    @pytest.mark.asyncio
    async def test_order_prefix_and_dequeue(self, queues):
        """T4：同一会话聚合成一个批次，正文按 first_ts 升序，成功后按 id 出队。"""
        q, _ = queues
        now = time.time()
        # 保持在 5 分钟窗口内，避免触发额外的「补投完成」控制消息；
        # 本用例只钉住聚合批次的顺序与出队语义。
        _write(q, [
            {"id": "b", "ts": now - 120, "first_ts": now - 120, "to": "u1", "text": "second",
             "source": "response", "status": "active", "attempts": 1, "no_dead_letter": True},
            {"id": "a", "ts": now - 240, "first_ts": now - 240, "to": "u1", "text": "first",
             "source": "response", "status": "active", "attempts": 1, "no_dead_letter": True},
        ])
        sent = []

        async def fake_send(self, chat_id, content, reply_to=None, metadata=None):
            sent.append((content, metadata or {}))
            return SendResult(success=True)

        with patch.object(WeixinAdapter, "send", new=fake_send):
            await _adapter()._drain_retry_queue(wake_suspended=True)

        assert len(sent) == 1, f"同一会话应只发一个聚合批次，实际 {len(sent)} 次"
        body, metadata = sent[0]
        assert body.find("first") < body.find("second"), f"聚合正文顺序不对：{body}"
        assert "_retry_prefix" not in metadata, f"聚合批次不应带旧前缀 metadata：{metadata}"
        assert _read(q) == [], "成功补投后必须按 id 出队"

    @pytest.mark.asyncio
    async def test_short_delay_has_no_annotation(self, queues):
        """反向：延迟 <5min 不在正文附加旧式 [延迟送达] 标注。"""
        q, _ = queues
        now = time.time()
        _write(q, [{"id": "a", "ts": now - 30, "first_ts": now - 30, "to": "u1",
                    "text": "fresh", "source": "response", "status": "active",
                    "attempts": 1, "no_dead_letter": True}])
        sent = []

        async def fake_send(self, chat_id, content, reply_to=None, metadata=None):
            sent.append((content, metadata or {}))
            return SendResult(success=True)

        with patch.object(WeixinAdapter, "send", new=fake_send):
            await _adapter()._drain_retry_queue(wake_suspended=True)
        assert len(sent) == 1, f"单条应只投递一个聚合批次，实际 {len(sent)} 次"
        body, metadata = sent[0]
        assert metadata.get("_in_drain") is True, f"补投批次 metadata 不对：{metadata}"
        assert "_retry_prefix" not in metadata, f"聚合批次不应带旧前缀 metadata：{metadata}"
        assert "fresh" in body, f"聚合批次必须包含原正文，实际 {body}"
        assert "[延迟送达" not in body, f"旧式正文标注不应出现：{body}"

    @pytest.mark.asyncio
    async def test_failure_marks_suspended_without_duplicate(self, queues):
        """T6：补投失败 → attempts+1、不新增条目（in_drain 不再入队）。"""
        q, _ = queues
        now = time.time()
        _write(q, [{"id": "a", "ts": now, "first_ts": now, "to": "u1", "text": "x",
                    "source": "response", "status": "active", "attempts": wx._RETRY_MAX_ATTEMPTS,
                    "no_dead_letter": True}])
        with patch.object(WeixinAdapter, "send",
                          new=AsyncMock(side_effect=RuntimeError("session not ready"))):
            await _adapter()._drain_retry_queue(wake_suspended=True)
        items = _read(q)
        assert len(items) == 1, "不得出现第二条（带前缀的副本）"
        assert items[0]["attempts"] == wx._RETRY_MAX_ATTEMPTS + 1
        assert items[0]["status"] == "suspended"


class TestNoEmptySpin:
    @pytest.mark.asyncio
    async def test_response_suspended_not_woken_by_fallback(self, queues):
        """T5：非入站 drain 不得用 30min 兜底叫醒 response。"""
        q, _ = queues
        since = time.time() - (wx._RETRY_SUSPEND_FALLBACK + 600)  # 已过兜底线
        _write(q, [{"id": "a", "ts": since, "first_ts": since, "to": "u1", "text": "x",
                    "source": "response", "status": "suspended", "suspended_since": since,
                    "attempts": 4, "no_dead_letter": True}])
        send = AsyncMock(return_value=SendResult(success=True))
        with patch.object(WeixinAdapter, "send", new=send):
            await _adapter()._drain_retry_queue(wake_suspended=False)
        assert send.await_count == 0, "response 只能由入站唤醒"
        assert send.call_count == 0, "不得调用 send（漏写 await 时 await_count 仍是 0）"

    @pytest.mark.asyncio
    async def test_cron_suspended_still_fallback_woken(self, queues):
        """反向：cron 的 30min 兜底保留（原行为不能被我改掉）。"""
        q, _ = queues
        since = time.time() - (wx._RETRY_SUSPEND_FALLBACK + 600)
        _write(q, [{"id": "a", "ts": since, "to": "u1", "text": "x", "source": "cron",
                    "status": "suspended", "suspended_since": since, "attempts": 4}])
        send = AsyncMock(return_value=SendResult(success=True))
        with patch.object(WeixinAdapter, "send", new=send):
            await _adapter()._drain_retry_queue(wake_suspended=False)
        # 1 条正文 + 1 条「补投完成」收尾行（收尾行是 2026-10-26 新增）
        assert send.await_count == 2, "cron 兜底唤醒必须保持"


# ── 零丢失（2026-10-27 插队修复）────────────────────────────────────
# 这两条钉的是「静默丢东西」，写成 happy-path 就等于没测：
#   L1 多 chunk 消息在第 k 条失败时，第 k..n 条必须全部进补投队列
#   L2 drain 正在跑时收到的入站唤醒不能被吞

@pytest.mark.asyncio
async def test_tail_not_lost(queues):
    retry, _dead = queues
    a = _adapter()
    chunks = ["A" * 600, "B" * 600, "C" * 600]
    a._split_text = lambda text: list(chunks)      # 绕开短块合并，稳定拿到 3 条
    a.format_message = lambda text: text
    a._acquire_sliding_window_slot = AsyncMock(return_value=True)
    a._send_chunk_delay_seconds = 0
    calls = {"n": 0}

    async def flaky_chunk(**kwargs):
        calls["n"] += 1
        if calls["n"] == 2:                        # 第 2 条失败（已入队后抛 deferred）
            # 真实现的契约：_send_text_chunk 先 _retry_enqueue 再抛 _DeferredToRetryQueue。
            # 桩必须履行这一步，否则测到的只是「桩没入队」，不是发送路径的零丢失保证。
            wx._retry_enqueue(kwargs["chat_id"], kwargs["chunk"], source=kwargs.get("source", "cron"))
            raise wx._DeferredToRetryQueue("chunk 2 deferred")

    a._send_text_chunk = flaky_chunk
    result = await a.send("chat-tail", "\n\n".join(chunks))

    assert result.deferred is True
    ids = {i.get("id") for i in _read(retry)}
    assert _mid("chat-tail", chunks[1]) in ids, "失败 chunk 未入队"
    assert _mid("chat-tail", chunks[2]) in ids, "尾部 chunk 丢了（零丢失回归）"
    # 反向对照：第 1 条已成功送达，不应再进补投队列（否则就是重复投递）
    assert _mid("chat-tail", chunks[0]) not in ids


@pytest.mark.asyncio
async def test_wake_not_swallowed(queues):
    """入站唤醒被并发闸门挡住时必须补跑，而不是等下一次入站（可能几小时后）。

    判据是**行为级**的：补跑那轮必须真把 suspended 条目发出去。
    （先前版本用 `_load_bucket` 调用次数 ≥2 判「补跑启动了」，恒真 —— 第一轮内部
      的 `_retry_remove_by_id` 自己就会调两次，等于没钉住任何东西。）
    """
    retry, _dead = queues
    now = time.time()
    _write(retry, [
        {"id": "a1", "ts": now, "first_ts": now, "to": "chat-live",
         "text": "ACTIVE", "attempts": 1, "source": "cron", "status": "active"},
        {"id": "s1", "ts": now, "first_ts": now, "to": "chat-sleep",
         "text": "SUSPENDED", "attempts": 4, "source": "response",
         "status": "suspended", "suspended_since": now, "no_dead_letter": True},
    ])
    a = _adapter()
    sent = []

    async def slow_send(to, text, metadata=None):
        sent.append(text)
        await asyncio.sleep(0.05)                  # 卡住第一轮 drain 的窗口
        return SendResult(success=True)

    a.send = slow_send
    first = asyncio.create_task(a._drain_retry_queue(wake_suspended=False))
    await asyncio.sleep(0.01)                      # 让第一轮进入并在 send 里挂起

    await a._drain_retry_queue(wake_suspended=True)
    assert getattr(a, "_drain_wake_pending", False) is True, "入站唤醒被吞（未记账）"

    await first
    for _ in range(20):                            # 等补跑那轮把 suspended 发完
        if any("SUSPENDED" in t for t in sent):
            break
        await asyncio.sleep(0.02)

    assert any("SUSPENDED" in t for t in sent), "补跑没发生：suspended 条目没被发出（唤醒丢了）"
    assert getattr(a, "_drain_wake_pending", False) is False, "唤醒记账未被消费"
