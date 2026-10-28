"""零丢失分桶验收 —— 2026-10-26（经 owner 授权口径：宁可半年后一次补一本小说，也不接受任何一条回复丢失）。

分桶要证明的只有四件事：
  B1 response 桶无上限（旧实现 `lines[-100:]` 会静默截断第 101 条起的积压）
  B2 cron 桶仍有 100 条上限，溢出只丢最旧 cron 且**全文进死信**（不是静默丢）
  B3 坏行移 `.corrupt` 保留；写盘原子（无 .tmp 残留）
  B4 drain 桶优先：response 整段先补，段内按 first_ts 升序；单轮 50 条上限、余量留桶

策略层（上限/挂起/死信/延迟标注）见 test_weixin_retry_policy.py。
"""
import json
import os
import time
from pathlib import Path
from types import SimpleNamespace

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


@pytest.fixture
def q(tmp_path, monkeypatch):
    """两个桶 + 死信全部指到 tmp —— 绝不碰真实 ~/.hermes。"""
    cron = tmp_path / "weixin_retry.jsonl"
    resp = tmp_path / "weixin_retry_response.jsonl"
    monkeypatch.setattr(wx, "_retry_path", str(cron))
    monkeypatch.setattr(wx, "_retry_response_path", str(resp))
    monkeypatch.setattr(wx, "get_hermes_home", lambda: str(tmp_path))
    monkeypatch.setattr(wx, "_RETRY_DRAIN_GAP_SECONDS", 0)  # 测试里不 sleep（50 条 × 1.5s 太慢）
    (tmp_path / "state").mkdir()
    return SimpleNamespace(cron=cron, resp=resp, dead=tmp_path / "state" / "weixin_retry_dead.jsonl")


def _write(path, items):
    path.write_text("".join(json.dumps(i, ensure_ascii=False) + "\n" for i in items), encoding="utf-8")


def _read(path):
    if not path.exists():
        return []
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]


def _mid(to: str, text: str) -> str:
    import hashlib
    return hashlib.sha1(f"{to}|{text}".encode()).hexdigest()[:16]


def _item(mid, text, ts, source="response", status="active", attempts=1, **kw):
    d = {"id": mid, "ts": ts, "first_ts": ts, "to": "u1", "text": text,
         "source": source, "status": status, "attempts": attempts}
    if source == "response":
        d["no_dead_letter"] = True
    d.update(kw)
    return d


class TestBuckets:
    def test_response_bucket_has_no_cap(self, q):
        """B1：120 条 + 新入 1 条 = 121 条，一条不少。"""
        now = time.time()
        _write(q.resp, [_item(f"id{i:03d}", f"m{i}", now - (120 - i)) for i in range(120)])
        assert wx._retry_enqueue("u1", "brand-new", source="response") is True
        items = _read(q.resp)
        assert len(items) == 121, f"response 桶被裁剪了：{len(items)}"
        assert any(i["text"] == "brand-new" for i in items)

    def test_cron_overflow_dead_letters_oldest(self, q):
        """B2：溢出只丢最旧 cron，且**全文**进死信（缺字段 = 无法重放/审计）。"""
        now = time.time()
        _write(q.cron, [_item(f"old{i:03d}", f"c{i}", now - (200 - i), source="cron")
                        for i in range(100)])
        assert wx._retry_enqueue("u1", "newest", source="cron") is True
        items = _read(q.cron)
        assert len(items) == 100
        assert any(i["text"] == "newest" for i in items)
        dead = _read(q.dead)
        assert any(d.get("reason") == "cron-overflow-dropped" for d in dead)
        matches = [d for d in dead if d.get("text") == "c0"]
        assert matches, "死信里找不到 text=c0 的条目（原用 next() 会抛 StopIteration，读不出原因）"
        oldest = matches[0]
        assert oldest.get("reason") == "cron-overflow-dropped", "丢的必须是最旧那条"
        # 只写 reason/text 摘要 = 死信无法重放（审查 2026-10-26 B2 命中），故逐字段钉住
        for field in ("id", "ts", "first_ts", "to", "text", "attempts", "source", "status"):
            assert field in oldest, f"死信缺字段 {field}：无法重放/审计"

    def test_buckets_do_not_mix(self, q):
        wx._retry_enqueue("u1", "r1", source="response")
        wx._retry_enqueue("u1", "c1", source="cron")
        assert [i["text"] for i in _read(q.resp)] == ["r1"]
        assert [i["text"] for i in _read(q.cron)] == ["c1"]


class TestCorruptAndAtomic:
    def test_corrupt_line_quarantined(self, q):
        """B3：坏行进 .corrupt，且下一次落盘后主桶不再残留（否则反复隔离）。"""
        good = json.dumps(_item("ok1", "kept", time.time()))
        q.resp.write_text(good + "\n{this is not json}\n", encoding="utf-8")
        items = wx._load_bucket(str(q.resp))
        assert len(items) == 1 and items[0]["id"] == "ok1"
        corrupt = q.resp.with_name(q.resp.name + ".corrupt")
        assert corrupt.exists() and "not json" in corrupt.read_text()
        # 只复制不删 ⇒ 主桶每次加载都重复隔离（审查 2026-10-26 B3 命中）。
        # 口径：load 只读不动，落盘（drain/enqueue）时坏行自然消失 —— 这里钉住该保证。
        wx._save_bucket(str(q.resp), items)
        assert "{this is not json}" not in q.resp.read_text(), "落盘后主桶不得残留坏行"
        assert "not json" in corrupt.read_text(), "坏行内容必须仍在 .corrupt（不删）"

    def test_save_is_atomic_no_tmp_left(self, q, monkeypatch):
        """B3：写盘原子 —— 正常路径无 .tmp 残留，替换失败时原文件仍完整。"""
        first = [_item("x", "old-content", time.time(), source="cron")]
        wx._save_bucket(str(q.cron), first)
        assert q.cron.exists()
        assert list(q.cron.parent.glob("*.tmp")) == []

        # 原断言只查「没有 .tmp」——把实现改回直写 open(path,'w') 也照样通过（审查僵尸用例）。
        # 真正该钉住的是：替换失败绝不能吃掉旧内容。
        def _boom(src, dst):
            raise OSError("simulated replace failure")

        monkeypatch.setattr(wx.os, "replace", _boom)
        with pytest.raises(OSError):
            wx._save_bucket(str(q.cron), [_item("y", "new-content", time.time(), source="cron")])
        assert [i["text"] for i in _read(q.cron)] == ["old-content"], "替换失败时旧队列必须完好"


class TestDrainOrderAndCap:
    @pytest.mark.asyncio
    async def test_response_bucket_first_then_time_order(self, q):
        """B4：response 整段先补（哪怕 cron 更老），段内按 first_ts 升序；同会话只发一个批次。"""
        now = time.time()
        # 保持在 5 分钟窗口内，避免触发额外的「补投完成」控制消息。
        # cron-oldest 仍是三者中最早的，但桶优先级要求 response 的两条先出现。
        _write(q.resp, [
            _item("r-b", "resp-second", now - 60),
            _item("r-a", "resp-first", now - 120),
        ])
        _write(q.cron, [_item("c-a", "cron-oldest", now - 180, source="cron")])
        sent = []

        async def _send(self, chat_id, content, reply_to=None, metadata=None):
            sent.append((content, metadata or {}))
            return SendResult(success=True)

        with patch.object(WeixinAdapter, "send", new=_send):
            await _adapter()._drain_retry_queue(wake_suspended=True)

        assert len(sent) == 1, f"同一会话应只发一个聚合批次，实际 {len(sent)} 次"
        body = sent[0][0]
        assert body.startswith("# 微信补投（3 条）"), f"批次标题不对：{body}"
        assert body.find("resp-first") < body.find("resp-second") < body.find("cron-oldest"), \
            f"聚合正文顺序不对：{body}"
        assert _read(q.resp) == [] and _read(q.cron) == []

    @pytest.mark.asyncio
    async def test_single_round_cap_keeps_rest(self, q):
        """单轮最多 50 条；同会话聚合成一个批次，剩下的 10 条留在桶里等下次入站。"""
        now = time.time()
        texts = [f"body-{n:03d}" for n in range(60)]
        _write(q.resp, [_item(f"i{n:03d}", texts[n], now - (120 - n)) for n in range(60)])
        sent = []

        async def _send(self, chat_id, content, reply_to=None, metadata=None):
            sent.append((content, metadata or {}))
            return SendResult(success=True)

        with patch.object(WeixinAdapter, "send", new=_send):
            await _adapter()._drain_retry_queue(wake_suspended=True)

        assert len(sent) == 1, f"单轮 50 条应聚合成一个批次，实际 {len(sent)} 次"
        body = sent[0][0]
        assert body.startswith("# 微信补投（50 条）"), f"批次标题不对：{body}"
        # 只包含排序后的前 50 条正文；后 10 条不得进入本批。
        for n in range(50):
            assert texts[n] in body, f"第 {n} 条应在本批次中：{body}"
        for n in range(50, 60):
            assert texts[n] not in body, f"第 {n} 条不应在本批次中：{body}"
        remaining = _read(q.resp)
        assert [item["text"] for item in remaining] == texts[50:], \
            f"剩余条目应为后 10 条，实际 {remaining}"
        assert len(remaining) == 10, f"单轮后应剩 10 条，实际 {len(remaining)}"


class TestScaleAndDurability:
    """文档点名的规模验收：300 条 response 一条不少 / 400 条 cron 溢出只丢 cron / 导出可用。"""

    def test_300_response_survive_and_dedupe(self, q):
        """300 条 response：重新加载仍在、条数不变、重复入队不新增。"""
        now = time.time()
        # id 必须是真 hash（to|text），否则入队的幂等 key 对不上，测不到去重
        _write(q.resp, [_item(_mid("u1", f"m{i}"), f"m{i}", now - 1000 + i) for i in range(300)])
        reloaded = wx._load_bucket(str(q.resp))
        assert len(reloaded) == 300, f"重新加载后只剩 {len(reloaded)} 条"

        first = reloaded[0]
        wx._retry_enqueue(first["to"], first["text"], source="response")
        after = _read(q.resp)
        assert len(after) == 300, "重复入队不得新增条目"
        assert sum(1 for i in after if i["id"] == first["id"]) == 1, "同 id 只能有一条"

        # 再入队一条新的 → 301，且最老的那条还在（没被裁）
        wx._retry_enqueue("u1", "newest-301", source="response")
        final = _read(q.resp)
        assert len(final) == 301
        assert any(i["id"] == first["id"] for i in final), "最老的 response 被裁掉了"

    def test_400_cron_overflow_does_not_touch_response(self, q):
        """400 条 cron + 300 条 response：cron 收到 100，response 一条不动，溢出进死信。"""
        now = time.time()
        _write(q.resp, [_item(f"r{i:03d}", f"m{i}", now - 500 + i) for i in range(300)])
        _write(q.cron, [_item(f"c{i:03d}", f"c{i}", now - 900 + i, source="cron")
                        for i in range(400)])
        assert wx._retry_enqueue("u1", "overflow-probe", source="cron") is True
        assert len(_read(q.cron)) == 100, "cron 桶必须收敛到上限 100"
        assert len(_read(q.resp)) == 300, "response 桶不得受影响"
        dead = _read(q.dead)
        assert len(dead) == 301, f"溢出 {len(dead)} 条应全部进死信（400+1-100）"
        assert all(d.get("reason") == "cron-overflow-dropped" for d in dead)

    @pytest.mark.asyncio
    async def test_drain_sends_300_in_six_rounds(self, q):
        """300 条 response：单轮 50 ⇒ 六轮补完；每轮余量准确、最终一条不剩且不丢。"""
        now = time.time()
        _write(q.resp, [_item(f"r{i:03d}", f"m{i}", now - 1000 + i) for i in range(300)])
        rounds = 0
        while _read(q.resp) and rounds < 10:
            rounds += 1
            send = AsyncMock(return_value=SendResult(success=True))
            with patch.object(WeixinAdapter, "send", new=send):
                await _adapter()._drain_retry_queue(wake_suspended=True)
            assert len(_read(q.resp)) == max(0, 300 - 50 * rounds), f"第 {rounds} 轮余量不对"
        assert rounds == 6, f"应为 6 轮，实际 {rounds}"
        assert _read(q.resp) == [], "补完后桶应为空"
        assert not q.dead.exists(), "response 一条都不该进死信"

    def test_export_script_writes_markdown(self, q, tmp_path, monkeypatch):
        """导出脚本可用：能生成含条数与正文的 md。"""
        import importlib.util
        now = time.time()
        _write(q.resp, [_item("r1", "导出测试正文", now)])
        # 原来硬编码本机用户目录 —— 换机器就 AttributeError，且泄露本地用户目录
        # （审查 2026-10-26 命中）。改为「按 HOME 推导 + 可用环境变量覆盖」，
        # 且不做 stat（home_io_guard 会拒绝探测真实 hermes home）；加载失败即 skip，不裸崩。
        script = Path(os.environ.get("WEIXIN_RETRY_EXPORT_SCRIPT")
                      or (Path.home() / ".hermes" / "scripts" / "weixin-retry-export.py"))
        spec = importlib.util.spec_from_file_location("wxexp", str(script))
        loader = getattr(spec, "loader", None)
        if spec is None or loader is None:
            pytest.skip(f"导出脚本不可加载：{script}")
        mod = importlib.util.module_from_spec(spec)
        try:
            loader.exec_module(mod)
        except OSError as exc:
            pytest.skip(f"导出脚本不可读（{script}）：{exc}")
        monkeypatch.setattr(mod, "STATE", str(tmp_path))
        monkeypatch.setattr(mod, "BUCKETS", {"response": str(q.resp), "cron": str(q.cron)})
        monkeypatch.setattr(mod, "DEAD", str(q.dead))
        out = tmp_path / "export.md"
        monkeypatch.setattr("sys.argv", ["weixin-retry-export.py", "--out", str(out)])
        assert mod.main() == 0
        text = out.read_text(encoding="utf-8")
        assert "response 补投桶（1 条）" in text
        assert "导出测试正文" in text
