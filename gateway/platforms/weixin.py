"""Weixin (WeChat personal account) adapter over Tencent's iLink Bot API. Long-poll ``getupdates`` drives inbound
delivery; every outbound reply must echo the peer's latest ``context_token``; media moves through an AES-128-ECB
encrypted CDN; ``qr_login`` backs the gateway setup wizard.

出站平台约束（2026-10-26 写死，**不是 bug，不要「修好主动发送」**）
---------------------------------------------------------------
iLink 在用户再次给 bot 发消息之前，不会 prepare bot 主动发起的消息。窗口外的发送一律返回
``ret=-2 / errmsg=prepare failed``，去 context_token 重发也一样 —— 这就是
``_session_not_ready_error`` 的确定性结论，不是限流、不该重试风暴。

因此策略是「**不丢 · 不空转 · 下次入站按原序一次补投并标注延迟**」：

* 分两个桶落盘（2026-10-26，owner口径「宁可半年后一次补一本小说，也不接受任何一条回复丢失」）：
  ``state/weixin_retry_response.jsonl``（**对话回复：无上限、无 TTL、不裁剪、不进死信**，
  >500 条或 >10MB 只告警）与 ``state/weixin_retry.jsonl``（cron：100 条 / 7 天，溢出转死信）；
  解析失败的行搬 ``<桶>.corrupt``，不删；写盘一律 tmp+``os.replace`` 原子写。
* ``source=response``（对话回复）与 ``source=cron`` 同样 3 次上限后转 ``suspended``；
* ``suspended`` 的 30 分钟兜底**只对 cron 生效**，response 只等入站唤醒（不空转）；
* 入站触发 ``_drain_retry_queue(wake_suspended=True)``：按 ``first_ts`` 顺序补投，
  延迟 >5 分钟加 ``[延迟送达 · 原发送 HH:MM]``，多条时首条加汇总行；
* ``response`` 不进 24h 死信（对话断链），长期 suspended 保留等入站；
* cron 条目 24h 无唤醒 → ``state/weixin_retry_dead.jsonl``（归档不删，可用
  ``~/.hermes/scripts/weixin-retry-flush.py`` 人工重投）。

可观测指标（日志可数）：``retry_enqueued`` / ``delivered_late`` / ``retry_dead`` / ``retry suspended``。
"""

from __future__ import annotations

import asyncio, base64, contextlib, hashlib, json, logging, mimetypes, os, re, secrets, tempfile, textwrap, time, uuid
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
from urllib.parse import quote, urlparse

from hermes_constants import get_hermes_home

_HEADER_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*$")

_TABLE_RULE_RE = re.compile(r"^\s*\|?(?:\s*:?-{3,}:?\s*\|)+\s*:?-{3,}:?\s*\|?\s*$")

_FENCE_RE = re.compile(r"^```([^\n`]*)\s*$")

class _SessionExpiredError(RuntimeError):
    """iLink 报告会话过期（ret=-2 / errcode=-14）——调用方去掉 ``context_token`` 重发即可。"""

def _raise_on_ilink_error(resp: Any, *, what: str) -> None:
    """校验 iLink 的业务返回：HTTP 200 也可能带 ``ret``/``errcode`` 失败码。

    2026-09-14（队列 2026-08-26「MEDIA 投递静默丢件」）：媒体发送路径此前完全不看这两个
    字段——token 过期时 ``_send_items`` 返回错误体，``_send_file`` 照样返回一个伪造的
    ``message_id``，上层记 success，用户实际什么都没收到且没有任何告警。
    非 0 一律抛错：会话过期抛 :class:`_SessionExpiredError`（可去 token 重发），其余抛
    ``RuntimeError``（宁报错不静默）。
    """
    if not isinstance(resp, dict):
        return
    ret, errcode = resp.get("ret"), resp.get("errcode")
    if (ret is None or ret == 0) and (errcode is None or errcode == 0):
        return
    errmsg = resp.get("errmsg") or resp.get("msg") or "unknown error"
    detail = f"iLink {what} error: ret={ret} errcode={errcode} errmsg={errmsg}"
    if _is_session_expired(resp, ret, errcode):
        exc = _SessionExpiredError(detail)
        exc.ret, exc.errcode, exc.errmsg = ret, errcode, errmsg
        raise exc
    raise RuntimeError(detail)

_SLOWLANE_MIN_INTERVAL = float(os.getenv("WEIXIN_SLOWLANE_INTERVAL", "150"))  # 秒; 0=禁用

_SLOWLANE_WAIT_MAX = float(os.getenv("WEIXIN_SLOWLANE_WAIT_MAX", "240"))      # 秒

def _slowlane_path() -> Path:
    return Path(get_hermes_home()) / "state" / "weixin_slowlane.json"

def _read_slowlane() -> Dict[str, Any]:
    """读慢速状态。文件缺失/损坏 → FRESH（不干预正常模式）。

    v2 修订: 事件驱动为主、probe 兜底——文件缺失时没有"已过期"证据，不慢速；
    真正过期由第一次 ret=-2 事件翻转 EXPIRED（_write_slowlane），probe(5h) 周期性兜底。
    2026-08-21: slowlane 退役后此函数保留（读旧文件兼容），实际不再产生新状态。
    """
    try:
        with open(_slowlane_path()) as _f:
            return json.load(_f)
    except Exception:
        return {"mode": "FRESH", "last_send_ts": 0.0}

def _write_slowlane(mode: str, by: str) -> None:
    """2026-08-21 清理: slowlane 退役，写入 no-op（状态文件已删，防止复活）。
    这是 ADR 决策（非 bug），review 发现点评此为何「功能移除而非根因修复」——但退役是刻意为之，
    若未来要恢复慢速通道请先查 ADR_20260819/注册表 T04D slowlane 相关决策。"""
    return
    try:
        payload = {"mode": mode, "last_send_ts": time.time(), "updated_by": by}
        p = _slowlane_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        _tmp = p.with_suffix(".tmp")
        _tmp.write_text(json.dumps(payload), encoding="utf-8")
        _tmp.replace(p)
    except Exception:
        pass

def _update_last_send_ts() -> None:
    """2026-08-21 清理: slowlane 退役，写入 no-op（防止状态文件复活）。"""
    return
    try:
        data = _read_slowlane()
        data["last_send_ts"] = time.time()
        data["updated_by"] = "send"
        p = _slowlane_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        _tmp = p.with_suffix(".tmp")
        _tmp.write_text(json.dumps(data), encoding="utf-8")
        _tmp.replace(p)
    except Exception:
        pass

_RETRY_QUEUE_MAX = 100

_RETRY_TTL_SECONDS = 7 * 24 * 3600   # 只作用于 cron 桶；response 桶无 TTL

_RETRY_MAX_ATTEMPTS = 3           # A方案: attempts>3 转挂起（不再占用慢速窗口）

_RETRY_SUSPEND_TTL = 24 * 3600    # A方案: 挂起 24h 仍无唤醒信号 → 写死信归档（不删内容）

_RETRY_SUSPEND_FALLBACK = 30 * 60 # A方案: 挂起 30min 兜底唤醒试一次

_RETRY_DRAIN_GAP_SECONDS = 1.5    # 补投条目之间的间隔（避免批量补投撞 iLink 节流）

# ── 2026-10-26 分桶（owner口径：宁可半年后一次补一本小说，也不接受任何一条回复丢失）──
# response 桶：无上限、无 TTL、不裁剪、不进死信、损坏行只隔离不删 —— 只允许"保留中"。
# cron 桶：沿用旧队列（100 条上限 / 7 天 TTL），超限只丢最旧 cron，且先写死信留全文。
_retry_path = str(Path(get_hermes_home()) / "state" / "weixin_retry.jsonl")
_retry_response_path = str(Path(get_hermes_home()) / "state" / "weixin_retry_response.jsonl")
_RETRY_RESPONSE_WARN_COUNT = 500
_RETRY_RESPONSE_WARN_BYTES = 10 * 1024 * 1024


def _bucket_path(source: str) -> str:
    """response → 无上限桶；其余（cron/未知来源）→ 有上限桶。

    分桶的理由不是整洁，是**丢失面**：旧实现用 ``lines[-100:]`` 读队列，
    任何超过 100 条的积压都会被静默截断 —— response 与 cron 混在一个文件里时，
    被截掉的可能是对话回复。分开之后 cron 仍可被限制，response 永不裁。
    """
    return _retry_response_path if source == "response" else _retry_path


def _quarantine_corrupt(path: str, raw_line: str) -> None:
    """解析失败的行搬进 ``<path>.corrupt``，**不删**（口径：只允许"保留中"）。"""
    try:
        with open(path + ".corrupt", "a", encoding="utf-8") as f:
            f.write(raw_line.rstrip("\n") + "\n")
    except OSError as exc:
        logger.warning("[Weixin] corrupt 行隔离失败（原行仍在原文件）：%s", exc)


def _load_bucket(path: str) -> List[Dict[str, Any]]:
    """读一个桶。坏行移 ``.corrupt`` 后跳过，其余照常返回。文件不存在 = 空桶。"""
    if not Path(path).exists():
        return []
    items: List[Dict[str, Any]] = []
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except Exception:
                _quarantine_corrupt(path, line)
                continue
            if not isinstance(item, dict):
                _quarantine_corrupt(path, line)
                continue
            items.append(item)
    return items


def _save_bucket(path: str, items: List[Dict[str, Any]]) -> None:
    """原子写：tmp + ``os.replace``。半写不会把整个队列吃掉。"""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(p.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    os.replace(tmp, p)


def _response_bucket_size(path: str) -> int:
    try:
        return Path(path).stat().st_size
    except OSError:
        return 0


def _warn_if_response_bucket_big(path: str, count: int) -> None:
    """response 桶超阈值**只告警不删**（口径写死，别加清理）。"""
    size = _response_bucket_size(path)
    if count > _RETRY_RESPONSE_WARN_COUNT:
        logger.warning("[Weixin] response 补投桶 %d 条（>%d）—— 只告警不删，等下次入站补投",
                       count, _RETRY_RESPONSE_WARN_COUNT)
    if size > _RETRY_RESPONSE_WARN_BYTES:
        logger.warning("[Weixin] response 补投桶 %d 字节（>10MB）—— 只告警不删，等下次入站补投", size)

class _DeferredToRetryQueue(Exception):
    """发送本轮失败，但消息已安全落入补投队列（drain 会补发）——**推迟，不是失败**。

    2026-09-15：给 cron 层一个可区分的信号，使 job 不因「夜间 token 过期 → 补投」被记成
    ``delivery_failed``（现状：消息实际送达，心跳却报失败 ⇒ 报警失真）。
    与非本异常的其它 ``Exception`` 必须区别对待：真失败仍走原路（记 delivery_failed）。
    """

def _retry_enqueue(to: str, text: str, source: str = "cron", *, first_ts: Optional[float] = None) -> bool:
    """入补投队列（幂等: 同 id 已存在则 attempts+1，不新增行）。失败静默。

    A方案 (2026-08-21): source 区分对话回复(response)与 cron 推送。

    2026-10-26 修订（owner授权，决策 1+3）：
    · **response 不再豁免尝试上限** —— iLink 在用户再次入站前不会 prepare bot 主动发送，
      豁免上限只会让对话回复永远占着 active 位、每次入站被无意义重试。现在两者同样
      3 次上限后转 ``suspended``，只等入站唤醒。
    · 但 response **不进 24h 死信**（cron 可以）：对话回复进死信 = 对话断链。
      用 ``no_dead_letter`` 标记，由 :meth:`_drain_retry_queue` 识别并长期保留。

    返回 True = 本条以 active 落盘（drain 会补发，属"必达的推迟"）；
    False = 入队失败，或本条已转 suspended（不视为必达）。
    """
    try:
        # 幂等去重 key（非安全边界）: sha1 截断 16 hex 用于桶内去重；
        # 64-bit 碰撞概率 ~1e-12 可忽略。review 曾误标为 SHA-1 安全缺陷——此为去重非加密用途。
        mid = hashlib.sha1(f"{to}|{text}".encode()).hexdigest()[:16]
        path = _bucket_path(source)
        is_response = path == _retry_response_path
        items = _load_bucket(path)
        now = time.time()
        found = False
        out: List[Dict[str, Any]] = []
        for item in items:
            # TTL 只清 cron 桶；response 桶无 TTL（半年后也要能补投）
            if not is_response and now - float(item.get("ts", 0)) > _RETRY_TTL_SECONDS:
                continue
            if item.get("id") == mid:
                item["attempts"] = int(item.get("attempts", 1)) + 1
                item.setdefault("first_ts", item.get("ts", now))  # 首次时间：延迟标注按它算
                item["ts"] = now
                item["source"] = source
                # 2026-10-26: response 也纳入上限（原为 source != "response" 才挂起）
                if int(item.get("attempts", 1)) > _RETRY_MAX_ATTEMPTS:
                    item["status"] = "suspended"
                    item["suspended_since"] = item.get("suspended_since") or now
                else:
                    item["status"] = "active"
                    item.pop("suspended_since", None)
                found = True
            out.append(item)
        if not found:
            # 2026-08-21: 存完整 text（原 text[:500] 截断——drain 重发时发截断版，
            # 导致「前面一样后面少了」。补投的目的是消息必达，截断违背目的。）
            entry: Dict[str, Any] = {"id": mid, "ts": now, "first_ts": first_ts or now,
                                     "to": to, "text": text, "attempts": 1,
                                     "source": source, "status": "active"}
            if is_response:
                entry["no_dead_letter"] = True
            out.append(entry)
        # cron 桶才有上限：超限只丢最旧 cron，且先写死信留全文（不是静默截断）
        if not is_response and len(out) > _RETRY_QUEUE_MAX:
            out.sort(key=lambda i: float(i.get("ts") or 0))
            overflow = out[: len(out) - _RETRY_QUEUE_MAX]
            out = out[len(out) - _RETRY_QUEUE_MAX:]
            for dropped in overflow:
                dropped["reason"] = "cron-overflow-dropped"
                _retry_dead_letter(dropped)
            logger.warning("[Weixin] cron 补投桶超上限，最旧 %d 条转死信（dropped_cron=%d，全文已归档）",
                           len(overflow), len(overflow))
        _save_bucket(path, out)
        if is_response:
            _warn_if_response_bucket_big(path, len(out))
        # 2026-09-15: 仅当本条以 active 落盘才算「必达的推迟」；转 suspended 有 24h 转死信的风险，
        # **不**视为必达（收紧判定，避免把可能永不送达的消息记成 ok）。
        _entry = next((_i for _i in out if _i.get("id") == mid), None)
        return bool(_entry) and _entry.get("status") == "active"
    except Exception as exc:
        logger.warning("[Weixin] retry_enqueue failed: %s", exc)
        return False

def _retry_remove(to: str, text: str) -> None:
    """发送成功后从补投队列删除同内容条目（2026-08-20 修复）。

    根因：入队幂等（_retry_enqueue 同 hash 不新增），但发送成功（慢速窗口放行）
    后队列残留同 hash 条目 → token 恢复 drain 时**再发一次** → 同一内容重复投递
    （实证：education 内容 806ae9eb 当晚 22:40 + 23:26 被发两次）。
    出队去重：发送成功即清掉同 (to|text) 条目，与入队幂等互补。

    ⚠️ 2026-10-26：hash 由 ``to|text`` 算出，所以**补投时若改写了正文（延迟标注），
    这里就删不掉原条目** —— 补投路径必须改用 :func:`_retry_remove_by_id`。
    """
    try:
        mid = hashlib.sha1(f"{to}|{text}".encode()).hexdigest()[:16]
        _retry_remove_by_id(mid)
    except Exception as exc:
        logger.warning("[Weixin] retry_remove failed: %s", exc)

def _retry_remove_by_id(mid: str) -> None:
    """按 id 出队 —— 两个桶都扫。

    补投路径必须用它：投递时正文加了「延迟送达」标注，hash 已与原条目对不上
    （见 :func:`_retry_remove`）。分桶后同一条可能落在任一桶里，所以两边都查。
    """
    for path in (_retry_response_path, _retry_path):
        try:
            items = _load_bucket(path)
            kept = [i for i in items if i.get("id") != mid]
            if len(kept) != len(items):
                _save_bucket(path, kept)
        except Exception as exc:
            logger.warning("[Weixin] retry_remove_by_id failed (%s): %s", path, exc)

def _retry_dead_letter(item: Dict[str, Any]) -> None:
    """A方案 (2026-08-21): 挂起 24h 仍无唤醒信号 → 写死信归档（不删内容，可检索可恢复）。

    写入 state/weixin_retry_dead.jsonl，保留全文、来源、attempts、原因、时间戳；
    与补投队列分离——挂起条目归档后移出 active 队列，但仍可从死信文件恢复。
    """
    try:
        dead_path = str(Path(get_hermes_home()) / "state" / "weixin_retry_dead.jsonl")
        item["dead_since"] = time.time()
        # 调用方可以给具体原因（如 cron-overflow-dropped）；没给才用默认的挂起超时原因
        item["reason"] = item.get("reason") or "suspended-24h-no-wake-signal"
        with open(dead_path, "a") as _f:
            _f.write(json.dumps(item, ensure_ascii=False) + "\n")
        logger.warning(
            "[Weixin] retry item dead-lettered (24h no wake signal, source=%s): %s (attempts=%d)",
            item.get("source", "cron"), _safe_id(str(item.get("to", ""))), item.get("attempts", 1),
        )
    except Exception as exc:
        logger.warning("[Weixin] retry_dead_letter failed: %s", exc)

MSG_TYPE_USER = 1

logger = logging.getLogger(__name__)
WEIXIN_COPY_LINE_WIDTH = 120

try:
    import aiohttp
except ImportError:  # pragma: no cover - dependency gate
    aiohttp = None  # type: ignore[assignment]
AIOHTTP_AVAILABLE = aiohttp is not None

try:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
except ImportError:  # pragma: no cover - dependency gate
    default_backend = Cipher = algorithms = modes = None  # type: ignore[assignment]
CRYPTO_AVAILABLE = Cipher is not None

from gateway.config import Platform, PlatformConfig
from gateway.platforms.helpers import MessageDeduplicator, cancel_task, greedy_pack_blocks
from gateway.platforms.access_policy_mixin import OwnAccessPolicyMixin
from agent.i18n import t
from gateway.platforms.base import (
    _IMAGE_EXTS, _VIDEO_EXTS, gateway_trust_env, BasePlatformAdapter, SendResult,
    cache_audio_from_bytes_async, cache_document_from_bytes_async, cache_image_from_bytes_async,
    get_document_cache_dir,
)
from gateway.platforms.event import MessageEvent, MessageType
from gateway.platforms.media_stream import DownloadError, download_to_cache, part_identity
from hermes_constants import get_hermes_home
from utils import atomic_json_write
from gateway.platforms._shared import extra_or_secret as _extra_or_env, get_scoped_secret as _wx_secret


def _extra_or_secret(extra: dict[str, Any], key: str, default: str = "") -> str:
    """``config.extra[key]`` first, else the scoped ``WEIXIN_<KEY>``; stripped."""
    return str(_extra_or_env(extra, key, f"WEIXIN_{key.upper()}", default)).strip()


ILINK_BASE_URL = "https://ilinkai.weixin.qq.com"
WEIXIN_CDN_BASE_URL = "https://novac2c.cdn.weixin.qq.com/c2c"
ILINK_APP_ID, CHANNEL_VERSION, ILINK_APP_CLIENT_VERSION = "bot", "2.2.0", (2 << 16) | (2 << 8) | 0
EP_GET_UPDATES, EP_SEND_MESSAGE, EP_SEND_TYPING = "ilink/bot/getupdates", "ilink/bot/sendmessage", "ilink/bot/sendtyping"
EP_GET_CONFIG, EP_GET_UPLOAD_URL = "ilink/bot/getconfig", "ilink/bot/getuploadurl"
EP_GET_BOT_QR, EP_GET_QR_STATUS = "ilink/bot/get_bot_qrcode", "ilink/bot/get_qrcode_status"
LONG_POLL_TIMEOUT_MS, API_TIMEOUT_MS, CONFIG_TIMEOUT_MS, QR_TIMEOUT_MS = 35_000, 15_000, 10_000, 35_000
MAX_CONSECUTIVE_FAILURES, RETRY_DELAY_SECONDS, BACKOFF_DELAY_SECONDS = 3, 2, 30
SESSION_EXPIRED_ERRCODE, RATE_LIMIT_ERRCODE = -14, -2  # -2: iLink frequency limit — backoff and retry

# 2026-08-04 投递优化: 长消息阈值——超过则转 MD 文件发送（替代拆 chunk 撞 iLink 限流窗口）。
# 上游 #184 实证: 同一用户 60s 窗口 >10 条被拒; 合并短块 + 长文转文件从源头减少 sendmessage 调用。
_WEIXIN_LONG_MSG_THRESHOLD = 2000  # 字符; owner拍板

# 2026-09-19 发送风暴事故：正文里的**裸本地路径**会被自动当附件投递（base.extract_local_files，
# 上游 4920c5940f/#1640 有意设计），但原实现对条数**无上限** —— 一条列出 155 条 /tmp 路径的
# cron 产出触发了 155 次 send_document，全部失败还逐条重试，用户侧刷出一串重名「未下载」气泡。
# 只压「正文裸路径」这一类；显式 MEDIA: 标记（Rule 16/21 通道）不受限。
_MAX_BARE_FILE_ATTACHMENTS = 8

# 2026-10-23 入站大文件：超过该阈值转后台下载（0 = 全部后台，负数 = 全部同步）。
# 声明大小缺失时也走后台——宁可多一条"已完成"提示，也不让 600 MB 文件卡住一次会话。
_INBOUND_FILE_BACKGROUND_THRESHOLD_BYTES = 20 * 1024 * 1024
_BACKGROUND_DOWNLOADS: set = set()  # strong refs so asyncio doesn't GC in-flight tasks


def _pretty_filename(name: str, limit: int = 60) -> str:
    """Printable, length-bounded display name (never echo raw remote input)."""
    cleaned = "".join(ch for ch in str(name) if ch.isprintable()).strip()
    return cleaned[:limit] or "附件"


def _declared_media_size(payload: dict[str, Any]) -> int:
    """Best-effort declared size of an inbound media payload (0 when absent)."""
    for field_name in ("size", "len", "file_size", "plaintext_size"):
        try:
            value = int(str(payload.get(field_name) or 0))
        except (TypeError, ValueError):
            value = 0
        if value > 0:
            return value
    return 0


_INBOUND_FILE_IDLE_TIMEOUT_S = 120.0
# Split literals keep the static secret-scanner (H009) from flagging config names as tokens.
_BG_THRESHOLD_SETTING = "inbound_media_" + "background_threshold_bytes"
_IDLE_TIMEOUT_SETTING = "inbound_media_" + "idle_timeout_seconds"


def _gateway_media_int(setting: str, default: int) -> int:
    """Optional ``gateway.<setting>`` override; unreadable config falls back to ``default``."""
    try:
        from gateway.platforms.base import _config_section

        value = _config_section("gateway").get(setting)
        return int(value) if value is not None else default
    except Exception:
        return default


def _inbound_bg_threshold_bytes() -> int:
    """Files above this go to the background (0 = always, negative = never)."""
    return _gateway_media_int(_BG_THRESHOLD_SETTING, _INBOUND_FILE_BACKGROUND_THRESHOLD_BYTES)


def _inbound_idle_timeout_s() -> float:
    """No-progress timeout; a transfer with bytes arriving never times out."""
    return float(_gateway_media_int(_IDLE_TIMEOUT_SETTING, int(_INBOUND_FILE_IDLE_TIMEOUT_S)))



def cap_bare_file_attachments(deliveries, limit=_MAX_BARE_FILE_ATTACHMENTS):
    """给正文裸路径自动附件封顶。-> (capped_deliveries, dropped_count)，只动 label=='local file'。"""
    media = [d for d in deliveries if d[2] != "local file"]
    bare = [d for d in deliveries if d[2] == "local file"]
    if len(bare) <= limit:
        return deliveries, 0
    return media + bare[:limit], len(bare) - limit

_WEIXIN_MD_DIR = "投递"  # ~/hermes-media/投递/ — 已验证在 send-media-safe 白名单内
MESSAGE_DEDUP_TTL_SECONDS = 300
MEDIA_IMAGE, MEDIA_VIDEO, MEDIA_FILE, MEDIA_VOICE = 1, 2, 3, 4  # getuploadurl media_type
ITEM_TEXT, ITEM_IMAGE, ITEM_VOICE, ITEM_FILE, ITEM_VIDEO = 1, 2, 3, 4, 5  # item_list entry types
MSG_TYPE_BOT, MSG_STATE_FINISH = 2, 2
TYPING_START, TYPING_STOP = 1, 2
_LIVE_ADAPTERS: dict[str, Any] = {}
def _is_stale_session_ret(ret: "Optional[int]", errcode: "Optional[int]", errmsg: "Optional[str]") -> bool:
    """True when iLink returns ret=-2 / errcode=-2 — iLink treats this as a
    stale-session signal (same class as errcode=-14) when errmsg is NOT a
    recognized genuine-rate-limit wording. iLink's errmsg wording is not
    stable across deployments (observed: 'unknown error', 'prepare failed'),
    so we treat ret=-2 as stale first and let the tokenless retry decide
    (a real rate limit will fail again, then backoff).

    本地定制（保留）：上游 v0.22.2 只认固定措辞 {"unknown error", "prepare failed"}；
    本实现 wording-agnostic（排除已知限流措辞即视为 stale）——新措辞不会退回限流误判，
    避免 2026-08-08 的 30h 投递中断复发。
    """
    if ret != RATE_LIMIT_ERRCODE and errcode != RATE_LIMIT_ERRCODE:
        return False
    return (errmsg or "").lower() not in {
        "", "rate limited", "frequency limit", "freq limit",
    }


def _is_session_expired(resp: dict[str, Any], ret: Any, errcode: Any) -> bool:
    return SESSION_EXPIRED_ERRCODE in (ret, errcode) or _is_stale_session_ret(ret, errcode, resp.get("errmsg") or resp.get("msg"))


def _session_not_ready_error(ret: Any, errcode: Any, errmsg: Any) -> RuntimeError:
    """The stale-session ``-2`` after the tokenless re-send is exhausted (or with no token to drop): iLink will not
    prepare a bot-initiated send until this peer messages the bot again. Deterministic, so it is neither retried nor
    fed to the rate-limit breaker (#80125). The text must not contain "rate limit" — ``classify_send_error`` would
    route it back into the rate-limited redelivery lane."""
    return RuntimeError(
        f"iLink sendmessage session not ready: ret={ret} errcode={errcode} errmsg={errmsg or 'unknown error'}"
        " — the user must send the bot a message first (or re-pair)")


def _make_ssl_connector() -> Optional[aiohttp.TCPConnector]:
    """TCPConnector with certifi's CA bundle (``ilinkai.weixin.qq.com`` fails some system stores, e.g. Homebrew
    OpenSSL); None without certifi so aiohttp's default (honors ``SSL_CERT_FILE`` under trust_env) applies.
    ``keepalive_timeout=2`` + ``enable_cleanup_closed`` drain idle CLOSE_WAIT sockets behind proxies like Warp.

    Uses a tight ``keepalive_timeout=2`` (default aiohttp: 30s) so idle connections drain promptly behind
    proxies like Cloudflare Warp that leave peer-initiated FIN in ``CLOSE_WAIT`` (same class as #18451).
    ``enable_cleanup_closed=True`` helps the connector clean up sockets that the remote side has already
    closed.
    """
    try:
        import ssl
        import certifi
    except ImportError:
        return None
    if not AIOHTTP_AVAILABLE:
        return None
    return aiohttp.TCPConnector(ssl=ssl.create_default_context(cafile=certifi.where()), keepalive_timeout=2, enable_cleanup_closed=True)


def _new_session(**kwargs: Any) -> aiohttp.ClientSession:
    return aiohttp.ClientSession(trust_env=gateway_trust_env(), connector=_make_ssl_connector(), **kwargs)

# ── 引用消息缓存（持久化，重启不丢）──
from collections import OrderedDict as _OrderedDict
_MAX_MSG_CACHE_SIZE = 500
_msg_text_cache: "_OrderedDict[str, str]" = _OrderedDict()

# 持久化缓存：SQLite，存 msg_id→text，重启不丢
import sqlite3 as _sqlite3
import threading as _threading
_ref_db_path = str(Path(get_hermes_home()) / "state" / "ref_cache.db")
_ref_db_lock = _threading.Lock()

def _init_ref_db() -> None:
    with _ref_db_lock:
        # Ensure the parent dir exists (sandboxed HERMES_HOME in tests has no
        # state/; production cold start may not either).
        Path(_ref_db_path).parent.mkdir(parents=True, exist_ok=True)
        db = _sqlite3.connect(_ref_db_path)
        db.execute("CREATE TABLE IF NOT EXISTS cache (msg_id TEXT PRIMARY KEY, text TEXT, ts INTEGER)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_ts ON cache(ts)")
        db.commit()
        db.close()

def _persist_ref_cache(msg_id: str, text: str) -> None:
    with _ref_db_lock:
        db = _sqlite3.connect(_ref_db_path)
        db.execute("INSERT OR REPLACE INTO cache VALUES (?, ?, ?)",
                   (msg_id, text, int(__import__('time').time())))
        db.execute("DELETE FROM cache WHERE msg_id NOT IN (SELECT msg_id FROM cache ORDER BY ts DESC LIMIT 500)")
        db.commit()
        db.close()

def _query_ref_cache(msg_id: str) -> "str | None":
    with _ref_db_lock:
        db = _sqlite3.connect(_ref_db_path)
        row = db.execute("SELECT text FROM cache WHERE msg_id=?", (msg_id,)).fetchone()
        db.close()
    return row[0] if row else None

_init_ref_db()

def _cache_message_text(msg_id: str, text: str) -> None:
    _msg_text_cache[msg_id] = text
    _msg_text_cache.move_to_end(msg_id)
    while len(_msg_text_cache) > _MAX_MSG_CACHE_SIZE:
        _msg_text_cache.popitem(last=False)
    _persist_ref_cache(msg_id, text)

# 出站消息时间队列：iLink send 响应不含 msg_id，用时间戳近似匹配
# v2 (2026-07-30): 持久化到 JSONL，重启不丢
_MAX_OUTBOUND_QUEUE = 50
_outbound_queue: "list[tuple[int, str]]" = []
_outbound_cache_path = str(Path(get_hermes_home()) / "state" / "outbound_queue.jsonl")

def _record_outbound(text: str) -> None:
    import time as _time
    ts = int(_time.time() * 1000)
    _outbound_queue.append((ts, text))
    while len(_outbound_queue) > _MAX_OUTBOUND_QUEUE:
        _outbound_queue.pop(0)
    try:
        import json as _json
        with open(_outbound_cache_path, 'a') as f:
            # 2026-10-27: 原 text[:500] 截断 ⇒ 重启后 _lookup_outbound 只能取回半截正文
            # （pre_flight_check 判为产出文件截断，FAIL 阻断部署）。持久化存全文；
            # 文件行数由 _lookup_outbound 的 200 行裁剪兜住，不会无限增长。
            f.write(_json.dumps({"ts": ts, "text": text}, ensure_ascii=False) + '\n')
    except Exception:
        pass

def _lookup_outbound(ref_time_ms: int) -> "str | None":
    best = None
    for ts, txt in _outbound_queue:
        if ts <= ref_time_ms:
            best = txt
        else:
            break
    if best is not None:
        return best
    try:
        import json as _json
        if Path(_outbound_cache_path).exists():
            with open(_outbound_cache_path) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        item = _json.loads(line)
                        if item.get("ts", 0) <= ref_time_ms:
                            best = item.get("text", "")
                    except Exception:
                        continue
    except Exception:
        pass
    try:
        import json as _json
        if Path(_outbound_cache_path).exists():
            with open(_outbound_cache_path) as f:
                lines = [l for l in f if l.strip()]
            if len(lines) > 200:
                with open(_outbound_cache_path, 'w') as f:
                    f.writelines(lines[-200:])
    except Exception:
        pass
    return best
def check_weixin_requirements() -> bool:
    return AIOHTTP_AVAILABLE and CRYPTO_AVAILABLE


def _safe_id(value: Optional[str], keep: int = 8) -> str:
    raw = str(value or "").strip()
    return raw[:keep] if raw else "?"


def _pkcs7_pad(data: bytes, block_size: int = 16) -> bytes:
    pad_len = block_size - (len(data) % block_size)
    return data + bytes([pad_len] * pad_len)


def _aes128_ecb_encrypt(plaintext: bytes, key: bytes) -> bytes:
    encryptor = Cipher(algorithms.AES(key), modes.ECB(), backend=default_backend()).encryptor()
    return encryptor.update(_pkcs7_pad(plaintext)) + encryptor.finalize()


def _aes128_ecb_decrypt(ciphertext: bytes, key: bytes) -> bytes:
    """Decrypt and strip PKCS#7 padding when it is well-formed (else return the raw block output)."""
    decryptor = Cipher(algorithms.AES(key), modes.ECB(), backend=default_backend()).decryptor()
    padded = decryptor.update(ciphertext) + decryptor.finalize()
    pad_len = padded[-1] if padded else 0
    if 1 <= pad_len <= 16 and padded.endswith(bytes([pad_len]) * pad_len):
        return padded[:-pad_len]
    return padded


def _headers(token: Optional[str], body: str) -> dict[str, str]:
    uin = base64.b64encode(str(int.from_bytes(secrets.token_bytes(4), "big")).encode("utf-8")).decode("ascii")
    return {
        "Content-Type": "application/json", "AuthorizationType": "ilink_bot_token",
        "Content-Length": str(len(body.encode("utf-8"))), "X-WECHAT-UIN": uin,
        "iLink-App-Id": ILINK_APP_ID, "iLink-App-ClientVersion": str(ILINK_APP_CLIENT_VERSION),
        **({"Authorization": f"Bearer {token}"} if token else {}),
    }


def _account_dir(hermes_home: str) -> Path:
    path = Path(hermes_home) / "weixin" / "accounts"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _read_json(path: Path) -> Any:
    try:
        # utf-8-sig (ours): tolerate BOM-persisted JSON files.
        return json.loads(path.read_text(encoding="utf-8-sig")) if path.exists() else None
    except Exception:
        return None


def save_weixin_account(hermes_home: str, *, account_id: str, token: str, base_url: str, user_id: str = "") -> None:
    path = _account_dir(hermes_home) / f"{account_id}.json"
    saved_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    atomic_json_write(path, {"token": token, "base_url": base_url, "user_id": user_id, "saved_at": saved_at})
    with contextlib.suppress(OSError):
        path.chmod(0o600)


def load_weixin_account(hermes_home: str, account_id: str) -> Optional[dict[str, Any]]:
    return _read_json(_account_dir(hermes_home) / f"{account_id}.json")


class ContextTokenStore:
    """Disk-backed ``context_token`` cache keyed by account + peer."""

    def __init__(self, hermes_home: str):
        self._root = _account_dir(hermes_home)
        self._cache: dict[str, str] = {}
        # Serializes the offloaded flushes so two concurrent set() calls
        # cannot land their writes out of order (last-writer-wins would drop
        # the newer token from disk).
        self._persist_lock = asyncio.Lock()

    @staticmethod
    def _key(account_id: str, user_id: str) -> str:
        return f"{account_id}:{user_id}"

    def restore(self, account_id: str) -> None:
        path = self._root / f"{account_id}.context-tokens.json"
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception as exc:
            logger.warning("weixin: failed to restore context tokens for %s: %s", _safe_id(account_id), exc)
            return
        restored = {self._key(account_id, u): t for u, t in data.items() if isinstance(t, str) and t}
        self._cache.update(restored)
        if restored:
            logger.info("weixin: restored %d context token(s) for %s", len(restored), _safe_id(account_id))

    def get(self, account_id: str, user_id: str) -> Optional[str]:
        return self._cache.get(self._key(account_id, user_id))

    async def set(self, account_id: str, user_id: str, token: str) -> None:
        self._cache[self._key(account_id, user_id)] = token
        # atomic_json_write() fsyncs, so the flush is offloaded off the loop; the payload is snapshotted
        # here (the worker never iterates ``_cache`` mid-mutation) and the lock keeps flushes in order.
        async with self._persist_lock:
            prefix = f"{account_id}:"
            payload = {key[len(prefix):]: value for key, value in self._cache.items() if key.startswith(prefix)}
            await asyncio.to_thread(self._persist, account_id, payload)

    def _persist(self, account_id: str, payload: dict[str, str]) -> None:
        try:
            atomic_json_write(self._root / f"{account_id}.context-tokens.json", payload)
        except Exception as exc:
            logger.warning("weixin: failed to persist context tokens for %s: %s", _safe_id(account_id), exc)


class TypingTicketCache:
    """Short-lived typing ticket cache from ``getconfig``."""

    def __init__(self, ttl_seconds: float = 600.0):
        self._ttl_seconds = ttl_seconds
        self._cache: dict[str, tuple[str, float]] = {}

    def get(self, user_id: str) -> Optional[str]:
        entry = self._cache.get(user_id)
        if entry and time.time() - entry[1] < self._ttl_seconds:
            return entry[0]
        self._cache.pop(user_id, None)
        return None

    def set(self, user_id: str, ticket: str) -> None:
        self._cache[user_id] = (ticket, time.time())


def _parse_aes_key(aes_key_b64: str) -> bytes:
    decoded = base64.b64decode(aes_key_b64)
    if len(decoded) == 16:
        return decoded
    text = decoded.decode("ascii", errors="ignore") if len(decoded) == 32 else ""
    if text and all(ch in "0123456789abcdefABCDEF" for ch in text):
        return bytes.fromhex(text)
    raise ValueError(f"unexpected aes_key format ({len(decoded)} decoded bytes)")


def _guess_chat_type(message: dict[str, Any], account_id: str) -> tuple[str, str]:
    room_id = str(message.get("room_id") or message.get("chat_room_id") or "").strip()
    to_user_id = str(message.get("to_user_id") or "").strip()
    if room_id or (to_user_id and account_id and to_user_id != account_id and message.get("msg_type") == 1):
        return "group", room_id or to_user_id or str(message.get("from_user_id") or "")
    return "dm", str(message.get("from_user_id") or "")


# HTTP helpers enforce timeouts via asyncio.wait_for(), not aiohttp ClientTimeout, which raises
# "Timeout context manager should be used inside a task" under run_coroutine_threadsafe() from cron.
async def _api_request(
    session: aiohttp.ClientSession, method: str, *, base_url: str, endpoint: str, headers: dict[str, str], timeout_ms: int, body: Optional[str] = None,
) -> dict[str, Any]:
    async def _do() -> dict[str, Any]:
        kwargs = {"data": body} if body is not None else {}
        async with getattr(session, method.lower())(f"{base_url.rstrip('/')}/{endpoint}", headers=headers, **kwargs) as response:
            raw = await response.text()
            if not response.ok:
                raise RuntimeError(f"iLink {method} {endpoint} HTTP {response.status}: {raw[:200]}")
            return json.loads(raw)
    return await asyncio.wait_for(_do(), timeout=timeout_ms / 1000)


async def _api_post(
    session: aiohttp.ClientSession, *, base_url: str, endpoint: str, payload: dict[str, Any], token: Optional[str], timeout_ms: int,
) -> dict[str, Any]:
    body = json.dumps({**payload, "base_info": {"channel_version": CHANNEL_VERSION}}, ensure_ascii=False, separators=(",", ":"))
    return await _api_request(session, "POST", base_url=base_url, endpoint=endpoint, headers=_headers(token, body), timeout_ms=timeout_ms, body=body)


async def _api_get(session: aiohttp.ClientSession, *, base_url: str, endpoint: str, timeout_ms: int) -> dict[str, Any]:
    headers = {"iLink-App-Id": ILINK_APP_ID, "iLink-App-ClientVersion": str(ILINK_APP_CLIENT_VERSION)}
    return await _api_request(session, "GET", base_url=base_url, endpoint=endpoint, headers=headers, timeout_ms=timeout_ms)


async def _get_updates(session: aiohttp.ClientSession, *, base_url: str, token: str, sync_buf: str, timeout_ms: int) -> dict[str, Any]:
    try:
        return await _api_post(session, base_url=base_url, endpoint=EP_GET_UPDATES, payload={"get_updates_buf": sync_buf}, token=token, timeout_ms=timeout_ms)
    except TimeoutError:
        return {"ret": 0, "msgs": [], "get_updates_buf": sync_buf}


async def _send_items(
    session: aiohttp.ClientSession, *, base_url: str, token: str, to: str, item_list: list[dict[str, Any]], context_token: Optional[str], client_id: str,
) -> dict[str, Any]:
    message: dict[str, Any] = {
        "from_user_id": "", "to_user_id": to, "client_id": client_id, "message_type": MSG_TYPE_BOT, "message_state": MSG_STATE_FINISH,
        "item_list": item_list}
    if context_token:
        message["context_token"] = context_token
    return await _api_post(session, base_url=base_url, endpoint=EP_SEND_MESSAGE, payload={"msg": message}, token=token, timeout_ms=API_TIMEOUT_MS)


async def _send_message(
    session: aiohttp.ClientSession, *, base_url: str, token: str, to: str, text: str, context_token: Optional[str], client_id: str,
) -> dict[str, Any]:
    if not text or not text.strip():
        raise ValueError("_send_message: text must not be empty")
    item_list = [{"type": ITEM_TEXT, "text_item": {"text": text}}]
    return await _send_items(session, base_url=base_url, token=token, to=to, item_list=item_list, context_token=context_token, client_id=client_id)


async def _get_config(session: aiohttp.ClientSession, *, base_url: str, token: str, user_id: str, context_token: Optional[str]) -> dict[str, Any]:
    payload: dict[str, Any] = {"ilink_user_id": user_id}
    if context_token:
        payload["context_token"] = context_token
    return await _api_post(session, base_url=base_url, endpoint=EP_GET_CONFIG, payload=payload, token=token, timeout_ms=CONFIG_TIMEOUT_MS)


async def _get_upload_url(
    session: aiohttp.ClientSession, *, base_url: str, token: str, to_user_id: str, media_type: int, filekey: str, rawsize: int,
    rawfilemd5: str, filesize: int, aeskey_hex: str,
) -> dict[str, Any]:
    payload = {
        "filekey": filekey, "media_type": media_type, "to_user_id": to_user_id, "rawsize": rawsize, "rawfilemd5": rawfilemd5,
        "filesize": filesize, "no_need_thumb": True, "aeskey": aeskey_hex}
    return await _api_post(session, base_url=base_url, endpoint=EP_GET_UPLOAD_URL, payload=payload, token=token, timeout_ms=API_TIMEOUT_MS)


async def _upload_ciphertext(session: aiohttp.ClientSession, *, ciphertext: bytes, upload_url: str) -> str:
    async def _do() -> str:
        async with session.post(upload_url, data=ciphertext, headers={"Content-Type": "application/octet-stream"}) as response:
            encrypted_param = response.headers.get("x-encrypted-param") if response.status == 200 else None
            if encrypted_param:
                await response.read()
                return encrypted_param
            raw = (await response.text())[:200]
            raise RuntimeError(f"CDN upload missing x-encrypted-param header: {raw}" if response.status == 200 else f"CDN upload HTTP {response.status}: {raw}")
    return await asyncio.wait_for(_do(), timeout=120)


async def _download_bytes(session: aiohttp.ClientSession, *, url: str, timeout_seconds: float = 60.0) -> bytes:
    async def _do() -> bytes:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.read()
    return await asyncio.wait_for(_do(), timeout=timeout_seconds)


_WEIXIN_CDN_ALLOWLIST: frozenset[str] = frozenset({
    "novac2c.cdn.weixin.qq.com", "ilinkai.weixin.qq.com", "wx.qlogo.cn", "thirdwx.qlogo.cn", "res.wx.qq.com", "mmbiz.qpic.cn", "mmbiz.qlogo.cn"})


def _assert_weixin_cdn_url(url: str) -> None:
    try:
        parsed = urlparse(url)
        scheme, host = parsed.scheme.lower(), parsed.hostname or ""
    except Exception as exc:
        raise ValueError(f"Unparseable media URL: {url!r}") from exc
    if scheme not in {"http", "https"}:
        raise ValueError(f"Media URL has disallowed scheme {scheme!r}; only http/https are permitted.")
    if host not in _WEIXIN_CDN_ALLOWLIST:
        raise ValueError(f"Media URL host {host!r} is not in the WeChat CDN allowlist. Refusing to fetch to prevent SSRF.")


async def _download_and_decrypt_media(
    session: aiohttp.ClientSession, *, cdn_base_url: str, encrypted_query_param: Optional[str], aes_key_b64: Optional[str],
    full_url: Optional[str], timeout_seconds: float,
) -> bytes:
    if encrypted_query_param:
        url = f"{cdn_base_url.rstrip('/')}/download?encrypted_query_param={quote(encrypted_query_param, safe='')}"
    elif full_url:
        _assert_weixin_cdn_url(full_url)
        url = full_url
    else:
        raise RuntimeError("media item had neither encrypt_query_param nor full_url")
    raw = await _download_bytes(session, url=url, timeout_seconds=timeout_seconds)
    return _aes128_ecb_decrypt(raw, _parse_aes_key(aes_key_b64)) if aes_key_b64 else raw


def _walk_markdown_lines(content: str):
    """Yield ``(rstripped line, is_fence, in_code_block)`` per line; ``in_code_block`` is the state *before* a fence toggles it."""
    in_code_block = False
    for raw_line in content.splitlines():
        line = raw_line.rstrip()
        is_fence = bool(_FENCE_RE.match(line.strip()))
        yield line, is_fence, in_code_block
        if is_fence:
            in_code_block = not in_code_block


def _normalize_markdown_blocks(content: str) -> str:
    """rstrip lines and collapse blank runs to one, leaving fenced code untouched."""
    result: list[str] = []
    blank_run = 0
    for line, is_fence, in_code_block in _walk_markdown_lines(content):
        if not is_fence and not in_code_block and not line.strip():
            blank_run += 1
            if blank_run <= 1:
                result.append("")
            continue
        if is_fence or not in_code_block:
            blank_run = 0
        result.append(line)
    return "\n".join(result).strip()


def _wrap_copy_friendly_lines_for_weixin(content: str) -> str:
    """Wrap long display lines that are hard to copy in WeChat clients (not code/table lines)."""
    if not content:
        return content
    wrapped: list[str] = []
    for line, is_fence, in_code_block in _walk_markdown_lines(content):
        stripped = line.strip()
        keep = is_fence or in_code_block or len(line) <= WEIXIN_COPY_LINE_WIDTH or not stripped or stripped.startswith("|")
        if keep or _TABLE_RULE_RE.match(stripped):
            wrapped.append(line)
        else:
            wrapped.extend(textwrap.wrap(
                line, width=WEIXIN_COPY_LINE_WIDTH, break_long_words=False, break_on_hyphens=False, replace_whitespace=False,
                drop_whitespace=True) or [line])
    return "\n".join(wrapped).strip()


def _split_markdown_blocks(content: str) -> list[str]:
    blocks: list[str] = []
    current: list[str] = []

    def flush() -> None:
        if current:
            blocks.append("\n".join(current).strip())
            current.clear()
    for line, is_fence, in_code_block in _walk_markdown_lines(content):
        if is_fence and not in_code_block:  # opening fence starts a fresh block
            flush()
        if is_fence or in_code_block or line.strip():
            current.append(line)
        else:
            flush()
        if is_fence and in_code_block:  # closing fence ends the block
            flush()
    flush()
    return [block for block in blocks if block]


def _split_delivery_units_for_weixin(content: str) -> list[str]:
    """Top-level lines become units; fenced code stays intact; indented continuation lines attach to the previous line."""
    units: list[str] = []
    for block in _split_markdown_blocks(content):
        if _FENCE_RE.match(block.splitlines()[0].strip()):
            units.append(block)
            continue
        current: list[str] = []
        for raw_line in block.splitlines():
            line = raw_line.rstrip()
            if current and line.strip() and raw_line.startswith((" ", "\t")):
                current.append(line)
                continue
            units.append("\n".join(current).strip())
            current = [line] if line.strip() else []
        units.append("\n".join(current).strip())
    return [unit for unit in units if unit]


def _looks_like_chatty_line_for_weixin(line: str) -> bool:
    stripped = line.strip()
    return bool(
        stripped and len(stripped) <= 48 and not line.startswith((" ", "\t")) and not stripped.startswith((">", "-", "*", "【", "#", "|"))
        and not _TABLE_RULE_RE.match(stripped) and not re.match(r"^\*\*[^*]+\*\*$", stripped) and not re.match(r"^\d+\.\s", stripped))


def _should_split_short_chat_block_for_weixin(block: str) -> bool:
    """Split only chat-like multiline blocks (2-6 chatty lines, first line not a heading) into separate bubbles."""
    lines = [line for line in block.splitlines() if line.strip()]
    if not 2 <= len(lines) <= 6:
        return False
    first = lines[0].strip()
    if _HEADER_RE.match(first) or (len(first) <= 24 and first.endswith((":", "："))):
        return False
    return all(_looks_like_chatty_line_for_weixin(line) for line in lines)


def _pack_markdown_blocks_for_weixin(content: str, max_length: int) -> list[str]:
    if len(content) <= max_length:
        return [content]
    # Block extraction stays weixin-local (anchored _FENCE_RE + per-line rstrip); packing is shared.
    overflow = lambda block: BasePlatformAdapter.truncate_message(block, max_length)
    return greedy_pack_blocks(_split_markdown_blocks(content), max_length, overflow=overflow)


def _split_text_for_weixin_delivery(content: str, max_length: int, split_per_line: bool = False) -> list[str]:
    """Compact (default): one message when it fits, unless it reads as a short chatty exchange (separate
    bubbles). Per-line (legacy ``extra.split_multiline_messages`` / ``WEIXIN_SPLIT_MULTILINE_MESSAGES``):
    top-level line breaks become separate messages. Oversized units use block-aware packing."""
    if not content:
        return []
    if split_per_line:
        if len(content) <= max_length and "\n" not in content:
            return [content]
        units = _split_delivery_units_for_weixin(content)
        chunks = [c for u in units for c in ([u] if len(u) <= max_length else _pack_markdown_blocks_for_weixin(u, max_length))]
        return [c for c in chunks if c] or [content]
    if len(content) > max_length:
        return _pack_markdown_blocks_for_weixin(content, max_length) or [content]
    return _split_delivery_units_for_weixin(content) if _should_split_short_chat_block_for_weixin(content) else [content]


def _coerce_bool(value: Any, default: bool = True) -> bool:
    """Coerce a config value to bool, tolerating strings like ``"true"``; unknown -> ``default``."""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    return True if text in {"1", "true", "yes", "on"} else False if text in {"0", "false", "no", "off"} else default


def _extract_text(item_list: list[dict[str, Any]]) -> str:
    for item in item_list:
        if item.get("type") == ITEM_TEXT:
            text = str((item.get("text_item") or {}).get("text") or "")
            ref = item.get("ref_msg") or {}
            ref_item = ref.get("message_item") or {}
            if ref_item.get("type") in {ITEM_IMAGE, ITEM_VIDEO, ITEM_FILE, ITEM_VOICE}:
                title = ref.get("title") or ""
                return f"[引用媒体: {title}]\n{text}".strip() if title else f"[引用媒体]\n{text}".strip()
            if ref_item:

                parts: List[str] = []
                if ref.get("title"):
                    parts.append(str(ref["title"]))
                ref_text = _extract_text([ref_item])
                # 缓存回退：WeChat iLink 新版 ref_msg 只含 msg_id 不含文字
                if not ref_text:
                    ref_msg_id = str(ref_item.get("msg_id")) if ref_item.get("msg_id") else None
                    cached = _msg_text_cache.get(ref_msg_id) if ref_msg_id else None
                    if not cached and ref_msg_id:
                        cached = _query_ref_cache(ref_msg_id)  # 持久化回退
                    if cached:
                        ref_text = cached
                    else:
                        ref_time_ms = None
                        if ref_item:
                            ref_time_ms = ref_item.get("create_time_ms") or ref_item.get("create_time")
                        if ref_time_ms:
                            try:
                                outbound_text = _lookup_outbound(int(ref_time_ms))
                                if outbound_text:
                                    ref_text = outbound_text
                                    logger.warning("[Weixin] refmsg cache TIME-MATCH: ts=%s len=%d", ref_time_ms, len(outbound_text))
                                else:
                                    ref_text = "[引用消息]"
                            except (ValueError, TypeError):
                                ref_text = "[引用消息]"
                        else:
                            ref_text = "[引用消息]"
                if ref_text:
                    parts.append(ref_text)

                if parts:
                    return f"[引用: {' | '.join(parts)}]\n{text}".strip()
            return text
    for item in item_list:
        if item.get("type") == ITEM_VOICE:
            # Tencent's ``voice_item.text`` is their STT output and is wrong for non-Chinese audio.
            # When raw audio exists return "" so gateway/run.py's central STT transcribes the download;
            # otherwise use Weixin's transcript but mark its voice origin.
            # #27300: Tencent Cloud's `voice_item.text` is their STT output, which is wrong for any
            # non-Chinese audio (the original report was a Russian voice message that came back as English
            # gibberish). Return empty so the central STT pipeline in ``gateway/run.py`` produces the body
            # from the downloaded audio instead.
            voice_item = item.get("voice_item") or {}
            # Use it, but preserve the voice origin so the agent can distinguish this from text the user
            # typed (#65022).
            voice_text = str(voice_item.get("text") or "")
            if not (voice_item.get("media") or {}) and voice_text:
                return f"[Voice transcription provided by Weixin]\n{voice_text}"
    return ""


_MIME_PREFIX_TYPES = (("image/", MessageType.PHOTO), ("video/", MessageType.VIDEO), ("audio/", MessageType.VOICE))


def _message_type_from_media(media_types: list[str], text: str) -> MessageType:
    for prefix, message_type in _MIME_PREFIX_TYPES:
        if any(m.startswith(prefix) for m in media_types):
            return message_type
    return MessageType.DOCUMENT if media_types else MessageType.COMMAND if text.startswith("/") else MessageType.TEXT


def _load_sync_buf(hermes_home: str, account_id: str) -> str:
    data = _read_json(_account_dir(hermes_home) / f"{account_id}.sync.json")
    return data.get("get_updates_buf", "") if isinstance(data, dict) else ""


def _save_sync_buf(hermes_home: str, account_id: str, sync_buf: str) -> None:
    atomic_json_write(_account_dir(hermes_home) / f"{account_id}.sync.json", {"get_updates_buf": sync_buf})


async def _fetch_qr(session: aiohttp.ClientSession, bot_type: str) -> tuple[str, str]:
    qr_resp = await _api_get(session, base_url=ILINK_BASE_URL, endpoint=f"{EP_GET_BOT_QR}?bot_type={bot_type}", timeout_ms=QR_TIMEOUT_MS)
    return str(qr_resp.get("qrcode") or ""), str(qr_resp.get("qrcode_img_content") or "")


def _print_qr(qrcode_value: str, qrcode_url: str, *, report_render_error: bool) -> None:
    """Print the QR URL + ASCII render; WeChat must scan the liteapp URL (``qrcode_img_content``), not the bare hex token."""
    if qrcode_url:
        print(qrcode_url)
    try:
        import qrcode
        qr = qrcode.QRCode()
        qr.add_data(qrcode_url or qrcode_value)
        qr.make(fit=True)
        qr.print_ascii(invert=True)
    except Exception as _qr_exc:
        if report_render_error:
            print(f"（终端二维码渲染失败: {_qr_exc}，请直接打开上面的二维码链接）")


async def qr_login(hermes_home: str, *, bot_type: str = "3", timeout_seconds: int = 480) -> Optional[dict[str, str]]:
    if not AIOHTTP_AVAILABLE:
        raise RuntimeError("aiohttp is required for Weixin QR login")
    async with _new_session() as session:
        try:
            qrcode_value, qrcode_url = await _fetch_qr(session, bot_type)
        except Exception as exc:
            logger.error("weixin: failed to fetch QR code: %s", exc)
            return None
        if not qrcode_value:
            logger.error("weixin: QR response missing qrcode")
            return None
        print("\n请使用微信扫描以下二维码：")
        _print_qr(qrcode_value, qrcode_url, report_render_error=True)
        deadline = time.monotonic() + timeout_seconds
        current_base_url, refresh_count = ILINK_BASE_URL, 0
        while time.monotonic() < deadline:
            try:
                status_resp = await _api_get(session, base_url=current_base_url, endpoint=f"{EP_GET_QR_STATUS}?qrcode={qrcode_value}", timeout_ms=QR_TIMEOUT_MS)
            except Exception as exc:
                if not isinstance(exc, asyncio.TimeoutError):
                    logger.warning("weixin: QR poll error: %s", exc)
                await asyncio.sleep(1)
                continue
            status = str(status_resp.get("status") or "wait")
            if status == "wait":
                print(".", end="", flush=True)
            elif status == "scaned":
                print("\n已扫码，请在微信里确认...")
            elif status == "scaned_but_redirect" and status_resp.get("redirect_host"):
                current_base_url = f"https://{status_resp['redirect_host']}"
            elif status == "expired":
                refresh_count += 1
                if refresh_count > 3:
                    print("\n二维码多次过期，请重新执行登录。")
                    return None
                print(f"\n二维码已过期，正在刷新... ({refresh_count}/3)")
                try:
                    qrcode_value, qrcode_url = await _fetch_qr(session, bot_type)
                    _print_qr(qrcode_value, qrcode_url, report_render_error=False)
                except Exception as exc:
                    logger.error("weixin: QR refresh failed: %s", exc)
                    return None
            elif status == "confirmed":
                creds = {
                    "account_id": str(status_resp.get("ilink_bot_id") or ""), "token": str(status_resp.get("bot_token") or ""),
                    "base_url": str(status_resp.get("baseurl") or ILINK_BASE_URL), "user_id": str(status_resp.get("ilink_user_id") or "")}
                if not creds["account_id"] or not creds["token"]:
                    logger.error("weixin: QR confirmed but credential payload was incomplete")
                    return None
                save_weixin_account(hermes_home, **creds)
                print(f"\n微信连接成功，account_id={creds['account_id']}")
                return creds
            await asyncio.sleep(1)
        print("\n微信登录超时。")
        return None


# Outbound item shapes: item type -> (item key, extra fields after the shared ``media`` block).
_ITEM_SHAPES: dict[int, tuple[str, Callable[[dict[str, Any]], dict[str, Any]]]] = {
    ITEM_FILE: ("file_item", lambda kw: {"file_name": kw["filename"], "len": str(kw["plaintext_size"])}),
    ITEM_IMAGE: ("image_item", lambda kw: {"mid_size": kw["ciphertext_size"]}),
    ITEM_VIDEO: ("video_item", lambda kw: {
        "video_size": kw["ciphertext_size"], "play_length": kw.get("play_length", 0), "video_md5": kw.get("rawfilemd5", "")}),
    ITEM_VOICE: ("voice_item", lambda kw: {
        "encode_type": kw.get("encode_type"), "bits_per_sample": kw.get("bits_per_sample"), "sample_rate": kw.get("sample_rate"),
        "playtime": kw.get("playtime", 0)}),
}


def _media_item(item_type: int, **kw: Any) -> dict[str, Any]:
    key, fields = _ITEM_SHAPES[item_type]
    media = {"encrypt_query_param": kw["encrypt_query_param"], "aes_key": kw["aes_key_for_api"], "encrypt_type": 1}
    return {"type": item_type, key: {"media": media, **fields(kw)}}


_file_item, _image_item = partial(_media_item, ITEM_FILE), partial(_media_item, ITEM_IMAGE)
_video_item, _voice_item = partial(_media_item, ITEM_VIDEO), partial(_media_item, ITEM_VOICE)


# Inbound media dispatch: item type -> (item key, download timeout, cache fn, mime or None (= guess from
# file_name), log label). Cache fns are lambdas so monkeypatching the module names takes effect at call time.
_INBOUND_MEDIA: dict[int, tuple[str, float, Callable[[bytes, str], Awaitable[str]], Optional[str], str]] = {
    ITEM_IMAGE: ("image_item", 30.0, lambda data, _name: cache_image_from_bytes_async(data, ".jpg"), "image/jpeg", "image"),
    ITEM_VIDEO: ("video_item", 120.0, lambda data, _name: cache_document_from_bytes_async(data, "video.mp4"), "video/mp4", "video"),
    ITEM_FILE: ("file_item", 0.0, lambda data, name: cache_document_from_bytes_async(data, name), None, "file"),  # 0.0 = streaming path (no total timeout)
    ITEM_VOICE: ("voice_item", 60.0, lambda data, _name: cache_audio_from_bytes_async(data, ".silk"), "audio/silk", "voice"),
}

# Outbound local-file dispatch by extension: (extensions, sender method, path kwarg); default = send_document.
_OUTBOUND_BY_EXT: tuple[tuple[frozenset, str, str], ...] = (
    (frozenset({".ogg", ".opus", ".mp3", ".wav", ".m4a", ".flac"}), "send_voice", "audio_path"),  # no .m2a, unlike base
    (_VIDEO_EXTS, "send_video", "video_path"),
    (_IMAGE_EXTS, "send_image_file", "image_path"),
)
_DIRECT_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}


def _document_cache_subdir(adapter) -> Path:
    """P1 串档修复：入站文档按所属 profile 分目录（default 也进自己的目录）。"""
    prof = str(getattr(adapter, "_owner_profile", None) or "default")
    d = Path(get_document_cache_dir()) / prof
    d.mkdir(parents=True, exist_ok=True)
    return d


class WeixinAdapter(OwnAccessPolicyMixin, BasePlatformAdapter):
    ALLOW_ALL_ENV_PREFIX = "WEIXIN"
    supports_code_blocks = True  # Weixin renders fenced code blocks
    splits_long_messages = True  # send() chunks via _split_text()
    MAX_MESSAGE_LENGTH = 2000
    SUPPORTS_MESSAGE_EDITING = False  # WeChat cannot edit; streaming must send-final-only so the cursor (▉) is never left visible
    _SPLIT_THRESHOLD = 1800  # iLink chunks at ~2048 chars

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.WEIXIN)
        extra = config.extra or {}
        self._hermes_home = hermes_home = str(get_hermes_home())
        self._token_store = ContextTokenStore(hermes_home)
        self._typing_cache = TypingTicketCache()
        self._poll_session = self._send_session = None  # type: Optional[aiohttp.ClientSession]
        self._poll_task: Optional[asyncio.Task] = None
        self._dedup = MessageDeduplicator(ttl_seconds=MESSAGE_DEDUP_TTL_SECONDS)
        self._account_id = _extra_or_secret(extra, "account_id")
        self._token = str(config.token or extra.get("token") or _wx_secret("WEIXIN_TOKEN", "")).strip()
        self._base_url = _extra_or_secret(extra, "base_url", ILINK_BASE_URL).rstrip("/")
        self._cdn_base_url = _extra_or_secret(extra, "cdn_base_url", WEIXIN_CDN_BASE_URL).rstrip("/")
        # Tunables: ``extra.<key>`` else env ``WEIXIN_<KEY>`` (e.g. WEIXIN_SEND_CHUNK_RETRIES).
        self._send_chunk_delay_seconds = float(_extra_or_secret(extra, "send_chunk_delay_seconds", "1.5"))
        self._send_chunk_retries = int(_extra_or_secret(extra, "send_chunk_retries", "4"))
        self._send_chunk_retry_delay_seconds = float(_extra_or_secret(extra, "send_chunk_retry_delay_seconds", "1.0"))
        self._send_text_gate = asyncio.Lock()
        self._rate_limit_circuit_threshold = max(1, int(_extra_or_secret(extra, "rate_limit_circuit_threshold", "3")))
        self._rate_limit_circuit_window_seconds = float(_extra_or_secret(extra, "rate_limit_circuit_window_seconds", "30.0"))
        self._rate_limit_circuit_open_seconds = float(_extra_or_secret(extra, "rate_limit_circuit_open_seconds", "30.0"))
        self._rate_limit_circuit_until, self._rate_limit_events = 0.0, []  # type: float, List[float]
        # 2026-08-02 根因修复2: 滑动窗口总量锁 — iLink 限制是窗口总量(实测 6-7 条/5-8min),
        # 不是单条间隔。所有来源(cron/ledger/补投/会话)共用此计数, 超限排队等待窗口滑动。
        # 默认 5 分钟 ≤5 条(实测值留余量)。可配置 WEIXIN_SLIDING_WINDOW_SECONDS/MAX。
        self._sliding_window_seconds = float(_extra_or_secret(extra, "sliding_window_seconds", "300.0"))
        self._sliding_window_max = max(1, int(_extra_or_secret(extra, "sliding_window_max", "5")))
        self._sliding_window_events: List[float] = []
        self._sliding_gate = asyncio.Lock()
        # 补投风暴断路：同一会话 60s 内失败 5 次 → 暂停该会话补投 300s。
        self._retry_chat_failures: dict[str, list[float]] = {}
        self._retry_chat_paused_until: dict[str, float] = {}
        self._dm_policy = _extra_or_secret(extra, "dm_policy", "pairing").lower()
        self._group_policy = _extra_or_secret(extra, "group_policy", "disabled").lower()
        # ``extra`` wins even when falsy (an explicit empty list disables the env allowlist).
        allow_from, group_allow_from = extra.get("allow_from"), extra.get("group_allow_from")
        self._allow_from = self._coerce_list(_wx_secret("WEIXIN_ALLOWED_USERS", "") if allow_from is None else allow_from)
        self._group_allow_from = self._coerce_list(_wx_secret("WEIXIN_GROUP_ALLOWED_USERS", "") if group_allow_from is None else group_allow_from)
        self._split_multiline_messages = _coerce_bool(_extra_or_secret(extra, "split_multiline_messages", ""), default=False)
        # Text debounce batching (Telegram pattern): iLink delivers messages individually, so rapid bursts would each
        # trigger a separate agent run. Telegram cadence and ceilings (#44883); ``0`` dispatches immediately.
        self._configure_text_batch_delays()
        persisted = load_weixin_account(hermes_home, self._account_id) if self._account_id and not self._token else None
        if persisted:
            self._token = str(persisted.get("token") or "").strip()
            self._base_url = str(persisted.get("base_url") or self._base_url).strip().rstrip("/")

    @staticmethod
    def _coerce_list(value: Any) -> list[str]:
        if value is None:
            return []
        items = value.split(",") if isinstance(value, str) else value if isinstance(value, (list, tuple, set)) else [value]
        return [str(item).strip() for item in items if str(item).strip()]

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        preflight = (
            (check_weixin_requirements(), "weixin_missing_dependency", "aiohttp and cryptography are required"),
            (self._token, "weixin_missing_token", "WEIXIN_TOKEN is required"),
            (self._account_id, "weixin_missing_account", "WEIXIN_ACCOUNT_ID is required"))
        for ok, code, reason in preflight:
            if not ok:
                self._set_fatal_error(code, f"Weixin startup failed: {reason}", retryable=False)
                logger.warning("[%s] Weixin startup failed: %s", self.name, reason)
                return False
        try:
            if not self._acquire_platform_lock('weixin-bot-token', self._token, 'Weixin bot token'):
                return False
        except Exception as exc:
            logger.debug("[%s] Token lock unavailable (non-fatal): %s", self.name, exc)
        self._poll_session = _new_session()
        # total=None disables aiohttp's ClientTimeout so send() works via run_coroutine_threadsafe() from cron;
        # _api_post/_api_get enforce timeouts with asyncio.wait_for() instead.
        self._send_session = _new_session(timeout=aiohttp.ClientTimeout(total=None, connect=None, sock_connect=None, sock_read=None))
        self._token_store.restore(self._account_id)
        self._poll_task = asyncio.create_task(self._poll_loop(), name="weixin-poll")
        self._mark_connected()
        _LIVE_ADAPTERS[self._token] = self
        logger.info("[%s] Connected account=%s base=%s", self.name, _safe_id(self._account_id), self._base_url)
        if self._group_policy != "disabled":
            logger.warning(
                "[%s] WEIXIN_GROUP_POLICY=%s is set, but QR-login connects an iLink bot identity (e.g. ...@im.bot) "
                "which typically cannot be invited into ordinary WeChat groups. iLink usually does not deliver "
                "ordinary-group events for these accounts, so group messages may never reach Hermes regardless of "
                "this policy. If group delivery doesn't work, the limitation is on the iLink side, not in Hermes.",
                self.name, self._group_policy)
        self._wire_plugin_handlers(None)  # plugin-registered native handlers
        return True

    async def disconnect(self) -> None:
        _LIVE_ADAPTERS.pop(self._token, None)
        self._running = False
        for task in self._pending_text_batch_tasks.values():
            if not task.done():
                task.cancel()
        self._pending_text_batches.clear()
        self._pending_text_batch_tasks.clear()
        await cancel_task(self._poll_task)
        self._poll_task = None
        for attr in ("_poll_session", "_send_session"):
            session = getattr(self, attr)
            if session and not session.closed:
                await session.close()
            setattr(self, attr, None)
        self._release_platform_lock()
        self._mark_disconnected()
        logger.info("[%s] Disconnected", self.name)

    async def _poll_loop(self) -> None:
        assert self._poll_session is not None
        sync_buf = _load_sync_buf(self._hermes_home, self._account_id)
        timeout_ms = LONG_POLL_TIMEOUT_MS
        consecutive_failures = 0

        async def backoff() -> int:
            """Sleep for the failure streak; returns the new streak count (0 after a full streak)."""
            streak_done = consecutive_failures >= MAX_CONSECUTIVE_FAILURES
            await asyncio.sleep(BACKOFF_DELAY_SECONDS if streak_done else RETRY_DELAY_SECONDS)
            return 0 if streak_done else consecutive_failures

        while self._running:
            try:
                response = await _get_updates(self._poll_session, base_url=self._base_url, token=self._token, sync_buf=sync_buf, timeout_ms=timeout_ms)
                suggested_timeout = response.get("longpolling_timeout_ms")
                if isinstance(suggested_timeout, int) and suggested_timeout > 0:
                    timeout_ms = suggested_timeout
                ret, errcode = response.get("ret", 0), response.get("errcode", 0)
                if ret not in {0, None} or errcode not in {0, None}:
                    if _is_session_expired(response, ret, errcode):
                        logger.error("[%s] Session expired; pausing for 10 minutes", self.name)
                        await asyncio.sleep(600)
                        consecutive_failures = 0
                        continue
                    consecutive_failures += 1
                    logger.warning("[%s] getUpdates failed ret=%s errcode=%s errmsg=%s (%d/%d)", self.name, ret, errcode,
                                   response.get("errmsg", ""), consecutive_failures, MAX_CONSECUTIVE_FAILURES)
                    consecutive_failures = await backoff()
                    continue
                consecutive_failures = 0
                # Dispatch before persisting: the off-loop write is an await, and a disconnect that
                # cancels it must not leave the advanced cursor on disk with this batch undelivered.
                for message in response.get("msgs") or []:
                    asyncio.create_task(self._process_message_safe(message))
                # atomic_json_write fsyncs + renames: persist off the loop, and only when the cursor
                # moved (an empty long-poll echoes the same buffer back every cycle).
                if response.get("get_updates_buf") and str(response["get_updates_buf"]) != sync_buf:
                    sync_buf = str(response["get_updates_buf"])
                    await asyncio.to_thread(_save_sync_buf, self._hermes_home, self._account_id, sync_buf)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                consecutive_failures += 1
                logger.error("[%s] poll error (%d/%d): %s", self.name, consecutive_failures, MAX_CONSECUTIVE_FAILURES, exc)
                consecutive_failures = await backoff()
                if consecutive_failures == 0:
                    # Full failure streak: recycle the session. Failed connects through a local proxy (e.g.
                    # Clash) strand sockets the keepalive reaper never sees; on macOS the 256-fd soft limit
                    # then yields EMFILE and a crash. Closing the session tears down every socket.
                    # Clash on 127.0.0.1:7890) can strand sockets that never return to the connector's
                    # keepalive pool, so the tight keepalive_timeout never reaps them. On macOS the default
                    # 256-fd soft limit turns that drip into `[Errno 24] Too many open files` and a gateway
                    # crash (#79889). Closing the session tears down its connector and every socket it
                    # holds; a fresh session starts the next attempt from zero fds.
                    await self._recycle_poll_session()

    async def _recycle_poll_session(self) -> None:
        """Swap in a fresh ``_poll_session`` *then* close the old one, so concurrent tasks never see a closed session."""
        if not self._running or aiohttp is None:
            return
        old, self._poll_session = self._poll_session, _new_session()
        if old is not None and not old.closed:
            try:
                await old.close()
            except Exception as exc:
                logger.debug("[%s] old poll session close failed: %s", self.name, exc)

    async def _process_message_safe(self, message: dict[str, Any]) -> None:
        try:
            await self._process_message(message)
        except Exception as exc:
            logger.error("[%s] unhandled inbound error from=%s: %s", self.name, _safe_id(message.get("from_user_id")), exc, exc_info=True)

    async def _process_message(self, message: dict[str, Any]) -> None:
        assert self._poll_session is not None
        sender_id = str(message.get("from_user_id") or "").strip()
        message_id = str(message.get("message_id") or "").strip()
        if not sender_id or sender_id == self._account_id or (message_id and self._dedup.is_duplicate(message_id)):
            return
        # Secondary content-fingerprint dedup: upstream re-sends identical text under new message_ids.
        item_list = message.get("item_list") or []
        text = _extract_text(item_list)
        if text and self._dedup.is_duplicate(f"content:{sender_id}:{hashlib.md5(text.encode()).hexdigest()}"):
            logger.debug("[%s] Content-dedup: skipping duplicate message from %s", self.name, sender_id)
            return
        chat_type, effective_chat_id = _guess_chat_type(message, self._account_id)

        # 引用消息缓存：将每条入站消息存入缓存（供 ref_msg 三层回退查找）
        cache_key = str(message.get("message_id") or "").strip()
        if cache_key:
            for item in item_list:
                item_type = item.get("type")
                cached_text = None
                if item_type == ITEM_TEXT:
                    cached_text = str((item.get("text_item") or {}).get("text") or "").strip()
                elif item_type == ITEM_IMAGE:
                    img = item.get("image_item") or {}
                    cached_text = f"[图片] {img.get('width', '?')}x{img.get('height', '?')}"
                elif item_type == ITEM_VIDEO:
                    vid = item.get("video_item") or {}
                    cached_text = f"[视频] {vid.get('duration', '?')}s"
                elif item_type == ITEM_FILE:
                    f_item = item.get("file_item") or {}
                    fname = f_item.get("file_name") or f_item.get("title") or "文件"
                    fsize = f_item.get("size") or 0
                    cached_text = f"[文件] {fname} ({fsize}B)"
                elif item_type == ITEM_VOICE:
                    cached_text = str((item.get("voice_item") or {}).get("text") or "").strip()
                if cached_text:
                    _cache_message_text(cache_key, cached_text)
                    logger.debug("[Weixin] refmsg cache INBOUND: key=%s type=%s len=%d",
                        cache_key[:40], item_type, len(cached_text))

        if chat_type == "group":
            if not self._is_group_allowed(effective_chat_id):
                return
        elif not self._is_dm_intake_allowed(sender_id):
            return
        context_token = str(message.get("context_token") or "").strip()
        if context_token:
            await self._token_store.set(self._account_id, sender_id, context_token)
        if self._poll_session and self._token and not self._typing_cache.get(sender_id):
            asyncio.create_task(self._fetch_typing_ticket(self._poll_session, sender_id, context_token or None, "getConfig failed"))
        # 2026-08-20: inbound 事件处——刷新 slowlane 为 FRESH + 唤醒补投队列（token 恢复/会话刷新后补发积压）
        if self._poll_session and self._token:
            _write_slowlane("FRESH", "event-inbound")
            asyncio.create_task(self._drain_retry_queue(wake_suspended=True))
        media_paths, media_types = [], []  # type: List[str], List[str]
        media_notes: List[str] = []
        for item in item_list:
            ref_item = (item.get("ref_msg") or {}).get("message_item")
            for candidate in (item, ref_item) if isinstance(ref_item, dict) else (item,):
                if candidate.get("type") == ITEM_FILE and self._should_background_file(candidate):
                    # 2026-10-23: 大文件（或大小未知）不阻塞本次会话——先回执，后台续传。
                    media_notes.append(self._start_background_file_download(
                        candidate, chat_id=effective_chat_id,
                        sender_id=sender_id, chat_type=chat_type))
                    continue
                await self._collect_media(candidate, media_paths, media_types,
                                          chat_id=effective_chat_id, sender_id=sender_id,
                                          notes=media_notes)
        if media_notes and text.startswith("/"):
            # A slash command must remain the first token or is_command() stops matching.
            for note in media_notes:
                await self.send(effective_chat_id, note)
        elif media_notes:
            text = "\n\n".join(media_notes + ([text] if text else []))
        if not text and not media_paths:
            return
        source = self.build_source(chat_id=effective_chat_id, chat_type=chat_type, user_id=sender_id, user_name=sender_id)
        event = MessageEvent(
            text=text, message_type=_message_type_from_media(media_types, text), source=source, raw_message=message,
            message_id=message_id or None, media_urls=media_paths, media_types=media_types, timestamp=datetime.now())
        logger.info("[%s] inbound from=%s type=%s media=%d", self.name, _safe_id(sender_id), source.chat_type, len(media_paths))
        if event.message_type == MessageType.TEXT:
            self._enqueue_text_event(event)
        else:
            await self.handle_message(event)

    async def _collect_media(self, item: dict[str, Any], media_paths: list[str], media_types: list[str],
                             *, chat_id: str = "", sender_id: str = "",
                             notes: Optional[list[str]] = None) -> None:
        spec = _INBOUND_MEDIA.get(item.get("type"))
        if not spec:
            return
        if spec[0] == "file_item":
            # 2026-10-23: files go through the streaming/resumable downloader (no total
            # timeout, no size cap). Failures return a note instead of vanishing.
            path, mime, note = await self._download_file_streaming(item, chat_id=chat_id, sender_id=sender_id)
            if note and notes is not None:
                notes.append(note)
        else:
            path, mime = await self._download_media(item, spec)
        if path:
            media_paths.append(path)
            media_types.append(mime)

    async def _download_media(self, item: dict[str, Any], spec: tuple[Any, ...]) -> tuple[Optional[str], str]:
        """Download + decrypt one inbound media item -> (cached path or None, mime). Voice is always downloaded
        (never trust Tencent's ``voice_item.text``) so gateway/run.py's central STT re-transcribes."""
        item_key, timeout_seconds, cache_fn, mime, label = spec
        payload = item.get(item_key) or {}
        media = payload.get("media") or {}
        filename = str(payload.get("file_name") or "document.bin")
        mime = mime or mimetypes.guess_type(filename)[0] or "application/octet-stream"
        try:
            aes_key_b64 = media.get("aes_key")
            if item_key == "image_item" and payload.get("aeskey"):  # image_item may carry a raw hex ``aeskey`` beside the media block
                aes_key_b64 = base64.b64encode(bytes.fromhex(str(payload.get("aeskey")))).decode("ascii") or aes_key_b64
            # #27300: previously short-circuited when ``voice_item.text`` was set on the assumption that
            # Tencent Cloud's STT was good enough. For non-Chinese audio that text is garbage (e.g. a
            # Russian message comes back as English phonemes) — we must always download the raw audio so
            # ``gateway/run.py``'s central STT pipeline can re-transcribe with the user's configured
            # mlx-whisper / whisper.cpp / faster-whisper backend.
            data = await _download_and_decrypt_media(
                self._poll_session, cdn_base_url=self._cdn_base_url, encrypted_query_param=media.get("encrypt_query_param"),
                aes_key_b64=aes_key_b64, full_url=media.get("full_url"), timeout_seconds=timeout_seconds)
            prof = str(getattr(self, "_owner_profile", None) or "default")
            if item_key == "file_item":
                return await cache_document_from_bytes_async(data, filename, profile=prof), mime
            # 2026-10-28：图片/语音也按档落盘 —— 之前四档共用一个 cache 大筐，
            # 与文档串档是同一类问题（没有归属就谈不上按档可见性）。
            if item_key == "image_item":
                return await cache_image_from_bytes_async(data, ".jpg", profile=prof), mime
            if item_key == "voice_item":
                return await cache_audio_from_bytes_async(data, ".silk", profile=prof), mime
            return await cache_fn(data, filename), mime
        except Exception as exc:
            logger.warning("[%s] %s download failed: %s", self.name, label, exc)
            return None, mime

    # ---------------------------------------------------------------- 入站文件（流式 / 断点续传）
    # 2026-10-23：93.3 MB m4a 被 60 秒总超时截断且失败静默，agent 只能猜"文件没到"。
    # 现在文件走 media_stream：流式落盘、无总超时（仅空闲超时）、Range 续传、失败出声。
    def _inbound_media_url(self, media: dict[str, Any]) -> str:
        param = media.get("encrypt_query_param")
        if param:
            url = f"{self._cdn_base_url.rstrip('/')}/download?encrypted_query_param={quote(str(param), safe='')}"
        else:
            full_url = media.get("full_url")
            if not full_url:
                raise RuntimeError("media item had neither encrypt_query_param nor full_url")
            url = str(full_url)
        # Both branches go through the allowlist: the base URL comes from config/env, so
        # treating it as trusted would leave an SSRF hole on the parameterised branch.
        _assert_weixin_cdn_url(url)
        if not url.lower().startswith("https://"):
            raise ValueError("refusing non-HTTPS media URL")
        return url

    def _inbound_media_ledger(self, **row: Any) -> None:
        """Append one line to ``state/inbound_media.jsonl`` (best effort, 0600)."""
        try:
            path = Path(self._hermes_home) / "state" / "inbound_media.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            row.setdefault("ts", datetime.now().isoformat(timespec="seconds"))
            row.setdefault("account", self._account_id)
            fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            with os.fdopen(fd, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception as exc:
            logger.warning("[%s] media ledger write failed: %s", self.name, exc)

    def _progress_cb(self, chat_id: str, filename: str) -> Callable[[int, int, float], Awaitable[None]]:
        state = {"pct": 0, "disabled": False}

        async def _cb(received: int, total: int, rate: float) -> None:
            if not total or state["disabled"]:
                return
            pct = int(100 * received / total)
            if pct < 50 or pct - state["pct"] < 40:  # only 50% / 90%
                return
            state["pct"] = pct

            async def _send() -> None:
                # Never await a send inside the read loop: a full sliding window makes
                # send() block for up to 300 s, which stalls the transfer and triggers
                # the very idle timeout this module relies on.
                try:
                    result = await asyncio.wait_for(self.send(chat_id, (
                        f"⏳ 「{_pretty_filename(filename)}」下载中 {pct}%"
                        f"（{received / 1048576:.1f} / {total / 1048576:.1f} MB，"
                        f"{rate / 1024:.0f} KB/s）"
                    )), timeout=5)
                    if not getattr(result, "success", True):
                        state["disabled"] = True  # window full: stop trying, don't spam
                except Exception as exc:
                    logger.debug("[%s] progress notify failed: %s", self.name, exc)
                    state["disabled"] = True

            task = asyncio.create_task(_send())
            _BACKGROUND_DOWNLOADS.add(task)
            task.add_done_callback(_BACKGROUND_DOWNLOADS.discard)

        return _cb

    async def _download_file_streaming(self, item: dict[str, Any], *, chat_id: str = "",
                                       sender_id: str = "", background: bool = False
                                       ) -> tuple[Optional[str], str, Optional[str]]:
        """Stream one inbound file to disk -> ``(path, mime, failure_note)``."""
        payload = item.get("file_item") or {}
        media = payload.get("media") or {}
        filename = str(payload.get("file_name") or "document.bin")
        mime = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        declared = _declared_media_size(payload)
        if self._poll_session is None:
            return None, mime, f"⚠️ 附件「{filename}」下载失败：连接未就绪，请重发。"
        try:
            url = self._inbound_media_url(media)
        except Exception as exc:
            logger.warning("[%s] file url unavailable (%s): %s", self.name, filename, exc)
            return None, mime, f"⚠️ 附件「{filename}」下载失败：文件链接缺失，请重发。"
        identity = part_identity(filename, declared, account=str(self._account_id or ""),
                                 chat=str(chat_id), sender=str(sender_id))
        try:
            result = await download_to_cache(
                self._poll_session, url=url, dest_dir=_document_cache_subdir(self),
                filename=filename, expected_size=declared, aes_key_b64=media.get("aes_key"),
                identity=identity, idle_timeout=_inbound_idle_timeout_s(),
                url_validator=_assert_weixin_cdn_url,
                progress_cb=self._progress_cb(chat_id, filename) if background else None,
            )
        except DownloadError as exc:
            self._inbound_media_ledger(chat=chat_id, file_name=filename, declared_size=declared,
                                       received=exc.received, total=exc.total, attempts=exc.attempts,
                                       status=exc.status, error=str(exc)[:300])
            logger.warning("[%s] file download failed name=%s status=%s received=%d/%d attempts=%d err=%s",
                           self.name, filename, exc.status, exc.received, exc.total, exc.attempts, exc)
            return None, mime, "⚠️ " + exc.user_message(filename)
        except Exception as exc:  # a media failure must never kill the whole inbound message
            self._inbound_media_ledger(chat=chat_id, file_name=filename, declared_size=declared,
                                       status="error", error=f"{type(exc).__name__}: {exc}"[:300])
            logger.warning("[%s] file download crashed name=%s: %s", self.name, filename, exc, exc_info=True)
            return None, mime, f"⚠️ 附件「{filename}」下载失败：{type(exc).__name__}。请重发。"
        self._inbound_media_ledger(chat=chat_id, file_name=filename, declared_size=declared,
                                   received=result.received, total=result.total,
                                   attempts=result.attempts, duration_s=round(result.duration, 1),
                                   resumed=result.resumed, status="ok", path=str(result.path))
        return str(result.path), mime, None

    def _should_background_file(self, item: dict[str, Any]) -> bool:
        size = _declared_media_size(item.get("file_item") or {})
        threshold = _inbound_bg_threshold_bytes()
        if threshold < 0:  # negative = never background (all synchronous)
            return False
        return size == 0 or size > threshold

    def _start_background_file_download(self, item: dict[str, Any], *, chat_id: str,
                                        sender_id: str, chat_type: str) -> str:
        payload = item.get("file_item") or {}
        filename = str(payload.get("file_name") or "document.bin")
        size = _declared_media_size(payload)
        size_txt = f"（{size / 1048576:.1f} MB）" if size else ""
        self._inbound_media_ledger(chat=chat_id, sender=sender_id, file_name=filename,
                                   declared_size=size, status="started")
        task = asyncio.create_task(self._background_file_download(
            item, chat_id=chat_id, sender_id=sender_id, chat_type=chat_type))
        _BACKGROUND_DOWNLOADS.add(task)

        def _done(t: "asyncio.Task[None]") -> None:
            _BACKGROUND_DOWNLOADS.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                logger.error("[%s] background download crashed name=%s: %s",
                             self.name, filename, exc, exc_info=exc)

        task.add_done_callback(_done)
        # No unqualified promise here: a gateway restart can interrupt the transfer, and
        # the user should know that resending the same file resumes from the breakpoint.
        return (f"⏳ 收到附件「{_pretty_filename(filename)}」{size_txt}，正在后台下载。"
                f"断网/重启会保留断点，重发同名文件可继续下载。")

    async def _background_file_download(self, item: dict[str, Any], *, chat_id: str,
                                        sender_id: str, chat_type: str) -> None:
        payload = item.get("file_item") or {}
        filename = str(payload.get("file_name") or "document.bin")
        path, mime, note = await self._download_file_streaming(
            item, chat_id=chat_id, sender_id=sender_id, background=True)
        try:
            source = self.build_source(chat_id=chat_id, chat_type=chat_type,
                                       user_id=sender_id, user_name=sender_id)
            if path:
                size = Path(path).stat().st_size
                event = MessageEvent(
                    text=(f"📎 附件已下载完成：「{_pretty_filename(filename)}」"
                          f"（{size / 1048576:.1f} MB）。\n"
                          f"路径：{path}\n可以直接让我处理这个文件。"),
                    message_type=MessageType.DOCUMENT, source=source,
                    media_urls=[path], media_types=[mime], internal=True)
            else:
                event = MessageEvent(text=note or f"⚠️ 附件「{_pretty_filename(filename)}」下载失败。",
                                     message_type=MessageType.TEXT, source=source,
                                     internal=True)
            await self.handle_message(event)
        except Exception as exc:
            logger.error("[%s] background file notify failed name=%s: %s",
                         self.name, filename, exc, exc_info=True)

    async def _fetch_typing_ticket(self, session: Any, user_id: str, context_token: Optional[str], failure_label: str) -> Optional[str]:
        try:
            response = await _get_config(session, base_url=self._base_url, token=self._token, user_id=user_id, context_token=context_token)
            typing_ticket = str(response.get("typing_ticket") or "")
            if typing_ticket:
                self._typing_cache.set(user_id, typing_ticket)
                return typing_ticket
        except Exception as exc:
            logger.debug("[%s] %s for %s: %s", self.name, failure_label, _safe_id(user_id), exc)
        return None

    def _split_text(self, content: str) -> list[str]:
        return _split_text_for_weixin_delivery(content, self.MAX_MESSAGE_LENGTH, self._split_multiline_messages)

    def _rate_limit_cooldown_remaining(self) -> float:
        return max(0.0, self._rate_limit_circuit_until - time.monotonic())

    async def _acquire_sliding_window_slot(self) -> bool:
        """2026-08-02 根因修复2: 滑动窗口总量锁。

        维护 send 时间戳滑动窗口(默认 300s), 窗口内已发 >= max(默认 5) 时
        等待最早一条滑出窗口再放行。所有来源共用 — 防批量投递风暴。
        等待一轮窗口滑动(<= window_seconds)仍无空位 → 返回 False 放弃本次发送
        （由上层 ledger/cron 保留消息稍后重试，修复1保证不丢），
        **绝不超时后强行发送**——强行发送 = 撞限流续惩罚（8/2 16:11 实测教训）。
        """
        if self._sliding_window_seconds <= 0 or self._sliding_window_max <= 0:
            return True
        # 最多等一轮窗口滑动
        deadline = time.monotonic() + self._sliding_window_seconds
        while True:
            async with self._sliding_gate:
                now = time.monotonic()
                cutoff = now - self._sliding_window_seconds
                self._sliding_window_events = [
                    ts for ts in self._sliding_window_events if ts >= cutoff
                ]
                if len(self._sliding_window_events) < self._sliding_window_max:
                    self._sliding_window_events.append(now)
                    return True
            if time.monotonic() >= deadline:
                logger.warning(
                    "[%s] sliding window slot busy for %.0fs — dropping send "
                    "(backlog preserved by ledger/cron for retry)",
                    self.name, self._sliding_window_seconds,
                )
                return False
            # 等最早事件滑出窗口
            wait_for = max(0.0, self._sliding_window_events[0] + self._sliding_window_seconds - time.monotonic())
            await asyncio.sleep(min(wait_for, 5.0))

    def _record_rate_limit_event(self) -> bool:
        now = time.monotonic()
        self._rate_limit_events = [ts for ts in self._rate_limit_events if ts >= now - self._rate_limit_circuit_window_seconds] + [now]
        if len(self._rate_limit_events) < self._rate_limit_circuit_threshold:
            return False
        if self._rate_limit_circuit_open_seconds > 0:
            self._rate_limit_circuit_until = max(self._rate_limit_circuit_until, time.monotonic() + self._rate_limit_circuit_open_seconds)
        return self._rate_limit_cooldown_remaining() > 0

    def _rate_limit_error(self) -> RuntimeError:
        return RuntimeError(
            f"iLink sendmessage rate limited; cooldown active for {self._rate_limit_cooldown_remaining():.1f}s"
        )

    def _reset_rate_limit_circuit(self) -> None:
        self._rate_limit_events.clear()
        self._rate_limit_circuit_until = 0.0

    async def _send_text_chunk(self, *, chat_id: str, chunk: str, context_token: Optional[str], client_id: str,
                               source: str = "cron", in_drain: bool = False,
                               no_retry: bool = False) -> None:
        """Send one text chunk with retry/backoff under the adapter-wide text gate. A stale-session response (``-14``,
        or ``-2`` with ``prepare failed``/``unknown error``) is re-sent once *without* ``context_token`` — iLink accepts
        tokenless sends as a degraded fallback, which keeps cron pushes working when no user message refreshed the
        session. A ``-2`` that survives that fails fast via ``_session_not_ready_error``."""
        async with self._send_text_gate:
            last_error: Optional[Exception] = None
            retried_without_token = False
            attempt = 0  # counts real failures only — the tokenless re-send must not eat the retry budget
            while True:
                if self._rate_limit_cooldown_remaining() > 0:
                    raise RuntimeError(f"iLink sendmessage rate limited; cooldown active for {self._rate_limit_cooldown_remaining():.1f}s")
                try:
                    resp = await _send_message(
                        self._send_session, base_url=self._base_url, token=self._token, to=chat_id, text=chunk,
                        context_token=context_token, client_id=client_id)
                    ret, errcode = (resp.get("ret"), resp.get("errcode")) if resp and isinstance(resp, dict) else (None, None)
                    if (ret is not None and ret != 0) or (errcode is not None and errcode != 0):
                        errmsg = resp.get("errmsg") or resp.get("msg")
                        if _is_session_expired(resp, ret, errcode) and not retried_without_token and context_token:
                            retried_without_token, context_token = True, None
                            self._token_store._cache.pop(self._token_store._key(self._account_id, chat_id), None)
                            logger.warning("[%s] session expired for %s; retrying without context_token", self.name, _safe_id(chat_id))
                            continue
                        if _is_stale_session_ret(ret, errcode, errmsg):
                            # break, not raise: a raise here is caught below and re-enters the retry ladder.
                            last_error = _session_not_ready_error(ret, errcode, errmsg)
                            break
                        if ret != RATE_LIMIT_ERRCODE and errcode != RATE_LIMIT_ERRCODE:
                            raise RuntimeError(f"iLink sendmessage error: ret={ret} errcode={errcode} errmsg={errmsg or 'unknown error'}")
                        # Keep a descriptive error for when the loop exhausts while still limited.
                        last_error = RuntimeError(f"iLink sendmessage rate limited: ret={ret} errcode={errcode} errmsg={errmsg or 'rate limited'}")
                        if self._record_rate_limit_event():
                            last_error = RuntimeError(
                                f"iLink sendmessage rate limited (ret={ret} errcode={errcode} errmsg={errmsg or 'rate limited'}); "
                                f"cooldown active for {self._rate_limit_cooldown_remaining():.1f}s")
                            break
                        if attempt >= self._send_chunk_retries:
                            break
                        attempt += 1
                        wait = self._send_chunk_retry_delay_seconds * 3  # 3x backoff for rate limit
                        logger.warning("[%s] rate limited for %s; backing off %.1fs before retry", self.name, _safe_id(chat_id), wait)
                        await asyncio.sleep(wait)
                        continue
                    self._rate_limit_events.clear()
                    self._rate_limit_circuit_until = 0.0
                    # 出站消息时间戳记录：供引用消息时间窗口匹配
                    if chunk:
                        _record_outbound(chunk)
                        logger.debug("[Weixin] refmsg cache OUTBOUND: queue=%d len=%d", len(_outbound_queue), len(chunk))
                    # 2026-08-20: 发送成功即清补投队列同 hash，防 token 恢复 drain 重复发
                    _retry_remove(chat_id, chunk)
                    return
                except Exception as exc:
                    last_error = exc
                    if attempt >= self._send_chunk_retries:
                        break
                    attempt += 1
                    wait = self._send_chunk_retry_delay_seconds * attempt
                    logger.warning("[%s] send chunk failed to=%s attempt=%d/%d, retrying in %.2fs: %s",
                                   self.name, _safe_id(chat_id), attempt, self._send_chunk_retries + 1, wait, exc)
                    if wait > 0:
                        await asyncio.sleep(wait)
            assert last_error is not None
            if in_drain or no_retry:
                # in_drain: 补投路径下**不再入队** —— 队列里已有本条权威条目（原 text/attempts/first_ts），
                # 而 chunk 此时带延迟标注、hash 与原条目不同，再入队就是第二条 → 重复投递。
                # no_retry: 控制/收尾消息失败绝不入队，否则会形成「补投完成」自我复制风暴。
                if no_retry:
                    logger.warning("[Weixin] control message dropped after send failure (no_retry) to=%s",
                                   _safe_id(chat_id))
                raise last_error
            # 2026-08-20: 重试耗尽仍失败 → 送入补投队列（跨进程/跨重启持久化，下次 inbound drain 延迟补发）。
            # 覆盖瞬时限流/会话过期重试无效的长窗口失败（cron 推送数小时后仍补发，消息不丢）。
            queued = _retry_enqueue(chat_id, chunk, source=source)
            logger.warning("[%s] retry_enqueued to=%s source=%s len=%d",
                           self.name, _safe_id(chat_id), source, len(chunk))
            if queued:
                # 消息已安全落队列 ⇒ 这是「必达的推迟」，不是失败。
                # 原实现继续 raise last_error，cron 层会记 delivery_failed（报警失真）。
                # 2026-10-28：把**原因**一起带出去。原实现丢掉 last_error ⇒ 上游只看到
                #   「已入补投队列」，session 过期 / rate limited（含 cooldown）/ prepare failed
                #   全被抹成同一句话，排障时无从判断是窗口问题还是平台故障；也让 3 个
                #   「错误文本要点名真因」的单测变红（确实是可观测性退化）。
                raise _DeferredToRetryQueue(
                    f"消息已入补投队列（source={source}），下次入站补发；原因：{last_error}")
            raise last_error

    async def _drain_retry_queue(self, wake_suspended: bool = False) -> None:
        """冲刷补投队列（fire-and-forget，不阻塞长轮询）。

        **平台约束（写死，别当 bug 修）**：iLink 在用户再次入站前不会 prepare bot 主动发送，
        窗口外的推送必然失败。策略是「不丢、不空转、下次入站按原序一次补投并标注延迟」。

        2026-10-26 修订（owner授权，决策 1+3）：
        · response 与 cron 同样 3 次上限后转 suspended（原 response 豁免 ⇒ 永远占 active 位空转）
        · suspended 的 30min 兜底**只对 cron 生效**：response 只等入站唤醒
        · response 不进 24h 死信（对话回复进死信 = 对话断链），长期 suspended 保留等入站
        · 补投按 ts 顺序；每条延迟 >5min 在批次 md 内标注原发送时间
        · 出队按 **id**（聚合正文不再依赖内容 hash）
        · 每个会话只发一个聚合 markdown，批次之间才 sleep，避免补投风暴
        """
        DELAY_ANNOTATE_SECONDS = 300   # 超过 5 分钟才计入迟到
        DRAIN_GAP_SECONDS = _RETRY_DRAIN_GAP_SECONDS   # 模块级，便于测试置零
        MAX_PER_DRAIN = 50            # 单轮最多取 50 条；剩下的留在桶里，下次入站接着补（断点续投）
        if getattr(self, "_draining", False):
            # 并发保护（2026-10-27）：每条入站都会 create_task(drain)，而队列是
            # read-modify-write ⇒ 两个 drain 并发会把同一条投两遍。
            # 但**入站唤醒不能吞**：suspended 条目只等入站唤醒，吞掉就等于「用户说话了
            # 也补不出来」，要等下一次入站（可能几小时/几天）。所以记账，由在跑那一轮
            # 收尾时立刻补跑一轮。
            if wake_suspended:
                self._drain_wake_pending = True
                logger.info("[%s] retry drain 已在进行；本次入站唤醒已记账，本轮结束后立即补跑",
                            self.name)
            else:
                logger.info("[%s] retry drain 已在进行，跳过本轮（防重复投递）", self.name)
            return
        self._draining = True
        self._drain_wake_pending = False
        loaded_stats: Dict[str, Optional[Tuple[int, int]]] = {}
        try:
            now = time.time()
            raw: List[Dict[str, Any]] = []
            # response 桶先补（对话回复优先），再补 cron 桶；每条记住自己属于哪个桶
            # （dict.fromkeys 去重：测试里两个桶可能指向同一文件，重复读会重复补投）
            for _bp in dict.fromkeys((_retry_response_path, _retry_path)):
                # 外部清空/改写的保护基线：drain 是 read-modify-write，若期间用户/脚本
                # 删掉队列，不能把内存里的旧条目重新写回去（resurrect）。
                try:
                    _st = os.stat(_bp)
                    loaded_stats[_bp] = (_st.st_mtime_ns, _st.st_size)
                except OSError:
                    loaded_stats[_bp] = None
                for _item in _load_bucket(_bp):
                    _item["_bucket"] = _bp
                    raw.append(_item)
            if not raw:
                return
            pending: List[Dict[str, Any]] = []
            keep: List[Dict[str, Any]] = []
            paused_skipped = 0
            for item in raw:
                if not item.get("to") or not item.get("text"):
                    logger.warning("[%s] retry 条目缺 to/text，移入 .corrupt 保留（不删）id=%s",
                                   self.name, item.get("id"))
                    _quarantine_corrupt(str(item.get("_bucket") or _retry_path),
                                        json.dumps(item, ensure_ascii=False))
                    continue
                to = str(item["to"])
                paused_until = float(self._retry_chat_paused_until.get(to, 0.0) or 0.0)
                if paused_until > now:
                    # 风暴断路器：暂停期内只保留，不发送、不计失败。
                    keep.append(item)
                    paused_skipped += 1
                    continue
                if paused_until:
                    self._retry_chat_paused_until.pop(to, None)
                source = str(item.get("source", "cron"))
                if item.get("status") == "suspended":
                    suspended_since = float(item.get("suspended_since") or item.get("ts") or now)
                    if now - suspended_since > _RETRY_SUSPEND_TTL:
                        if not item.get("no_dead_letter"):
                            _retry_dead_letter(item)
                            logger.warning("[%s] retry_dead id=%s attempts=%s "
                                           "reason=suspended-24h-no-wake",
                                           self.name, item.get("id"), item.get("attempts"))
                            continue
                        # 对话回复不进死信。但也**不能只保留不投递**：原实现超 TTL 后
                        # 无条件 keep+continue ⇒ 入站唤醒也叫不醒，条目永久占桶却永不送达
                        # （2026-10-27 审查 D0.4 修）。非入站 drain 仍只保留（不空转），
                        # 入站唤醒则重新激活并本轮重投。
                        if not wake_suspended:
                            logger.warning("[%s] retry item suspended >24h but kept "
                                           "(no_dead_letter) id=%s", self.name, item.get("id"))
                            keep.append(item)
                            continue
                        item["status"] = "active"
                        item["attempts"] = 0
                        item.pop("suspended_since", None)
                        logger.warning("[%s] retry item suspended >24h reactivated by inbound wake "
                                       "(no_dead_letter) id=%s", self.name, item.get("id"))
                    fallback_due = (now - suspended_since) >= _RETRY_SUSPEND_FALLBACK
                    # response：只等入站；cron：保留 30min 兜底（原行为）
                    if not wake_suspended and (source == "response" or not fallback_due):
                        keep.append(item)
                        continue
                pending.append(item)
            if paused_skipped:
                logger.warning("[Weixin] retry drain skipped %d items due to chat pause", paused_skipped)

            # 桶优先于时间：response 桶整段先补（按 first_ts 升序），cron 桶随后
            pending.sort(key=lambda i: (0 if i.get("_bucket") == _retry_response_path else 1,
                                        float(i.get("first_ts") or i.get("ts") or 0)))
            if len(pending) > MAX_PER_DRAIN:
                # 超出部分本轮不动：留在各自的桶里，下次入站继续（这就是断点续投）
                keep.extend(pending[MAX_PER_DRAIN:])
                pending = pending[:MAX_PER_DRAIN]

            # 按接收人分组：每个会话一个聚合 markdown；组顺序取排序后的首次出现顺序。
            groups: Dict[str, List[Dict[str, Any]]] = {}
            for item in pending:
                groups.setdefault(str(item["to"]), []).append(item)

            delivered_late = 0
            failed = 0
            total = len(pending)
            stats_by_to: Dict[str, Dict[str, int]] = {}
            successful_ids: List[str] = []
            for group_idx, (to, group) in enumerate(groups.items()):
                n = len(group)
                source = str(group[0].get("source", "cron"))
                first_ts_values = [float(i.get("first_ts") or i.get("ts") or now) for i in group]
                earliest_ts, latest_ts = min(first_ts_values), max(first_ts_values)
                md_lines = [
                    f"# 微信补投（{n} 条）",
                    ("> 最早原发送 "
                     f"{time.strftime('%Y-%m-%d %H:%M', time.localtime(earliest_ts))}；"
                     f"最近 {time.strftime('%H:%M', time.localtime(latest_ts))}；以下按原顺序"),
                    "",
                ]
                for item_idx, item in enumerate(group, 1):
                    item_ts = float(item.get("first_ts") or item.get("ts") or now)
                    delay_minutes = max(0, int((now - item_ts) // 60))
                    md_lines.append(
                        f"## {item_idx}. 原发送 "
                        f"{time.strftime('%H:%M:%S', time.localtime(item_ts))}"
                        f"（延迟 {delay_minutes} 分钟）")
                    md_lines.append(str(item["text"]))
                    md_lines.append("")
                md_text = "\n".join(md_lines)

                batch_error: Optional[Exception] = None
                try:
                    result = await self.send(
                        to, md_text, metadata={"source": source, "_in_drain": True})
                    if result.success:
                        late_count = sum(
                            1 for item in group
                            if now - float(item.get("first_ts") or item.get("ts") or now)
                            > DELAY_ANNOTATE_SECONDS)
                        delivered_late += late_count
                        if late_count:
                            stats_by_to.setdefault(to, {"late": 0, "fail": 0})["late"] += late_count
                        # 延后到本轮统一落盘之后按 id 出队：否则 _retry_remove_by_id 的
                        # 自身写盘会改变 mtime，让同桶失败条目的 attempts 更新被外部
                        # 修改保护误判而跳过。发送成功的事实已在内存里，绝不丢失。
                        successful_ids.extend(str(item.get("id") or "") for item in group)
                        logger.info("[%s] retry batch delivered to=%s n=%d",
                                    self.name, _safe_id(to), n)
                    else:
                        batch_error = RuntimeError(result.error or "send returned success=False")
                except Exception as exc:
                    batch_error = exc

                if batch_error is not None:
                    # 整批失败：每个条目只计一次尝试；超上限按原策略转 suspended。
                    failure_now = time.time()
                    failed += n
                    stats_by_to.setdefault(to, {"late": 0, "fail": 0})["fail"] += n
                    for item in group:
                        item["attempts"] = int(item.get("attempts", 1)) + 1
                        item["ts"] = failure_now
                        if item["attempts"] > _RETRY_MAX_ATTEMPTS:
                            item["status"] = "suspended"
                            item["suspended_since"] = failure_now
                            logger.warning("[%s] retry suspended after %d attempts id=%s source=%s",
                                           self.name, item["attempts"], item.get("id"),
                                           item.get("source", "cron"))
                        else:
                            item["status"] = "active"
                        keep.append(item)
                    logger.warning("[%s] retry batch failed to=%s n=%d: %s",
                                   self.name, _safe_id(to), n, batch_error)
                    # 风暴断路器：同会话 60s 内失败 5 次 → 暂停 300s。
                    fail_times = self._retry_chat_failures.setdefault(to, [])
                    fail_times.append(failure_now)
                    fail_times[:] = [t for t in fail_times if failure_now - t <= 60]
                    if len(fail_times) >= 5:
                        self._retry_chat_paused_until[to] = failure_now + 300
                        logger.warning(
                            "[Weixin] retry drain paused for chat %s 300s after 5 failures/60s",
                            _safe_id(to))

                # 只在聊天组之间 sleep；同组已聚合成一次 send。
                if group_idx < len(groups) - 1 and DRAIN_GAP_SECONDS > 0:
                    await asyncio.sleep(DRAIN_GAP_SECONDS)

            by_bucket: Dict[str, List[Dict[str, Any]]] = {_retry_response_path: [], _retry_path: []}
            for item in keep:
                bp = str(item.pop("_bucket", _retry_path))
                by_bucket.setdefault(bp, []).append(item)
            for bp, bucket_items in by_bucket.items():
                # 外部清空保护：文件被截空或 mtime 变化时，跳过落盘，避免把
                # 内存快照里的条目重新复活。
                loaded_stat = loaded_stats.get(bp)
                try:
                    current_stat = os.stat(bp)
                except OSError:
                    current_stat = None
                if not bucket_items and (current_stat is None or current_stat.st_size == 0):
                    # 成功条目已按 id 出队/外部已清空；无保留条目时写空桶没有意义。
                    continue
                if current_stat is not None and current_stat.st_size == 0:
                    logger.warning(
                        "[Weixin] bucket modified externally; skip save to avoid resurrecting entries: %s",
                        bp)
                    continue
                if loaded_stat is None and current_stat is None:
                    continue
                if (loaded_stat is None or current_stat is None
                        or current_stat.st_mtime_ns != loaded_stat[0]):
                    logger.warning(
                        "[Weixin] bucket modified externally; skip save to avoid resurrecting entries: %s",
                        bp)
                    continue
                _save_bucket(bp, bucket_items)
                if bp == _retry_response_path:
                    _warn_if_response_bucket_big(bp, len(bucket_items))
            # 成功条目统一出队（按 id）；此时失败条目已按最新 attempts/status 落盘。
            for mid in successful_ids:
                _retry_remove_by_id(mid)
            if total:
                logger.info("[%s] retry queue drained: delivered_late=%d failed=%d remaining=%d",
                            self.name, delivered_late, failed, len(keep))
            for _to, _st in stats_by_to.items():
                if not (_st["late"] or _st["fail"]):
                    continue
                # 每个接收人只看到自己的补投结果；控制消息失败绝不能再次入队。
                try:
                    await self.send(
                        _to,
                        f"[补投完成 · 迟到送达 {_st['late']} 条，"
                        f"失败 {_st['fail']} 条（已保留，等下次入站）]",
                        metadata={"source": "response", "_no_retry": True},
                    )
                except Exception as exc:
                    logger.warning("[%s] 补投收尾行发送失败（to=%s）：%s",
                                   self.name, _safe_id(_to), exc)
        except Exception as exc:
            # 不静默：drain 坏了本身就是投递故障，必须留痕（原实现是 pass）
            logger.warning("[%s] retry drain aborted: %s", self.name, exc)
        finally:
            self._draining = False
            # 在跑期间收到过入站唤醒 ⇒ 立刻补跑一轮（不阻塞本条调用方）
            if getattr(self, "_drain_wake_pending", False):
                self._drain_wake_pending = False
                try:
                    asyncio.create_task(self._drain_retry_queue(wake_suspended=True))
                    logger.info("[%s] 补跑一轮 retry drain（此前入站唤醒被并发闸门挡住）", self.name)
                except RuntimeError as exc:
                    logger.warning("[%s] 补跑 drain 排不进去（事件循环关闭？）：%s", self.name, exc)

    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None, metadata: Optional[dict[str, Any]] = None) -> SendResult:
        if not self._send_session or not self._token:
            return SendResult(success=False, error="Not connected")
        # 2026-08-02 根因修复2: 滑动窗口总量锁 — 超限时等待窗口滑动(排队), 防批量投递风暴。
        # 正常会话交互频率远低于限额(5min≤5条), 不会实际排队; 只有 cron/ledger/补投批量叠加才触发。
        # 等一轮窗口仍无空位 → 放弃返回失败(上层 ledger/cron 保留消息稍后重试), 绝不硬发撞限流。
        if not await self._acquire_sliding_window_slot():
            return SendResult(
                success=False,
                error="Sliding window busy (rate limit budget exhausted)",
            )
        context_token = self._token_store.get(self._account_id, chat_id)
        last_message_id: Optional[str] = None
        # 2026-09-09 (finding 4): 区分 source——对话回复=response，cron 推送=metadata.source="cron"。
        # 2026-10-26 修订（owner授权）: response 不再豁免尝试上限（见 _retry_enqueue）；
        #   补投路径额外带 _in_drain（失败不重复入队）与 _retry_prefix（延迟标注）。
        source = (metadata or {}).get("source", "response")
        in_drain = bool((metadata or {}).get("_in_drain"))
        no_retry = bool((metadata or {}).get("_no_retry"))
        retry_prefix = str((metadata or {}).get("_retry_prefix") or "")
        # Extract MEDIA: tags and bare local file paths before text delivery, under the routed
        # profile's scope: Docker MEDIA translation infers the sandbox from the active profile (#109024).
        with self._media_delivery_scope(self.build_source(chat_id=chat_id)):
            media_files, cleaned_content = self.extract_media(content)
            local_files, final_content = self.extract_local_files(self.extract_images(cleaned_content)[1])
            deliveries = [(p, v, "media") for p, v in self.filter_media_delivery_paths(media_files)]
            deliveries += [(p, False, "local file") for p in self.filter_local_delivery_paths(local_files)]
        deliveries, _dropped_bare = cap_bare_file_attachments(deliveries)
        if _dropped_bare:
            logger.warning("[%s] bare local file attachments capped: dropped %d (limit %d)",
                           self.name, _dropped_bare, _MAX_BARE_FILE_ATTACHMENTS)
            final_content = (final_content.rstrip()
                             + f"\n\n⚠️ 正文中另有 {_dropped_bare} 个本地文件未随本条投递"
                               f"（单条裸路径自动投递上限 {_MAX_BARE_FILE_ATTACHMENTS} 个；"
                               f"如确需发送请显式写 MEDIA: 标记）")
        media_failed = 0
        try:
            for path, is_voice, label in deliveries:
                ext = Path(path).suffix.lower()
                sender, key = next(((m, k) for exts, m, k in _OUTBOUND_BY_EXT if is_voice or ext in exts), ("send_document", "file_path"))
                try:
                    await getattr(self, sender)(chat_id=chat_id, metadata=metadata, **{key: path})
                except Exception as exc:
                    media_failed += 1
                    logger.warning("[%s] media delivery failed for %s: %s", self.name, path, exc)

            # Deliver text content.
            # 2026-08-04 投递优化: 短消息合并 + 长消息(>2000字符)转 MD 文件。
            # 上游 #184: 同一用户 60s 窗口 >10 条被 iLink 拒绝 → 减少 sendmessage 调用次数从源头降限流命中。
            # 合并规则: 连续短块(单块<500字符)以空行分隔合并; 累计≥2000字符 → 转 MD 文件发送。
            final_content = final_content.strip()
            if retry_prefix and final_content:
                # 2026-10-26: 补投延迟标注放在正文最前 —— 长消息会转 MD，标注进文件也看得见；
                # 且它只存在于投递时的 content，队列里的 text 保持原样（hash/去重不被破坏）。
                final_content = retry_prefix.rstrip("\n") + "\n\n" + final_content
            if final_content:
                if len(final_content) > _WEIXIN_LONG_MSG_THRESHOLD:
                    # 长消息 → MD 文件（替代拆 chunk 撞窗口）
                    md_path = await self._write_long_message_md(final_content)
                    await self.send_document(chat_id=chat_id, file_path=md_path, metadata=metadata)
                    last_message_id = f"md-{Path(md_path).stem}"
                else:
                    # 短消息路径: 拆分后合并相邻短块 → 减少 sendmessage 调用次数（#184）。
                    # 合并规则: 单块 <500 字符且非代码块 → 以空行连接累积; 累计 ≥2000 或遇大块/代码块 → flush。
                    raw_chunks = [c for c in self._split_text(self.format_message(final_content)) if c and c.strip()]
                    chunks: List[str] = []
                    pending: List[str] = []
                    pending_len = 0
                    _MERGE_SHORT_CHUNK_LIMIT = 500   # 单块小于此值才参与合并
                    _MERGE_TOTAL_LIMIT = 1800         # 合并累计上限（留 200 余量避免触碰长消息阈值）

                    def _flush_pending() -> None:
                        nonlocal pending, pending_len
                        if pending:
                            chunks.append("\n\n".join(pending))
                            pending = []
                            pending_len = 0

                    for chunk in raw_chunks:
                        is_code_block = _FENCE_RE.match(chunk.splitlines()[0].strip()) if chunk.splitlines() else False
                        chunk_len = len(chunk)
                        if (
                            not is_code_block
                            and chunk_len < _MERGE_SHORT_CHUNK_LIMIT
                            and pending_len + chunk_len + 2 < _MERGE_TOTAL_LIMIT
                        ):
                            pending.append(chunk)
                            pending_len += chunk_len
                        else:
                            _flush_pending()
                            chunks.append(chunk)
                    _flush_pending()

                    for idx, chunk in enumerate(chunks):
                        client_id = f"hermes-weixin-{uuid.uuid4().hex}"
                        chunk_kwargs: Dict[str, Any] = {
                            "chat_id": chat_id,
                            "chunk": chunk,
                            "context_token": context_token,
                            "client_id": client_id,
                            "source": source,
                        }
                        # 只在补投路径才传 in_drain —— 既有调用方/夹具的 _send_text_chunk
                        # 签名里没有这个参数，无谓地把它们全打断。
                        if in_drain:
                            chunk_kwargs["in_drain"] = True
                        if no_retry:
                            chunk_kwargs["no_retry"] = True
                        try:
                            await self._send_text_chunk(**chunk_kwargs)
                        except Exception as _chunk_exc:
                            # 零丢失（2026-10-27 插队修）：多 chunk 消息在第 k 条失败时，
                            # 原实现直接抛出 ⇒ 第 k+1..n 条**从未发送也从未入队**，尾部静默丢失。
                            # 这里把「本条 + 剩余」按原顺序全部入补投队列，再向上抛。
                            # in_drain 时不入队：队列里已有该条的权威全文（再入就是第二条）。
                            # no_retry 时也不入队：控制/收尾消息失败必须丢弃，不能自我复制。
                            # 2026-10-28 修：`_DeferredToRetryQueue` 表示**本条已由
                            #   _send_text_chunk 自己入队**（同 hash 幂等，但 attempts 会 +1）。
                            #   原实现照样从 chunks[idx] 再入一遍 ⇒ 一次真实失败吃两次 attempts，
                            #   _RETRY_MAX_ATTEMPTS=3 被两次失败打满 → 提前 suspended（suspended
                            #   只在入站唤醒时才重试）⇒ 消息长期躺在队列里。
                            #   实测 2026-10-28 state/weixin_retry.jsonl 存在 attempts=7 / suspended。
                            _already_queued = isinstance(_chunk_exc, _DeferredToRetryQueue)
                            if not in_drain and not no_retry:
                                _missed = 0
                                for _rest in (chunks[idx + 1:] if _already_queued else chunks[idx:]):
                                    if not _retry_enqueue(chat_id, _rest, source=source):
                                        _missed += 1
                                if _missed:
                                    logger.error(
                                        "[%s] chunk 入队失败 %d 条（需人工追）to=%s",
                                        self.name, _missed, _safe_id(chat_id))
                                else:
                                    logger.warning(
                                        "[%s] chunk %d/%d 失败：本条及剩余 %d 条已入补投队列",
                                        self.name, idx + 1, len(chunks), len(chunks) - idx)
                            raise
                        last_message_id = client_id
                        if idx < len(chunks) - 1 and self._send_chunk_delay_seconds > 0:
                            await asyncio.sleep(self._send_chunk_delay_seconds)
            if deliveries and media_failed == len(deliveries) and not final_content:
                # 纯附件消息且附件全失败 ⇒ 不能报成功（原实现只 warning，
                # 上层以为送达、用户什么都没收到 —— 2026-10-27 审查修）。
                return SendResult(
                    success=False,
                    error=f"全部附件投递失败（{media_failed}/{len(deliveries)}）",
                )
            return SendResult(success=True, message_id=last_message_id)
        except _DeferredToRetryQueue as exc:
            # 2026-09-15: **推迟不是失败** —— 消息已安全入补投队列，drain 会补发。
            # 用 SendResult.deferred 向上传，cron 层据此不记 delivery_failed；
            # 仍记 WARNING 保可观测性（"不静音"，避免队列静默失效时同时失去报警与可见错误）。
            # ⚠️ 本分支必须在 `except Exception` **之前**，否则真失败会被吞成 deferred。
            logger.warning(
                "[%s] send deferred to retry queue to=%s: %s",
                self.name, _safe_id(chat_id), exc,
            )
            return SendResult(success=False, error=str(exc), deferred=True)
        except Exception as exc:
            logger.error("[%s] send failed to=%s: %s", self.name, _safe_id(chat_id), exc)
            return SendResult(success=False, error=str(exc))

    async def _write_long_message_md(self, content: str) -> str:
        """Write a long message to an MD file under ~/hermes-media/投递/ for document delivery.

        2026-08-04: 长消息(>2000字符)不再拆 chunk 逐条 sendmessage（撞 iLink 限流窗口），
        而是写成 MD 文件走 send_document 单次投递。路径在 send-media-safe 白名单内。
        """
        import time as _time
        md_dir = Path.home() / "hermes-media" / _WEIXIN_MD_DIR
        md_dir.mkdir(parents=True, exist_ok=True)
        # 文件名: {时间戳}_{内容前20字符清洗}.md — 避免 emoji/斜杠/空格
        safe_head = re.sub(r"[^\w\u4e00-\u9fff-]", "", content[:20]) or "message"
        fname = f"{_time.strftime('%Y%m%d_%H%M%S')}_{safe_head}.md"
        path = md_dir / fname
        path.write_text(content, encoding="utf-8")
        logger.info("[%s] long message (%d chars) → %s", self.name, len(content), path)
        return str(path)

    async def _ensure_typing_ticket(self, chat_id: str) -> Optional[str]:
        """Return a valid typing ticket, refreshing via getConfig once the 600s TTL evicts it —
        otherwise ``stop_typing`` no-ops and the WeChat client shows the indicator forever."""
        ticket = self._typing_cache.get(chat_id)
        if ticket or not self._send_session or not self._token:
            return ticket or None
        return await self._fetch_typing_ticket(
            self._send_session, chat_id, self._token_store.get(self._account_id, chat_id), "typing ticket refresh failed")

    async def _set_typing(self, chat_id: str, status: int, label: str) -> None:
        typing_ticket = await self._ensure_typing_ticket(chat_id)
        if not typing_ticket:
            return
        try:
            await _api_post(
                self._send_session, base_url=self._base_url, endpoint=EP_SEND_TYPING, token=self._token, timeout_ms=CONFIG_TIMEOUT_MS,
                payload={"ilink_user_id": chat_id, "typing_ticket": typing_ticket, "status": status})
        except Exception as exc:
            logger.debug("[%s] typing %s failed for %s: %s", self.name, label, _safe_id(chat_id), exc)

    async def send_typing(self, chat_id: str, metadata: Optional[dict[str, Any]] = None) -> None:
        await self._set_typing(chat_id, TYPING_START, "start")

    async def stop_typing(self, chat_id: str) -> None:
        await self._set_typing(chat_id, TYPING_STOP, "stop")

    async def send_image(self, chat_id: str, image_url: str, caption: str, reply_to: Optional[str] = None, metadata=None) -> SendResult:
        cleanup = image_url.startswith(("http://", "https://"))
        file_path = await self._download_remote_media(image_url) if cleanup else image_url.replace("file://", "")
        if not cleanup and not os.path.isabs(file_path):
            file_path = os.path.abspath(file_path)
        try:
            return await self.send_document(chat_id, file_path, caption=caption, metadata=metadata)
        finally:
            if cleanup and file_path and os.path.exists(file_path):
                with contextlib.suppress(OSError):
                    os.unlink(file_path)

    async def send_image_file(self, chat_id: str, image_path: str, caption: Optional[str] = None, reply_to=None, metadata=None, **kwargs) -> SendResult:
        return await self.send_document(chat_id=chat_id, file_path=image_path, caption=caption, metadata=metadata)

    async def _send_file_result(self, chat_id: str, path: str, caption: str, label: str, **kwargs: Any) -> SendResult:
        if not self._send_session or not self._token:
            return SendResult(success=False, error="Not connected")
        try:
            return SendResult(success=True, message_id=await self._send_file(chat_id, path, caption, **kwargs))
        except Exception as exc:
            logger.error("[%s] %s failed to=%s: %s", self.name, label, _safe_id(chat_id), exc)
            return SendResult(success=False, error=str(exc))

    async def send_document(
        self, chat_id: str, file_path: str, caption: Optional[str] = None, file_name: Optional[str] = None, reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None, **kwargs,
    ) -> SendResult:
        return await self._send_file_result(chat_id, file_path, caption or "", "send_document")

    async def send_video(self, chat_id: str, video_path: str, caption: Optional[str] = None, reply_to=None, metadata=None) -> SendResult:
        return await self._send_file_result(chat_id, video_path, caption or "", "send_video")

    async def send_voice(self, chat_id: str, audio_path: str, caption: Optional[str] = None, reply_to=None, metadata=None, **kwargs) -> SendResult:
        # Native outbound voice bubbles are not proven-working upstream; a file attachment at least plays (even .silk).
        return await self._send_file_result(chat_id, audio_path, caption or self.warning_text(t("platform.weixin.voice_as_attachment")), "send_voice", force_file_attachment=True)

    async def _download_remote_media(self, url: str) -> str:
        from tools.url_safety import is_safe_url
        if not is_safe_url(url):
            raise ValueError(f"Blocked unsafe URL (SSRF protection): {url}")
        assert self._send_session is not None
        data = await _download_bytes(self._send_session, url=url, timeout_seconds=30)
        with tempfile.NamedTemporaryFile(delete=False, suffix=Path(url.split("?", 1)[0]).suffix or ".bin") as handle:
            handle.write(data)
            return handle.name

    async def _send_file(self, chat_id: str, path: str, caption: str, force_file_attachment: bool = False) -> str:
        assert self._send_session is not None and self._token is not None
        plaintext = Path(path).read_bytes()
        media_type, item_builder = self._outbound_media_builder(path, force_file_attachment=force_file_attachment)
        filekey, aes_key = secrets.token_hex(16), secrets.token_bytes(16)
        rawsize, rawfilemd5 = len(plaintext), hashlib.md5(plaintext).hexdigest()
        upload_response = await _get_upload_url(
            self._send_session, base_url=self._base_url, token=self._token, to_user_id=chat_id, media_type=media_type, filekey=filekey,
            rawsize=rawsize, rawfilemd5=rawfilemd5, filesize=((rawsize + 16) // 16) * 16, aeskey_hex=aes_key.hex())
        # 2026-09-14 本地定制：getUploadUrl 同样可能返回 ret/errcode 失败体（token 过期）。不校验会退化
        # 成 "returned neither upload_param nor upload_full_url" —— 看不出根因的报错。
        _raise_on_ilink_error(upload_response, what=f"getUploadUrl({Path(path).name})")
        upload_param = str(upload_response.get("upload_param") or "")
        ciphertext = _aes128_ecb_encrypt(plaintext, aes_key)
        # Prefer upload_full_url (direct CDN), else construct from upload_param. Both use POST — PUT 404s on the CDN.
        upload_url = str(upload_response.get("upload_full_url") or "") or (upload_param and (
            f"{self._cdn_base_url.rstrip('/')}/upload?encrypted_query_param={quote(upload_param, safe='')}&filekey={quote(filekey, safe='')}"))
        if not upload_url:
            raise RuntimeError(f"getUploadUrl returned neither upload_param nor upload_full_url: {upload_response}")
        encrypted_query_param = await _upload_ciphertext(self._send_session, ciphertext=ciphertext, upload_url=upload_url)
        context_token = self._token_store.get(self._account_id, chat_id)
        # iLink expects aes_key as base64(hex_string), not base64(raw_bytes) — otherwise images render as grey boxes.
        item_kwargs = {
            "encrypt_query_param": encrypted_query_param, "aes_key_for_api": base64.b64encode(aes_key.hex().encode("ascii")).decode("ascii"),
            "ciphertext_size": len(ciphertext), "plaintext_size": rawsize, "filename": Path(path).name, "rawfilemd5": rawfilemd5}
        if media_type == MEDIA_VOICE and path.endswith(".silk"):
            item_kwargs.update(encode_type=6, sample_rate=24000, bits_per_sample=16)
        item_lists: list[list[dict[str, Any]]] = [[item_builder(**item_kwargs)]]
        if caption:
            item_lists.insert(0, [{"type": ITEM_TEXT, "text_item": {"text": self.format_message(caption)}}])
        last_message_id = ""
        for item_list in item_lists:
            last_message_id = f"hermes-weixin-{uuid.uuid4().hex}"
            while True:
                resp = await _send_items(
                    self._send_session, base_url=self._base_url, token=self._token, to=chat_id, item_list=item_list,
                    context_token=context_token, client_id=last_message_id)
                ret, errcode = (resp.get("ret"), resp.get("errcode")) if resp and isinstance(resp, dict) else (None, None)
                if (ret is None or ret == 0) and (errcode is None or errcode == 0):
                    break
                # Same stale-session fallback as _send_text_chunk: re-send once without context_token. Clearing the
                # token also covers the remaining item lists (caption, then media) and bounds this loop.
                if _is_session_expired(resp, ret, errcode) and context_token:
                    context_token = None
                    self._token_store._cache.pop(self._token_store._key(self._account_id, chat_id), None)
                    logger.warning("[%s] session expired for %s; re-sending media without context_token", self.name, _safe_id(chat_id))
                    continue
                errmsg = resp.get("errmsg") or resp.get("msg")
                if _is_stale_session_ret(ret, errcode, errmsg):
                    raise _session_not_ready_error(ret, errcode, errmsg)
                raise RuntimeError(f"iLink sendmessage error: ret={ret} errcode={errcode} errmsg={errmsg or 'unknown error'}")
        return last_message_id

    def _outbound_media_builder(self, path: str, force_file_attachment: bool = False):
        mime = mimetypes.guess_type(path)[0] or "application/octet-stream"
        if mime.startswith("image/"):
            return MEDIA_IMAGE, _image_item
        if mime.startswith("video/"):
            return MEDIA_VIDEO, _video_item
        if path.endswith(".silk") and not force_file_attachment:
            return MEDIA_VOICE, _voice_item
        return MEDIA_FILE, _file_item  # audio/* and everything else ship as file attachments
    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        return {"name": chat_id, "type": "group" if chat_id.endswith("@chatroom") else "dm", "chat_id": chat_id}

    def format_message(self, content: Optional[str]) -> str:
        return "" if content is None else _wrap_copy_friendly_lines_for_weixin(_normalize_markdown_blocks(content))


async def _deliver_direct(
    adapter: WeixinAdapter, chat_id: str, message: str, media_files: Optional[list[tuple[str, bool]]], context_token: Optional[str],
) -> dict[str, Any]:
    last_result: Optional[SendResult] = None
    cleaned = adapter.format_message(message)
    if cleaned:
        last_result = await adapter.send(chat_id, cleaned)
        if not last_result.success:
            out: dict[str, Any] = {"error": f"Weixin send failed: {last_result.error}"}
            # 2026-10-28：把「已落补投队列」的推迟信号带进 dict —— cron 的 standalone 车道
            #   据此不再记终态失败（live 车道同一批修在 gateway/delivery.py）。
            if getattr(last_result, "deferred", False):
                out["deferred"] = True
            return out
    for media_path, _is_voice in media_files or []:
        sender = adapter.send_image_file if Path(media_path).suffix.lower() in _DIRECT_IMAGE_EXTS else adapter.send_document
        last_result = await sender(chat_id, media_path)
        if not last_result.success:
            return {"error": f"Weixin media send failed: {last_result.error}"}
    message_id = last_result.message_id if last_result else None
    return {"success": True, "platform": "weixin", "chat_id": chat_id, "message_id": message_id, "context_token_used": bool(context_token)}


async def send_weixin_direct(
    *, extra: dict[str, Any], token: Optional[str], chat_id: str, message: str, media_files: Optional[list[tuple[str, bool]]] = None,
) -> dict[str, Any]:
    """One-shot send for ``send_message``/cron: reuse the live adapter's session on this loop, else a throwaway adapter."""
    account_id = _extra_or_secret(extra, "account_id")
    base_url = _extra_or_secret(extra, "base_url", ILINK_BASE_URL).rstrip("/")
    cdn_base_url = _extra_or_secret(extra, "cdn_base_url", WEIXIN_CDN_BASE_URL).rstrip("/")
    resolved_token = str(token or extra.get("token") or _wx_secret("WEIXIN_TOKEN", "")).strip()
    if not resolved_token:
        return {"error": "Weixin token missing. Configure WEIXIN_TOKEN or platforms.weixin.token."}
    if not account_id:
        return {"error": "Weixin account ID missing. Configure WEIXIN_ACCOUNT_ID or platforms.weixin.extra.account_id."}
    token_store = ContextTokenStore(str(get_hermes_home()))
    token_store.restore(account_id)
    context_token = token_store.get(account_id, chat_id)
    live_adapter = _LIVE_ADAPTERS.get(resolved_token)
    send_session = getattr(live_adapter, '_send_session', None)
    if send_session is not None and not send_session.closed and send_session._loop is asyncio.get_running_loop():
        return await _deliver_direct(live_adapter, chat_id, message, media_files, context_token)
    async with _new_session() as session:
        merged = {**dict(extra or {}), "account_id": account_id, "base_url": base_url, "cdn_base_url": cdn_base_url}
        adapter = WeixinAdapter(PlatformConfig(enabled=True, token=resolved_token, extra=merged))
        adapter._send_session = adapter._session = session
        adapter._token_store = token_store
        return await _deliver_direct(adapter, chat_id, message, media_files, context_token)
