"""Streaming, resumable inbound media download (2026-10-23, review-hardened).

Replaces the "read the whole body into memory under a fixed total timeout" path for
large inbound files. Driven by the 2026-10-23 incident: a 93.3 MB WeChat voice memo
was cut off at 60.0 s / 11.4 MB twice, the adapter swallowed the failure, and the
agent consequently reported "the file never reached the server".

Design (see ADR_20261023_微信入站大文件下载_流式断点续传.md):

* **Streaming** — download -> (optional) incremental AES-128-ECB decrypt -> disk, fixed
  buffer. Memory use is independent of file size.
* **No total timeout** — the request carries an explicit
  ``ClientTimeout(total=None, sock_read=None)``; aiohttp's 300 s default would otherwise
  silently reimpose exactly the deadline this module exists to remove. Protection is an
  *idle* timeout: healthy as long as bytes keep arriving.
* **No size cap** — the only hard stop is free disk space (checked up front *and* every
  64 MiB during the transfer).
* **Resumable** — ``.part`` + ``.json`` metadata keyed by (account, chat, sender, name,
  size); transient errors retry with ``Range`` + exponential backoff. A ``206`` reply is
  trusted only when ``Content-Range`` starts at exactly the requested offset; otherwise
  the transfer restarts from zero (a mismatched offset would splice two regions into a
  plausible-length but corrupt file).
* **Fail-closed** — an AES key that cannot be parsed is an error, never "download as-is";
  a response without a declared length is an error, never "assume complete"; PKCS#7
  unpadding happens only on a provably complete stream.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import re
import shutil
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

try:  # cryptography ships with the gateway; keep the module importable without it
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
except Exception:  # pragma: no cover - exercised only on broken installs
    Cipher = None  # type: ignore[assignment]
    algorithms = None  # type: ignore[assignment]
    modes = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

AES_BLOCK = 16
DEFAULT_IDLE_TIMEOUT_S = 120.0
DEFAULT_MAX_ATTEMPTS = 300
# The WeChat CDN commonly drops a connection every 5-20 s (~5 MB at 300 KB/s). Resuming is
# cheap, so the stop condition is *lack of progress*, not an attempt count.
MAX_STALLED_ATTEMPTS = 5
BACKOFF_S: tuple[float, ...] = (2, 4, 8, 8, 15, 15, 30, 30, 60, 60)
DISK_MARGIN_BYTES = 1024 ** 3
DISK_RECHECK_BYTES = 64 * 1024 * 1024
META_SAVE_INTERVAL_S = 2.0
META_SAVE_EVERY_BYTES = 4 * 1024 * 1024
PROGRESS_MIN_INTERVAL_S = 5.0
PARTS_TTL_SECONDS = 24 * 3600
MAX_NAME_BYTES = 120
MAX_CONCURRENT_DOWNLOADS = 2

# Process-wide cap: N concurrent multi-hundred-MB streams would otherwise each pass the
# disk check against the same free space and saturate the link.
_DOWNLOAD_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

ProgressCb = Callable[[int, int, float], Optional[Awaitable[None]]]
UrlValidator = Callable[[str], None]

_URL_QUERY_RE = re.compile(r"(https?://[^\s'\"]+?)\?[^\s'\"]*")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
_CONTENT_RANGE_RE = re.compile(r"\s*bytes\s+(\d+)-(\d+)/(\d+|\*)\s*$")
_WINDOWS_RESERVED = {
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}


# --------------------------------------------------------------------------- errors
class DownloadError(Exception):
    """Terminal download failure carrying enough context to explain itself."""

    def __init__(
        self,
        message: str,
        *,
        status: str = "failed",
        received: int = 0,
        total: int = 0,
        attempts: int = 1,
        part_path: Optional[Path] = None,
    ) -> None:
        super().__init__(_redact(message))
        self.status = status
        self.received = received
        self.total = total
        self.attempts = attempts
        self.part_path = part_path

    def user_message(self, filename: str) -> str:
        pct = f"（{100 * self.received / self.total:.0f}%）" if self.total else ""
        head = {
            "disk_full": "磁盘空间不足",
            "http_4xx": "文件链接已失效",
            "bad_key": "解密参数异常",
            "no_length": "服务端未提供文件长度",
        }.get(self.status, "连接中断")
        got = f"已下载 {_human(self.received)}" + (f" / {_human(self.total)}" if self.total else "") + pct
        # Only promise resume when a partial file survives (4xx / bad key discard it).
        tail = ("断点已保留，重发同一文件可从断点继续。"
                if self.part_path and self.status not in {"http_4xx", "bad_key", "disk_full"}
                else "请重新发送。")
        return f"附件「{_pretty_name(filename)}」下载失败：{head}，{got}，共尝试 {self.attempts} 次。{tail}"


class _Retryable(Exception):
    """Internal marker: the attempt failed in a way worth resuming."""


class _Restart(Exception):
    """Internal marker: drop the partial file and start over from byte 0."""


@dataclass
class DownloadResult:
    path: Path
    received: int
    total: int
    attempts: int
    resumed: bool
    duration: float


# --------------------------------------------------------------------------- helpers
def _human(n: int) -> str:
    value = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{value:.1f} GB"


def _redact(text: str) -> str:
    """Strip URL query strings (signed ``encrypted_query_param``) from logs/ledger text."""
    return _URL_QUERY_RE.sub(r"\1?<redacted>", str(text))


def _pretty_name(filename: str, limit: int = 80) -> str:
    cleaned = "".join(ch for ch in str(filename) if ch.isprintable())
    return cleaned[:limit] or "附件"


def part_identity(filename: str, size: int, *, account: str = "", chat: str = "",
                  sender: str = "") -> str:
    """Stable resume key: same account+chat+sender+name+size => same partial file.

    Known boundary: two *different* files with identical name and byte size from the
    same sender in the same chat share a part. WeChat file names carry per-send
    uniqueness in practice, and the size-mismatch guard catches the common case.
    """
    raw = f"{account}\x00{chat}\x00{sender}\x00{filename}\x00{size}".encode("utf-8", "replace")
    return hashlib.sha1(raw).hexdigest()[:20]


def _parts_dir(dest_dir: Path) -> Path:
    path = Path(dest_dir) / ".parts"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _free_bytes(path: Path) -> int:
    try:
        return shutil.disk_usage(str(path)).free
    except OSError as exc:
        logger.warning("[media_stream] disk_usage failed for %s: %s", path, exc)
        return 0


def _disk_guard(dest_dir: Path, remaining: Optional[int]) -> None:
    free = _free_bytes(dest_dir)
    need = (remaining or 0) + DISK_MARGIN_BYTES
    if free < need:
        raise DownloadError(
            f"insufficient disk space: free={free} need={need}", status="disk_full")


def _write_json_atomic(path: Path, payload: dict) -> None:
    tmp = path.with_suffix(path.suffix + f".{uuid.uuid4().hex[:8]}.tmp")
    try:
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _load_json(path: Path) -> Optional[dict]:
    """Return the JSON object at ``path`` or ``None`` when absent/corrupt."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except Exception as exc:
        logger.warning("[media_stream] unreadable meta %s: %s", path, exc)
        return None
    return data if isinstance(data, dict) else None


def cleanup_stale_parts(dest_dir: Path, ttl_seconds: int = PARTS_TTL_SECONDS,
                        *, exclude: Optional[str] = None) -> int:
    """Drop ``.part``/``.json`` pairs untouched for ``ttl_seconds``.

    ``exclude`` is the identity of the transfer about to run: it is never reclaimed even
    if its mtime is old — a resumed transfer must not be deleted by its own cleanup (that
    produced "tail-only file, reported ok" before this guard existed).
    """
    parts = Path(dest_dir) / ".parts"
    if not parts.is_dir():
        return 0
    now = time.time()
    removed = 0
    for entry in parts.iterdir():
        try:
            if exclude and entry.name.startswith(str(exclude)):
                continue
            if now - entry.stat().st_mtime <= ttl_seconds:
                continue
            entry.unlink()
            removed += 1
        except OSError:
            continue
    if removed:
        logger.info("[media_stream] cleaned %d stale partial(s) in %s", removed, parts)
    return removed


def parse_aes_key(aes_key_b64: str) -> bytes:
    """Decode an iLink media key.

    Accepts both wire formats in this codebase: raw 16-byte key, or 32 bytes of ASCII hex
    (``base64(key.hex())``). Anything else raises — silently skipping decryption would
    write ciphertext to disk and report success.
    """
    try:
        decoded = base64.b64decode(aes_key_b64)
    except Exception as exc:
        raise DownloadError(f"undecodable aes_key: {type(exc).__name__}", status="bad_key") from exc
    if len(decoded) == 16:
        return decoded
    if len(decoded) == 32:
        text = decoded.decode("ascii", errors="ignore")
        if len(text) == 32 and all(ch in "0123456789abcdefABCDEF" for ch in text):
            return bytes.fromhex(text)
    raise DownloadError(f"unexpected aes_key format ({len(decoded)} bytes)", status="bad_key")


class _StreamingAES:
    """Incremental AES-ECB decryption holding back the final block for PKCS#7 unpadding.

    ECB decrypts each block independently, so a resumed transfer can start a fresh
    decryptor at any 16-byte boundary. Plaintext length tracks ciphertext length exactly,
    which is what makes ``part_size == resume_offset`` hold.
    """

    def __init__(self, key: bytes) -> None:
        if Cipher is None or algorithms is None or modes is None:  # pragma: no cover
            raise DownloadError("cryptography is unavailable; cannot decrypt", status="failed")
        self._dec = Cipher(algorithms.AES(key), modes.ECB()).decryptor()
        self._hold = b""
        self._buf = b""

    def update(self, chunk: bytes) -> bytes:
        if not chunk:
            return b""
        # ``cryptography`` only accepts whole blocks, so buffer the remainder here.
        self._buf += chunk
        usable = len(self._buf) - (len(self._buf) % AES_BLOCK)
        if usable <= 0:
            return b""
        data, self._buf = self._buf[:usable], self._buf[usable:]
        out = self._dec.update(data)
        buf = self._hold + out
        if len(buf) <= AES_BLOCK:
            self._hold = buf
            return b""
        self._hold = buf[-AES_BLOCK:]
        return buf[:-AES_BLOCK]

    def finish(self, *, complete: bool) -> bytes:
        """Return the tail to write.

        Padding is stripped **only** when the caller proved the stream reached its
        declared length; on a truncated stream the trailing bytes are dropped (that block
        is re-fetched on resume) instead of being mis-read as padding.
        """
        leftover, held = self._buf, self._hold
        self._buf = b""
        self._hold = b""
        if not complete:
            logger.info("[media_stream] incomplete AES stream: dropping %d buffered bytes",
                        len(leftover) + len(held))
            return b""
        if leftover:
            raise DownloadError(
                f"ciphertext not block aligned ({len(leftover)} trailing bytes)",
                status="failed")
        buf = held + self._dec.finalize()
        if buf:
            pad = buf[-1]
            if 1 <= pad <= AES_BLOCK and buf.endswith(bytes([pad]) * pad):
                return buf[:-pad]
        return buf


def sanitize_name(filename: str) -> str:
    """Filesystem-safe basename: no separators/control chars, bounded UTF-8 length."""
    name = Path(str(filename or "")).name.replace("\x00", "")
    name = _CONTROL_RE.sub(" ", name).strip().strip(".")
    if not name or name in {".", ".."}:
        return "document"
    stem, dot, ext = name.rpartition(".")
    if not dot or len(ext) > 10:
        stem, ext = name, ""
    if stem.upper() in _WINDOWS_RESERVED:
        stem = f"_{stem}"
    suffix = f".{ext}" if ext else ""
    budget = max(MAX_NAME_BYTES - len(suffix.encode("utf-8")), 8)
    stem = stem.encode("utf-8")[:budget].decode("utf-8", errors="ignore") or "document"
    return f"{stem}{suffix}"


def _parse_content_range(value: Optional[str]) -> Optional[tuple[int, int, int]]:
    """``bytes start-end/total`` -> (start, end, total); ``*`` total becomes 0."""
    if not value:
        return None
    match = _CONTENT_RANGE_RE.match(value)
    if not match:
        return None
    total = 0 if match.group(3) == "*" else int(match.group(3))
    return int(match.group(1)), int(match.group(2)), total


# --------------------------------------------------------------------------- core
async def download_to_cache(
    session: Any,
    *,
    url: str,
    dest_dir: Path,
    filename: str,
    expected_size: int = 0,
    aes_key_b64: Optional[str] = None,
    idle_timeout: float = DEFAULT_IDLE_TIMEOUT_S,
    max_retries: int = DEFAULT_MAX_ATTEMPTS,
    progress_cb: Optional[ProgressCb] = None,
    identity: Optional[str] = None,
    url_validator: Optional[UrlValidator] = None,
) -> DownloadResult:
    """Download ``url`` into ``dest_dir`` with streaming decryption and Range resume.

    ``max_retries`` is the **total attempt count** (not "extra retries"). Raises
    :class:`DownloadError` on terminal failure.
    """
    if url_validator is not None:
        url_validator(url)
    async with _DOWNLOAD_SEMAPHORE:
        return await _download_locked(
            session, url=url, dest_dir=Path(dest_dir), filename=filename,
            expected_size=expected_size, aes_key_b64=aes_key_b64,
            idle_timeout=idle_timeout, max_retries=max(1, int(max_retries)),
            progress_cb=progress_cb, identity=identity, url_validator=url_validator,
        )


async def _download_locked(
    session: Any, *, url: str, dest_dir: Path, filename: str, expected_size: int,
    aes_key_b64: Optional[str], idle_timeout: float, max_retries: int,
    progress_cb: Optional[ProgressCb], identity: Optional[str],
    url_validator: Optional[UrlValidator] = None,
) -> DownloadResult:
    dest_dir.mkdir(parents=True, exist_ok=True)
    parts = _parts_dir(dest_dir)
    safe_name = sanitize_name(filename)
    key = identity or part_identity(safe_name, expected_size)
    part = parts / f"{key}.part"
    meta_path = parts / f"{key}.json"

    # Cleanup BEFORE reading the partial size, and never reclaim the transfer we are
    # about to resume.
    cleanup_stale_parts(dest_dir, exclude=key)

    meta = _load_json(meta_path) or {}
    written = part.stat().st_size if part.exists() else 0
    if expected_size and written > expected_size:
        part.unlink(missing_ok=True)
        written = 0
    total = int(expected_size or meta.get("total") or 0)
    key_bytes: Optional[bytes] = parse_aes_key(aes_key_b64) if aes_key_b64 else None
    if key_bytes and written % AES_BLOCK:
        part.unlink(missing_ok=True)
        written = 0
    resumed = written > 0

    attempts = 0
    stalled = 0
    last_error = "unknown"
    started = time.time()
    last_progress = 0.0
    last_meta_save = 0.0
    last_meta_bytes = 0
    last_disk_check = 0
    url_fp = hashlib.sha1(url.encode()).hexdigest()[:12]

    while attempts < max_retries:
        attempts += 1
        restart_requested = False
        written_before = written
        try:
            if part.exists():
                written = part.stat().st_size  # never trust the in-memory counter alone
            written_before = written
            consumed = written
            if attempts > 1 and consumed > 0:
                resumed = True
            _disk_guard(dest_dir, (total - consumed) if total else None)
            headers = {"Range": f"bytes={consumed}-"} if consumed else {}
            request_timeout = None
            if "aiohttp" in type(session).__module__:
                import aiohttp

                # aiohttp's default is ClientTimeout(total=300); leaving it in place would
                # silently restore the very deadline this module removes.
                request_timeout = aiohttp.ClientTimeout(
                    total=None, connect=None, sock_connect=30, sock_read=None)
            async with session.get(url, headers=headers, allow_redirects=True,
                                   timeout=request_timeout) as response:
                status = int(getattr(response, "status", 0) or 0)
                final_url = str(getattr(response, "url", "") or url)
                if final_url and final_url != url:
                    # Old path followed redirects (aiohttp default); keep that behaviour but
                    # re-assert the allowlist on the host we actually landed on.
                    if url_validator is not None:
                        try:
                            url_validator(final_url)
                        except Exception as exc:
                            raise DownloadError(
                                f"redirected off the CDN allowlist: {_redact(final_url)}",
                                status="http_4xx", received=written, total=total,
                                attempts=attempts, part_path=None) from exc
                if status in (301, 302, 303, 307, 308):
                    raise DownloadError(
                        f"unexpected redirect (HTTP {status})", status="http_4xx",
                        received=written, total=total, attempts=attempts,
                        part_path=None)
                if status == 416:
                    logger.info("[media_stream] HTTP 416 for %s, restarting from 0", safe_name)
                    part.unlink(missing_ok=True)
                    meta_path.unlink(missing_ok=True)
                    written = consumed = total = 0
                    resumed = False
                    raise _Restart("HTTP 416")
                if consumed and status == 200:
                    logger.info("[media_stream] range unsupported, restarting %s from 0", safe_name)
                    part.unlink(missing_ok=True)
                    written = consumed = total = 0
                    resumed = False
                    raise _Restart("server ignored Range")
                if status == 206 and consumed:
                    parsed = _parse_content_range(response.headers.get("Content-Range"))
                    if parsed is None or parsed[0] != consumed:
                        logger.warning(
                            "[media_stream] bad Content-Range %r for offset %d, restarting",
                            response.headers.get("Content-Range"), consumed)
                        part.unlink(missing_ok=True)
                        written = consumed = total = 0
                        resumed = False
                        raise _Restart("Content-Range mismatch")
                    if parsed[2]:
                        total = parsed[2]
                elif status >= 400:
                    if status in (408, 429) or status >= 500:
                        raise _Retryable(f"HTTP {status}")
                    part.unlink(missing_ok=True)
                    meta_path.unlink(missing_ok=True)
                    raise DownloadError(
                        f"HTTP {status}", status="http_4xx", received=written, total=total,
                        attempts=attempts, part_path=None)
                else:
                    response.raise_for_status()

                clen = 0
                try:
                    clen = int(response.headers.get("Content-Length") or 0)
                except ValueError:
                    clen = 0
                if clen:
                    total = consumed + clen if status == 206 else clen
                if not total:
                    part.unlink(missing_ok=True)
                    meta_path.unlink(missing_ok=True)
                    raise DownloadError(
                        "response carried no length", status="no_length",
                        received=written, total=0, attempts=attempts, part_path=None)

                aes = _StreamingAES(key_bytes) if key_bytes else None
                mode = "ab" if written else "wb"
                complete = False
                with open(part, mode) as fh:
                    while True:
                        try:
                            chunk = await asyncio.wait_for(
                                response.content.readany(), timeout=idle_timeout)
                        except asyncio.TimeoutError:
                            raise _Retryable(
                                f"idle for {idle_timeout:.0f}s at {consumed} bytes")
                        except OSError as exc:
                            if getattr(exc, "errno", None) == 28:  # ENOSPC
                                raise DownloadError(
                                    "disk full while writing", status="disk_full",
                                    received=written, total=total, attempts=attempts,
                                    part_path=part) from exc
                            raise
                        if not chunk:
                            break
                        consumed += len(chunk)
                        if total and consumed > total + AES_BLOCK:
                            raise _Retryable(
                                f"server sent more than declared ({consumed}>{total})")
                        out = aes.update(chunk) if aes else chunk
                        if out:
                            fh.write(out)
                            written += len(out)
                            if written - last_disk_check >= DISK_RECHECK_BYTES:
                                last_disk_check = written
                                _disk_guard(dest_dir, (total - consumed) if total else None)
                        now = time.time()
                        if (now - last_meta_save >= META_SAVE_INTERVAL_S
                                or written - last_meta_bytes >= META_SAVE_EVERY_BYTES):
                            _write_json_atomic(meta_path, {
                                "expected_size": expected_size or 0,
                                "total": total,
                                "filename": safe_name,
                                "updated": now,
                                "url_fp": url_fp,
                            })
                            last_meta_save = now
                            last_meta_bytes = written
                        if progress_cb and now - last_progress >= PROGRESS_MIN_INTERVAL_S:
                            last_progress = now
                            maybe = progress_cb(
                                written, total, (written / max(now - started, 0.001)))
                            if asyncio.iscoroutine(maybe):
                                await maybe
                    complete = bool(total) and consumed >= total
                    if aes:
                        tail = aes.finish(complete=complete)
                        if tail:
                            fh.write(tail)
                            written += len(tail)
                    fh.flush()
                    os.fsync(fh.fileno())

                if not complete:
                    raise _Retryable(f"short body: {consumed}/{total}")

            final = dest_dir / f"doc_{uuid.uuid4().hex[:12]}_{safe_name}"
            if not final.resolve().is_relative_to(dest_dir.resolve()):
                raise DownloadError("path traversal rejected", status="failed")
            os.chmod(part, 0o600)
            os.replace(part, final)
            meta_path.unlink(missing_ok=True)
            duration = time.time() - started
            logger.info(
                "[media_stream] ok name=%s bytes=%d attempts=%d resumed=%s duration=%.1fs",
                safe_name, written, attempts, resumed, duration,
            )
            return DownloadResult(
                path=final, received=written, total=max(total, written),
                attempts=attempts, resumed=resumed, duration=duration,
            )

        except DownloadError:
            raise
        except _Restart:
            restart_requested = True
        except _Retryable as exc:
            last_error = str(exc)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            name = type(exc).__name__
            if getattr(exc, "errno", None) == 28:  # ENOSPC anywhere in the write path
                raise DownloadError(
                    "disk full", status="disk_full", received=written, total=total,
                    attempts=attempts, part_path=part if part.exists() else None) from exc
            retryable = (
                "ClientError" in name or "ServerDisconnected" in name
                or "ClientPayload" in name or "ClientConnection" in name
                or isinstance(exc, (asyncio.TimeoutError, ConnectionError, OSError))
            )
            if not retryable:
                raise DownloadError(
                    f"{name}: {exc}", status="failed", received=written, total=total,
                    attempts=attempts, part_path=part if part.exists() else None) from exc
            last_error = _redact(f"{name}: {exc}")

        if written > written_before:
            stalled = 0
        else:
            stalled += 1
            if stalled >= MAX_STALLED_ATTEMPTS:
                logger.warning(
                    "[media_stream] %d consecutive attempts made no progress at %d bytes; giving up",
                    stalled, written)
                break

        if restart_requested:
            continue  # the restart itself consumes this attempt
        if attempts >= max_retries:
            break
        delay = BACKOFF_S[min(attempts - 1, len(BACKOFF_S) - 1)]
        logger.warning(
            "[media_stream] attempt %d/%d failed at %d bytes (%s); retrying in %.0fs",
            attempts, max_retries, written, last_error, delay,
        )
        await asyncio.sleep(delay)

    raise DownloadError(
        f"exhausted {attempts} attempts: {last_error}",
        status="failed", received=written, total=total, attempts=attempts,
        part_path=part if part.exists() else None,
    )
