"""Tests for gateway.platforms.media_stream (2026-10-23 inbound large-file fix).

Covers the four failure modes of the old path (fixed total timeout, whole-body in
memory, no resume, silent drop) plus Range fallback and disk guard.

Run:  python -m pytest tests/gateway/test_weixin_stream_download.py -q
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
from pathlib import Path

import aiohttp
import pytest
from aiohttp import web
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from gateway.platforms import media_stream as ms

PAYLOAD = os.urandom(2 * 1024 * 1024 + 777)  # ~2 MB, non block-aligned
CHUNK = 64 * 1024


# --------------------------------------------------------------------------- helpers
def _run(coro):
    return asyncio.run(coro)


async def _serve(handler):
    app = web.Application()
    app.router.add_get("/download", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    return runner, f"http://127.0.0.1:{port}/download"


def _pkcs7(data: bytes) -> bytes:
    pad = 16 - len(data) % 16
    return data + bytes([pad]) * pad


def _encrypt(data: bytes, key: bytes) -> bytes:
    enc = Cipher(algorithms.AES(key), modes.ECB()).encryptor()
    return enc.update(_pkcs7(data)) + enc.finalize()


def _body_handler(payload: bytes, *, support_range: bool = True, drop_once_after: int | None = None,
                  stall_after: int | None = None, stall_seconds: float = 3.0,
                  status: int | None = None):
    """Build a handler; ``drop_once_after`` truncates only the FIRST response,
    ``stall_after`` stops sending mid-body (idle-timeout probe)."""
    state = {"served": 0}

    async def handler(request: web.Request) -> web.StreamResponse:
        if status is not None:
            return web.Response(status=status, text="nope")
        state["served"] += 1
        rng = request.headers.get("Range")
        start = 0
        if rng and support_range:
            start = int(rng.split("=", 1)[1].split("-", 1)[0])
        body = payload[start:]
        resp = web.StreamResponse(status=206 if (rng and support_range) else 200)
        resp.headers["Content-Length"] = str(len(body))
        if rng and support_range:
            resp.headers["Content-Range"] = f"bytes {start}-{len(payload) - 1}/{len(payload)}"
        await resp.prepare(request)
        drop_now = drop_once_after is not None and state["served"] == 1
        if drop_now:
            limit = min(drop_once_after, len(body))
        elif stall_after is not None:
            limit = min(stall_after, len(body))
        else:
            limit = len(body)
        sent = 0
        while sent < limit:
            n = min(CHUNK, limit - sent)
            await resp.write(body[sent:sent + n])
            sent += n
            await asyncio.sleep(0)
        if drop_now:
            resp.force_close()
            if request.transport is not None:
                request.transport.close()
            return resp
        if stall_after is not None and sent < len(body):
            await asyncio.sleep(stall_seconds)
            while sent < len(body):
                n = min(CHUNK, len(body) - sent)
                await resp.write(body[sent:sent + n])
                sent += n
                await asyncio.sleep(0)
        await resp.write_eof()
        return resp

    return handler


def _download(url: str, dest: Path, **kwargs):
    async def _go():
        async with aiohttp.ClientSession() as session:
            return await ms.download_to_cache(session, url=url, dest_dir=dest,
                                              filename="rec.m4a", **kwargs)
    return _run(_go())


# --------------------------------------------------------------------------- tests
@pytest.fixture(autouse=True)
def _fast_backoff(monkeypatch):
    monkeypatch.setattr(ms, "BACKOFF_S", (0,))
    monkeypatch.setattr(ms, "PROGRESS_MIN_INTERVAL_S", 0.0)


def test_t1_streaming_download_matches_payload(tmp_path):
    async def _go():
        runner, url = await _serve(_body_handler(PAYLOAD))
        try:
            async with aiohttp.ClientSession() as session:
                result = await ms.download_to_cache(
                    session, url=url, dest_dir=tmp_path, filename="rec.m4a",
                    expected_size=len(PAYLOAD))
            data = Path(result.path).read_bytes()
            assert data == PAYLOAD
            assert result.attempts == 1
            assert not list((tmp_path / ".parts").glob("*.part"))
            assert not list((tmp_path / ".parts").glob("*.json"))
            assert Path(result.path).stat().st_size == len(PAYLOAD) == result.received
        finally:
            await runner.cleanup()

    _run(_go())


def test_t2_resume_with_range_after_drop(tmp_path):
    async def _go():
        handler = _body_handler(PAYLOAD, support_range=True, drop_once_after=512 * 1024)
        runner, url = await _serve(handler)
        try:
            async with aiohttp.ClientSession() as session:
                result = await ms.download_to_cache(
                    session, url=url, dest_dir=tmp_path, filename="rec.m4a",
                    expected_size=len(PAYLOAD), max_retries=2)
            assert Path(result.path).read_bytes() == PAYLOAD
            assert result.attempts == 2          # one drop + one successful resume
            assert result.resumed is True
        finally:
            await runner.cleanup()

    _run(_go())


def test_t3_server_ignores_range_restarts_cleanly(tmp_path):
    async def _go():
        handler = _body_handler(PAYLOAD, support_range=False, drop_once_after=512 * 1024)
        runner, url = await _serve(handler)
        try:
            async with aiohttp.ClientSession() as session:
                result = await ms.download_to_cache(
                    session, url=url, dest_dir=tmp_path, filename="rec.m4a",
                    expected_size=len(PAYLOAD), max_retries=3)
            assert Path(result.path).read_bytes() == PAYLOAD
        finally:
            await runner.cleanup()

    _run(_go())


def test_t4_idle_timeout_keeps_partial(tmp_path):
    async def _go():
        handler = _body_handler(PAYLOAD, stall_after=512 * 1024, stall_seconds=3)
        runner, url = await _serve(handler)
        try:
            async with aiohttp.ClientSession() as session:
                with pytest.raises(ms.DownloadError) as excinfo:
                    await ms.download_to_cache(
                        session, url=url, dest_dir=tmp_path, filename="rec.m4a",
                        expected_size=len(PAYLOAD), idle_timeout=0.3, max_retries=0)
            err = excinfo.value
            assert err.attempts == 1
            assert err.part_path is not None and Path(err.part_path).exists()
        finally:
            await runner.cleanup()

    _run(_go())


def test_t5_permanent_4xx_is_not_retried(tmp_path):
    async def _go():
        runner, url = await _serve(_body_handler(PAYLOAD, status=404))
        try:
            async with aiohttp.ClientSession() as session:
                with pytest.raises(ms.DownloadError) as excinfo:
                    await ms.download_to_cache(session, url=url, dest_dir=tmp_path,
                                               filename="rec.m4a", max_retries=5)
            assert excinfo.value.status == "http_4xx"
            assert excinfo.value.attempts == 1
        finally:
            await runner.cleanup()

    _run(_go())


def test_t7_streaming_aes_matches_one_shot(tmp_path):
    key = os.urandom(16)
    cipher_text = _encrypt(PAYLOAD, key)

    async def _go():
        runner, url = await _serve(_body_handler(cipher_text))
        try:
            async with aiohttp.ClientSession() as session:
                result = await ms.download_to_cache(
                    session, url=url, dest_dir=tmp_path, filename="rec.m4a",
                    expected_size=len(cipher_text),
                    aes_key_b64=__import__("base64").b64encode(key).decode())
            assert Path(result.path).read_bytes() == PAYLOAD
        finally:
            await runner.cleanup()

    _run(_go())


def test_t7b_aes_resume_is_block_aligned(tmp_path):
    key = os.urandom(16)
    cipher_text = _encrypt(PAYLOAD, key)

    async def _go():
        handler = _body_handler(cipher_text, support_range=True, drop_once_after=500 * 1024)
        runner, url = await _serve(handler)
        try:
            async with aiohttp.ClientSession() as session:
                result = await ms.download_to_cache(
                    session, url=url, dest_dir=tmp_path, filename="rec.m4a",
                    expected_size=len(cipher_text), max_retries=2,
                    aes_key_b64=__import__("base64").b64encode(key).decode())
            assert Path(result.path).read_bytes() == PAYLOAD
        finally:
            await runner.cleanup()

    _run(_go())


def test_t5b_disk_guard_fails_closed(tmp_path, monkeypatch):
    monkeypatch.setattr(ms, "_free_bytes", lambda _p: 0)

    async def _go():
        runner, url = await _serve(_body_handler(PAYLOAD))
        try:
            async with aiohttp.ClientSession() as session:
                with pytest.raises(ms.DownloadError) as excinfo:
                    await ms.download_to_cache(session, url=url, dest_dir=tmp_path,
                                               filename="rec.m4a", expected_size=len(PAYLOAD))
            assert excinfo.value.status == "disk_full"
        finally:
            await runner.cleanup()

    _run(_go())


def test_t8_part_identity_is_stable_and_scoped():
    a = ms.part_identity("a.zip", 100, account="acc", chat="c1")
    assert a == ms.part_identity("a.zip", 100, account="acc", chat="c1")
    assert a != ms.part_identity("a.zip", 101, account="acc", chat="c1")
    assert a != ms.part_identity("a.zip", 100, account="acc", chat="c2")


def test_t9_cleanup_removes_stale_parts(tmp_path):
    parts = tmp_path / ".parts"
    parts.mkdir()
    stale = parts / "old.part"
    stale.write_bytes(b"x")
    os.utime(stale, (0, 0))
    assert ms.cleanup_stale_parts(tmp_path, ttl_seconds=60) >= 1
    assert not stale.exists()


# ------------------------------------------------------------------ adapter wiring
def _bare_adapter(tmp_path, monkeypatch, session):
    """A WeixinAdapter without __init__ — enough to exercise the streaming path."""
    import gateway.platforms.weixin as wx
    from gateway.config import Platform

    adapter = object.__new__(wx.WeixinAdapter)
    adapter.platform = Platform.WEIXIN
    adapter._hermes_home = str(tmp_path)
    adapter._account_id = "acct-test"
    adapter._cdn_base_url = "https://novac2c.cdn.weixin.qq.com/c2c"
    adapter._poll_session = session
    monkeypatch.setattr(wx, "get_document_cache_dir", lambda: str(tmp_path / "documents"))
    # The real allowlist rejects 127.0.0.1; the adapter test targets the wiring, not SSRF.
    monkeypatch.setattr(wx, "_assert_weixin_cdn_url", lambda url: None)
    return adapter


def test_t10_adapter_streams_file_and_writes_ledger(tmp_path, monkeypatch):
    import gateway.platforms.weixin as wx

    async def _go():
        runner, url = await _serve(_body_handler(PAYLOAD))
        try:
            async with aiohttp.ClientSession() as session:
                adapter = _bare_adapter(tmp_path, monkeypatch, session)
                monkeypatch.setattr(adapter, "_inbound_media_url", lambda media: url)
                item = {"type": 4, "file_item": {
                    "file_name": "新录音 2.m4a", "size": str(len(PAYLOAD)), "media": {}}}
                path, mime, note = await adapter._download_file_streaming(
                    item, chat_id="chat-1", sender_id="user-1")
            assert note is None
            assert path and Path(path).read_bytes() == PAYLOAD
            assert mime.startswith("audio/")
            rows = (tmp_path / "state" / "inbound_media.jsonl").read_text(encoding="utf-8").splitlines()
            assert rows and '"status": "ok"' in rows[-1]
            assert '"resumed": false' in rows[-1]
        finally:
            await runner.cleanup()

    _run(_go())


def test_t11_adapter_surfaces_download_failure(tmp_path, monkeypatch):
    async def _go():
        runner, url = await _serve(_body_handler(PAYLOAD, status=404))
        try:
            async with aiohttp.ClientSession() as session:
                adapter = _bare_adapter(tmp_path, monkeypatch, session)
                monkeypatch.setattr(adapter, "_inbound_media_url", lambda media: url)
                item = {"type": 4, "file_item": {
                    "file_name": "big.iso", "size": "800000000", "media": {}}}
                path, _mime, note = await adapter._download_file_streaming(
                    item, chat_id="chat-1", sender_id="user-1")
            assert path is None
            assert note and "下载失败" in note and "big.iso" in note
            rows = (tmp_path / "state" / "inbound_media.jsonl").read_text(encoding="utf-8").splitlines()
            assert rows and "http_4xx" in rows[-1]
        finally:
            await runner.cleanup()

    _run(_go())


# ------------------------------------------- review regressions (2026-10-23 三路审查)
def test_t12_cleanup_keeps_the_active_part(tmp_path):
    """P1 regression: a stale mtime must not let cleanup delete the part being resumed."""
    async def _go():
        handler = _body_handler(PAYLOAD, support_range=True, drop_once_after=512 * 1024)
        runner, url = await _serve(handler)
        try:
            async with aiohttp.ClientSession() as session:
                with pytest.raises(ms.DownloadError):
                    await ms.download_to_cache(
                        session, url=url, dest_dir=tmp_path, filename="rec.m4a",
                        expected_size=len(PAYLOAD), max_retries=1)
                parts = list((tmp_path / ".parts").glob("*.part"))
                assert parts, "a failed attempt must leave a partial for resume"
                os.utime(parts[0], (0, 0))  # looks 24h+ stale to cleanup
                result = await ms.download_to_cache(
                    session, url=url, dest_dir=tmp_path, filename="rec.m4a",
                    expected_size=len(PAYLOAD), max_retries=3)
            assert Path(result.path).read_bytes() == PAYLOAD
            assert result.resumed is True
        finally:
            await runner.cleanup()

    _run(_go())


def test_t13_lying_content_range_restarts_instead_of_splicing(tmp_path):
    """A 206 starting at 0 must be rejected — splicing yields a valid-length bad file."""
    state = {"served": 0}

    async def handler(request):
        state["served"] += 1
        rng = request.headers.get("Range")
        body = PAYLOAD
        if state["served"] == 1:
            resp = web.StreamResponse(status=200)
            resp.headers["Content-Length"] = str(len(body))
            await resp.prepare(request)
            await resp.write(body[:512 * 1024])
            resp.force_close()
            if request.transport is not None:
                request.transport.close()
            return resp
        resp = web.StreamResponse(status=206 if rng else 200)
        resp.headers["Content-Length"] = str(len(body))
        if rng:
            resp.headers["Content-Range"] = f"bytes 0-{len(body) - 1}/{len(body)}"
        await resp.prepare(request)
        for i in range(0, len(body), CHUNK):
            await resp.write(body[i:i + CHUNK])
        await resp.write_eof()
        return resp

    async def _go():
        runner, url = await _serve(handler)
        try:
            async with aiohttp.ClientSession() as session:
                result = await ms.download_to_cache(
                    session, url=url, dest_dir=tmp_path, filename="rec.m4a",
                    expected_size=len(PAYLOAD), max_retries=4)
            assert Path(result.path).read_bytes() == PAYLOAD
        finally:
            await runner.cleanup()

    _run(_go())


def test_t14_aes_key_hex_text_format(tmp_path):
    """base64(hex-string) is the other wire format this project uses."""
    key = os.urandom(16)
    cipher_text = _encrypt(PAYLOAD, key)
    b64_hex = __import__("base64").b64encode(key.hex().encode("ascii")).decode("ascii")

    async def _go():
        runner, url = await _serve(_body_handler(cipher_text))
        try:
            async with aiohttp.ClientSession() as session:
                result = await ms.download_to_cache(
                    session, url=url, dest_dir=tmp_path, filename="rec.m4a",
                    expected_size=len(cipher_text), aes_key_b64=b64_hex)
            assert Path(result.path).read_bytes() == PAYLOAD
        finally:
            await runner.cleanup()

    _run(_go())


def test_t14b_bad_aes_key_fails_closed(tmp_path):
    cipher_text = _encrypt(PAYLOAD, os.urandom(16))
    bad = __import__("base64").b64encode(b"not-a-valid-key!!").decode()

    async def _go():
        runner, url = await _serve(_body_handler(cipher_text))
        try:
            async with aiohttp.ClientSession() as session:
                with pytest.raises(ms.DownloadError) as excinfo:
                    await ms.download_to_cache(
                        session, url=url, dest_dir=tmp_path, filename="rec.m4a",
                        expected_size=len(cipher_text), aes_key_b64=bad)
            assert excinfo.value.status == "bad_key"
            assert not list(tmp_path.glob("doc_*"))
        finally:
            await runner.cleanup()

    _run(_go())


def test_t15_missing_length_fails_closed(tmp_path):
    async def handler(request):
        resp = web.StreamResponse()  # chunked, no Content-Length
        await resp.prepare(request)
        await resp.write(PAYLOAD[:4096])
        await resp.write_eof()
        return resp

    async def _go():
        runner, url = await _serve(handler)
        try:
            async with aiohttp.ClientSession() as session:
                with pytest.raises(ms.DownloadError) as excinfo:
                    await ms.download_to_cache(session, url=url, dest_dir=tmp_path,
                                               filename="rec.m4a", max_retries=1)
            assert excinfo.value.status == "no_length"
        finally:
            await runner.cleanup()

    _run(_go())


def test_t16_sanitize_name_bounds_and_filters():
    out = ms.sanitize_name("a" * 300 + ".zip")
    assert len(out.encode("utf-8")) <= ms.MAX_NAME_BYTES + 5
    assert ms.sanitize_name("file\nFAKE ok.txt") == "file FAKE ok.txt"
    assert ms.sanitize_name("../../etc/passwd") == "passwd"
    assert ms.sanitize_name("CON.txt").startswith("_CON")
