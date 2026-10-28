#!/usr/bin/env python3
"""vendor 源完整性校验（唯一判据：CI / 每日巡检 / 打包共用）。

R1 vendor 下每个 *.whl 必须按**相对路径**登记且哈希一致
R2 SHA256SUMS 每个条目：该路径存在 + 哈希一致（不限扩展名）
R3 同 basename 出现多次即违规（不论哈希是否相同）——防覆盖绕过
R4 uv.sources 的每个 path 必须存在
R5 path 必须落在**真实** vendor/（非符号链接逃逸）内、是 .whl、按路径登记、哈希一致
R6 vendor 缺失/是符号链接/解析到仓库外 ⇒ fail-closed
R7 uv.sources 的 str / dict / list-of-sources 三种形态都要覆盖（不许静默跳过）

仅标准库；Python ≥ 3.11（tomllib），低版本 exit 2。
"""
from __future__ import annotations

import argparse
import hashlib
import os
import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    print(f"[FAIL] 需要 Python >= 3.11（tomllib）；当前 {sys.version.split()[0]}", file=sys.stderr)
    sys.exit(2)


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_sums(path: Path):
    sums, problems, seen = {}, [], {}
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        digest, name = parts[0].lower(), parts[1].strip()
        name = name.removeprefix("*")
        name = name.lstrip("./").replace(os.sep, "/")
        if not name:
            continue
        base = name.rsplit("/", 1)[-1]
        seen[base] = seen.get(base, 0) + 1
        if name in sums and sums[name] != digest:
            problems.append(f"[R3] 同一路径登记两次且哈希不同：{name}")
        sums[name] = digest
    for base, n in seen.items():
        if n > 1:
            problems.append(f"[R3] 同 basename 登记 {n} 次（{base}）—— 不论哈希是否相同都违规")
    return sums, problems


def iter_uv(spec):
    if isinstance(spec, str):
        yield spec
    elif isinstance(spec, dict):
        if "path" in spec:
            yield str(spec["path"])
    elif isinstance(spec, (list, tuple)):
        for item in spec:
            yield from iter_uv(item)


def uv_paths(pyproject: Path):
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    sources = (data.get("tool", {}).get("uv", {}) or {}).get("sources", {}) or {}
    out, non_path = [], []
    for name, spec in sources.items():
        got = list(iter_uv(spec))
        out.extend((name, p) for p in got) if got else non_path.append(name)
    return out, non_path


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=os.path.expanduser("~/.hermes/hermes-agent"))
    ap.add_argument("-q", "--quiet", action="store_true")
    args = ap.parse_args(argv)

    repo = Path(args.repo).resolve()
    pyproject = repo / "pyproject.toml"
    if not pyproject.is_file():
        print(f"[FAIL] 找不到 {pyproject}", file=sys.stderr)
        return 2

    problems = []
    raw_vendor = repo / "vendor"
    if raw_vendor.is_symlink():
        problems.append(f"[R6] vendor 是符号链接（→ {raw_vendor.resolve()}），必须是仓库内真实目录")
    vendor = raw_vendor.resolve()
    try:
        vendor.relative_to(repo)
    except ValueError:
        problems.append(f"[R6] vendor 解析到仓库外：{vendor}")

    paths, non_path = uv_paths(pyproject)
    if non_path and not args.quiet:
        print(f"[info] 非 path 源（交由 uv 索引校验）：{non_path}")

    sums, sums_file = {}, vendor / "SHA256SUMS"
    if sums_file.is_file():
        sums, r3 = read_sums(sums_file)
        problems.extend(r3)
    elif paths or raw_vendor.is_dir():
        problems.append(f"[R6] 缺 {sums_file}（无法证明 wheel 未被替换）")

    for name, rel in paths:
        target = Path(rel) if Path(rel).is_absolute() else (repo / rel)
        if not target.exists():
            problems.append(f"[R4] uv.sources[{name}] 指向不存在的路径：{rel}")
            continue
        resolved = target.resolve()
        try:
            rel_in = resolved.relative_to(vendor).as_posix()
        except ValueError:
            problems.append(f"[R5] uv.sources[{name}] 指向 vendor 以外：{rel}")
            continue
        if resolved.suffix != ".whl":
            problems.append(f"[R5] uv.sources[{name}] 不是 wheel：{rel}")
            continue
        want = sums.get(rel_in)
        if not want:
            problems.append(f"[R5] uv.sources[{name}] 的 vendor/{rel_in} 未登记（按相对路径）")
            continue
        got = sha256_of(resolved)
        if got != want:
            problems.append(f"[R5] vendor/{rel_in} 哈希不一致：登记 {want[:12]}… 实际 {got[:12]}…")

    if raw_vendor.is_dir():
        for wheel in sorted(vendor.rglob("*.whl")):
            rel = wheel.relative_to(vendor).as_posix()
            want = sums.get(rel)
            if not want:
                problems.append(f"[R1] vendor/{rel} 未登记（必须按相对路径登记到 SHA256SUMS）")
                continue
            got = sha256_of(wheel)
            if got != want:
                problems.append(f"[R1] vendor/{rel} 哈希不一致：登记 {want[:12]}… 实际 {got[:12]}…")
            elif not args.quiet:
                print(f"✅ vendor/{rel} {got[:16]}…")
        for rel, want in sums.items():
            p = vendor / rel
            if not p.is_file():
                problems.append(f"[R2] SHA256SUMS 登记了 vendor/{rel}，但该路径不存在")
                continue
            got = sha256_of(p)
            if got != want:
                problems.append(f"[R2] vendor/{rel} 哈希不一致：登记 {want[:12]}… 实际 {got[:12]}…")
    elif paths:
        problems.append("[R6] vendor 不存在但 pyproject 仍引用 path 源")

    if problems:
        print("🔴 vendor 源完整性校验未通过：", file=sys.stderr)
        for p in problems:
            print(f"   · {p}", file=sys.stderr)
        print(f"   权威清单：{sums_file}", file=sys.stderr)
        return 1
    if not args.quiet:
        print(f"[vendor] 通过：{len(paths)} 个 uv path 源，SHA256SUMS 全部对上")
    return 0


if __name__ == "__main__":
    sys.exit(main())
