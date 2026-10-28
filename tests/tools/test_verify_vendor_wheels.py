#!/usr/bin/env python3
"""tools/verify_vendor_wheels.py 的判据回归 —— 每条规则都要有一条**会红**的用例。

为什么这么写：这类「门禁脚本」最容易变成纸老虎——正例永远绿，负例没人试。
所以这里每个用例都构造一个真实的 fixture 仓库（vendor/ + pyproject.toml + SHA256SUMS），
断言脚本的退出码和命中规则编号。判据清单见脚本 docstring 的 R1–R6。

跑法：./venv/bin/pytest tests/tools/test_verify_vendor_wheels.py -q
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "verify_vendor_wheels.py"


def _write(path: Path, data: str | bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, bytes):
        path.write_bytes(data)
    else:
        path.write_text(data, encoding="utf-8")
    return path


def _sha(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


def make_repo(tmp_path: Path, *, wheel_name: str = "demo-1.0-py3-none-any.whl",
              wheel_bytes: bytes = b"WHEEL-BYTES", sums_lines: list[str] | None = None,
              pyproject: str | None = None, extra_files: dict[str, bytes] | None = None) -> Path:
    """搭一个最小 vendor 仓库；sums_lines 为 None 时按 wheel 自动生成正确条目。"""
    wheel = _write(tmp_path / "vendor" / wheel_name, wheel_bytes)
    for rel, blob in (extra_files or {}).items():
        _write(tmp_path / rel, blob)
    if sums_lines is None:
        sums_lines = [f"{_sha(wheel)}  {wheel_name}"]
    _write(tmp_path / "vendor" / "SHA256SUMS", "\n".join(sums_lines) + "\n")
    if pyproject is None:
        pyproject = (
            "[project]\nname='demo'\nversion='0'\n\n"
            f'[tool.uv.sources]\ndemo = {{ path = "vendor/{wheel_name}" }}\n'
        )
    _write(tmp_path / "pyproject.toml", pyproject)
    return tmp_path


def run(repo: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--repo", str(repo), "-q"],
        capture_output=True, text=True, timeout=120, check=False,
    )


def test_script_exists():
    assert SCRIPT.is_file(), f"缺 {SCRIPT}"


def test_happy_path_green(tmp_path):
    p = run(make_repo(tmp_path))
    assert p.returncode == 0, p.stdout + p.stderr


def test_r1_unregistered_wheel_red(tmp_path):
    """vendor 下新增未登记 wheel（顶层或子目录）必须红 —— 防「误提交/被替换」。"""
    repo = make_repo(tmp_path)
    _write(repo / "vendor" / "evil-0.1-py3-none-any.whl", b"EVIL")
    p = run(repo)
    assert p.returncode == 1, p.stdout
    assert "R1" in p.stderr and "未登记" in p.stderr


def test_r1_unregistered_wheel_in_subdir_red(tmp_path):
    """同名 wheel 藏在 vendor 子目录也必须被 rglob 抓到（旧版只 glob 顶层）。"""
    repo = make_repo(tmp_path)
    _write(repo / "vendor" / "sub" / "demo-1.0-py3-none-any.whl", b"SWAPPED")
    p = run(repo)
    assert p.returncode == 1, p.stdout
    assert "R1" in p.stderr


def test_r1_hash_drift_red(tmp_path):
    repo = make_repo(tmp_path)
    (repo / "vendor" / "demo-1.0-py3-none-any.whl").write_bytes(b"TAMPERED")
    p = run(repo)
    assert p.returncode == 1
    assert "哈希不一致" in p.stderr


def test_r2_non_wheel_registered_entry_hashed(tmp_path):
    """SHA256SUMS 里登记的非 wheel 文件（tar.gz/so）也要算哈希，不能只查存在性。"""
    repo = make_repo(tmp_path)
    tarball = _write(repo / "vendor" / "demo-1.0.tar.gz", b"TAR")
    sums = (repo / "vendor" / "SHA256SUMS").read_text(encoding="utf-8")
    _write(repo / "vendor" / "SHA256SUMS", sums + f"{_sha(tarball)}  demo-1.0.tar.gz\n")
    tarball.write_bytes(b"TAMPERED-TAR")
    p = run(repo)
    assert p.returncode == 1
    assert "R2" in p.stderr and "哈希不一致" in p.stderr


def test_r2_registered_but_missing_red(tmp_path):
    repo = make_repo(tmp_path)
    sums = (repo / "vendor" / "SHA256SUMS").read_text(encoding="utf-8")
    _write(repo / "vendor" / "SHA256SUMS", sums + "0" * 64 + "  ghost.whl\n")
    p = run(repo)
    assert p.returncode == 1
    assert "R2" in p.stderr


def test_r3_duplicate_basename_different_hash_red(tmp_path):
    """同名不同哈希两条 ⇒ 旧版 dict 会覆盖，前一条被静默忽略。"""
    repo = make_repo(tmp_path)
    good = _sha(repo / "vendor" / "demo-1.0-py3-none-any.whl")
    _write(repo / "vendor" / "SHA256SUMS",
           f"{good}  demo-1.0-py3-none-any.whl\n{'f' * 64}  demo-1.0-py3-none-any.whl\n")
    p = run(repo)
    assert p.returncode == 1
    assert "R3" in p.stderr


def test_r4_path_missing_red(tmp_path):
    repo = make_repo(tmp_path)
    _write(repo / "pyproject.toml",
           "[project]\nname='demo'\nversion='0'\n\n"
           "[tool.uv.sources]\ndemo = { path = \"vendor/nope.whl\" }\n")
    p = run(repo)
    assert p.returncode == 1
    assert "R4" in p.stderr


def test_r5_path_outside_vendor_red(tmp_path):
    """path 指到 vendor 外（仓库内其它目录）必须红 —— 否则绕过 SHA256SUMS。"""
    repo = make_repo(tmp_path)
    _write(repo / "local" / "demo-1.0-py3-none-any.whl", b"OUTSIDE")
    _write(repo / "pyproject.toml",
           "[project]\nname='demo'\nversion='0'\n\n"
           "[tool.uv.sources]\ndemo = { path = \"local/demo-1.0-py3-none-any.whl\" }\n")
    p = run(repo)
    assert p.returncode == 1
    assert "R5" in p.stderr and "vendor 以外" in p.stderr


def test_r5_path_is_directory_red(tmp_path):
    """指向 vendor 内的目录（未锁定源码包）必须红。"""
    repo = make_repo(tmp_path)
    _write(repo / "vendor" / "src" / "demo" / "__init__.py", b"")
    _write(repo / "pyproject.toml",
           "[project]\nname='demo'\nversion='0'\n\n"
           "[tool.uv.sources]\ndemo = { path = \"vendor/src\" }\n")
    p = run(repo)
    assert p.returncode == 1
    assert "R5" in p.stderr and "不是 wheel" in p.stderr


def test_r5_path_inside_vendor_but_unregistered_red(tmp_path):
    repo = make_repo(tmp_path)
    _write(repo / "vendor" / "other-2.0-py3-none-any.whl", b"OTHER")
    _write(repo / "pyproject.toml",
           "[project]\nname='demo'\nversion='0'\n\n"
           "[tool.uv.sources]\ndemo = { path = \"vendor/other-2.0-py3-none-any.whl\" }\n")
    p = run(repo)
    assert p.returncode == 1
    assert "R5" in p.stderr


def test_r6_vendor_missing_with_path_source_red(tmp_path):
    """vendor 被整目录删除但 pyproject 仍引用 ⇒ 必须红，不能 exit 0 假通过。"""
    _write(tmp_path / "pyproject.toml",
           "[project]\nname='demo'\nversion='0'\n\n"
           "[tool.uv.sources]\ndemo = { path = \"vendor/demo-1.0-py3-none-any.whl\" }\n")
    p = run(tmp_path)
    assert p.returncode == 1
    assert "R6" in p.stderr


def test_env_missing_pyproject_exit_2(tmp_path):
    p = run(tmp_path)
    assert p.returncode == 2, p.stdout + p.stderr


def test_single_quoted_toml_path_is_checked(tmp_path):
    """单引号 path 是合法 TOML。旧正则只认双引号 ⇒ 漏检。现在必须照样校验并报红。"""
    repo = make_repo(tmp_path)
    _write(repo / "pyproject.toml",
           "[project]\nname='demo'\nversion='0'\n\n"
           "[tool.uv.sources]\ndemo = { path = 'vendor/missing.whl' }\n")
    p = run(repo)
    assert p.returncode == 1
    assert "R4" in p.stderr


def test_commented_out_path_is_ignored(tmp_path):
    """段内被注释掉的 path 不是真实源。旧正则会把注释当源，造成 CI 假失败。"""
    repo = make_repo(tmp_path)
    _write(repo / "pyproject.toml",
           "[project]\nname='demo'\nversion='0'\n\n"
           "[tool.uv.sources]\n"
           "# demo = { path = \"vendor/old-does-not-exist.whl\" }\n"
           "demo = { path = \"vendor/demo-1.0-py3-none-any.whl\" }\n")
    p = run(repo)
    assert p.returncode == 0, p.stdout + p.stderr


def test_trailing_comment_on_path_line(tmp_path):
    """path 行尾带注释也要能校验（旧正则要求行尾结束）。"""
    repo = make_repo(tmp_path)
    _write(repo / "pyproject.toml",
           "[project]\nname='demo'\nversion='0'\n\n"
           "[tool.uv.sources]\n"
           "demo = { path = \"vendor/missing.whl\" }  # 行尾注释\n")
    p = run(repo)
    assert p.returncode == 1
    assert "R4" in p.stderr


@pytest.mark.parametrize("target_kind", ["absolute", "relative"])
def test_escaping_path_red(tmp_path, target_kind):
    """vendor 外的**真实文件**必须命中 [R5]（路径越界），不能靠「文件不存在」蒙过。

    2026-10-28 修（审查发现 6 条，同一用例）：
      ① 绝对路径与相对路径都要覆盖（原来只剩相对路径，绝对路径越界无测试）
      ② pyproject 补 [project] 段（原来缺，可能先触发配置错误而非 R5）
      ③ PYTHONPATH 不再清空，改为指向 fixture 仓库（清空会让脚本导入项目模块时崩）
      ④ 断言 [R5] 规则编号，不再只凭中文短语（防其它校验失败造成假绿）
      ⑤ subprocess 加 timeout（防解析/网络/锁阻塞导致 CI 挂死）
    """
    import os
    tool = REPO / "tools" / "verify_vendor_wheels.py"
    outside = tmp_path / "outside_real.whl"
    outside.write_bytes(b"evil-bytes")
    repo = tmp_path / "repo"
    (repo / "vendor").mkdir(parents=True)
    (repo / "vendor" / "SHA256SUMS").write_text("")
    target = str(outside) if target_kind == "absolute" else "../outside_real.whl"
    (repo / "pyproject.toml").write_text(
        '[project]\nname = "fixture"\nversion = "0.0.0"\n\n'
        f'[tool.uv.sources]\nbh = {{ path = "{target}" }}\n')
    env = {**os.environ, "PYTHONPATH": str(repo)}
    r = subprocess.run([sys.executable, str(tool), "--repo", str(repo)],
                       capture_output=True, text=True, env=env, timeout=60, check=False)
    out = r.stdout + r.stderr
    assert r.returncode == 1, out
    assert "[R5]" in out, f"必须命中 R5 路径越界，实际输出：{out}"

def test_real_repo_is_green():
    """本仓库真实状态必须通过（防止测试夹具绿而生产红）。"""
    p = subprocess.run([sys.executable, str(SCRIPT), "--repo", str(REPO), "-q"],
                       capture_output=True, text=True, timeout=120, check=False)
    assert p.returncode == 0, p.stdout + p.stderr
