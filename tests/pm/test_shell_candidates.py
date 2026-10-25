"""Windows bash resolution as pure data: Program Files Git beats PATH, and
the WSL / MSIX stubs never win (#116818). Host-independent — the candidate
ladder takes the ``which`` result and env as arguments."""
import pytest

from pm.shell import git_install_roots, windows_bash_candidates

PF = r"D:\Progs"


@pytest.mark.parametrize(
    "stub",
    [r"C:\Windows\System32\bash.exe", r"C:\WINDOWS\system32\bash.exe",
     r"C:\Users\u\AppData\Local\Microsoft\WindowsApps\bash.exe"],
)
def test_system_stub_bash_is_never_a_candidate(stub):
    assert stub not in windows_bash_candidates(stub, {"ProgramFiles": PF})


def test_program_files_git_precedes_the_path_bash():
    on_path = r"C:\msys64\usr\bin\bash.exe"
    candidates = windows_bash_candidates(on_path, {"ProgramFiles": PF})
    assert candidates[0] == PF + r"\Git\bin\bash.exe"
    assert candidates[-1] == on_path

def test_per_user_and_32bit_git_roots_are_candidates():
    candidates = windows_bash_candidates(None, {
        "ProgramFiles": PF, "ProgramFiles(x86)": r"D:\Progs32",
        "LOCALAPPDATA": r"C:\Users\u\AppData\Local",
    })
    assert r"D:\Progs32\Git\bin\bash.exe" in candidates
    assert r"C:\Users\u\AppData\Local\Programs\Git\bin\bash.exe" in candidates
    assert r"C:\Users\u\AppData\Local\hermes\git\usr\bin\bash.exe" in candidates

def test_nonstarting_bash_is_rejected(monkeypatch):
    import subprocess
    from pm import shell

    monkeypatch.setattr(shell.subprocess, "run", lambda *a, **kw: subprocess.CompletedProcess(a, 1))
    assert shell._bash_starts("broken-bash.exe") is False


def test_git_on_path_outside_program_files_is_a_candidate(monkeypatch):
    """Vendor-store install: git.exe in <root>/cmd, bash in <root>/bin.

    The Program Files ladder and `which bash` both miss it, which is exactly the
    "Git Bash not found" report from a machine that does have Git.

    2026-10-21 审查修（队列 62b9ee66）：去掉具名厂商/内部工单标签（隐私）。
    2026-10-21 审查修（队列 4c1207c8）：把注册表读取打桩，避免在装了 Git for Windows
    的 Windows 上读到真实 ``InstallPath`` 而多出 root、精确断言失败。
    """
    from pm import shell

    monkeypatch.setattr(shell, "_registry_git_roots", lambda: [])
    roots = git_install_roots({}, git_path=r"D:\VendorStore\Git\cmd\git.exe")
    assert roots == [r"D:\VendorStore\Git"]
    candidates = windows_bash_candidates(None, {"ProgramFiles": PF}, roots)
    assert r"D:\VendorStore\Git\bin\bash.exe" in candidates


def test_git_install_roots_ignores_a_git_that_is_not_under_cmd_or_bin(monkeypatch):
    from pm import shell

    monkeypatch.setattr(shell, "_registry_git_roots", lambda: [])
    assert git_install_roots({}, git_path=r"C:\Windows\system32\notepad.exe") == []
