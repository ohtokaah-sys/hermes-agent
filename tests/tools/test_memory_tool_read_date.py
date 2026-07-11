"""Tests for PR: memory_tool date auto-injection (2026-07-11)."""

import os
import pytest


class TestDateAutoInjection:

    @pytest.fixture()
    def store(self, tmp_path, monkeypatch):
        from tools.memory_tool import MemoryStore
        monkeypatch.setattr("tools.memory_tool.get_memory_dir", lambda: tmp_path)
        s = MemoryStore()
        s.load_from_disk()
        return s

    def _disable_pytest_skip(self):
        self._env_val = os.environ.pop("PYTEST_CURRENT_TEST", None)

    def _restore_pytest_skip(self):
        if self._env_val is not None:
            os.environ["PYTEST_CURRENT_TEST"] = self._env_val
        self._env_val = None

    def test_new_entry_gets_date(self, store):
        self._disable_pytest_skip()
        try:
            store.add("memory", "Test entry.")
            entry = store.memory_entries[0]
            assert entry.startswith("（20") or entry.startswith("(20")
            assert "Test entry" in entry
        finally:
            self._restore_pytest_skip()

    def test_already_dated_not_duplicated(self, store):
        self._disable_pytest_skip()
        try:
            store.add("memory", "（2026-07-11）Already dated.")
            store.add("memory", "Another entry.")
            dated = [e for e in store.memory_entries if e.startswith("（20") or e.startswith("(20")]
            assert len(dated) == 2
        finally:
            self._restore_pytest_skip()

    def test_ensure_date_prefix_new(self, store):
        self._disable_pytest_skip()
        try:
            from datetime import date
            today = date.today().isoformat()
            assert store._ensure_date_prefix("Test") == f"（{today}）Test"
        finally:
            self._restore_pytest_skip()

    def test_ensure_date_prefix_already_has_date(self, store):
        from datetime import date
        today = date.today().isoformat()
        assert store._ensure_date_prefix(f"（{today}）X") == f"（{today}）X"
        assert store._ensure_date_prefix(f"({today})X") == f"({today})X"
        assert store._ensure_date_prefix("（2025-01-01）Old") == "（2025-01-01）Old"

    def test_date_not_injected_during_pytest(self, store):
        os.environ["PYTEST_CURRENT_TEST"] = "active"
        try:
            assert store._ensure_date_prefix("Test") == "Test"
        finally:
            del os.environ["PYTEST_CURRENT_TEST"]

    def test_date_persists_to_disk(self, store):
        self._disable_pytest_skip()
        try:
            store.add("memory", "Disk entry.")
            store2 = type(store)()
            store2.load_from_disk()
            assert store2.memory_entries[0].startswith("（20")
        finally:
            self._restore_pytest_skip()

    def test_replace_injects_date(self, store):
        self._disable_pytest_skip()
        try:
            store.add("memory", "Old entry.")
            store.replace("memory", "Old entry", "Replacement.")
            entry = store.memory_entries[0]
            assert entry.startswith("（20") or entry.startswith("(20")
            assert "Replacement" in entry
        finally:
            self._restore_pytest_skip()
