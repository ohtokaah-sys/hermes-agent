"""Unit tests for tools/budget_config.py.

Covers default values, resolve_threshold() priority chain
(pinned > tool_overrides (verbatim) > mcp_ prefix (capped) > registry (capped)
> default), immutability,
and the PINNED_THRESHOLDS escape-hatch for read_file.
"""

from unittest.mock import patch


from tools.budget_config import (
    DEFAULT_BUDGET,
    DEFAULT_RESULT_SIZE_CHARS,
    PINNED_THRESHOLDS,
    BudgetConfig,
    budget_for_context_window,
)


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------






# ---------------------------------------------------------------------------
# BudgetConfig defaults
# ---------------------------------------------------------------------------




# ---------------------------------------------------------------------------
# Immutability (frozen=True)
# ---------------------------------------------------------------------------




# ---------------------------------------------------------------------------
# Custom construction
# ---------------------------------------------------------------------------




# ---------------------------------------------------------------------------
# resolve_threshold() priority chain
# ---------------------------------------------------------------------------


class TestResolveThreshold:
    """Priority: pinned > tool_overrides (verbatim) > mcp_ prefix (capped) > registry (capped) > default."""

    def test_pinned_wins_over_override(self):
        """Even if tool_overrides contains read_file, pinned value wins."""
        cfg = BudgetConfig(tool_overrides={"read_file": 1})
        result = cfg.resolve_threshold("read_file")
        assert result == float("inf")

    def test_tool_override_wins_over_default(self):
        """tool_overrides should be returned before falling back to registry."""
        cfg = BudgetConfig(tool_overrides={"my_tool": 42})
        result = cfg.resolve_threshold("my_tool")
        assert result == 42


    @patch("tools.registry.registry")
    def test_registry_value_capped_at_default(self, mock_registry):
        """A scaled-down budget caps an oversized registry value (#23767).

        web/terminal/x_search register max_result_size_chars=100_000; a small
        model's scaled budget must not be re-inflated by that.
        """
        mock_registry.get_max_result_size.return_value = 100_000
        cfg = BudgetConfig(default_result_size=30_000)
        assert cfg.resolve_threshold("web_search") == 30_000


    @patch("tools.registry.registry")
    def test_default_budget_unchanged_for_100k_tool(self, mock_registry):
        """Default budget keeps 100K registry tools at 100K (no behavior change)."""
        mock_registry.get_max_result_size.return_value = 100_000
        cfg = BudgetConfig()  # default_result_size == 100_000
        assert cfg.resolve_threshold("web_search") == 100_000


# ---------------------------------------------------------------------------
# budget_for_context_window() — context-aware scaling (#23767)
# ---------------------------------------------------------------------------


class TestBudgetForContextWindow:
    """Scaling the tool-output budget to the active model's context window."""

    def test_none_returns_default(self):
        assert budget_for_context_window(None) is DEFAULT_BUDGET

    def test_zero_or_negative_returns_default(self):
        assert budget_for_context_window(0) is DEFAULT_BUDGET
        assert budget_for_context_window(-5) is DEFAULT_BUDGET


    def test_scaled_budget_constrains_oversized_result(self):
        """A 279K-char result against a 65K model is capped at the *exact* scaled value.

        Asserting only ``threshold < huge_len`` stays green even if scaling silently
        regressed to, say, 270K — so both effective caps are pinned exactly:
        65_536 tokens x 4 chars/token x 0.15 (per-result window fraction) = 39_321,
        which is also what the 50K ``mcp_`` value gets capped to.
        """
        cfg = budget_for_context_window(65_536)
        huge_len = 279_549
        assert cfg.default_result_size == 39_321
        assert cfg.resolve_threshold("mcp_firecrawl_firecrawl_search") == 39_321
        assert cfg.resolve_threshold("mcp_x") == cfg.default_result_size
        assert huge_len > cfg.default_result_size


# ---------------------------------------------------------------------------
# MCP-prefix threshold (mcp_result_size)
# ---------------------------------------------------------------------------


class TestMcpPrefixThreshold:
    """mcp_* tools get the tighter 50K default, config-overridable."""

    def test_default_mcp_threshold_is_tighter_than_generic(self):
        from tools.budget_config import DEFAULT_MCP_RESULT_SIZE_CHARS
        assert DEFAULT_MCP_RESULT_SIZE_CHARS < DEFAULT_RESULT_SIZE_CHARS
        assert DEFAULT_BUDGET.resolve_threshold("mcp_composio_search_tools") == DEFAULT_MCP_RESULT_SIZE_CHARS

    def test_non_mcp_tools_keep_generic_default(self):
        assert DEFAULT_BUDGET.resolve_threshold("some_random_tool") == DEFAULT_RESULT_SIZE_CHARS

    def test_pinned_wins_over_mcp_prefix(self):
        with patch.dict(PINNED_THRESHOLDS, {"mcp_pinned_tool": float("inf")}):
            assert DEFAULT_BUDGET.resolve_threshold("mcp_pinned_tool") == float("inf")

    def test_tool_override_wins_over_mcp_prefix(self):
        cfg = BudgetConfig(tool_overrides={"mcp_special": 75_000})
        assert cfg.resolve_threshold("mcp_special") == 75_000

    def test_mcp_threshold_capped_by_scaled_default(self):
        """On a small model the scaled default_result_size caps the MCP value."""
        cfg = BudgetConfig(default_result_size=20_000, mcp_result_size=50_000)
        assert cfg.resolve_threshold("mcp_anything") == 20_000


    def test_config_override_via_hermes_home(self, tmp_path, monkeypatch):
        (tmp_path / "config.yaml").write_text(
            "tool_budget:\n  mcp_result_size_chars: 30000\n"
        )
        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        cfg = budget_for_context_window(None)
        assert cfg.resolve_threshold("mcp_composio_multi_execute") == 30_000
        # Generic tools are untouched by the MCP knob.
        assert cfg.default_result_size == DEFAULT_RESULT_SIZE_CHARS

    def test_config_override_survives_window_scaling(self, tmp_path, monkeypatch):
        (tmp_path / "config.yaml").write_text(
            "tool_budget:\n  mcp_result_size_chars: 30000\n"
        )
        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        cfg = budget_for_context_window(200_000)
        assert cfg.mcp_result_size == 30_000

    def test_malformed_config_falls_back_to_default(self, tmp_path, monkeypatch):
        (tmp_path / "config.yaml").write_text("tool_budget: not-a-mapping\n")
        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        from tools.budget_config import DEFAULT_MCP_RESULT_SIZE_CHARS
        cfg = budget_for_context_window(None)
        assert cfg.resolve_threshold("mcp_x_y") == DEFAULT_MCP_RESULT_SIZE_CHARS

    def test_scaled_small_window_caps_mcp_threshold(self, tmp_path, monkeypatch):
        """A tiny model's scaled default_result_size caps even the MCP value."""
        monkeypatch.setenv("HERMES_HOME", str(tmp_path))  # no config.yaml
        cfg = budget_for_context_window(16_384)  # scaled default < 50K
        assert cfg.default_result_size < 50_000
        assert cfg.resolve_threshold("mcp_tool") == cfg.default_result_size


# ---------------------------------------------------------------------------
# tool_budget.tool_overrides — config-driven per-tool caps (00.4 改动 2)
# ---------------------------------------------------------------------------


class TestToolOverridesConfig:
    """``tool_budget.tool_overrides`` fills resolve_threshold's 2nd priority slot.

    The knob exists so a threshold can be tightened (or dialed back) without
    editing code and restarting the gateway — the staged rollout in 00.4 改动 3
    depends on it. With the key absent the budget must stay byte-identical.
    """

    def test_absent_key_keeps_default_singleton(self, tmp_path, monkeypatch):
        """No config.yaml → same singleton object, i.e. zero behavior change."""
        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        assert budget_for_context_window(None) is DEFAULT_BUDGET
        assert budget_for_context_window(200_000) == BudgetConfig()

    def test_override_without_context_window(self, tmp_path, monkeypatch):
        (tmp_path / "config.yaml").write_text(
            "tool_budget:\n  tool_overrides:\n    terminal: 50000\n"
        )
        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        cfg = budget_for_context_window(None)
        assert cfg.resolve_threshold("terminal") == 50_000
        # Tools not named in the override keep the generic default.
        assert cfg.resolve_threshold("web_search") == DEFAULT_RESULT_SIZE_CHARS

    def test_override_survives_window_scaling(self, tmp_path, monkeypatch):
        (tmp_path / "config.yaml").write_text(
            "tool_budget:\n  tool_overrides:\n    terminal: 50000\n"
        )
        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        cfg = budget_for_context_window(200_000)
        assert cfg.tool_overrides == {"terminal": 50_000}
        assert cfg.resolve_threshold("terminal") == 50_000

    def test_pinned_tool_still_wins_over_override(self, tmp_path, monkeypatch):
        """read_file stays unbounded even if a user tries to cap it."""
        (tmp_path / "config.yaml").write_text(
            "tool_budget:\n  tool_overrides:\n    read_file: 1234\n"
        )
        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        cfg = budget_for_context_window(None)
        assert cfg.resolve_threshold("read_file") == float("inf")

    def test_override_wins_over_mcp_prefix_default(self, tmp_path, monkeypatch):
        """Explicit config beats the 50K mcp_* heuristic."""
        (tmp_path / "config.yaml").write_text(
            "tool_budget:\n  tool_overrides:\n    mcp_special: 75000\n"
        )
        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        cfg = budget_for_context_window(None)
        assert cfg.resolve_threshold("mcp_special") == 75_000

    def test_non_positive_non_int_entries_are_skipped(self, tmp_path, monkeypatch):
        """Bad entries are dropped, never coerced (zero / negative / bool / str)."""
        (tmp_path / "config.yaml").write_text(
            "tool_budget:\n"
            "  tool_overrides:\n"
            "    good: 42000\n"
            "    zero: 0\n"
            "    negative: -1\n"
            "    quoted: '50000'\n"
            "    boolean: true\n"
            "    nullish: null\n"
        )
        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        cfg = budget_for_context_window(None)
        assert cfg.tool_overrides == {"good": 42_000}
        assert cfg.resolve_threshold("quoted") == DEFAULT_RESULT_SIZE_CHARS

    def test_malformed_block_falls_back_to_defaults(self, tmp_path, monkeypatch):
        """A broken tool_overrides block must not blow up the budget lookup."""
        bodies = (
            "tool_budget: not-a-mapping\n",
            "tool_budget:\n  tool_overrides: not-a-mapping\n",
            "tool_budget:\n  tool_overrides:\n    - 1\n    - 2\n",
        )
        for idx, body in enumerate(bodies):
            case_dir = tmp_path / f"case{idx}"
            case_dir.mkdir()
            (case_dir / "config.yaml").write_text(body)
            monkeypatch.setenv("HERMES_HOME", str(case_dir))
            cfg = budget_for_context_window(None)
            assert cfg.tool_overrides == {}
            assert cfg.resolve_threshold("terminal") == DEFAULT_RESULT_SIZE_CHARS
