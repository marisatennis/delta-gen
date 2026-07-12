"""Unit tests for the default starter plugins.

Tests cover:
- Plugin registration (verify plugins are registered at import)
- Plugin metadata (tags, descriptions)
- Error handling (missing required config)

Note: Tests requiring actual Spark execution belong in test/integration/.
These unit tests verify registration and configuration validation only.
"""
import pytest
from unittest.mock import MagicMock

from deltagen.plugins.registry import (
    get_column_plugin,
    get_stage_plugin,
    get_plugin_info,
    list_plugins,
    clear_registry,
)
from deltagen.plugins.context import create_null_context


@pytest.fixture(autouse=True)
def setup_plugins():
    """Ensure default plugins are registered for each test."""
    import importlib
    import deltagen.plugins.defaults as defaults_module

    clear_registry()
    importlib.reload(defaults_module)
    yield
    clear_registry()


class TestDefaultPluginRegistration:
    """Tests that default plugins are registered correctly."""

    def test_mask_email_registered(self):
        """mask_email plugin is registered with correct metadata."""
        plugin = get_column_plugin("mask_email")
        assert plugin is not None

        info = get_plugin_info("mask_email")
        assert info.plugin_type == "column"
        assert info.name == "mask_email"
        assert "pii" in info.tags
        assert "gdpr" in info.tags
        assert "masking" in info.tags
        assert info.version == "1.0.0"
        assert info.author == "Delta-Gen"
        assert "GDPR" in info.description

    def test_not_null_registered(self):
        """not_null plugin is registered with correct metadata."""
        plugin = get_column_plugin("not_null")
        assert plugin is not None

        info = get_plugin_info("not_null")
        assert info.plugin_type == "column"
        assert info.name == "not_null"
        assert "dq" in info.tags
        assert "validation" in info.tags
        assert "quality" in info.tags
        assert "null" in info.description.lower()

    def test_in_set_registered(self):
        """in_set plugin is registered with correct metadata."""
        plugin = get_column_plugin("in_set")
        assert plugin is not None

        info = get_plugin_info("in_set")
        assert info.plugin_type == "column"
        assert info.name == "in_set"
        assert "dq" in info.tags
        assert "validation" in info.tags
        assert "allowed" in info.description.lower()

    def test_dedupe_keep_last_registered(self):
        """dedupe_keep_last plugin is registered with correct metadata."""
        plugin = get_stage_plugin("dedupe_keep_last")
        assert plugin is not None

        info = get_plugin_info("dedupe_keep_last")
        assert info.plugin_type == "stage"
        assert info.name == "dedupe_keep_last"
        assert "dedupe" in info.tags
        assert "quality" in info.tags
        assert "latest" in info.description.lower()

    def test_check_duplicates_registered(self):
        """check_duplicates plugin is registered with correct metadata."""
        plugin = get_stage_plugin("check_duplicates")
        assert plugin is not None

        info = get_plugin_info("check_duplicates")
        assert info.plugin_type == "stage"
        assert info.name == "check_duplicates"
        assert "dq" in info.tags
        assert "validation" in info.tags
        assert "duplicates" in info.tags
        assert "duplicate" in info.description.lower()

    def test_all_default_plugins_listed(self):
        """All 6 default plugins appear in list_plugins."""
        all_plugins = list_plugins()
        plugin_names = {p.name for p in all_plugins}

        # Column plugins
        assert "mask_email" in plugin_names
        assert "not_null" in plugin_names
        assert "in_set" in plugin_names
        # Stage plugins
        assert "dedupe_keep_last" in plugin_names
        assert "check_duplicates" in plugin_names
        assert "period_replace" in plugin_names
        assert "distinct" in plugin_names
        assert "filter_latest_file_per_period" in plugin_names
        assert len(all_plugins) == 8

    def test_column_plugins_filtered(self):
        """Column plugins can be filtered by type."""
        column_plugins = list_plugins(plugin_type="column")
        plugin_names = {p.name for p in column_plugins}

        assert "mask_email" in plugin_names
        assert "not_null" in plugin_names
        assert "in_set" in plugin_names
        assert "dedupe_keep_last" not in plugin_names
        assert len(column_plugins) == 3

    def test_stage_plugins_filtered(self):
        """Stage plugins can be filtered by type."""
        stage_plugins = list_plugins(plugin_type="stage")
        plugin_names = {p.name for p in stage_plugins}

        assert "dedupe_keep_last" in plugin_names
        assert "check_duplicates" in plugin_names
        assert "period_replace" in plugin_names
        assert "distinct" in plugin_names
        assert "filter_latest_file_per_period" in plugin_names
        assert len(stage_plugins) == 5

    def test_dq_plugins_filtered_by_tag(self):
        """DQ plugins can be filtered by tag."""
        dq_plugins = list_plugins(tags={"dq"})
        plugin_names = {p.name for p in dq_plugins}

        assert "not_null" in plugin_names
        assert "in_set" in plugin_names
        assert "mask_email" not in plugin_names


class TestInSetConfigValidation:
    """Tests for in_set plugin configuration validation."""

    def test_in_set_missing_allowed_values_returns_original(self):
        """in_set returns original df if allowed_values not configured."""
        mock_df = MagicMock()

        mock_column = MagicMock()
        mock_column.name = "status"
        mock_column.extensions = {}  # No allowed_values

        ctx = create_null_context()

        plugin = get_column_plugin("in_set")
        result = plugin(mock_df, mock_column, ctx)

        # Should return original df without processing
        assert result is mock_df
        # DataFrame methods should not have been called
        mock_df.filter.assert_not_called()


class TestCheckDuplicatesConfigValidation:
    """Tests for check_duplicates plugin configuration validation."""

    def test_check_duplicates_missing_natural_keys_returns_original(self):
        """check_duplicates returns original if natural_keys missing."""
        mock_df = MagicMock()

        mock_stage = MagicMock()
        mock_stage.extensions = {}

        ctx = create_null_context()

        plugin = get_stage_plugin("check_duplicates")
        result = plugin(mock_df, mock_stage, ctx)

        assert result is mock_df

    def test_check_duplicates_callable(self):
        """check_duplicates has correct signature."""
        plugin = get_stage_plugin("check_duplicates")
        assert callable(plugin)


class TestDedupeConfigValidation:
    """Tests for dedupe_keep_last plugin configuration validation."""

    def test_dedupe_missing_partition_by_returns_original(self):
        """dedupe_keep_last returns original if partition_by missing and no natural keys."""
        mock_df = MagicMock()

        mock_stage = MagicMock()
        mock_stage.extensions = {"order_by": "updated_at"}  # No partition_by

        # Context with no config (no natural keys available)
        ctx = create_null_context()
        ctx.config = None

        plugin = get_stage_plugin("dedupe_keep_last")
        result = plugin(mock_df, mock_stage, ctx)

        assert result is mock_df
        mock_df.withColumn.assert_not_called()

    def test_dedupe_missing_order_by_returns_original(self):
        """dedupe_keep_last returns original if order_by missing."""
        mock_df = MagicMock()

        mock_stage = MagicMock()
        mock_stage.extensions = {"partition_by": ["customer_id"]}  # No order_by

        ctx = create_null_context()

        plugin = get_stage_plugin("dedupe_keep_last")
        result = plugin(mock_df, mock_stage, ctx)

        assert result is mock_df
        mock_df.withColumn.assert_not_called()

    def test_dedupe_missing_both_returns_original(self):
        """dedupe_keep_last returns original if both configs missing."""
        mock_df = MagicMock()

        mock_stage = MagicMock()
        mock_stage.extensions = {}

        ctx = create_null_context()

        plugin = get_stage_plugin("dedupe_keep_last")
        result = plugin(mock_df, mock_stage, ctx)

        assert result is mock_df


class TestPluginCallable:
    """Tests that plugins are callable with correct signatures."""

    def test_mask_email_callable(self):
        """mask_email has correct signature."""
        plugin = get_column_plugin("mask_email")
        assert callable(plugin)

    def test_not_null_callable(self):
        """not_null has correct signature."""
        plugin = get_column_plugin("not_null")
        assert callable(plugin)

    def test_in_set_callable(self):
        """in_set has correct signature."""
        plugin = get_column_plugin("in_set")
        assert callable(plugin)

    def test_dedupe_keep_last_callable(self):
        """dedupe_keep_last has correct signature."""
        plugin = get_stage_plugin("dedupe_keep_last")
        assert callable(plugin)
