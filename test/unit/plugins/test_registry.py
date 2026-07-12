"""Unit tests for the plugin registry.

Tests cover:
- Plugin registration with decorators
- Plugin lookup by name
- Plugin metadata (description, version, tags)
- Registry introspection (list_plugins)
- Registry clearing (for test isolation)
- Overwrite warnings

Note: Writer customization uses hooks in DeltaWriter, not plugins.
"""
import pytest
from unittest.mock import MagicMock

from deltagen.plugins.registry import (
    register_column,
    register_stage,
    get_column_plugin,
    get_stage_plugin,
    get_plugin_info,
    list_plugins,
    clear_registry,
    PluginInfo,
)


@pytest.fixture(autouse=True)
def clean_registry():
    """Clear the registry before and after each test."""
    clear_registry()
    yield
    clear_registry()


class TestColumnPluginRegistration:
    """Tests for column plugin registration."""

    def test_register_column_basic(self):
        """Basic column plugin registration."""
        @register_column("test_transform")
        def my_transform(df, column, ctx):
            return df

        plugin = get_column_plugin("test_transform")
        assert plugin is not None
        assert plugin is my_transform

    def test_register_column_with_metadata(self):
        """Column plugin with full metadata."""
        @register_column(
            "mask_email",
            description="GDPR-compliant email masking",
            version="2.0.0",
            author="Data Team",
            tags={"pii", "gdpr"},
        )
        def mask_email(df, column, ctx):
            return df

        info = get_plugin_info("mask_email")
        assert info is not None
        assert info.name == "mask_email"
        assert info.plugin_type == "column"
        assert info.description == "GDPR-compliant email masking"
        assert info.version == "2.0.0"
        assert info.author == "Data Team"
        assert info.tags == {"pii", "gdpr"}

    def test_register_column_uses_docstring(self):
        """Plugin uses function docstring if no description provided."""
        @register_column("documented_plugin")
        def documented(df, column, ctx):
            """This is the docstring description."""
            return df

        info = get_plugin_info("documented_plugin")
        assert info.description == "This is the docstring description."

    def test_get_nonexistent_column_plugin(self):
        """Lookup of non-existent plugin returns None."""
        plugin = get_column_plugin("does_not_exist")
        assert plugin is None


class TestStagePluginRegistration:
    """Tests for stage plugin registration."""

    def test_register_stage_basic(self):
        """Basic stage plugin registration."""
        @register_stage("dedupe_latest")
        def dedupe(df, stage, ctx):
            return df

        plugin = get_stage_plugin("dedupe_latest")
        assert plugin is not None
        assert plugin is dedupe

    def test_register_stage_with_metadata(self):
        """Stage plugin with metadata."""
        @register_stage(
            "log_nulls",
            description="Log null values to DQ table",
            tags={"quality", "logging"},
        )
        def log_nulls(df, stage, ctx):
            return df

        info = get_plugin_info("log_nulls", plugin_type="stage")
        assert info is not None
        assert info.plugin_type == "stage"
        assert "quality" in info.tags

    def test_get_nonexistent_stage_plugin(self):
        """Lookup of non-existent stage plugin returns None."""
        plugin = get_stage_plugin("does_not_exist")
        assert plugin is None


class TestPluginExecution:
    """Tests for plugin execution."""

    def test_column_plugin_receives_correct_args(self):
        """Column plugin receives df, column, ctx."""
        received_args = {}

        @register_column("capture_args")
        def capture(df, column, ctx):
            received_args["df"] = df
            received_args["column"] = column
            received_args["ctx"] = ctx
            return df

        plugin = get_column_plugin("capture_args")

        mock_df = MagicMock()
        mock_column = MagicMock()
        mock_ctx = MagicMock()

        result = plugin(mock_df, mock_column, mock_ctx)

        assert received_args["df"] is mock_df
        assert received_args["column"] is mock_column
        assert received_args["ctx"] is mock_ctx
        assert result is mock_df

    def test_stage_plugin_receives_correct_args(self):
        """Stage plugin receives df, stage, ctx."""
        received_args = {}

        @register_stage("capture_stage_args")
        def capture(df, stage, ctx):
            received_args["df"] = df
            received_args["stage"] = stage
            received_args["ctx"] = ctx
            return df

        plugin = get_stage_plugin("capture_stage_args")

        mock_df = MagicMock()
        mock_stage = MagicMock()
        mock_ctx = MagicMock()

        result = plugin(mock_df, mock_stage, mock_ctx)

        assert received_args["df"] is mock_df
        assert received_args["stage"] is mock_stage
        assert received_args["ctx"] is mock_ctx


class TestRegistryIntrospection:
    """Tests for registry introspection."""

    def test_list_all_plugins(self):
        """List all registered plugins."""
        @register_column("col1")
        def col1(df, col, ctx):
            return df

        @register_stage("stage1")
        def stage1(df, stage, ctx):
            return df

        plugins = list_plugins()
        assert len(plugins) == 2

        names = {p.name for p in plugins}
        assert names == {"col1", "stage1"}

    def test_list_plugins_by_type(self):
        """List plugins filtered by type."""
        @register_column("col1")
        def col1(df, col, ctx):
            return df

        @register_column("col2")
        def col2(df, col, ctx):
            return df

        @register_stage("stage1")
        def stage1(df, stage, ctx):
            return df

        column_plugins = list_plugins(plugin_type="column")
        assert len(column_plugins) == 2
        assert all(p.plugin_type == "column" for p in column_plugins)

        stage_plugins = list_plugins(plugin_type="stage")
        assert len(stage_plugins) == 1

    def test_list_plugins_by_tags(self):
        """List plugins filtered by tags."""
        @register_column("pii_mask", tags={"pii", "gdpr"})
        def pii_mask(df, col, ctx):
            return df

        @register_column("format_date", tags={"formatting"})
        def format_date(df, col, ctx):
            return df

        @register_stage("dq_check", tags={"quality", "gdpr"})
        def dq_check(df, stage, ctx):
            return df

        gdpr_plugins = list_plugins(tags={"gdpr"})
        assert len(gdpr_plugins) == 2

        names = {p.name for p in gdpr_plugins}
        assert names == {"pii_mask", "dq_check"}

    def test_get_plugin_info_any_type(self):
        """get_plugin_info finds plugin regardless of type."""
        @register_column("shared_name")
        def col_fn(df, col, ctx):
            return df

        # Without type filter, finds it
        info = get_plugin_info("shared_name")
        assert info is not None
        assert info.name == "shared_name"

    def test_get_plugin_info_wrong_type(self):
        """get_plugin_info returns None if type doesn't match."""
        @register_column("col_only")
        def col_fn(df, col, ctx):
            return df

        # With wrong type filter, returns None
        info = get_plugin_info("col_only", plugin_type="stage")
        assert info is None


class TestRegistryClearing:
    """Tests for registry clearing."""

    def test_clear_all(self):
        """Clear all registries."""
        @register_column("col1")
        def col1(df, col, ctx):
            return df

        @register_stage("stage1")
        def stage1(df, stage, ctx):
            return df

        clear_registry()

        assert get_column_plugin("col1") is None
        assert get_stage_plugin("stage1") is None

    def test_clear_by_type(self):
        """Clear specific registry type."""
        @register_column("col1")
        def col1(df, col, ctx):
            return df

        @register_stage("stage1")
        def stage1(df, stage, ctx):
            return df

        clear_registry(plugin_type="column")

        assert get_column_plugin("col1") is None
        assert get_stage_plugin("stage1") is not None  # Not cleared


class TestPluginOverwrite:
    """Tests for plugin overwrite behavior."""

    def test_overwrite_warns(self, caplog):
        """Overwriting a plugin logs a warning."""
        @register_column("duplicate")
        def first(df, col, ctx):
            return df

        @register_column("duplicate")
        def second(df, col, ctx):
            return df

        # Should have logged a warning
        assert "Overwriting existing column plugin: duplicate" in caplog.text

        # Second registration wins
        plugin = get_column_plugin("duplicate")
        assert plugin is second


class TestPluginInfoDataclass:
    """Tests for PluginInfo dataclass."""

    def test_plugin_info_defaults(self):
        """PluginInfo has sensible defaults."""
        info = PluginInfo(
            name="test",
            plugin_type="column",
            fn=lambda: None,
        )

        assert info.name == "test"
        assert info.plugin_type == "column"
        assert info.description is None
        assert info.version == "1.0.0"
        assert info.author is None
        assert info.tags == set()
