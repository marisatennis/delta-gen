"""Unit tests for the plugin context module.

Tests cover:
- PluginContext creation and factory
- State management
- Logging helpers
- Timing utilities
- Null context for testing
"""
import pytest
from unittest.mock import MagicMock
import time

from deltagen.plugins.context import (
    create_plugin_context,
    create_null_context,
    NullMetricsCollector,
)
from deltagen.plugins.metrics import MetricsCollector


class TestPluginContextCreation:
    """Tests for PluginContext creation."""

    def test_create_with_factory(self):
        """Create context with factory function."""
        ctx = create_plugin_context(
            table_name="customer_dim",
            load_id="batch_001",
            environment="prod",
            debug=True,
        )

        assert ctx.metrics is not None
        assert ctx.load_id == "batch_001"
        assert ctx.environment == "prod"
        assert ctx.debug is True
        assert ctx.run_id != ""

    def test_create_with_config(self):
        """Create context with table config."""
        mock_config = MagicMock()
        mock_config.extensions = {"writer": "fabric_lakehouse"}

        ctx = create_plugin_context(
            table_name="test",
            config=mock_config,
        )

        assert ctx.config is mock_config
        assert ctx.get_extension("writer") == "fabric_lakehouse"

    def test_create_with_options(self):
        """Create context with runtime options."""
        ctx = create_plugin_context(
            table_name="test",
            options={"batch_size": 1000, "retry_count": 3},
        )

        assert ctx.get_option("batch_size") == 1000
        assert ctx.get_option("retry_count") == 3

    def test_run_id_from_metrics(self):
        """Run ID is set from metrics collector."""
        ctx = create_plugin_context(table_name="test")

        assert ctx.run_id == ctx.metrics.run_id


class TestStateManagement:
    """Tests for shared state management."""

    def test_get_set_state(self):
        """Basic state get/set."""
        ctx = create_plugin_context(table_name="test")

        ctx.set_state("processed_count", 100)
        assert ctx.get_state("processed_count") == 100

    def test_get_state_default(self):
        """Get state with default value."""
        ctx = create_plugin_context(table_name="test")

        assert ctx.get_state("missing_key") is None
        assert ctx.get_state("missing_key", "default") == "default"

    def test_update_state(self):
        """Update multiple state values."""
        ctx = create_plugin_context(table_name="test")

        ctx.update_state({
            "key1": "value1",
            "key2": "value2",
            "key3": 123,
        })

        assert ctx.get_state("key1") == "value1"
        assert ctx.get_state("key2") == "value2"
        assert ctx.get_state("key3") == 123

    def test_state_persistence_across_calls(self):
        """State persists across plugin calls."""
        ctx = create_plugin_context(table_name="test")

        # Simulate first plugin setting state
        ctx.set_state("stage1_count", 1000)

        # Simulate second plugin reading state
        count = ctx.get_state("stage1_count")
        assert count == 1000


class TestOptionsAndExtensions:
    """Tests for options and extensions access."""

    def test_get_option(self):
        """Get runtime option."""
        ctx = create_plugin_context(
            table_name="test",
            options={"timeout": 30},
        )

        assert ctx.get_option("timeout") == 30
        assert ctx.get_option("missing") is None
        assert ctx.get_option("missing", 10) == 10

    def test_get_extension_from_config(self):
        """Get extension from table config."""
        mock_config = MagicMock()
        mock_config.extensions = {
            "writer": "delta",
            "dq_rules": ["not_null", "unique"],
        }

        ctx = create_plugin_context(table_name="test", config=mock_config)

        assert ctx.get_extension("writer") == "delta"
        assert ctx.get_extension("dq_rules") == ["not_null", "unique"]

    def test_get_extension_no_config(self):
        """Get extension when no config provided."""
        ctx = create_plugin_context(table_name="test")

        assert ctx.get_extension("any_key") is None
        assert ctx.get_extension("any_key", "default") == "default"


class TestLoggingHelpers:
    """Tests for logging helper methods."""

    def test_log_debug_only_when_debug(self, caplog):
        """Debug logs only when debug=True."""
        import logging
        caplog.set_level(logging.DEBUG)

        ctx_no_debug = create_plugin_context(table_name="test", debug=False)
        ctx_debug = create_plugin_context(table_name="test", debug=True)

        ctx_no_debug.log_debug("should not appear")
        ctx_debug.log_debug("should appear")

        assert "should not appear" not in caplog.text
        assert "should appear" in caplog.text

    def test_log_info(self, caplog):
        """Info logging includes run ID."""
        import logging
        caplog.set_level(logging.INFO)

        ctx = create_plugin_context(table_name="test")
        ctx.log_info("test message")

        assert ctx.run_id in caplog.text
        assert "test message" in caplog.text

    def test_log_warning(self, caplog):
        """Warning logging works."""
        import logging
        caplog.set_level(logging.WARNING)

        ctx = create_plugin_context(table_name="test")
        ctx.log_warning("warning message")

        assert "warning message" in caplog.text

    def test_log_error(self, caplog):
        """Error logging works."""
        import logging
        caplog.set_level(logging.ERROR)

        ctx = create_plugin_context(table_name="test")
        ctx.log_error("error message")

        assert "error message" in caplog.text


class TestTimingHelpers:
    """Tests for timing utilities."""

    def test_timed_operation(self):
        """timed_operation context manager records duration."""
        ctx = create_plugin_context(table_name="test", debug=True)

        with ctx.timed_operation("test_op") as timing:
            time.sleep(0.01)

        assert "duration_ms" in timing
        assert timing["duration_ms"] >= 10

    def test_record_plugin_execution(self):
        """Record plugin start/end."""
        ctx = create_plugin_context(table_name="test")

        start = ctx.record_plugin_start("test_plugin", "column")
        time.sleep(0.01)
        ctx.record_plugin_end(
            "test_plugin",
            "column",
            start,
            input_rows=100,
            output_rows=95,
        )

        # Check it was recorded in metrics
        plugins = ctx.metrics.metrics.plugins_executed
        assert len(plugins) == 1
        assert plugins[0]["plugin_name"] == "test_plugin"
        assert plugins[0]["duration_ms"] >= 10


class TestNullContext:
    """Tests for null context (testing helper)."""

    def test_create_null_context(self):
        """Create null context."""
        ctx = create_null_context()

        assert isinstance(ctx.metrics, NullMetricsCollector)
        assert ctx.metrics.run_id == "null"

    def test_null_metrics_no_op(self):
        """Null metrics methods are no-ops."""
        ctx = create_null_context()

        # These should not raise
        ctx.metrics.record_source_read("test", 100)
        ctx.metrics.start_stage("test", 100)
        ctx.metrics.end_stage("test", 100)
        ctx.metrics.record_nulls("col", 10)
        ctx.metrics.record_duplicates(["col"], 5)
        ctx.metrics.record_write("table", "append", inserted=100)
        ctx.metrics.complete()

    def test_null_context_stage_context(self):
        """Null metrics stage_context works."""
        ctx = create_null_context()

        with ctx.metrics.stage_context("test"):
            pass  # Should not raise


class TestPluginContextIntegration:
    """Integration tests for context with metrics."""

    def test_full_plugin_workflow(self):
        """Simulate a full plugin workflow."""
        ctx = create_plugin_context(
            table_name="orders_fact",
            load_id="batch_001",
            debug=False,
        )

        # Source read
        ctx.metrics.record_source_read("raw_orders", 10000)

        # First plugin
        start = ctx.record_plugin_start("validate_nulls", "stage")
        ctx.metrics.record_nulls("order_id", 5, action="rejected")
        ctx.record_plugin_end("validate_nulls", "stage", start, 10000, 9995)

        # Store state for next plugin
        ctx.set_state("validated_count", 9995)

        # Second plugin
        start = ctx.record_plugin_start("dedupe", "stage")
        count = ctx.get_state("validated_count")
        ctx.metrics.record_duplicates(["order_id"], 10, action="kept_latest")
        ctx.record_plugin_end("dedupe", "stage", start, count, 9985)

        # Write
        ctx.metrics.record_write(
            "silver.orders",
            "merge",
            inserted=9000,
            updated=985,
        )

        # Complete
        metrics = ctx.metrics.complete()

        assert metrics.status == "completed"
        assert metrics.total_rows_read == 10000
        assert metrics.total_rows_written == 9985
        assert metrics.total_rows_rejected == 5
        assert len(metrics.plugins_executed) == 2
