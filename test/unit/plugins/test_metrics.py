"""Unit tests for the metrics/observability module.

Tests cover:
- MetricsCollector initialization and lifecycle
- Source read metrics
- Stage timing and row counts
- Data quality metrics (nulls, duplicates, validation)
- Schema drift tracking
- Write operation metrics
- JSON serialization
- Summary output
"""
import json
import time

from deltagen.plugins.metrics import (
    MetricsCollector,
    MetricAction,
    SchemaChangeType,
    create_run_metrics,
    SourceReadMetric,
    StageMetric,
    WriteMetric,
)


class TestMetricsCollectorInitialization:
    """Tests for MetricsCollector initialization."""

    def test_basic_initialization(self):
        """Basic collector initialization."""
        collector = MetricsCollector(table_name="test_table")

        assert collector.metrics.table_name == "test_table"
        assert collector.metrics.status == "running"
        assert collector.run_id.startswith("run_")

    def test_initialization_with_all_params(self):
        """Collector with all parameters."""
        collector = MetricsCollector(
            table_name="customer_dim",
            run_id="custom_run_123",
            load_id="batch_2024_01",
            environment="prod",
            auto_log=False,
        )

        assert collector.metrics.table_name == "customer_dim"
        assert collector.run_id == "custom_run_123"
        assert collector.metrics.load_id == "batch_2024_01"
        assert collector.metrics.environment == "prod"

    def test_factory_function(self):
        """create_run_metrics factory."""
        collector = create_run_metrics(
            table_name="orders_fact",
            load_id="daily_001",
            environment="test",
        )

        assert collector.metrics.table_name == "orders_fact"
        assert collector.metrics.load_id == "daily_001"


class TestSourceReadMetrics:
    """Tests for source read tracking."""

    def test_record_source_read(self):
        """Record a source read."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        collector.record_source_read(
            source_name="raw_customers",
            row_count=10000,
            columns_read=25,
            bytes_read=1024000,
            duration_ms=150,
        )

        assert len(collector.metrics.source_reads) == 1
        read = collector.metrics.source_reads[0]
        assert read.source_name == "raw_customers"
        assert read.row_count == 10000
        assert read.columns_read == 25
        assert read.bytes_read == 1024000
        assert read.read_duration_ms == 150

    def test_multiple_source_reads(self):
        """Track multiple source reads."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        collector.record_source_read("source_a", 1000)
        collector.record_source_read("source_b", 500)
        collector.record_source_read("source_c", 200)

        assert len(collector.metrics.source_reads) == 3
        assert collector.metrics.total_rows_read == 1700


class TestStageMetrics:
    """Tests for stage timing and row counts."""

    def test_start_and_end_stage(self):
        """Track stage start and end."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        collector.start_stage("transform", input_row_count=1000)
        time.sleep(0.01)  # Small delay for timing
        collector.end_stage("transform", output_row_count=950)

        assert len(collector.metrics.stages) == 1
        stage = collector.metrics.stages[0]
        assert stage.stage_name == "transform"
        assert stage.input_row_count == 1000
        assert stage.output_row_count == 950
        assert stage.rows_removed == 50
        assert stage.duration_ms > 0

    def test_stage_context_manager(self):
        """Stage context manager for timing."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        with collector.stage_context("process", input_row_count=500):
            time.sleep(0.01)

        collector.end_stage("process", output_row_count=480)

        stage = collector.metrics.stages[0]
        assert stage.stage_name == "process"
        assert stage.duration_ms > 0

    def test_stage_columns_tracking(self):
        """Track columns added/removed in stage."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        collector.start_stage("enrich")
        collector.end_stage(
            "enrich",
            output_row_count=100,
            columns_added=["new_col1", "new_col2"],
            columns_removed=["old_col"],
        )

        stage = collector.metrics.stages[0]
        assert stage.columns_added == ["new_col1", "new_col2"]
        assert stage.columns_removed == ["old_col"]

    def test_multiple_stages(self):
        """Track multiple stages."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        collector.start_stage("stage1", 1000)
        collector.end_stage("stage1", 900)

        collector.start_stage("stage2", 900)
        collector.end_stage("stage2", 850)

        collector.start_stage("stage3", 850)
        collector.end_stage("stage3", 850)

        assert len(collector.metrics.stages) == 3
        assert collector.metrics.stages[0].rows_removed == 100
        assert collector.metrics.stages[1].rows_removed == 50
        assert collector.metrics.stages[2].rows_removed == 0


class TestDataQualityMetrics:
    """Tests for data quality tracking."""

    def test_record_nulls(self):
        """Record null values."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        collector.record_nulls(
            column_name="customer_id",
            count=15,
            action=MetricAction.REJECTED,
            sample_values=["row_1", "row_5"],
        )

        assert len(collector.metrics.data_quality) == 1
        dq = collector.metrics.data_quality[0]
        assert dq.issue_type == "null"
        assert dq.column_name == "customer_id"
        assert dq.row_count == 15
        assert dq.action == "rejected"
        assert dq.rule_name == "not_null"
        assert collector.metrics.total_rows_rejected == 15

    def test_record_nulls_filled(self):
        """Record nulls that were filled with default."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        collector.record_nulls(
            column_name="status",
            count=50,
            action=MetricAction.FILLED_DEFAULT,
        )

        dq = collector.metrics.data_quality[0]
        assert dq.action == "filled_default"
        # Rows with FILLED_DEFAULT action remain in the pipeline (nulls replaced
        # with defaults), so they are tracked in data_quality but not counted
        # in total_rows_rejected.
        assert collector.metrics.total_rows_rejected == 0

    def test_record_duplicates(self):
        """Record duplicate rows."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        collector.record_duplicates(
            columns=["customer_id", "order_date"],
            count=25,
            action=MetricAction.KEPT_LATEST,
            sample_values=[("C001", "2024-01-01")],
        )

        dq = collector.metrics.data_quality[0]
        assert dq.issue_type == "duplicate"
        assert dq.columns == ["customer_id", "order_date"]
        assert dq.row_count == 25
        assert dq.action == "kept_latest"

    def test_record_duplicates_rejected(self):
        """Rejected duplicates count toward total_rows_rejected."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        collector.record_duplicates(
            columns=["id"],
            count=10,
            action=MetricAction.REJECTED,
        )

        assert collector.metrics.total_rows_rejected == 10

    def test_record_duplicates_distinct(self):
        """DISTINCT duplicates are tracked separately and don't count as rejected."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        collector.record_duplicates(
            columns=["*"],
            count=7,
            action=MetricAction.DISTINCT,
        )

        dq = collector.metrics.data_quality[0]
        assert dq.issue_type == "duplicate"
        assert dq.action == "distinct"
        assert dq.row_count == 7
        assert collector.metrics.total_rows_rejected == 0

    def test_record_validation_failure(self):
        """Record validation rule failures."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        collector.record_validation_failure(
            rule_name="in_set",
            column_name="status",
            count=5,
            action=MetricAction.FLAGGED,
            sample_values=["UNKNOWN", "INVALID"],
        )

        dq = collector.metrics.data_quality[0]
        assert dq.issue_type == "validation_failed"
        assert dq.rule_name == "in_set"
        assert dq.action == "flagged"

    def test_multiple_dq_issues(self):
        """Track multiple data quality issues."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        collector.record_nulls("col1", 10, MetricAction.REJECTED)
        collector.record_nulls("col2", 5, MetricAction.FILLED_DEFAULT)
        collector.record_duplicates(["id"], 3, MetricAction.KEPT_LATEST)

        assert len(collector.metrics.data_quality) == 3
        assert collector.metrics.total_rows_rejected == 10


class TestSchemaChangeMetrics:
    """Tests for schema drift tracking."""

    def test_record_column_added(self):
        """Record column added."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        collector.record_schema_change(
            change_type=SchemaChangeType.COLUMN_ADDED,
            column_name="new_field",
            new_value="STRING",
            action="applied",
        )

        assert len(collector.metrics.schema_changes) == 1
        sc = collector.metrics.schema_changes[0]
        assert sc.change_type == "column_added"
        assert sc.column_name == "new_field"
        assert sc.new_value == "STRING"

    def test_record_column_type_changed(self):
        """Record column type change."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        collector.record_schema_change(
            change_type=SchemaChangeType.COLUMN_TYPE_CHANGED,
            column_name="amount",
            old_value="INT",
            new_value="DECIMAL(18,2)",
            action="applied",
        )

        sc = collector.metrics.schema_changes[0]
        assert sc.change_type == "column_type_changed"
        assert sc.old_value == "INT"
        assert sc.new_value == "DECIMAL(18,2)"

    def test_record_schema_change_ignored(self):
        """Record schema change that was ignored."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        collector.record_schema_change(
            change_type=SchemaChangeType.COLUMN_REMOVED,
            column_name="deprecated_field",
            action="ignored",
        )

        sc = collector.metrics.schema_changes[0]
        assert sc.action == "ignored"


class TestWriteMetrics:
    """Tests for write operation tracking."""

    def test_record_append_write(self):
        """Record append write."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        collector.record_write(
            target_table="silver.customer_dim",
            write_mode="append",
            inserted=1000,
            duration_ms=500,
        )

        write = collector.metrics.write
        assert write is not None
        assert write.target_table == "silver.customer_dim"
        assert write.write_mode == "append"
        assert write.rows_inserted == 1000
        assert write.total_rows_written == 1000
        assert collector.metrics.total_rows_written == 1000

    def test_record_merge_write(self):
        """Record merge write with all operations."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        collector.record_write(
            target_table="gold.orders_fact",
            write_mode="merge",
            merge_strategy="update_all",
            inserted=500,
            updated=200,
            deleted=10,
            unchanged=50,
            duration_ms=1500,
            target_rows_before=10000,
            target_rows_after=10490,
        )

        write = collector.metrics.write
        assert write.write_mode == "merge"
        assert write.merge_strategy == "update_all"
        assert write.rows_inserted == 500
        assert write.rows_updated == 200
        assert write.rows_deleted == 10
        assert write.rows_unchanged == 50
        assert write.total_rows_written == 700  # inserted + updated
        assert write.total_rows_in_target_before == 10000
        assert write.total_rows_in_target_after == 10490

    def test_record_scd2_write(self):
        """Record SCD Type 2 write with expired rows."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        collector.record_write(
            target_table="dim_customer",
            write_mode="merge",
            merge_strategy="scd_type2",
            inserted=100,
            updated=0,
            expired=50,
        )

        write = collector.metrics.write
        assert write.merge_strategy == "scd_type2"
        assert write.rows_expired == 50


class TestRunCompletion:
    """Tests for run completion."""

    def test_complete_success(self):
        """Complete a successful run."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        collector.record_source_read("source", 1000)
        collector.record_write("target", "append", inserted=1000)

        result = collector.complete()

        assert result.status == "completed"
        assert result.end_time is not None
        assert result.duration_ms >= 0  # Can be 0 if test runs very fast
        assert result.error_message is None

    def test_complete_failure(self):
        """Mark run as failed."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        result = collector.fail("Connection timeout to source database")

        assert result.status == "failed"
        assert result.error_message == "Connection timeout to source database"

    def test_get_summary_without_completion(self):
        """Get summary without completing."""
        collector = MetricsCollector(table_name="test", auto_log=False)
        collector.record_source_read("source", 500)

        summary = collector.get_summary()
        assert summary.total_rows_read == 500
        assert summary.status == "running"

    def test_complete_called_twice(self):
        """Calling complete() twice returns existing metrics without updating."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        result1 = collector.complete()
        assert result1.status == "completed"

        # Second call should return same metrics without error
        result2 = collector.complete(status="failed")
        assert result2.status == "completed"  # Status unchanged from first call
        assert result2 is result1

    def test_fail_after_complete(self):
        """Calling fail() after complete() returns existing metrics without updating error."""
        collector = MetricsCollector(table_name="test", auto_log=False)

        result1 = collector.complete()
        assert result1.status == "completed"
        assert result1.error_message is None

        # fail() after complete() should not update error_message
        result2 = collector.fail("This error should not be recorded")
        assert result2.status == "completed"  # Status unchanged
        assert result2.error_message is None  # Error not set
        assert result2 is result1


class TestMetricsSerialization:
    """Tests for metrics serialization."""

    def test_to_dict(self):
        """Convert metrics to dictionary."""
        collector = MetricsCollector(
            table_name="test_table",
            run_id="test_run_001",
            auto_log=False,
        )
        collector.record_source_read("source", 100)
        metrics = collector.complete()

        data = metrics.to_dict()

        assert data["table_name"] == "test_table"
        assert data["run_id"] == "test_run_001"
        assert data["status"] == "completed"
        assert len(data["source_reads"]) == 1

    def test_to_json(self):
        """Convert metrics to JSON."""
        collector = MetricsCollector(
            table_name="test_table",
            run_id="test_run_001",
            auto_log=False,
        )
        collector.record_source_read("source", 100)
        metrics = collector.complete()

        json_str = metrics.to_json()
        data = json.loads(json_str)

        assert data["table_name"] == "test_table"
        assert data["total_rows_read"] == 100

    def test_summary_table(self):
        """Generate human-readable summary."""
        collector = MetricsCollector(
            table_name="customer_dim",
            run_id="test_run",
            load_id="batch_001",
            auto_log=False,
        )

        collector.record_source_read("raw_customers", 10000)
        collector.start_stage("transform", 10000)
        collector.end_stage("transform", 9500)
        collector.record_nulls("email", 50, MetricAction.REJECTED)
        collector.record_duplicates(["id"], 25, MetricAction.KEPT_LATEST)
        collector.record_write("silver.customer_dim", "merge", inserted=9000, updated=500)

        metrics = collector.complete()
        summary = metrics.get_summary_table()

        # Check key elements are in summary
        assert "customer_dim" in summary
        assert "batch_001" in summary
        assert "10,000" in summary  # rows read
        assert "9,500" in summary  # rows written
        assert "null" in summary.lower()
        assert "duplicate" in summary.lower()


class TestMetricDataclasses:
    """Tests for metric dataclasses."""

    def test_source_read_metric_timestamp(self):
        """SourceReadMetric has auto timestamp."""
        metric = SourceReadMetric(source_name="test", row_count=100)
        assert metric.timestamp is not None

    def test_stage_metric_defaults(self):
        """StageMetric has sensible defaults."""
        metric = StageMetric(stage_name="test")
        assert metric.rows_added == 0
        assert metric.rows_removed == 0
        assert metric.columns_added == []

    def test_write_metric_defaults(self):
        """WriteMetric has sensible defaults."""
        metric = WriteMetric(target_table="test", write_mode="append")
        assert metric.rows_inserted == 0
        assert metric.rows_updated == 0
        assert metric.rows_deleted == 0
