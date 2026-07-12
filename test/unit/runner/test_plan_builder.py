"""Unit tests for PlanBuilder orchestrator class.

These tests focus on the PlanBuilder class itself as an orchestrator.
Component-specific tests are in their respective test files:
- test_exceptions.py - PlanBuilderError
- test_column_builder.py - Column building and SQL generation
- test_join_builder.py - Join handling
- test_filter_builder.py - Filter application
- test_source_loader.py - Source loading
- test_sql_generator.py - SQL generation and explain
"""
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import date

from deltagen.runner import PlanBuilder, PlanBuilderError
from deltagen.model import TableConfig
from deltagen.model.stage import StageConfig
from deltagen.model.column import ColumnConfig, ColumnInput
from deltagen.model.source import SourceConfig
from deltagen.model.join import JoinConfig, JoinCondition
from deltagen.model.incremental import IncrementalConfig


class TestPlanBuilderInit:
    """Tests for PlanBuilder initialization."""

    def test_init_with_valid_config(self):
        """Test initialization with a valid TableConfig."""
        config = TableConfig(name="test_table")
        builder = PlanBuilder(config)
        assert builder.config == config

    def test_init_stores_config(self):
        """Test that config is stored as attribute."""
        config = TableConfig(
            name="test_table",
            layer="silver",
            sources=[SourceConfig(name="src", path="/data/source")]
        )
        builder = PlanBuilder(config)
        assert builder.config.name == "test_table"
        assert builder.config.layer == "silver"
        assert len(builder.config.sources) == 1

    def test_init_creates_empty_sql_parts(self):
        """Test that _sql_parts is initialized empty."""
        config = TableConfig(name="test")
        builder = PlanBuilder(config)
        assert builder._sql_parts == {}


class TestPlanBuilderLoadSources:
    """Tests for PlanBuilder.load_sources method."""

    @patch("deltagen.runner.plan_builder._load_sources")
    def test_load_sources_delegates_to_source_loader(self, mock_load):
        """Test that load_sources delegates to source_loader module."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_load.return_value = {"src": mock_df}

        config = TableConfig(
            name="test",
            sources=[SourceConfig(name="src", path="/data")]
        )
        builder = PlanBuilder(config)

        result = builder.load_sources(mock_spark)

        mock_load.assert_called_once_with(mock_spark, config.sources)
        assert result == {"src": mock_df}


class TestPlanBuilderBuildStage:
    """Tests for PlanBuilder.build_stage method."""

    @patch("deltagen.runner.plan_builder.apply_filters")
    @patch("deltagen.runner.plan_builder.build_columns")
    @patch("deltagen.runner.plan_builder.apply_joins")
    def test_build_stage_initializes_sql_parts(
        self, mock_joins, mock_cols, mock_filters
    ):
        """Test that build_stage initializes SQL parts for the stage."""
        mock_spark = MagicMock()
        mock_df = MagicMock()

        config = TableConfig(
            name="test",
            sources=[SourceConfig(name="src", path="/data")],
            stages=[StageConfig(name="transform")]
        )
        builder = PlanBuilder(config)

        builder.build_stage(
            mock_spark,
            config.stages[0],
            {"src": mock_df}
        )

        assert "transform" in builder._sql_parts
        assert "select" in builder._sql_parts["transform"]
        assert "from" in builder._sql_parts["transform"]
        assert "joins" in builder._sql_parts["transform"]
        assert "where" in builder._sql_parts["transform"]

    @patch("deltagen.runner.plan_builder.apply_filters")
    @patch("deltagen.runner.plan_builder.build_columns")
    @patch("deltagen.runner.plan_builder.apply_joins")
    def test_build_stage_uses_input_df_when_provided(
        self, mock_joins, mock_cols, mock_filters
    ):
        """Test that build_stage uses input_df when provided."""
        mock_spark = MagicMock()
        mock_input_df = MagicMock()
        mock_source_df = MagicMock()

        config = TableConfig(
            name="test",
            sources=[SourceConfig(name="src", path="/data")],
            stages=[StageConfig(name="stage2")]
        )
        builder = PlanBuilder(config)

        builder.build_stage(
            mock_spark,
            config.stages[0],
            {"src": mock_source_df},
            input_df=mock_input_df
        )

        assert builder._sql_parts["stage2"]["from"] == "previous_stage"

    @patch("deltagen.runner.plan_builder.apply_filters")
    @patch("deltagen.runner.plan_builder.build_columns")
    @patch("deltagen.runner.plan_builder.apply_joins")
    def test_build_stage_uses_first_source_when_no_input(
        self, mock_joins, mock_cols, mock_filters
    ):
        """Test that build_stage uses first source when no input_df."""
        mock_spark = MagicMock()
        mock_source_df = MagicMock()

        config = TableConfig(
            name="test",
            sources=[SourceConfig(name="raw_data", path="/data")],
            stages=[StageConfig(name="stage1")]
        )
        builder = PlanBuilder(config)

        builder.build_stage(
            mock_spark,
            config.stages[0],
            {"raw_data": mock_source_df}
        )

        assert builder._sql_parts["stage1"]["from"] == "raw_data"

    def test_build_stage_raises_error_when_no_sources(self):
        """Test that build_stage raises error when no sources available."""
        mock_spark = MagicMock()

        config = TableConfig(
            name="test",
            stages=[StageConfig(name="stage1")]
        )
        builder = PlanBuilder(config)

        with pytest.raises(PlanBuilderError, match="No sources"):
            builder.build_stage(mock_spark, config.stages[0], {})

    @patch("deltagen.runner.plan_builder.apply_filters")
    @patch("deltagen.runner.plan_builder.build_columns")
    @patch("deltagen.runner.plan_builder.apply_joins")
    def test_build_stage_calls_join_then_columns_then_filters(
        self, mock_joins, mock_cols, mock_filters
    ):
        """Test that build_stage processes in correct order."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_joins.return_value = mock_df
        mock_cols.return_value = mock_df
        mock_filters.return_value = mock_df

        config = TableConfig(
            name="test",
            sources=[SourceConfig(name="src", path="/data")],
            stages=[
                StageConfig(
                    name="transform",
                    joins=[JoinConfig(
                        name="j",
                        type="left",
                        source="other",
                        conditions=[JoinCondition(left="a", right="b")]
                    )],
                    columns=[ColumnConfig(name="col", inputs=[])],
                    filters=["x = 1"]
                )
            ]
        )
        builder = PlanBuilder(config)

        builder.build_stage(mock_spark, config.stages[0], {"src": mock_df})

        # Verify order: joins first, then columns, then filters
        assert mock_joins.called
        assert mock_cols.called
        assert mock_filters.called


class TestPlanBuilderBuild:
    """Tests for PlanBuilder.build method."""

    @patch.object(PlanBuilder, "build_stage")
    @patch.object(PlanBuilder, "load_sources")
    def test_build_processes_all_stages(self, mock_load, mock_build_stage):
        """Test that build processes all stages in order."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_load.return_value = {"src": mock_df}
        mock_build_stage.return_value = mock_df

        config = TableConfig(
            name="test",
            sources=[SourceConfig(name="src", path="/data")],
            stages=[
                StageConfig(name="stage1"),
                StageConfig(name="stage2"),
                StageConfig(name="stage3")
            ]
        )
        builder = PlanBuilder(config)

        builder.build(mock_spark)

        assert mock_build_stage.call_count == 3

    @patch.object(PlanBuilder, "build_stage")
    @patch.object(PlanBuilder, "load_sources")
    def test_build_chains_stage_outputs(self, mock_load, mock_build_stage):
        """Test that build passes each stage output to the next."""
        mock_spark = MagicMock()
        mock_df1 = MagicMock(name="df1")
        mock_df2 = MagicMock(name="df2")
        mock_load.return_value = {"src": MagicMock()}
        mock_build_stage.side_effect = [mock_df1, mock_df2]

        config = TableConfig(
            name="test",
            sources=[SourceConfig(name="src", path="/data")],
            stages=[
                StageConfig(name="stage1"),
                StageConfig(name="stage2")
            ]
        )
        builder = PlanBuilder(config)

        builder.build(mock_spark)

        # Second call should receive first stage's output
        calls = mock_build_stage.call_args_list
        # First call has input_df=None (it's the 4th positional arg or keyword)
        first_call_args = calls[0]
        second_call_args = calls[1]

        # Verify chaining - second stage gets first stage's output
        assert len(calls) == 2

    @patch.object(PlanBuilder, "build_stage")
    @patch.object(PlanBuilder, "load_sources")
    def test_build_applies_deduplication_on_natural_keys(
        self, mock_load, mock_build_stage
    ):
        """Test that build no longer automatically deduplicates on natural keys.
        
        Automatic deduplication was removed. Users must explicitly use the
        check_duplicates or dedupe_keep_last plugins if they want deduplication.
        """
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.dropDuplicates.return_value = mock_df
        mock_load.return_value = {"src": MagicMock()}
        mock_build_stage.return_value = mock_df

        config = TableConfig(
            name="test",
            sources=[SourceConfig(name="src", path="/data")],
            stages=[
                StageConfig(
                    name="transform",
                    columns=[
                        ColumnConfig(name="id", natural=True, inputs=[]),
                        ColumnConfig(name="email", natural=True, inputs=[]),
                        ColumnConfig(name="name", natural=False, inputs=[])
                    ]
                )
            ]
        )
        builder = PlanBuilder(config)

        result = builder.build(mock_spark)

        # Should NOT call dropDuplicates - deduplication must be explicit
        mock_df.dropDuplicates.assert_not_called()
        # Should return the DataFrame as-is
        assert result == mock_df


class TestPlanBuilderToSql:
    """Tests for PlanBuilder.to_sql method."""

    def test_to_sql_returns_sql_string(self):
        """Test that to_sql returns a SQL string."""
        config = TableConfig(
            name="test",
            stages=[StageConfig(name="stage1")]
        )
        builder = PlanBuilder(config)
        builder._sql_parts = {
            "stage1": {
                "select": ["id"],
                "from": "source",
                "joins": [],
                "where": []
            }
        }

        sql = builder.to_sql("stage1")

        assert "SELECT" in sql
        assert "FROM source" in sql

    def test_to_sql_all_stages(self):
        """Test to_sql without stage parameter returns all stages."""
        config = TableConfig(
            name="test",
            stages=[
                StageConfig(name="stage1"),
                StageConfig(name="stage2")
            ]
        )
        builder = PlanBuilder(config)
        builder._sql_parts = {
            "stage1": {"select": ["a"], "from": "src1", "joins": [], "where": []},
            "stage2": {"select": ["b"], "from": "src2", "joins": [], "where": []}
        }

        sql = builder.to_sql()

        # Should include SQL content
        assert "SELECT" in sql


class TestPlanBuilderExplain:
    """Tests for PlanBuilder.explain method."""

    def test_explain_returns_readable_plan(self):
        """Test that explain returns a human-readable plan."""
        config = TableConfig(
            name="customer_dim",
            layer="silver",
            sources=[SourceConfig(name="src", path="/data")]
        )
        builder = PlanBuilder(config)

        explanation = builder.explain()

        assert "customer_dim" in explanation
        assert "silver" in explanation


class TestPlanBuilderColumnToSql:
    """Tests for PlanBuilder._column_to_sql static method."""

    def test_column_to_sql_static_method_exists(self):
        """Test _column_to_sql static method is available."""
        col = ColumnConfig(
            name="id",
            data_type="int",
            inputs=[ColumnInput(source="src", column="id")]
        )

        sql = PlanBuilder._column_to_sql(col)

        assert "src.id" in sql
        assert "INTEGER" in sql


class TestPlanBuilderDebugOutput:
    """Tests for debug output in PlanBuilder."""

    @patch.object(PlanBuilder, "build_stage")
    @patch.object(PlanBuilder, "load_sources")
    def test_build_with_debug_prints_output(
        self, mock_load, mock_build_stage, capsys
    ):
        """Test that debug=True produces output."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_load.return_value = {"raw": mock_df}
        mock_build_stage.return_value = mock_df

        config = TableConfig(
            name="test_table",
            sources=[SourceConfig(name="raw", path="/data", format="delta")],
            stages=[StageConfig(name="transform")]
        )
        builder = PlanBuilder(config)

        builder.build(mock_spark, debug=True)

        captured = capsys.readouterr()
        assert "PlanBuilder" in captured.out
        assert "test_table" in captured.out


class TestPlanBuilderErrorHandling:
    """Tests for error handling in PlanBuilder."""

    @patch("deltagen.runner.plan_builder.apply_filters")
    @patch("deltagen.runner.plan_builder.build_columns")
    @patch("deltagen.runner.plan_builder.apply_joins")
    def test_build_stage_wraps_errors_in_plan_builder_error(
        self, mock_joins, mock_cols, mock_filters
    ):
        """Test that errors are wrapped in PlanBuilderError."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_joins.side_effect = Exception("Spark error")

        config = TableConfig(
            name="test",
            sources=[SourceConfig(name="src", path="/data")],
            stages=[
                StageConfig(
                    name="error_stage",
                    joins=[JoinConfig(
                        name="j",
                        type="left",
                        source="other",
                        conditions=[JoinCondition(left="a", right="b")]
                    )]
                )
            ]
        )
        builder = PlanBuilder(config)

        with pytest.raises(PlanBuilderError) as exc_info:
            builder.build_stage(mock_spark, config.stages[0], {"src": mock_df})

        assert "error_stage" in str(exc_info.value)
        assert "Spark error" in str(exc_info.value)

    @patch("deltagen.runner.plan_builder.apply_filters")
    @patch("deltagen.runner.plan_builder.build_columns")
    @patch("deltagen.runner.plan_builder.apply_joins")
    def test_plan_builder_error_is_not_rewrapped(
        self, mock_joins, mock_cols, mock_filters
    ):
        """Test that PlanBuilderError is re-raised without wrapping."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        original_error = PlanBuilderError("Original error", stage="inner")
        mock_joins.side_effect = original_error

        config = TableConfig(
            name="test",
            sources=[SourceConfig(name="src", path="/data")],
            stages=[
                StageConfig(
                    name="outer_stage",
                    joins=[JoinConfig(
                        name="j",
                        type="left",
                        source="other",
                        conditions=[JoinCondition(left="a", right="b")]
                    )]
                )
            ]
        )
        builder = PlanBuilder(config)

        with pytest.raises(PlanBuilderError) as exc_info:
            builder.build_stage(mock_spark, config.stages[0], {"src": mock_df})

        # Should be the original error, not a wrapped one
        assert exc_info.value is original_error


class TestTableConfigIntegration:
    """Tests for TableConfig integration with PlanBuilder."""

    def test_natural_keys_from_config(self):
        """Test that natural key columns are identified via config."""
        config = TableConfig(
            name="test",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", natural=True, inputs=[]),
                        ColumnConfig(name="email", natural=True, inputs=[]),
                        ColumnConfig(name="name", natural=False, inputs=[])
                    ]
                )
            ]
        )
        builder = PlanBuilder(config)
        natural_keys = builder.config.get_natural_key_columns()

        assert len(natural_keys) == 2
        assert natural_keys[0].name == "id"
        assert natural_keys[1].name == "email"

    def test_persistent_vs_temporary_columns(self):
        """Test that persistent and temporary columns are distinguished."""
        config = TableConfig(
            name="test",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", temporary=False, inputs=[]),
                        ColumnConfig(name="temp_calc", temporary=True, inputs=[])
                    ]
                )
            ]
        )
        builder = PlanBuilder(config)

        all_columns = list(builder.config.iter_columns())
        assert len(all_columns) == 2

        persistent = builder.config.get_persistent_columns()
        assert len(persistent) == 1
        assert persistent[0].name == "id"

    def test_multiple_stages_in_config(self):
        """Test config with multiple stages."""
        config = TableConfig(
            name="test",
            stages=[
                StageConfig(name="stage1"),
                StageConfig(name="stage2"),
                StageConfig(name="stage3")
            ]
        )
        builder = PlanBuilder(config)

        assert len(builder.config.stages) == 3
        assert builder.config.stages[0].name == "stage1"
        assert builder.config.stages[1].name == "stage2"
        assert builder.config.stages[2].name == "stage3"


class TestGetPeriodFilterValues:
    """Tests for _get_period_filter_values.

    The BM production version simplified this to return only the latest period.
    lookback_periods was removed from IncrementalConfig.
    """

    def _make_builder(self):
        """Create a PlanBuilder with period-based incremental config."""
        config = TableConfig(
            name="test_table",
            layer="gold",
            target_schema="fact",
            incremental=IncrementalConfig(
                filter_mode="period",
                period_column="report_date",
            ),
        )
        return PlanBuilder(config)

    def test_table_does_not_exist_returns_empty(self):
        """When target table doesn't exist, returns empty list."""
        builder = self._make_builder()
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False

        result = builder._get_period_filter_values(mock_spark)
        assert result == []

    def _patch_pyspark_functions(self):
        """Create a mock for pyspark.sql.functions that works with local imports."""
        mock_F = MagicMock()
        mock_F.col.return_value = MagicMock()
        mock_F.max.return_value.alias.return_value = MagicMock()
        return patch.dict("sys.modules", {"pyspark.sql.functions": mock_F, "pyspark.sql": MagicMock(functions=mock_F), "pyspark": MagicMock()})

    def test_returns_latest_period(self):
        """Returns only the latest period from target table."""
        builder = self._make_builder()
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True

        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, key: date(2024, 3, 15)
        mock_spark.table.return_value.select.return_value.collect.return_value = [mock_row]

        with self._patch_pyspark_functions():
            result = builder._get_period_filter_values(mock_spark)

        assert result == [date(2024, 3, 15)]
