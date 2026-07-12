"""Unit tests for aggregation_builder module."""
import pytest
from unittest.mock import MagicMock, patch

from deltagen.model.stage import StageConfig, GroupByConfig, AggregationConfig
from deltagen.runner.aggregation_builder import (
    apply_group_by,
    _build_aggregation_expressions,
    _build_single_aggregation,
)
from deltagen.runner.exceptions import PlanBuilderError


class TestApplyGroupBy:
    """Tests for apply_group_by function."""

    @patch("deltagen.runner.aggregation_builder._get_spark_functions")
    def test_apply_group_by_basic(self, mock_get_f):
        """Test basic GROUP BY with aggregations."""
        mock_F = MagicMock()
        mock_get_f.return_value = mock_F

        mock_col = MagicMock()
        mock_F.col.return_value = mock_col
        mock_F.sum.return_value = mock_col
        mock_col.alias.return_value = mock_col

        mock_df = MagicMock()
        mock_grouped = MagicMock()
        mock_result = MagicMock()
        mock_result.columns = ["category", "total_sales"]

        mock_df.groupBy.return_value = mock_grouped
        mock_grouped.agg.return_value = mock_result

        stage = StageConfig(
            name="agg_stage",
            group_by=GroupByConfig(
                columns=["category"],
                aggregations=[
                    AggregationConfig(
                        column="sales_amount",
                        function="sum",
                        alias="total_sales",
                    )
                ]
            )
        )

        sql_parts = {"agg_stage": {}}

        result = apply_group_by(mock_df, stage, sql_parts, debug=False)

        mock_df.groupBy.assert_called_once()
        mock_grouped.agg.assert_called_once()
        assert result is mock_result

    def test_apply_group_by_no_config(self):
        """Test that apply_group_by raises error when no group_by config."""
        stage = StageConfig(name="no_group")
        mock_df = MagicMock()
        sql_parts = {}

        with pytest.raises(PlanBuilderError, match="no group_by config defined"):
            apply_group_by(mock_df, stage, sql_parts)

    @patch("deltagen.runner.aggregation_builder._get_spark_functions")
    def test_apply_group_by_with_having(self, mock_get_f):
        """Test GROUP BY with HAVING clause."""
        mock_F = MagicMock()
        mock_get_f.return_value = mock_F

        mock_col = MagicMock()
        mock_F.col.return_value = mock_col
        mock_F.sum.return_value = mock_col
        mock_col.alias.return_value = mock_col

        mock_df = MagicMock()
        mock_grouped = MagicMock()
        mock_result = MagicMock()
        mock_result.columns = ["customer_id", "total_spent"]
        mock_result.filter.return_value = mock_result

        mock_df.groupBy.return_value = mock_grouped
        mock_grouped.agg.return_value = mock_result

        stage = StageConfig(
            name="agg_stage",
            group_by=GroupByConfig(
                columns=["customer_id"],
                aggregations=[
                    AggregationConfig(
                        column="amount",
                        function="sum",
                        alias="total_spent",
                    )
                ],
                having=["total_spent > 1000"]
            )
        )

        sql_parts = {"agg_stage": {}}

        result = apply_group_by(mock_df, stage, sql_parts, debug=False)

        mock_result.filter.assert_called_once_with("total_spent > 1000")

    @patch("deltagen.runner.aggregation_builder._get_spark_functions")
    def test_apply_group_by_no_aggregations(self, mock_get_f):
        """Test GROUP BY without aggregations (like SELECT DISTINCT)."""
        mock_F = MagicMock()
        mock_get_f.return_value = mock_F

        mock_col = MagicMock()
        mock_F.col.return_value = mock_col

        mock_df = MagicMock()
        mock_selected = MagicMock()
        mock_result = MagicMock()
        mock_result.columns = ["category", "region"]

        mock_df.select.return_value = mock_selected
        mock_selected.distinct.return_value = mock_result

        stage = StageConfig(
            name="distinct_stage",
            group_by=GroupByConfig(
                columns=["category", "region"],
                aggregations=[],
            )
        )

        sql_parts = {"distinct_stage": {}}

        result = apply_group_by(mock_df, stage, sql_parts, debug=False)

        mock_df.select.assert_called_once()
        mock_selected.distinct.assert_called_once()


class TestBuildAggregationExpressions:
    """Tests for _build_aggregation_expressions function."""

    @patch("deltagen.runner.aggregation_builder._get_spark_functions")
    def test_build_multiple_aggregations(self, mock_get_f):
        """Test building multiple aggregation expressions."""
        mock_F = MagicMock()
        mock_get_f.return_value = mock_F

        mock_col = MagicMock()
        mock_F.col.return_value = mock_col
        mock_F.sum.return_value = mock_col
        mock_F.count.return_value = mock_col
        mock_F.avg.return_value = mock_col
        mock_col.alias.return_value = mock_col

        aggregations = [
            AggregationConfig(column="amount", function="sum", alias="total"),
            AggregationConfig(column="id", function="count", alias="count"),
            AggregationConfig(column="price", function="avg", alias="avg_price"),
        ]

        result = _build_aggregation_expressions(aggregations, "test_stage", debug=False)

        assert len(result) == 3
        mock_F.sum.assert_called_once()
        mock_F.count.assert_called_once()
        mock_F.avg.assert_called_once()

    @patch("deltagen.runner.aggregation_builder._get_spark_functions")
    def test_build_empty_aggregations(self, mock_get_f):
        """Test building empty aggregations list."""
        mock_F = MagicMock()
        mock_get_f.return_value = mock_F

        result = _build_aggregation_expressions([], "test_stage", debug=False)

        assert result == []


class TestBuildSingleAggregation:
    """Tests for _build_single_aggregation function."""

    @patch("deltagen.runner.aggregation_builder._get_spark_functions")
    def test_sum_aggregation(self, mock_get_f):
        """Test SUM aggregation."""
        mock_F = MagicMock()
        mock_get_f.return_value = mock_F

        mock_col = MagicMock()
        mock_F.col.return_value = mock_col
        mock_F.sum.return_value = mock_col

        agg = AggregationConfig(column="amount", function="sum", alias="total")

        result = _build_single_aggregation(agg, "test_stage", mock_F)

        mock_F.sum.assert_called_once_with(mock_col)

    @patch("deltagen.runner.aggregation_builder._get_spark_functions")
    def test_count_distinct(self, mock_get_f):
        """Test COUNT DISTINCT aggregation."""
        mock_F = MagicMock()
        mock_get_f.return_value = mock_F

        mock_col = MagicMock()
        mock_F.col.return_value = mock_col
        mock_F.countDistinct.return_value = mock_col

        agg = AggregationConfig(
            column="customer_id",
            function="count",
            alias="unique_customers",
            distinct=True,
        )

        result = _build_single_aggregation(agg, "test_stage", mock_F)

        mock_F.countDistinct.assert_called_once_with(mock_col)

    @patch("deltagen.runner.aggregation_builder._get_spark_functions")
    def test_all_standard_functions(self, mock_get_f):
        """Test all standard aggregation functions."""
        mock_F = MagicMock()
        mock_get_f.return_value = mock_F

        mock_col = MagicMock()
        mock_F.col.return_value = mock_col

        # Set up all standard functions
        for func in ["sum", "avg", "mean", "min", "max", "count", "first", "last",
                     "collect_list", "collect_set", "stddev", "variance"]:
            setattr(mock_F, func, MagicMock(return_value=mock_col))

        functions = [
            "sum", "avg", "mean", "min", "max", "count",
            "first", "last", "collect_list", "collect_set",
            "stddev", "variance",
        ]

        for func in functions:
            agg = AggregationConfig(column="x", function=func, alias=f"{func}_x")
            result = _build_single_aggregation(agg, "test_stage", mock_F)
            assert result is not None

    @patch("deltagen.runner.aggregation_builder._get_spark_functions")
    def test_unsupported_function_raises_error(self, mock_get_f):
        """Test that unsupported function raises error."""
        mock_F = MagicMock()
        mock_get_f.return_value = mock_F

        mock_col = MagicMock()
        mock_F.col.return_value = mock_col

        agg = AggregationConfig(
            column="amount",
            function="unsupported_func",
            alias="result",
        )

        with pytest.raises(PlanBuilderError, match="Unsupported aggregation function"):
            _build_single_aggregation(agg, "test_stage", mock_F)


class TestAggregationConfigModel:
    """Tests for AggregationConfig model validation."""

    def test_aggregation_config_basic(self):
        """Test basic AggregationConfig creation."""
        agg = AggregationConfig(
            column="amount",
            function="sum",
            alias="total",
        )

        assert agg.column == "amount"
        assert agg.function == "sum"
        assert agg.alias == "total"
        assert agg.distinct is False

    def test_aggregation_config_auto_alias(self):
        """Test that alias is auto-generated if not provided."""
        agg = AggregationConfig(
            column="amount",
            function="sum",
        )

        assert agg.alias == "sum_amount"

    def test_aggregation_config_auto_alias_with_dot(self):
        """Test auto-alias with dot in column name."""
        agg = AggregationConfig(
            column="src.amount",
            function="avg",
        )

        assert agg.alias == "avg_src_amount"

    def test_aggregation_config_with_distinct(self):
        """Test AggregationConfig with distinct."""
        agg = AggregationConfig(
            column="customer_id",
            function="count",
            alias="unique_customers",
            distinct=True,
        )

        assert agg.distinct is True


class TestGroupByConfigModel:
    """Tests for GroupByConfig model validation."""

    def test_group_by_config_basic(self):
        """Test basic GroupByConfig creation."""
        config = GroupByConfig(
            columns=["category", "region"],
            aggregations=[
                AggregationConfig(column="amount", function="sum", alias="total")
            ]
        )

        assert config.columns == ["category", "region"]
        assert len(config.aggregations) == 1
        assert config.having == []

    def test_group_by_config_requires_columns(self):
        """Test that GroupByConfig requires at least one column."""
        with pytest.raises(ValueError):
            GroupByConfig(columns=[])

    def test_group_by_config_with_having(self):
        """Test GroupByConfig with HAVING clause."""
        config = GroupByConfig(
            columns=["customer_id"],
            aggregations=[
                AggregationConfig(column="amount", function="sum", alias="total_spent")
            ],
            having=["total_spent > 1000", "total_spent < 100000"]
        )

        assert config.having == ["total_spent > 1000", "total_spent < 100000"]


class TestStageConfigWithGroupBy:
    """Tests for StageConfig with group_by configuration."""

    def test_stage_with_group_by(self):
        """Test creating a stage with GROUP BY configuration."""
        stage = StageConfig(
            name="agg_stage",
            group_by=GroupByConfig(
                columns=["region"],
                aggregations=[
                    AggregationConfig(column="sales", function="sum", alias="total_sales"),
                    AggregationConfig(column="order_id", function="count", alias="order_count"),
                ]
            )
        )

        assert stage.group_by is not None
        assert stage.group_by.columns == ["region"]
        assert len(stage.group_by.aggregations) == 2

    def test_stage_without_group_by(self):
        """Test that stage without group_by has None."""
        stage = StageConfig(name="no_group")

        assert stage.group_by is None

    def test_stage_with_columns_and_group_by(self):
        """Test stage with both columns and group_by."""
        stage = StageConfig(
            name="transform_and_agg",
            group_by=GroupByConfig(
                columns=["category"],
                aggregations=[
                    AggregationConfig(column="amount", function="sum", alias="total")
                ]
            ),
            filters=["total > 100"]
        )

        assert stage.group_by is not None
        assert stage.filters == ["total > 100"]
