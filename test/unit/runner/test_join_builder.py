"""Unit tests for join_builder module."""
import pytest
from unittest.mock import MagicMock, patch

from deltagen.runner.join_builder import apply_joins, apply_single_join, _should_broadcast
from deltagen.runner.exceptions import PlanBuilderError
from deltagen.model.stage import StageConfig
from deltagen.model.source import SourceConfig
from deltagen.model.join import JoinConfig, JoinCondition


class TestJoinConfig:
    """Tests for JoinConfig model validation."""

    def test_inner_join_config(self):
        """Test inner join configuration."""
        join = JoinConfig(
            name="product_join",
            type="inner",
            source="products",
            conditions=[JoinCondition(left="f.prod_id", right="p.id")]
        )
        assert join.type == "inner"
        assert join.source == "products"
        assert len(join.conditions) == 1

    def test_left_join_config(self):
        """Test left join configuration."""
        join = JoinConfig(
            name="customer_join",
            type="left",
            source="customers",
            conditions=[JoinCondition(left="o.cust_id", right="c.id")]
        )
        assert join.type == "left"

    def test_right_join_config(self):
        """Test right join configuration."""
        join = JoinConfig(
            name="right_join",
            type="right",
            source="dim_table",
            conditions=[JoinCondition(left="a.id", right="b.id")]
        )
        assert join.type == "right"

    def test_full_join_config(self):
        """Test full outer join configuration."""
        join = JoinConfig(
            name="full_join",
            type="full",
            source="other",
            conditions=[JoinCondition(left="a.id", right="b.id")]
        )
        assert join.type == "full"

    def test_cross_join_config(self):
        """Test cross join configuration."""
        join = JoinConfig(
            name="cross_join",
            type="cross",
            source="lookup",
            conditions=[]
        )
        assert join.type == "cross"

    def test_multiple_join_conditions(self):
        """Test join with multiple conditions."""
        join = JoinConfig(
            name="complex_join",
            type="inner",
            source="lookup",
            conditions=[
                JoinCondition(left="a.id", right="b.id", operator="="),
                JoinCondition(left="a.region", right="b.region", operator="="),
                JoinCondition(left="a.date", right="b.effective_date", operator=">=")
            ]
        )
        assert len(join.conditions) == 3


class TestJoinCondition:
    """Tests for JoinCondition model."""

    def test_equals_operator_default(self):
        """Test that default operator is equals."""
        cond = JoinCondition(left="a.id", right="b.id")
        assert cond.operator == "="

    def test_not_equals_operator(self):
        """Test not equals operator."""
        cond = JoinCondition(left="a.status", right="b.status", operator="!=")
        assert cond.operator == "!="

    def test_less_than_operator(self):
        """Test less than operator."""
        cond = JoinCondition(left="a.date", right="b.date", operator="<")
        assert cond.operator == "<"

    def test_less_than_equals_operator(self):
        """Test less than or equals operator."""
        cond = JoinCondition(left="a.date", right="b.date", operator="<=")
        assert cond.operator == "<="

    def test_greater_than_operator(self):
        """Test greater than operator."""
        cond = JoinCondition(left="a.date", right="b.date", operator=">")
        assert cond.operator == ">"

    def test_greater_than_equals_operator(self):
        """Test greater than or equals operator."""
        cond = JoinCondition(left="a.date", right="b.date", operator=">=")
        assert cond.operator == ">="


class TestApplySingleJoin:
    """Tests for apply_single_join function with mocked Spark."""

    @patch("deltagen.runner.join_builder._get_spark_functions")
    def test_join_source_not_found_raises_error(self, mock_spark):
        """Test that missing join source raises PlanBuilderError."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F
        mock_df = MagicMock()

        join_config = JoinConfig(
            name="missing_join",
            type="inner",
            source="nonexistent",
            conditions=[JoinCondition(left="a.id", right="b.id")]
        )
        sources = {}  # Empty sources
        source_configs = []
        sql_parts = {"stage1": {"joins": []}}

        with pytest.raises(PlanBuilderError, match="nonexistent.*not found"):
            apply_single_join(mock_df, join_config, sources, source_configs, sql_parts, "stage1")

    @patch("deltagen.runner.join_builder._get_spark_functions")
    def test_inner_join_uses_correct_spark_type(self, mock_spark):
        """Test inner join maps to Spark 'inner' type."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F
        mock_df = MagicMock()
        mock_join_df = MagicMock()

        join_config = JoinConfig(
            name="inner_join",
            type="inner",
            source="dim_table",
            conditions=[JoinCondition(left="a.id", right="b.id")]
        )
        sources = {"dim_table": mock_join_df}
        source_configs = [SourceConfig(name="dim_table", path="/data")]
        sql_parts = {"stage1": {"joins": []}}

        apply_single_join(mock_df, join_config, sources, source_configs, sql_parts, "stage1")

        # Verify join was called with 'inner' type
        mock_df.join.assert_called_once()
        call_args = mock_df.join.call_args
        assert call_args[0][0] == mock_join_df
        assert call_args[1] == "inner" or call_args[0][2] == "inner"

    @patch("deltagen.runner.join_builder._get_spark_functions")
    def test_left_join_uses_correct_spark_type(self, mock_spark):
        """Test left join maps to Spark 'left' type."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F
        mock_df = MagicMock()
        mock_join_df = MagicMock()

        join_config = JoinConfig(
            name="left_join",
            type="left",
            source="dim_table",
            conditions=[JoinCondition(left="a.id", right="b.id")]
        )
        sources = {"dim_table": mock_join_df}
        source_configs = [SourceConfig(name="dim_table", path="/data")]
        sql_parts = {"stage1": {"joins": []}}

        apply_single_join(mock_df, join_config, sources, source_configs, sql_parts, "stage1")

        mock_df.join.assert_called_once()
        call_args = mock_df.join.call_args
        assert "left" in str(call_args)

    @patch("deltagen.runner.join_builder._get_spark_functions")
    def test_full_join_maps_to_outer(self, mock_spark):
        """Test full join maps to Spark 'outer' type."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F
        mock_df = MagicMock()
        mock_join_df = MagicMock()

        join_config = JoinConfig(
            name="full_join",
            type="full",
            source="dim_table",
            conditions=[JoinCondition(left="a.id", right="b.id")]
        )
        sources = {"dim_table": mock_join_df}
        source_configs = [SourceConfig(name="dim_table", path="/data")]
        sql_parts = {"stage1": {"joins": []}}

        apply_single_join(mock_df, join_config, sources, source_configs, sql_parts, "stage1")

        mock_df.join.assert_called_once()
        call_args = mock_df.join.call_args
        assert "outer" in str(call_args)

    @patch("deltagen.runner.join_builder._get_spark_functions")
    def test_sql_parts_updated(self, mock_spark):
        """Test that SQL parts dictionary is updated."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F
        mock_df = MagicMock()
        mock_join_df = MagicMock()

        join_config = JoinConfig(
            name="test_join",
            type="left",
            source="customers",
            conditions=[JoinCondition(left="order.cust_id", right="cust.id")]
        )
        sources = {"customers": mock_join_df}
        source_configs = [SourceConfig(name="customers", path="/data")]
        sql_parts = {"stage1": {"joins": []}}

        apply_single_join(mock_df, join_config, sources, source_configs, sql_parts, "stage1")

        # Check SQL was recorded
        assert len(sql_parts["stage1"]["joins"]) == 1
        join_sql = sql_parts["stage1"]["joins"][0]
        assert "LEFT JOIN" in join_sql
        assert "customers" in join_sql
        assert "order.cust_id" in join_sql

    @patch("deltagen.runner.join_builder._get_spark_functions")
    def test_multiple_conditions_combined_with_and(self, mock_spark):
        """Test multiple conditions are combined with AND."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F
        mock_df = MagicMock()
        mock_join_df = MagicMock()

        # Create mock column objects
        mock_col = MagicMock()
        mock_F.col.return_value = mock_col
        mock_col.__eq__ = MagicMock(return_value=mock_col)
        mock_col.__and__ = MagicMock(return_value=mock_col)

        join_config = JoinConfig(
            name="multi_cond_join",
            type="inner",
            source="lookup",
            conditions=[
                JoinCondition(left="a.id", right="b.id"),
                JoinCondition(left="a.date", right="b.date")
            ]
        )
        sources = {"lookup": mock_join_df}
        source_configs = [SourceConfig(name="lookup", path="/data")]
        sql_parts = {"stage1": {"joins": []}}

        apply_single_join(mock_df, join_config, sources, source_configs, sql_parts, "stage1")

        # Check SQL shows AND
        join_sql = sql_parts["stage1"]["joins"][0]
        assert "a.id = b.id" in join_sql
        assert "a.date = b.date" in join_sql
        assert "AND" in join_sql


class TestApplyJoins:
    """Tests for apply_joins function."""

    @patch("deltagen.runner.join_builder._get_spark_functions")
    def test_no_joins_returns_original_df(self, mock_spark):
        """Test that stage with no joins returns original DataFrame."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F
        mock_df = MagicMock()

        stage = StageConfig(name="no_joins", joins=[])
        sources = {}
        source_configs = []
        sql_parts = {"no_joins": {"joins": []}}

        result = apply_joins(mock_df, stage, sources, source_configs, sql_parts)

        assert result == mock_df
        mock_df.join.assert_not_called()

    @patch("deltagen.runner.join_builder.apply_single_join")
    def test_multiple_joins_applied_sequentially(self, mock_apply_single):
        """Test that multiple joins are applied in order."""
        mock_df = MagicMock()
        mock_apply_single.return_value = mock_df

        stage = StageConfig(
            name="multi_join",
            joins=[
                JoinConfig(
                    name="join1",
                    type="left",
                    source="src1",
                    conditions=[JoinCondition(left="a.id", right="b.id")]
                ),
                JoinConfig(
                    name="join2",
                    type="left",
                    source="src2",
                    conditions=[JoinCondition(left="a.id", right="c.id")]
                )
            ]
        )
        sources = {"src1": MagicMock(), "src2": MagicMock()}
        source_configs = [
            SourceConfig(name="src1", path="/data1"),
            SourceConfig(name="src2", path="/data2")
        ]
        sql_parts = {"multi_join": {"joins": []}}

        apply_joins(mock_df, stage, sources, source_configs, sql_parts)

        # Verify apply_single_join was called twice
        assert mock_apply_single.call_count == 2


class TestBroadcastHints:
    """Tests for broadcast hint functionality."""

    def test_should_broadcast_from_join_config(self):
        """Test broadcast flag on join config takes priority."""
        join_config = JoinConfig(
            name="test",
            type="left",
            source="dim_table",
            conditions=[],
            broadcast=True
        )
        source_configs = [SourceConfig(name="dim_table", path="/data", broadcast=False)]

        assert _should_broadcast(join_config, source_configs) is True

    def test_should_not_broadcast_when_join_says_no(self):
        """Test broadcast=False on join config overrides source."""
        join_config = JoinConfig(
            name="test",
            type="left",
            source="dim_table",
            conditions=[],
            broadcast=False
        )
        source_configs = [SourceConfig(name="dim_table", path="/data", broadcast=True)]

        assert _should_broadcast(join_config, source_configs) is False

    def test_should_broadcast_from_source_config(self):
        """Test broadcast flag inherited from source config."""
        join_config = JoinConfig(
            name="test",
            type="left",
            source="dim_table",
            conditions=[]
            # broadcast not set
        )
        source_configs = [SourceConfig(name="dim_table", path="/data", broadcast=True)]

        assert _should_broadcast(join_config, source_configs) is True

    def test_should_not_broadcast_by_default(self):
        """Test no broadcast when neither join nor source specify it."""
        join_config = JoinConfig(
            name="test",
            type="left",
            source="dim_table",
            conditions=[]
        )
        source_configs = [SourceConfig(name="dim_table", path="/data")]

        assert _should_broadcast(join_config, source_configs) is False

    def test_should_not_broadcast_source_not_found(self):
        """Test no broadcast when source config not found."""
        join_config = JoinConfig(
            name="test",
            type="left",
            source="missing_table",
            conditions=[]
        )
        source_configs = [SourceConfig(name="other_table", path="/data")]

        assert _should_broadcast(join_config, source_configs) is False


class TestJoinTypeMapping:
    """Tests for join type to Spark type mapping."""

    def test_all_join_types_valid(self):
        """Test all supported join types."""
        valid_types = ["inner", "left", "right", "full", "cross"]
        for join_type in valid_types:
            join = JoinConfig(
                name=f"{join_type}_test",
                type=join_type,
                source="test",
                conditions=[JoinCondition(left="a.id", right="b.id")]
            )
            assert join.type == join_type


class TestDuplicateColumnHandling:
    """Tests for automatic duplicate column handling in joins."""

    @patch("deltagen.runner.join_builder._get_spark_functions")
    def test_duplicate_columns_are_detected(self, mock_spark):
        """Test that duplicate columns between DataFrames are detected."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F

        # Setup left DataFrame with columns
        mock_df = MagicMock()
        mock_df.columns = ["id", "ProductNaturalID", "amount"]

        # Setup right DataFrame with overlapping columns
        mock_join_df = MagicMock()
        mock_join_df.columns = ["ProductNaturalID", "ProductName", "ProductID"]

        # Setup join result that would have duplicates
        mock_result = MagicMock()
        mock_result.columns = ["id", "ProductNaturalID", "amount", "ProductNaturalID", "ProductName", "ProductID"]
        mock_df.join.return_value = mock_result

        # Setup mock select to return a DataFrame
        mock_selected = MagicMock()
        mock_result.select.return_value = mock_selected

        join_config = JoinConfig(
            name="test_join",
            type="left",
            source="dim_product",
            conditions=[JoinCondition(left="ProductNaturalID", right="dim_product.ProductNaturalID")]
        )
        sources = {"dim_product": mock_join_df}
        source_configs = [SourceConfig(name="dim_product", path="/data")]
        sql_parts = {"stage1": {"joins": []}}

        result = apply_single_join(mock_df, join_config, sources, source_configs, sql_parts, "stage1")

        # Verify select was called to handle duplicates
        mock_result.select.assert_called_once()
        # Verify the result is the selected DataFrame
        assert result == mock_selected

    @patch("deltagen.runner.join_builder._get_spark_functions")
    def test_no_duplicates_no_select_applied(self, mock_spark):
        """Test that when there are no duplicate columns, no special handling occurs."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F

        # Setup DataFrames with no overlapping columns
        mock_df = MagicMock()
        mock_df.columns = ["id", "amount"]

        mock_join_df = MagicMock()
        mock_join_df.columns = ["ProductName", "ProductID"]

        # Join result has no duplicates
        mock_result = MagicMock()
        mock_result.columns = ["id", "amount", "ProductName", "ProductID"]
        mock_df.join.return_value = mock_result

        join_config = JoinConfig(
            name="test_join",
            type="left",
            source="dim_product",
            conditions=[JoinCondition(left="id", right="dim_product.ProductID")]
        )
        sources = {"dim_product": mock_join_df}
        source_configs = [SourceConfig(name="dim_product", path="/data")]
        sql_parts = {"stage1": {"joins": []}}

        result = apply_single_join(mock_df, join_config, sources, source_configs, sql_parts, "stage1")

        # Verify select was NOT called since no duplicates
        mock_result.select.assert_not_called()
        # Result should be the join result directly
        assert result == mock_result

    @patch("deltagen.runner.join_builder._get_spark_functions")
    def test_duplicate_columns_debug_message(self, mock_spark):
        """Test that debug mode prints duplicate column information."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F

        mock_df = MagicMock()
        mock_df.columns = ["id", "ProductNaturalID"]

        mock_join_df = MagicMock()
        mock_join_df.columns = ["ProductNaturalID", "ProductName"]

        mock_result = MagicMock()
        mock_result.columns = ["id", "ProductNaturalID", "ProductNaturalID", "ProductName"]
        mock_df.join.return_value = mock_result
        mock_result.select.return_value = MagicMock()

        join_config = JoinConfig(
            name="test_join",
            type="left",
            source="dim_product",
            conditions=[JoinCondition(left="ProductNaturalID", right="dim_product.ProductNaturalID")]
        )
        sources = {"dim_product": mock_join_df}
        source_configs = [SourceConfig(name="dim_product", path="/data")]
        sql_parts = {"stage1": {"joins": []}}

        # Capture printed output
        import io
        import sys
        captured_output = io.StringIO()
        sys.stdout = captured_output

        apply_single_join(mock_df, join_config, sources, source_configs, sql_parts, "stage1", debug=True)

        sys.stdout = sys.__stdout__
        output = captured_output.getvalue()

        # Verify debug message contains information about duplicates
        assert "Duplicate columns detected" in output
        assert "ProductNaturalID" in output
        assert "dim_product" in output


class TestAliasColumnLeftSide:
    """Tests for alias.column notation on the left side of join conditions."""

    @patch("deltagen.runner.join_builder._get_spark_functions")
    def test_alias_dot_column_resolved_via_alias_maps(self, mock_spark):
        """Test that 'alias.column' on the left side resolves correctly using
        accumulated alias maps from previous joins stored in sql_parts."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F

        mock_df = MagicMock()
        mock_join_df = MagicMock()
        mock_join_df.columns = ["ul_model_id", "ul_model_name"]

        # Simulate: a previous join stored alias maps for "mapping_ifa"
        # "ul_model_id" is a direct (non-renamed) column from that join
        sql_parts = {
            "stage1": {
                "joins": [],
                "join_alias_maps": {
                    "mapping_ifa": {
                        "renamed": {"ifa": "mapping_ifa__ifa"},
                        "direct": ["ul_model_id", "firm_name"],
                    }
                },
            }
        }

        join_config = JoinConfig(
            name="model_lookup",
            type="left",
            source="model_table",
            conditions=[
                # alias.column notation: ul_model_id came from mapping_ifa join
                JoinCondition(left="mapping_ifa.ul_model_id", right="ul_model_id")
            ]
        )
        sources = {"model_table": mock_join_df}
        source_configs = [SourceConfig(name="model_table", path="/data")]

        apply_single_join(mock_df, join_config, sources, source_configs, sql_parts, "stage1")

        # The left-side column should have been resolved to "ul_model_id" (direct)
        # and accessed via df["ul_model_id"], not df["mapping_ifa.ul_model_id"]
        mock_df.__getitem__.assert_called_with("ul_model_id")

    @patch("deltagen.runner.join_builder._get_spark_functions")
    def test_alias_dot_renamed_column_resolved_correctly(self, mock_spark):
        """Test that 'alias.column' for a renamed duplicate resolves to alias__column."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F

        mock_df = MagicMock()
        mock_join_df = MagicMock()
        mock_join_df.columns = ["bm_model_id"]

        # "ifa" was a duplicate and got renamed to "mapping_ifa__ifa"
        sql_parts = {
            "stage1": {
                "joins": [],
                "join_alias_maps": {
                    "mapping_ifa": {
                        "renamed": {"ifa": "mapping_ifa__ifa"},
                        "direct": ["ul_model_id"],
                    }
                },
            }
        }

        join_config = JoinConfig(
            name="lookup",
            type="left",
            source="dim",
            conditions=[
                # References a renamed column via alias notation
                JoinCondition(left="mapping_ifa.ifa", right="bm_model_id")
            ]
        )
        sources = {"dim": mock_join_df}
        source_configs = [SourceConfig(name="dim", path="/data")]

        apply_single_join(mock_df, join_config, sources, source_configs, sql_parts, "stage1")

        # Should resolve "mapping_ifa.ifa" → "mapping_ifa__ifa"
        mock_df.__getitem__.assert_called_with("mapping_ifa__ifa")


class TestJoinExpression:
    """Tests for SQL-expression join conditions (e.g. LOWER(col))."""

    @patch("deltagen.runner.join_builder._get_spark_functions")
    def test_left_side_expression_uses_F_expr(self, mock_spark):
        """A left-side join condition with parentheses is parsed as a SQL expression."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F

        mock_df = MagicMock()
        mock_join_df = MagicMock()
        mock_join_df.columns = ["ifa_clean"]

        join_config = JoinConfig(
            name="case_insensitive_join",
            type="left",
            source="dim",
            conditions=[
                JoinCondition(left="LOWER(ifa)", right="ifa_clean"),
            ],
        )
        sources = {"dim": mock_join_df}
        source_configs = [SourceConfig(name="dim", path="/data")]
        sql_parts = {"stage1": {"joins": [], "join_alias_maps": {}}}

        apply_single_join(mock_df, join_config, sources, source_configs, sql_parts, "stage1")

        # The left side should be parsed as F.expr("LOWER(ifa)"), not df[...]
        mock_F.expr.assert_any_call("LOWER(ifa)")

    @patch("deltagen.runner.join_builder._get_spark_functions")
    def test_right_side_expression_uses_F_expr(self, mock_spark):
        """A right-side join condition with parentheses is parsed as a SQL expression."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F

        mock_df = MagicMock()
        mock_join_df = MagicMock()
        mock_join_df.columns = ["ifa"]

        join_config = JoinConfig(
            name="case_insensitive_join",
            type="left",
            source="dim",
            conditions=[
                JoinCondition(left="ifa_clean", right="LOWER(ifa)"),
            ],
        )
        sources = {"dim": mock_join_df}
        source_configs = [SourceConfig(name="dim", path="/data")]
        sql_parts = {"stage1": {"joins": [], "join_alias_maps": {}}}

        apply_single_join(mock_df, join_config, sources, source_configs, sql_parts, "stage1")

        mock_F.expr.assert_any_call("LOWER(ifa)")
