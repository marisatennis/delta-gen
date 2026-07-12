"""Unit tests for column_builder module."""
import pytest
from unittest.mock import MagicMock, patch

from deltagen.runner.column_builder import (
    column_to_sql,
    build_column_input,
    build_single_column,
    analyze_required_columns,
    _extract_column_refs_from_expression,
)
from deltagen.model.column import ColumnConfig, ColumnInput
from deltagen.model.join import JoinConfig, JoinCondition
from deltagen.model.source import SourceConfig
from deltagen.model.stage import StageConfig


class TestColumnToSql:
    """Tests for column_to_sql function - SQL representation generation."""

    def test_simple_column_reference(self):
        """Test SQL for direct column reference with source."""
        config = ColumnConfig(
            name="customer_id",
            data_type="int",
            inputs=[ColumnInput(source="raw_customers", column="id")]
        )
        sql = column_to_sql(config)
        assert "raw_customers.id" in sql
        assert "CAST" in sql
        assert "INTEGER" in sql

    def test_column_passthrough_via_expression(self):
        """Test SQL for column pass-through using expression."""
        # When passing through a column without transformation,
        # use expression to reference it directly
        config = ColumnConfig(
            name="id",
            inputs=[ColumnInput(expression="id")]
        )
        sql = column_to_sql(config)
        assert "id" in sql

    def test_expression_column(self):
        """Test SQL for expression-based column."""
        config = ColumnConfig(
            name="full_name",
            data_type="varchar(255)",
            inputs=[ColumnInput(expression="CONCAT(first_name, ' ', last_name)")]
        )
        sql = column_to_sql(config)
        assert "CONCAT(first_name, ' ', last_name)" in sql
        assert "CAST" in sql
        assert "STRING" in sql

    def test_column_with_string_default(self):
        """Test SQL for column with string default value."""
        config = ColumnConfig(
            name="status",
            data_type="varchar(50)",
            default="active",
            inputs=[ColumnInput(source="src", column="status")]
        )
        sql = column_to_sql(config)
        assert "COALESCE" in sql
        assert "'active'" in sql
        assert "src.status" in sql

    def test_column_with_numeric_default(self):
        """Test SQL for column with numeric default value."""
        config = ColumnConfig(
            name="quantity",
            data_type="int",
            default=0,
            inputs=[ColumnInput(source="src", column="qty")]
        )
        sql = column_to_sql(config)
        assert "COALESCE" in sql
        assert "0" in sql

    def test_multi_input_concat_default_delimiter(self):
        """Test SQL for multiple inputs with default delimiter."""
        config = ColumnConfig(
            name="composite_key",
            data_type="varchar(500)",
            inputs=[
                ColumnInput(source="t1", column="region"),
                ColumnInput(source="t1", column="id")
            ]
        )
        sql = column_to_sql(config)
        assert "CONCAT_WS" in sql
        assert "||" in sql
        assert "t1.region" in sql
        assert "t1.id" in sql

    def test_multi_input_concat_custom_delimiter(self):
        """Test SQL for multiple inputs with custom delimiter."""
        config = ColumnConfig(
            name="composite_key",
            data_type="varchar(500)",
            inputs=[
                ColumnInput(source="t1", column="region"),
                ColumnInput(source="t1", column="id")
            ],
            extensions={"delimiter": "-"}
        )
        sql = column_to_sql(config)
        assert "CONCAT_WS('-'" in sql

    def test_no_inputs_with_default(self):
        """Test SQL for column with no inputs but has default."""
        config = ColumnConfig(
            name="created_by",
            default="system"
        )
        sql = column_to_sql(config)
        assert sql == "'system'"

    def test_no_inputs_no_default(self):
        """Test SQL for column with no inputs and no default."""
        config = ColumnConfig(name="placeholder")
        sql = column_to_sql(config)
        assert sql == "NULL"

    def test_varchar_type_conversion(self):
        """Test varchar type is converted to STRING."""
        config = ColumnConfig(
            name="name",
            data_type="varchar(255)",
            inputs=[ColumnInput(source="src", column="name")]
        )
        sql = column_to_sql(config)
        assert "STRING" in sql

    def test_decimal_type_preserves_precision(self):
        """Test decimal type preserves precision and scale."""
        config = ColumnConfig(
            name="amount",
            data_type="decimal(18,2)",
            inputs=[ColumnInput(source="src", column="amount")]
        )
        sql = column_to_sql(config)
        assert "DECIMAL(18,2)" in sql

    def test_money_type_conversion(self):
        """Test money type is converted to DECIMAL(19,4)."""
        config = ColumnConfig(
            name="price",
            data_type="money",
            inputs=[ColumnInput(source="src", column="price")]
        )
        sql = column_to_sql(config)
        assert "DECIMAL(19,4)" in sql

    def test_bigint_type(self):
        """Test bigint type."""
        config = ColumnConfig(
            name="big_id",
            data_type="bigint",
            inputs=[ColumnInput(source="src", column="id")]
        )
        sql = column_to_sql(config)
        assert "BIGINT" in sql

    def test_date_type(self):
        """Test date type."""
        config = ColumnConfig(
            name="birth_date",
            data_type="date",
            inputs=[ColumnInput(source="src", column="dob")]
        )
        sql = column_to_sql(config)
        assert "DATE" in sql

    def test_timestamp_type(self):
        """Test timestamp type."""
        config = ColumnConfig(
            name="created_at",
            data_type="timestamp",
            inputs=[ColumnInput(source="src", column="created")]
        )
        sql = column_to_sql(config)
        assert "TIMESTAMP" in sql

    def test_expression_with_type_cast(self):
        """Test expression column with type casting."""
        config = ColumnConfig(
            name="calculated",
            data_type="decimal(18,2)",
            inputs=[ColumnInput(expression="quantity * unit_price")]
        )
        sql = column_to_sql(config)
        assert "quantity * unit_price" in sql
        assert "CAST" in sql
        assert "DECIMAL(18,2)" in sql

    def test_mixed_inputs_expression_and_column(self):
        """Test concat with mixed expression and column inputs."""
        config = ColumnConfig(
            name="mixed",
            inputs=[
                ColumnInput(expression="'PREFIX'"),
                ColumnInput(source="src", column="id")
            ]
        )
        sql = column_to_sql(config)
        assert "CONCAT_WS" in sql
        assert "'PREFIX'" in sql
        assert "src.id" in sql


class TestBuildColumnInput:
    """Tests for build_column_input function with mocked Spark."""

    @patch("deltagen.runner.column_builder._get_spark_functions")
    def test_expression_input(self, mock_spark):
        """Test building expression input."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F

        inp = ColumnInput(expression="UPPER(name)")
        build_column_input(inp)

        mock_F.expr.assert_called_once_with("UPPER(name)")

    @patch("deltagen.runner.column_builder._get_spark_functions")
    def test_source_column_input(self, mock_spark):
        """Test building source.column input."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F

        inp = ColumnInput(source="customers", column="id")
        build_column_input(inp)

        mock_F.col.assert_called_once_with("customers.id")

    @patch("deltagen.runner.column_builder._get_spark_functions")
    def test_column_passthrough_via_expression(self, mock_spark):
        """Test building column pass-through via expression."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F

        # Column pass-through uses expression
        inp = ColumnInput(expression="existing_col")
        build_column_input(inp)

        mock_F.expr.assert_called_once_with("existing_col")

    def test_invalid_input_rejected_by_validation(self):
        """Test that empty input is rejected at validation time."""
        from pydantic import ValidationError

        # Pydantic validation catches this at construction time
        with pytest.raises(ValidationError):
            ColumnInput()  # No expression or column


class TestBuildSingleColumn:
    """Tests for build_single_column function with mocked Spark."""

    @patch("deltagen.runner.column_builder._get_spark_functions")
    def test_no_inputs_with_default(self, mock_spark):
        """Test column with no inputs but has default."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F

        config = ColumnConfig(name="status", default="active")
        build_single_column(config, {})

        mock_F.lit.assert_called_with("active")

    @patch("deltagen.runner.column_builder._get_spark_functions")
    def test_no_inputs_no_default(self, mock_spark):
        """Test column with no inputs and no default returns null."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F

        config = ColumnConfig(name="placeholder")
        build_single_column(config, {})

        mock_F.lit.assert_called_with(None)

    @patch("deltagen.runner.column_builder._get_spark_functions")
    def test_single_input_calls_build_column_input(self, mock_spark):
        """Test single input column builds correctly."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F
        mock_col = MagicMock()
        mock_F.col.return_value = mock_col
        mock_col.cast.return_value = mock_col

        config = ColumnConfig(
            name="id",
            data_type="int",
            inputs=[ColumnInput(source="src", column="id")]
        )
        build_single_column(config, {})

        mock_F.col.assert_called_with("src.id")
        mock_col.cast.assert_called()

    @patch("deltagen.runner.column_builder._get_spark_functions")
    def test_multiple_inputs_uses_concat_ws(self, mock_spark):
        """Test multiple inputs are concatenated."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F
        mock_col = MagicMock()
        mock_F.col.return_value = mock_col
        mock_F.concat_ws.return_value = mock_col
        mock_col.cast.return_value = mock_col

        config = ColumnConfig(
            name="key",
            data_type="varchar(100)",
            inputs=[
                ColumnInput(source="t1", column="a"),
                ColumnInput(source="t1", column="b")
            ],
            extensions={"delimiter": "-"}
        )
        build_single_column(config, {})

        # Check concat_ws was called with delimiter
        mock_F.concat_ws.assert_called_once()
        args = mock_F.concat_ws.call_args[0]
        assert args[0] == "-"

    @patch("deltagen.runner.column_builder._get_spark_functions")
    def test_default_value_applies_coalesce(self, mock_spark):
        """Test default value wraps expression in coalesce."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F
        mock_col = MagicMock()
        mock_F.col.return_value = mock_col
        mock_F.coalesce.return_value = mock_col
        mock_col.cast.return_value = mock_col

        config = ColumnConfig(
            name="status",
            data_type="varchar(50)",
            default="pending",
            inputs=[ColumnInput(source="src", column="status")]
        )
        build_single_column(config, {})

        mock_F.coalesce.assert_called_once()

    @patch("deltagen.runner.column_builder._get_spark_functions")
    def test_type_casting_applied(self, mock_spark):
        """Test that type casting is applied when data_type specified."""
        mock_F = MagicMock()
        mock_spark.return_value = mock_F
        mock_col = MagicMock()
        mock_F.col.return_value = mock_col
        mock_col.cast.return_value = mock_col

        config = ColumnConfig(
            name="amount",
            data_type="decimal(18,2)",
            inputs=[ColumnInput(source="src", column="amount")]
        )
        build_single_column(config, {})

        mock_col.cast.assert_called_with("DECIMAL(18,2)")


class TestColumnFlags:
    """Tests for column configuration flags."""

    def test_natural_key_flag(self):
        """Test natural key flag is preserved."""
        config = ColumnConfig(
            name="customer_id",
            data_type="int",
            natural=True,
            inputs=[ColumnInput(source="src", column="id")]
        )
        assert config.natural is True

    def test_temporary_flag(self):
        """Test temporary flag is preserved."""
        config = ColumnConfig(
            name="intermediate_calc",
            temporary=True,
            inputs=[ColumnInput(expression="a + b")]
        )
        assert config.temporary is True

    def test_nullable_flag(self):
        """Test nullable flag defaults to True and can be set to False."""
        config_nullable = ColumnConfig(name="optional")
        config_not_null = ColumnConfig(name="required", nullable=False)

        assert config_nullable.nullable is True
        assert config_not_null.nullable is False


class TestExtractColumnRefsFromExpression:
    """Tests for _extract_column_refs_from_expression function."""

    def test_single_reference(self):
        """Test extracting single source.column reference."""
        refs = _extract_column_refs_from_expression("src.amount")
        assert refs == [("src", "amount")]

    def test_multiple_references(self):
        """Test extracting multiple references."""
        refs = _extract_column_refs_from_expression("src.amount * fx.rate")
        assert ("src", "amount") in refs
        assert ("fx", "rate") in refs

    def test_no_references(self):
        """Test expression without references."""
        refs = _extract_column_refs_from_expression("1 + 2")
        assert refs == []

    def test_excludes_sql_functions(self):
        """Test that SQL function patterns are excluded."""
        refs = _extract_column_refs_from_expression("CAST.value + COALESCE.other")
        assert refs == []

    def test_mixed_with_sql_functions(self):
        """Test extracting refs while ignoring SQL functions."""
        refs = _extract_column_refs_from_expression(
            "COALESCE(src.value, 0) + DATE.something"
        )
        # Only src.value should be extracted, not COALESCE.x or DATE.something
        assert refs == [("src", "value")]

    def test_complex_expression(self):
        """Test complex expression with multiple sources."""
        refs = _extract_column_refs_from_expression(
            "COALESCE(unit_price * fx.rate_to_usd, unit_price)"
        )
        assert ("fx", "rate_to_usd") in refs


class TestAnalyzeRequiredColumns:
    """Tests for analyze_required_columns function."""

    def test_column_input_direct_reference(self):
        """Test detecting columns from direct source.column inputs."""
        stages = [
            StageConfig(
                name="stage1",
                columns=[
                    ColumnConfig(
                        name="order_id",
                        inputs=[ColumnInput(source="src", column="id")]
                    ),
                    ColumnConfig(
                        name="customer_id",
                        inputs=[ColumnInput(source="src", column="cust_id")]
                    ),
                ]
            )
        ]
        sources = [SourceConfig(name="src", path="/data")]

        required = analyze_required_columns(stages, sources)

        assert "src" in required
        assert "id" in required["src"]
        assert "cust_id" in required["src"]

    def test_column_input_expression(self):
        """Test detecting columns from expression inputs."""
        stages = [
            StageConfig(
                name="stage1",
                columns=[
                    ColumnConfig(
                        name="total",
                        inputs=[ColumnInput(expression="src.qty * src.price")]
                    ),
                ]
            )
        ]
        sources = [SourceConfig(name="src", path="/data")]

        required = analyze_required_columns(stages, sources)

        assert "src" in required
        assert "qty" in required["src"]
        assert "price" in required["src"]

    def test_join_conditions(self):
        """Test detecting columns from join conditions."""
        stages = [
            StageConfig(
                name="stage1",
                joins=[
                    JoinConfig(
                        name="customer_lookup",
                        type="left",
                        source="dim_customer",
                        conditions=[
                            JoinCondition(
                                left="src.customer_id",
                                right="cust.id"
                            )
                        ]
                    )
                ]
            )
        ]
        sources = [
            SourceConfig(name="src", path="/data"),
            SourceConfig(name="dim_customer", path="/dim", alias="cust"),
        ]

        required = analyze_required_columns(stages, sources)

        assert "customer_id" in required["src"]
        assert "id" in required["dim_customer"]  # resolved from alias

    def test_filter_references(self):
        """Test detecting columns from filters."""
        stages = [
            StageConfig(
                name="stage1",
                filters=["src.order_date >= '2024-01-01'", "src.status = 'active'"]
            )
        ]
        sources = [SourceConfig(name="src", path="/data")]

        required = analyze_required_columns(stages, sources)

        assert "src" in required
        assert "order_date" in required["src"]
        assert "status" in required["src"]

    def test_source_filters(self):
        """Test detecting columns from source_filters."""
        stages = [
            StageConfig(
                name="stage1",
                source_filters={
                    "dim_customer": ["is_current = true", "region = 'US'"]
                }
            )
        ]
        sources = [SourceConfig(name="dim_customer", path="/dim")]

        required = analyze_required_columns(stages, sources)

        assert "dim_customer" in required
        assert "is_current" in required["dim_customer"]
        assert "region" in required["dim_customer"]

    def test_alias_resolution(self):
        """Test that aliases are resolved to source names."""
        stages = [
            StageConfig(
                name="stage1",
                columns=[
                    ColumnConfig(
                        name="customer_name",
                        inputs=[ColumnInput(source="c", column="name")]
                    )
                ]
            )
        ]
        sources = [SourceConfig(name="dim_customer", path="/dim", alias="c")]

        required = analyze_required_columns(stages, sources)

        # Should be stored under source name, not alias
        assert "dim_customer" in required
        assert "name" in required["dim_customer"]

    def test_multiple_stages(self):
        """Test analyzing multiple stages."""
        stages = [
            StageConfig(
                name="stage1",
                columns=[
                    ColumnConfig(
                        name="id",
                        inputs=[ColumnInput(source="src", column="order_id")]
                    )
                ]
            ),
            StageConfig(
                name="stage2",
                columns=[
                    ColumnConfig(
                        name="amount",
                        inputs=[ColumnInput(source="src", column="total_amount")]
                    )
                ]
            )
        ]
        sources = [SourceConfig(name="src", path="/data")]

        required = analyze_required_columns(stages, sources)

        assert "order_id" in required["src"]
        assert "total_amount" in required["src"]

    def test_empty_stages(self):
        """Test with empty stages."""
        required = analyze_required_columns([], [])
        assert required == {}

    def test_comprehensive_example(self):
        """Test a comprehensive example with all reference types."""
        stages = [
            StageConfig(
                name="enrich",
                columns=[
                    ColumnConfig(
                        name="order_id",
                        inputs=[ColumnInput(source="orders", column="id")]
                    ),
                    ColumnConfig(
                        name="customer_name",
                        inputs=[ColumnInput(source="cust", column="name")]
                    ),
                    ColumnConfig(
                        name="total_usd",
                        inputs=[ColumnInput(expression="orders.amount * fx.rate")]
                    ),
                ],
                joins=[
                    JoinConfig(
                        name="customer_join",
                        type="left",
                        source="dim_customer",
                        conditions=[
                            JoinCondition(left="orders.customer_id", right="cust.customer_id")
                        ]
                    ),
                    JoinConfig(
                        name="fx_join",
                        type="left",
                        source="exchange_rates",
                        conditions=[
                            JoinCondition(left="orders.currency", right="fx.from_currency")
                        ]
                    ),
                ],
                filters=["orders.status = 'completed'"],
                source_filters={
                    "dim_customer": ["is_current = true"]
                }
            )
        ]
        sources = [
            SourceConfig(name="raw_orders", path="/orders", alias="orders"),
            SourceConfig(name="dim_customer", path="/customers", alias="cust"),
            SourceConfig(name="exchange_rates", path="/fx", alias="fx"),
        ]

        required = analyze_required_columns(stages, sources)

        # Check orders source (via alias)
        assert "raw_orders" in required
        orders_cols = required["raw_orders"]
        assert "id" in orders_cols
        assert "customer_id" in orders_cols
        assert "amount" in orders_cols
        assert "currency" in orders_cols
        assert "status" in orders_cols

        # Check customer source
        assert "dim_customer" in required
        cust_cols = required["dim_customer"]
        assert "name" in cust_cols
        assert "customer_id" in cust_cols
        assert "is_current" in cust_cols

        # Check fx source
        assert "exchange_rates" in required
        fx_cols = required["exchange_rates"]
        assert "rate" in fx_cols
        assert "from_currency" in fx_cols

    def test_alias_dot_column_on_left_adds_to_correct_source(self):
        """Test that alias.column on the left side of a join condition adds the
        column to the correct preceding join source (not to an unrelated source)."""
        stages = [
            StageConfig(
                name="enrich",
                joins=[
                    JoinConfig(
                        name="ifa_join",
                        type="left",
                        source="mapping_ifa",
                        alias="mapping_ifa",
                        conditions=[
                            JoinCondition(left="ifa", right="firm_name")
                        ]
                    ),
                    JoinConfig(
                        name="model_lookup",
                        type="left",
                        source="model_table",
                        conditions=[
                            # Explicit alias.column: ul_model_id came from ifa_join
                            JoinCondition(left="mapping_ifa.ul_model_id", right="ul_model_id")
                        ]
                    ),
                ]
            )
        ]
        sources = [
            SourceConfig(name="mapping_ifa", path="/ifa"),
            SourceConfig(name="model_table", path="/model"),
        ]

        required = analyze_required_columns(stages, sources)

        # ul_model_id must be loaded from mapping_ifa (the aliased source)
        assert "ul_model_id" in required["mapping_ifa"]
        # ul_model_id must also be loaded from model_table (right side of join)
        assert "ul_model_id" in required["model_table"]

    def test_join_alias_differs_from_source_name(self):
        """Test that join alias (≠ source name) is resolved correctly.

        Real-world case: alias='mapping_product', source='mapping_product_pmps'.
        A subsequent join with left='mapping_product.bm_model_id' must add
        bm_model_id to mapping_product_pmps, NOT to a phantom 'mapping_product' key.
        """
        stages = [
            StageConfig(
                name="enrich",
                joins=[
                    JoinConfig(
                        name="mapping_product_join",
                        type="left",
                        source="mapping_product_pmps",
                        alias="mapping_product",
                        conditions=[
                            JoinCondition(left="model", right="title")
                        ]
                    ),
                    JoinConfig(
                        name="master_lookup_join",
                        type="left",
                        source="master_lookup_platform_fum_models",
                        alias="master_lookup",
                        conditions=[
                            # alias.column where alias refers to the preceding join
                            JoinCondition(left="mapping_product.bm_model_id", right="bm_model_id")
                        ]
                    ),
                ]
            )
        ]
        sources = [
            SourceConfig(name="mapping_product_pmps", path="/product"),
            SourceConfig(name="master_lookup_platform_fum_models", path="/lookup"),
        ]

        required = analyze_required_columns(stages, sources)

        # bm_model_id must be loaded from the actual source, not from a phantom alias bucket
        assert "bm_model_id" in required["mapping_product_pmps"]
        assert "mapping_product" not in required  # no phantom key
        # right-side bm_model_id also tracked for the lookup source
        assert "bm_model_id" in required["master_lookup_platform_fum_models"]

    def test_bare_left_condition_does_not_pollute_preceding_source(self):
        """Test that a bare (un-prefixed) left join condition does NOT add
        the column to any preceding join source — it comes from the initial
        stage DataFrame and is always available."""
        stages = [
            StageConfig(
                name="enrich",
                joins=[
                    JoinConfig(
                        name="first_join",
                        type="left",
                        source="lookup_a",
                        conditions=[JoinCondition(left="id", right="lookup_id")]
                    ),
                    JoinConfig(
                        name="second_join",
                        type="left",
                        source="lookup_b",
                        conditions=[
                            # Bare left — comes from initial DataFrame, not from lookup_a
                            JoinCondition(left="model", right="model_name")
                        ]
                    ),
                ]
            )
        ]
        sources = [
            SourceConfig(name="lookup_a", path="/a"),
            SourceConfig(name="lookup_b", path="/b"),
        ]

        required = analyze_required_columns(stages, sources)

        # "model" must NOT be added to lookup_a (it comes from the initial DF)
        assert "model" not in required.get("lookup_a", set())
        # Right-side columns are still correctly tracked
        assert "model_name" in required["lookup_b"]
