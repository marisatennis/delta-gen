"""Unit tests for sql_generator module."""
from deltagen.runner.sql_generator import generate_sql, explain_plan
from deltagen.model import TableConfig
from deltagen.model.stage import StageConfig
from deltagen.model.column import ColumnConfig, ColumnInput
from deltagen.model.source import SourceConfig
from deltagen.model.join import JoinConfig, JoinCondition


class TestGenerateSql:
    """Tests for generate_sql function."""

    def test_simple_select(self):
        """Test SQL generation with simple SELECT."""
        config = TableConfig(
            name="test",
            stages=[StageConfig(name="stage1")]
        )
        sql_parts = {
            "stage1": {
                "select": ["id", "name"],
                "from": "customers",
                "joins": [],
                "where": []
            }
        }

        sql = generate_sql(config, sql_parts, "stage1")

        assert "SELECT" in sql
        assert "id" in sql
        assert "name" in sql
        assert "FROM customers" in sql

    def test_select_with_where(self):
        """Test SQL generation with WHERE clause."""
        config = TableConfig(
            name="test",
            stages=[StageConfig(name="stage1")]
        )
        sql_parts = {
            "stage1": {
                "select": ["*"],
                "from": "orders",
                "joins": [],
                "where": ["status = 'active'", "amount > 0"]
            }
        }

        sql = generate_sql(config, sql_parts, "stage1")

        assert "WHERE" in sql
        assert "status = 'active'" in sql
        assert "amount > 0" in sql
        assert "AND" in sql

    def test_select_with_joins(self):
        """Test SQL generation with JOIN clause."""
        config = TableConfig(
            name="test",
            stages=[StageConfig(name="stage1")]
        )
        sql_parts = {
            "stage1": {
                "select": ["o.id", "c.name"],
                "from": "orders",
                "joins": ["LEFT JOIN customers\n    ON o.customer_id = c.id"],
                "where": []
            }
        }

        sql = generate_sql(config, sql_parts, "stage1")

        assert "LEFT JOIN customers" in sql
        assert "o.customer_id = c.id" in sql

    def test_all_stages_without_stage_param(self):
        """Test SQL generation for all stages when stage is None."""
        config = TableConfig(
            name="test",
            stages=[
                StageConfig(name="stage1"),
                StageConfig(name="stage2")
            ]
        )
        sql_parts = {
            "stage1": {
                "select": ["id"],
                "from": "source",
                "joins": [],
                "where": []
            },
            "stage2": {
                "select": ["id", "name"],
                "from": "previous_stage",
                "joins": [],
                "where": []
            }
        }

        sql = generate_sql(config, sql_parts, stage=None)

        assert "Stage: stage2" in sql

    def test_empty_sql_parts(self):
        """Test SQL generation with empty/missing parts."""
        config = TableConfig(
            name="test",
            stages=[StageConfig(name="stage1")]
        )
        sql_parts = {}  # Empty

        sql = generate_sql(config, sql_parts, "stage1")

        # Should handle gracefully with defaults
        assert "SELECT" in sql
        assert "FROM source" in sql

    def test_select_with_cast_expressions(self):
        """Test SQL with CAST expressions in SELECT."""
        config = TableConfig(
            name="test",
            stages=[StageConfig(name="stage1")]
        )
        sql_parts = {
            "stage1": {
                "select": [
                    "CAST(src.id AS INTEGER) AS id",
                    "CAST(src.name AS STRING) AS name"
                ],
                "from": "raw_customers",
                "joins": [],
                "where": []
            }
        }

        sql = generate_sql(config, sql_parts, "stage1")

        assert "CAST(src.id AS INTEGER)" in sql
        assert "CAST(src.name AS STRING)" in sql


class TestExplainPlan:
    """Tests for explain_plan function."""

    def test_basic_explanation(self):
        """Test basic plan explanation."""
        config = TableConfig(
            name="customer_dim",
            layer="silver"
        )

        explanation = explain_plan(config)

        assert "Table: customer_dim" in explanation
        assert "Layer: silver" in explanation

    def test_explanation_with_sources(self):
        """Test explanation includes source information."""
        config = TableConfig(
            name="test",
            sources=[
                SourceConfig(
                    name="raw_customers",
                    path="/lakehouse/bronze/customers",
                    format="delta"
                )
            ]
        )

        explanation = explain_plan(config)

        assert "Sources:" in explanation
        assert "raw_customers" in explanation
        assert "/lakehouse/bronze/customers" in explanation
        assert "delta" in explanation

    def test_explanation_with_catalog_source(self):
        """Test explanation includes catalog.schema.table reference."""
        config = TableConfig(
            name="test",
            sources=[
                SourceConfig(
                    name="dim_customer",
                    catalog="main",
                    schema="gold",
                    table="customer"
                )
            ]
        )

        explanation = explain_plan(config)

        assert "main.gold.customer" in explanation

    def test_explanation_with_columns(self):
        """Test explanation includes column details."""
        config = TableConfig(
            name="test",
            sources=[SourceConfig(name="src", path="/data")],
            stages=[
                StageConfig(
                    name="transform",
                    columns=[
                        ColumnConfig(
                            name="customer_id",
                            data_type="int",
                            natural=True,
                            inputs=[ColumnInput(source="src", column="id")]
                        ),
                        ColumnConfig(
                            name="temp_value",
                            data_type="decimal(18,2)",
                            temporary=True,
                            inputs=[ColumnInput(expression="a * b")]
                        ),
                        ColumnConfig(
                            name="required_field",
                            data_type="varchar(100)",
                            nullable=False,
                            inputs=[ColumnInput(source="src", column="field")]
                        )
                    ]
                )
            ]
        )

        explanation = explain_plan(config)

        assert "Columns:" in explanation
        assert "customer_id" in explanation
        assert "natural" in explanation
        assert "temp_value" in explanation
        assert "temp" in explanation
        assert "required_field" in explanation
        assert "not null" in explanation

    def test_explanation_with_joins(self):
        """Test explanation includes join details."""
        config = TableConfig(
            name="test",
            sources=[
                SourceConfig(name="orders", path="/data/orders"),
                SourceConfig(name="customers", path="/data/customers")
            ],
            stages=[
                StageConfig(
                    name="enrich",
                    joins=[
                        JoinConfig(
                            name="customer_lookup",
                            type="left",
                            source="customers",
                            conditions=[
                                JoinCondition(left="o.cust_id", right="c.id", operator="=")
                            ]
                        )
                    ]
                )
            ]
        )

        explanation = explain_plan(config)

        assert "Joins:" in explanation
        assert "LEFT" in explanation
        assert "customers" in explanation
        assert "o.cust_id" in explanation

    def test_explanation_with_filters(self):
        """Test explanation includes filter details."""
        config = TableConfig(
            name="test",
            sources=[SourceConfig(name="src", path="/data")],
            stages=[
                StageConfig(
                    name="transform",
                    filters=[
                        "status = 'active'",
                        "created_date >= '2020-01-01'"
                    ]
                )
            ]
        )

        explanation = explain_plan(config)

        assert "Filters:" in explanation
        assert "status = 'active'" in explanation
        assert "created_date >= '2020-01-01'" in explanation

    def test_explanation_with_natural_keys(self):
        """Test explanation includes natural key summary."""
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

        explanation = explain_plan(config)

        assert "Natural Keys:" in explanation
        assert "id" in explanation
        assert "email" in explanation

    def test_explanation_multiple_stages(self):
        """Test explanation shows all stages."""
        config = TableConfig(
            name="test",
            sources=[SourceConfig(name="src", path="/data")],
            stages=[
                StageConfig(name="extract", mode="transformation"),
                StageConfig(name="transform", mode="transformation"),
                StageConfig(name="load", mode="transformation")
            ]
        )

        explanation = explain_plan(config)

        assert "Stage: extract" in explanation
        assert "Stage: transform" in explanation
        assert "Stage: load" in explanation

    def test_explanation_layer_not_specified(self):
        """Test explanation when layer is not set."""
        config = TableConfig(name="test")

        explanation = explain_plan(config)

        assert "Layer: not specified" in explanation


class TestSqlFormatting:
    """Tests for SQL output formatting."""

    def test_columns_on_separate_lines(self):
        """Test that multiple columns are formatted on separate lines."""
        config = TableConfig(
            name="test",
            stages=[StageConfig(name="stage1")]
        )
        sql_parts = {
            "stage1": {
                "select": [
                    "CAST(id AS INTEGER) AS id",
                    "CAST(name AS STRING) AS name",
                    "CAST(amount AS DECIMAL(18,2)) AS amount"
                ],
                "from": "source",
                "joins": [],
                "where": []
            }
        }

        sql = generate_sql(config, sql_parts, "stage1")

        # Columns should be comma-separated
        lines = sql.split("\n")
        assert len(lines) > 1  # Multi-line output

    def test_where_conditions_on_separate_lines(self):
        """Test that multiple WHERE conditions are formatted."""
        config = TableConfig(
            name="test",
            stages=[StageConfig(name="stage1")]
        )
        sql_parts = {
            "stage1": {
                "select": ["*"],
                "from": "source",
                "joins": [],
                "where": ["a = 1", "b = 2", "c = 3"]
            }
        }

        sql = generate_sql(config, sql_parts, "stage1")

        assert "WHERE" in sql
        assert "AND" in sql
