"""Unit tests for the new Pydantic models."""
from pydantic import ValidationError

from deltagen.model import (
    ColumnConfig,
    ColumnInput,
    EnvironmentConfig,
    EnvironmentLayerConfig,
    EnvironmentSourceConfig,
    JoinCondition,
    JoinConfig,
    SourceConfig,
    StageConfig,
    TableConfig,
    TypeSpec,
)


def test_column_extensions_are_captured():
    column = ColumnConfig(name="id", extensions={"foo": "bar"}, nullable=False)

    assert column.extensions["foo"] == "bar"
    assert column.foo == "bar"


def test_strict_base_model_get_extension_and_missing_attr():
    column = ColumnConfig(name="id", extensions={"foo": "bar"})

    # get_extension returns value or default
    assert column.get_extension("foo") == "bar"
    assert column.get_extension("missing", "default") == "default"

    # accessing a non-existent attribute should raise AttributeError
    try:
        _ = column.missing  # type: ignore[attr-defined]
    except AttributeError as exc:
        assert "missing" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Accessing a missing attribute should raise AttributeError")


def test_column_inputs_handle_source_column_and_expression():
    # expression-only column
    expr_col = ColumnConfig(
        name="active_flag",
        data_type="int",
        nullable=False,
        inputs=[ColumnInput(expression="CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END")],
    )

    assert len(expr_col.inputs) == 1
    assert expr_col.inputs[0].expression.startswith("CASE WHEN")
    assert expr_col.inputs[0].source is None
    assert expr_col.inputs[0].column is None

    # simple source column mapping
    mapped_col = ColumnConfig(
        name="customer_id",
        data_type="string",
        nullable=False,
        inputs=[ColumnInput(source="bronze_customers", column="customer_id")],
    )

    assert len(mapped_col.inputs) == 1
    assert mapped_col.inputs[0].source == "bronze_customers"
    assert mapped_col.inputs[0].column == "customer_id"
    assert mapped_col.inputs[0].expression is None

    # invalid: both expression and source/column set
    try:
        ColumnInput(
            source="bronze_customers",
            column="customer_id",
            expression="customer_id",
        )
    except ValidationError as exc:
        assert "either expression or source+column" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("ColumnInput should not allow both expression and source+column")

    # invalid: neither expression nor source/column set
    try:
        ColumnInput()
    except ValidationError as exc:
        assert "requires either expression or source+column" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("ColumnInput should require expression or source+column")


def test_environment_config_layers_and_sources():
    env = EnvironmentConfig(
        layers=[
            EnvironmentLayerConfig(
                name="raw",
                sources=[
                    EnvironmentSourceConfig(
                        name="workday", path="abfss:///Tables", format="delta"
                    ),
                    EnvironmentSourceConfig(
                        name="manualexcel", path="abfss:/Tables", format="delta"
                    ),
                ],
            ),
            EnvironmentLayerConfig(
                name="cleansed",
                sources=[
                    EnvironmentSourceConfig(
                        name="default", path="abfss:/Tables", format="delta"
                    )
                ],
            ),
        ]
    )

    assert [layer.name for layer in env.layers] == ["raw", "cleansed"]
    raw = env.layers[0]
    assert {s.name for s in raw.sources} == {"workday", "manualexcel"}


def test_stage_temporary_columns_helper():
    stage = StageConfig(
        name="transform",
        columns=[
            ColumnConfig(name="persisted", temporary=False),
            ColumnConfig(name="tmp_work", temporary=True),
        ],
    )

    assert stage.temporary_columns() == ("tmp_work",)


def test_table_iter_columns_yields_all_columns_across_stages():
    table = TableConfig(
        name="t",
        stages=[
            StageConfig(
                name="s1",
                columns=[ColumnConfig(name="a"), ColumnConfig(name="b", temporary=True)],
            ),
            StageConfig(
                name="s2",
                columns=[ColumnConfig(name="c")],
            ),
        ],
    )

    names = [col.name for col in table.iter_columns()]
    assert names == ["a", "b", "c"]


def test_table_collects_temporary_columns():
    table = TableConfig(
        name="customers",
        sources=[SourceConfig(name="bronze_customers")],
        stages=[
            StageConfig(
                name="transform",
                columns=[
                    ColumnConfig(name="c1", temporary=True),
                    ColumnConfig(name="c2", temporary=False),
                ],
            )
        ],
    )

    assert table.temporary_columns == ("c1",)


def test_join_condition_fields_are_captured():
    join = JoinConfig(
        name="ref",
        source="dim",
        conditions=[JoinCondition(left="l.id", right="r.id", operator="=")],
    )

    cond = join.conditions[0]
    assert cond.left == "l.id"
    assert cond.right == "r.id"
    assert cond.operator == "="


def test_models_are_frozen():
    column = ColumnConfig(name="id")

    try:
        column.name = "other"  # type: ignore[misc]
    except Exception as exc:  # pydantic raises ValidationError for frozen models
        assert "frozen" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("ColumnConfig should be immutable")


def test_column_get_typespec_returns_typespec_for_valid_type():
    col = ColumnConfig(name="customer_id", data_type="varchar(255)")
    spec = col.get_typespec()
    
    assert isinstance(spec, TypeSpec)
    assert spec.base_type == "STRING"
    assert spec.precision == 255
    assert spec.to_spark_sql() == "STRING"
    assert spec.to_standard_sql() == "VARCHAR(255)"


def test_column_get_typespec_handles_money_type():
    col = ColumnConfig(name="price", data_type="money")
    spec = col.get_typespec()
    
    assert spec.base_type == "DECIMAL"
    assert spec.precision == 19
    assert spec.scale == 4
    assert spec.to_spark_sql() == "DECIMAL(19,4)"


def test_column_get_typespec_handles_decimal():
    col = ColumnConfig(name="amount", data_type="decimal(18,2)")
    spec = col.get_typespec()
    
    assert spec.to_spark_sql() == "DECIMAL(18,2)"
    assert spec.to_standard_sql() == "DECIMAL(18,2)"


def test_column_get_typespec_returns_none_when_no_data_type():
    col = ColumnConfig(name="some_column")
    spec = col.get_typespec()
    
    assert spec is None


def test_column_get_typespec_raises_error_for_invalid_type():
    col = ColumnConfig(name="bad_column", data_type="invalid_type_xyz")
    
    try:
        col.get_typespec()
    except ValueError as exc:
        assert "Unrecognized type" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected ValueError for invalid data type")
