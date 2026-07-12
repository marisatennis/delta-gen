"""Unit tests for TypeSpec and parse_type()."""
import pytest

from deltagen.model import TypeSpec, parse_type


class TestTypeSpecOutput:
    """Test TypeSpec output methods."""

    def test_string_to_spark_sql(self):
        spec = TypeSpec(base_type="STRING", precision=255)
        assert spec.to_spark_sql() == "STRING"

    def test_string_to_standard_sql(self):
        spec = TypeSpec(base_type="STRING", precision=255)
        assert spec.to_standard_sql() == "VARCHAR(255)"

    def test_string_no_precision_to_standard_sql(self):
        spec = TypeSpec(base_type="STRING")
        assert spec.to_standard_sql() == "VARCHAR"

    def test_decimal_to_spark_sql(self):
        spec = TypeSpec(base_type="DECIMAL", precision=18, scale=2)
        assert spec.to_spark_sql() == "DECIMAL(18,2)"

    def test_decimal_to_standard_sql(self):
        spec = TypeSpec(base_type="DECIMAL", precision=18, scale=2)
        assert spec.to_standard_sql() == "DECIMAL(18,2)"

    def test_decimal_with_zero_scale(self):
        spec = TypeSpec(base_type="DECIMAL", precision=10)
        assert spec.to_spark_sql() == "DECIMAL(10,0)"
        assert spec.to_standard_sql() == "DECIMAL(10,0)"

    def test_decimal_no_params_defaults(self):
        spec = TypeSpec(base_type="DECIMAL")
        assert spec.to_spark_sql() == "DECIMAL(10,0)"
        assert spec.to_standard_sql() == "DECIMAL"

    def test_integer_to_spark_sql(self):
        spec = TypeSpec(base_type="INTEGER")
        assert spec.to_spark_sql() == "INTEGER"

    def test_integer_to_standard_sql(self):
        spec = TypeSpec(base_type="INTEGER")
        assert spec.to_standard_sql() == "INT"

    def test_bigint_output(self):
        spec = TypeSpec(base_type="BIGINT")
        assert spec.to_spark_sql() == "BIGINT"
        assert spec.to_standard_sql() == "BIGINT"

    def test_date_output(self):
        spec = TypeSpec(base_type="DATE")
        assert spec.to_spark_sql() == "DATE"
        assert spec.to_standard_sql() == "DATE"

    def test_timestamp_output(self):
        spec = TypeSpec(base_type="TIMESTAMP")
        assert spec.to_spark_sql() == "TIMESTAMP"
        assert spec.to_standard_sql() == "TIMESTAMP"

    def test_boolean_output(self):
        spec = TypeSpec(base_type="BOOLEAN")
        assert spec.to_spark_sql() == "BOOLEAN"
        assert spec.to_standard_sql() == "BOOLEAN"

    def test_binary_to_spark_sql(self):
        spec = TypeSpec(base_type="BINARY", precision=100)
        assert spec.to_spark_sql() == "BINARY"

    def test_binary_to_standard_sql(self):
        spec = TypeSpec(base_type="BINARY", precision=100)
        assert spec.to_standard_sql() == "VARBINARY(100)"


class TestParseTypeSimple:
    """Test parsing simple (non-parameterized) types."""

    def test_parse_int(self):
        spec = parse_type("int")
        assert spec.base_type == "INTEGER"
        assert spec.precision is None
        assert spec.scale is None

    def test_parse_integer(self):
        spec = parse_type("integer")
        assert spec.base_type == "INTEGER"

    def test_parse_bigint(self):
        spec = parse_type("bigint")
        assert spec.base_type == "BIGINT"

    def test_parse_long(self):
        spec = parse_type("long")
        assert spec.base_type == "BIGINT"

    def test_parse_smallint(self):
        spec = parse_type("smallint")
        assert spec.base_type == "SMALLINT"

    def test_parse_tinyint(self):
        spec = parse_type("tinyint")
        assert spec.base_type == "TINYINT"

    def test_parse_string(self):
        spec = parse_type("string")
        assert spec.base_type == "STRING"
        assert spec.precision is None

    def test_parse_text(self):
        spec = parse_type("text")
        assert spec.base_type == "STRING"

    def test_parse_double(self):
        spec = parse_type("double")
        assert spec.base_type == "DOUBLE"

    def test_parse_float(self):
        spec = parse_type("float")
        assert spec.base_type == "FLOAT"

    def test_parse_real(self):
        spec = parse_type("real")
        assert spec.base_type == "FLOAT"

    def test_parse_date(self):
        spec = parse_type("date")
        assert spec.base_type == "DATE"

    def test_parse_timestamp(self):
        spec = parse_type("timestamp")
        assert spec.base_type == "TIMESTAMP"

    def test_parse_datetime(self):
        spec = parse_type("datetime")
        assert spec.base_type == "TIMESTAMP"

    def test_parse_datetime2(self):
        spec = parse_type("datetime2")
        assert spec.base_type == "TIMESTAMP"

    def test_parse_boolean(self):
        spec = parse_type("boolean")
        assert spec.base_type == "BOOLEAN"

    def test_parse_bool(self):
        spec = parse_type("bool")
        assert spec.base_type == "BOOLEAN"

    def test_parse_bit(self):
        spec = parse_type("bit")
        assert spec.base_type == "BOOLEAN"

    def test_parse_money(self):
        spec = parse_type("money")
        assert spec.base_type == "DECIMAL"
        assert spec.precision == 19
        assert spec.scale == 4

    def test_parse_smallmoney(self):
        spec = parse_type("smallmoney")
        assert spec.base_type == "DECIMAL"
        assert spec.precision == 10
        assert spec.scale == 4

    def test_parse_case_insensitive(self):
        spec1 = parse_type("INT")
        spec2 = parse_type("Int")
        spec3 = parse_type("int")
        assert spec1 == spec2 == spec3


class TestParseTypeParameterized:
    """Test parsing parameterized types."""

    def test_parse_varchar_with_length(self):
        spec = parse_type("varchar(255)")
        assert spec.base_type == "STRING"
        assert spec.precision == 255
        assert spec.scale is None

    def test_parse_varchar_uppercase(self):
        spec = parse_type("VARCHAR(255)")
        assert spec.base_type == "STRING"
        assert spec.precision == 255

    def test_parse_varchar_with_spaces(self):
        spec = parse_type("varchar( 100 )")
        assert spec.base_type == "STRING"
        assert spec.precision == 100

    def test_parse_char(self):
        spec = parse_type("char(50)")
        assert spec.base_type == "STRING"
        assert spec.precision == 50

    def test_parse_character(self):
        spec = parse_type("character(10)")
        assert spec.base_type == "STRING"
        assert spec.precision == 10

    def test_parse_decimal_with_precision_only(self):
        spec = parse_type("decimal(10)")
        assert spec.base_type == "DECIMAL"
        assert spec.precision == 10
        assert spec.scale == 0

    def test_parse_decimal_with_precision_and_scale(self):
        spec = parse_type("decimal(18,2)")
        assert spec.base_type == "DECIMAL"
        assert spec.precision == 18
        assert spec.scale == 2

    def test_parse_decimal_with_spaces(self):
        spec = parse_type("decimal( 18 , 2 )")
        assert spec.base_type == "DECIMAL"
        assert spec.precision == 18
        assert spec.scale == 2

    def test_parse_numeric(self):
        spec = parse_type("numeric(10,5)")
        assert spec.base_type == "DECIMAL"
        assert spec.precision == 10
        assert spec.scale == 5

    def test_parse_number(self):
        spec = parse_type("number(20,4)")
        assert spec.base_type == "DECIMAL"
        assert spec.precision == 20
        assert spec.scale == 4

    def test_parse_varbinary(self):
        spec = parse_type("varbinary(100)")
        assert spec.base_type == "BINARY"
        assert spec.precision == 100


class TestParseTypeErrors:
    """Test error handling in parse_type()."""

    def test_empty_string_raises_error(self):
        with pytest.raises(ValueError, match="must be a non-empty string"):
            parse_type("")

    def test_none_raises_error(self):
        with pytest.raises(ValueError, match="must be a non-empty string"):
            parse_type(None)

    def test_unrecognized_type_raises_error(self):
        with pytest.raises(ValueError, match="Unrecognized type"):
            parse_type("foobar")

    def test_varchar_without_length_raises_error(self):
        with pytest.raises(ValueError, match="VARCHAR/CHAR expects exactly 1 parameter"):
            parse_type("varchar()")

    def test_varchar_with_multiple_params_raises_error(self):
        with pytest.raises(ValueError, match="VARCHAR/CHAR expects exactly 1 parameter"):
            parse_type("varchar(255,10)")

    def test_varchar_with_invalid_length_raises_error(self):
        with pytest.raises(ValueError, match="Invalid length parameter"):
            parse_type("varchar(abc)")

    def test_varchar_with_negative_length_raises_error(self):
        with pytest.raises(ValueError, match="length must be positive"):
            parse_type("varchar(-10)")

    def test_varchar_with_zero_length_raises_error(self):
        with pytest.raises(ValueError, match="length must be positive"):
            parse_type("varchar(0)")

    def test_decimal_with_too_many_params_raises_error(self):
        with pytest.raises(ValueError, match="DECIMAL expects 1 or 2 parameters"):
            parse_type("decimal(18,2,5)")

    def test_decimal_with_invalid_precision_raises_error(self):
        with pytest.raises(ValueError, match="Invalid.*precision"):
            parse_type("decimal(abc)")

    def test_decimal_with_invalid_scale_raises_error(self):
        with pytest.raises(ValueError, match="Invalid DECIMAL parameters"):
            parse_type("decimal(18,abc)")

    def test_decimal_with_negative_precision_raises_error(self):
        with pytest.raises(ValueError, match="precision must be positive"):
            parse_type("decimal(-10)")

    def test_decimal_with_negative_scale_raises_error(self):
        with pytest.raises(ValueError, match="scale must be non-negative"):
            parse_type("decimal(10,-2)")

    def test_decimal_with_scale_exceeding_precision_raises_error(self):
        with pytest.raises(ValueError, match="scale.*cannot exceed precision"):
            parse_type("decimal(10,15)")

    def test_unrecognized_parameterized_type_raises_error(self):
        with pytest.raises(ValueError, match="Unrecognized parameterized type"):
            parse_type("foobar(10)")


class TestTypeSpecIntegration:
    """Integration tests combining parse_type and output methods."""

    def test_varchar_round_trip(self):
        spec = parse_type("varchar(255)")
        assert spec.to_spark_sql() == "STRING"
        assert spec.to_standard_sql() == "VARCHAR(255)"

    def test_money_round_trip(self):
        spec = parse_type("money")
        assert spec.to_spark_sql() == "DECIMAL(19,4)"
        assert spec.to_standard_sql() == "DECIMAL(19,4)"

    def test_int_round_trip(self):
        spec = parse_type("int")
        assert spec.to_spark_sql() == "INTEGER"
        assert spec.to_standard_sql() == "INT"

    def test_decimal_round_trip(self):
        spec = parse_type("decimal(18,2)")
        assert spec.to_spark_sql() == "DECIMAL(18,2)"
        assert spec.to_standard_sql() == "DECIMAL(18,2)"

    def test_date_round_trip(self):
        spec = parse_type("date")
        assert spec.to_spark_sql() == "DATE"
        assert spec.to_standard_sql() == "DATE"

    def test_timestamp_round_trip(self):
        spec = parse_type("timestamp")
        assert spec.to_spark_sql() == "TIMESTAMP"
        assert spec.to_standard_sql() == "TIMESTAMP"

    def test_text_round_trip(self):
        spec = parse_type("text")
        assert spec.to_spark_sql() == "STRING"
        assert spec.to_standard_sql() == "VARCHAR"
