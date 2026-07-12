"""Unit tests for source_loader module."""
import pytest
from unittest.mock import MagicMock

from deltagen.runner.source_loader import load_sources, load_single_source
from deltagen.runner.exceptions import PlanBuilderError
from deltagen.model.source import SourceConfig


class TestSourceConfig:
    """Tests for SourceConfig model validation."""

    def test_path_based_source(self):
        """Test source config with path."""
        config = SourceConfig(
            name="raw_data",
            path="/lakehouse/bronze/customers",
            format="delta"
        )
        assert config.name == "raw_data"
        assert config.path == "/lakehouse/bronze/customers"
        assert config.format == "delta"

    def test_catalog_based_source(self):
        """Test source config with catalog.schema.table."""
        config = SourceConfig(
            name="dim_customer",
            catalog="main",
            schema="silver",
            table="customer"
        )
        assert config.catalog == "main"
        assert config.schema == "silver"
        assert config.table == "customer"

    def test_source_with_alias(self):
        """Test source with alias."""
        config = SourceConfig(
            name="raw_customers",
            path="/data/customers",
            alias="cust"
        )
        assert config.alias == "cust"

    def test_source_with_options(self):
        """Test source with reader options."""
        config = SourceConfig(
            name="csv_data",
            path="/data/file.csv",
            format="csv",
            options={"header": "true", "inferSchema": "true"}
        )
        assert config.options["header"] == "true"
        assert config.options["inferSchema"] == "true"

    def test_source_with_columns(self):
        """Test source with column pruning."""
        config = SourceConfig(
            name="raw_data",
            path="/data/orders",
            format="delta",
            columns=["order_id", "customer_id", "amount"]
        )
        assert config.columns == ["order_id", "customer_id", "amount"]

    def test_source_without_columns(self):
        """Test source without column pruning defaults to None."""
        config = SourceConfig(
            name="raw_data",
            path="/data/orders"
        )
        assert config.columns is None


class TestLoadSingleSource:
    """Tests for load_single_source function."""

    def test_path_based_load(self):
        """Test loading from path-based source."""
        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_df = MagicMock()
        mock_spark.read = mock_reader
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = mock_df

        source = SourceConfig(
            name="raw_data",
            path="/lakehouse/bronze/data",
            format="delta"
        )

        result = load_single_source(mock_spark, source)

        mock_reader.format.assert_called_with("delta")
        mock_reader.load.assert_called_with("/lakehouse/bronze/data")
        assert result == mock_df

    def test_path_based_load_with_options(self):
        """Test loading from path with reader options."""
        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_df = MagicMock()
        mock_spark.read = mock_reader
        mock_reader.format.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.return_value = mock_df

        source = SourceConfig(
            name="csv_data",
            path="/data/file.csv",
            format="csv",
            options={"header": "true", "delimiter": "|"}
        )

        result = load_single_source(mock_spark, source)

        mock_reader.options.assert_called_with(header="true", delimiter="|")

    def test_path_without_format(self):
        """Test loading from path without explicit format."""
        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_df = MagicMock()
        mock_spark.read = mock_reader
        mock_reader.load.return_value = mock_df

        source = SourceConfig(
            name="parquet_data",
            path="/data/parquet_files"
        )

        result = load_single_source(mock_spark, source)

        mock_reader.format.assert_not_called()
        mock_reader.load.assert_called_with("/data/parquet_files")

    def test_table_based_load(self):
        """Test loading from catalog table."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.table.return_value = mock_df

        source = SourceConfig(
            name="dim_customer",
            table="customer"
        )

        result = load_single_source(mock_spark, source)

        mock_spark.table.assert_called_with("customer")
        assert result == mock_df

    def test_table_with_schema(self):
        """Test loading from schema.table."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.table.return_value = mock_df

        source = SourceConfig(
            name="dim_customer",
            schema="silver",
            table="customer"
        )

        result = load_single_source(mock_spark, source)

        mock_spark.table.assert_called_with("silver.customer")

    def test_table_with_catalog_and_schema(self):
        """Test loading from catalog.schema.table."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.table.return_value = mock_df

        source = SourceConfig(
            name="dim_customer",
            catalog="main",
            schema="silver",
            table="customer"
        )

        result = load_single_source(mock_spark, source)

        mock_spark.table.assert_called_with("main.silver.customer")

    def test_no_path_or_table_raises_error(self):
        """Test that source without path or table raises error."""
        mock_spark = MagicMock()

        source = SourceConfig(name="invalid_source")

        with pytest.raises(PlanBuilderError, match="neither path.*table.*generated"):
            load_single_source(mock_spark, source)

    def test_load_error_wrapped_in_plan_builder_error(self):
        """Test that Spark errors are wrapped in PlanBuilderError."""
        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_spark.read = mock_reader
        mock_reader.format.return_value = mock_reader
        mock_reader.load.side_effect = Exception("File not found")

        source = SourceConfig(
            name="missing_data",
            path="/nonexistent/path",
            format="delta"
        )

        with pytest.raises(PlanBuilderError) as exc_info:
            load_single_source(mock_spark, source)

        assert "missing_data" in str(exc_info.value)
        assert "File not found" in str(exc_info.value)

    def test_column_pruning_applied_on_path_source(self):
        """Test that column pruning is applied when columns are specified."""
        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_df = MagicMock()
        mock_pruned_df = MagicMock()
        mock_spark.read = mock_reader
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        mock_df.select.return_value = mock_pruned_df

        source = SourceConfig(
            name="orders",
            path="/data/orders",
            format="delta",
            columns=["order_id", "customer_id", "amount"]
        )

        result = load_single_source(mock_spark, source)

        mock_df.select.assert_called_once_with("order_id", "customer_id", "amount")
        assert result == mock_pruned_df

    def test_column_pruning_applied_on_table_source(self):
        """Test that column pruning is applied on catalog table sources."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_pruned_df = MagicMock()
        mock_spark.table.return_value = mock_df
        mock_df.select.return_value = mock_pruned_df

        source = SourceConfig(
            name="dim_customer",
            table="customer",
            columns=["id", "name", "email"]
        )

        result = load_single_source(mock_spark, source)

        mock_df.select.assert_called_once_with("id", "name", "email")
        assert result == mock_pruned_df

    def test_no_column_pruning_when_columns_not_specified(self):
        """Test that select is not called when columns is None."""
        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_df = MagicMock()
        mock_spark.read = mock_reader
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = mock_df

        source = SourceConfig(
            name="orders",
            path="/data/orders",
            format="delta"
        )

        result = load_single_source(mock_spark, source)

        mock_df.select.assert_not_called()
        assert result == mock_df


class TestLoadSources:
    """Tests for load_sources function."""

    def test_empty_sources(self):
        """Test loading with no sources."""
        mock_spark = MagicMock()

        result = load_sources(mock_spark, [])

        assert result == {}

    def test_single_source(self):
        """Test loading a single source."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_reader = MagicMock()
        mock_spark.read = mock_reader
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = mock_df

        sources = [
            SourceConfig(name="data", path="/data", format="delta")
        ]

        result = load_sources(mock_spark, sources)

        assert "data" in result
        assert result["data"] == mock_df

    def test_multiple_sources(self):
        """Test loading multiple sources."""
        mock_spark = MagicMock()
        mock_df1 = MagicMock(name="df1")
        mock_df2 = MagicMock(name="df2")
        mock_reader = MagicMock()
        mock_spark.read = mock_reader
        mock_reader.format.return_value = mock_reader
        mock_reader.load.side_effect = [mock_df1, mock_df2]

        sources = [
            SourceConfig(name="source1", path="/data1", format="delta"),
            SourceConfig(name="source2", path="/data2", format="delta")
        ]

        result = load_sources(mock_spark, sources)

        assert len(result) == 2
        assert "source1" in result
        assert "source2" in result

    def test_source_with_alias_registered_twice(self):
        """Test that source with alias is registered under both names."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_reader = MagicMock()
        mock_spark.read = mock_reader
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = mock_df

        sources = [
            SourceConfig(
                name="raw_customers",
                path="/data/customers",
                format="delta",
                alias="cust"
            )
        ]

        result = load_sources(mock_spark, sources)

        assert "raw_customers" in result
        assert "cust" in result
        assert result["raw_customers"] == result["cust"]

    def test_mixed_path_and_table_sources(self):
        """Test loading mix of path-based and table-based sources."""
        mock_spark = MagicMock()
        mock_df_path = MagicMock(name="df_path")
        mock_df_table = MagicMock(name="df_table")
        mock_reader = MagicMock()
        mock_spark.read = mock_reader
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = mock_df_path
        mock_spark.table.return_value = mock_df_table

        sources = [
            SourceConfig(name="raw_data", path="/data", format="delta"),
            SourceConfig(name="dim_table", catalog="main", schema="gold", table="dimension")
        ]

        result = load_sources(mock_spark, sources)

        assert "raw_data" in result
        assert "dim_table" in result
        mock_reader.load.assert_called_once()
        mock_spark.table.assert_called_once_with("main.gold.dimension")
