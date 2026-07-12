"""Unit tests for YamlConfigProvider."""
from pathlib import Path

import pytest

from deltagen.model import TableConfig
from deltagen.providers import YamlConfigProvider


# Path to test fixtures
FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "configs"


class TestYamlConfigProviderBasicLoading:
    """Test basic YAML config loading."""

    def test_load_customer_dim_config(self):
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")
        table = provider.load(FIXTURES_DIR / "customer_dim.yaml")

        assert isinstance(table, TableConfig)
        assert table.name == "customer_dim"
        assert table.layer == "silver"
        assert table.natural_id == "customer_id"
        assert table.description == "Customer dimension table with SCD2 tracking"

    def test_load_product_dim_config(self):
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")
        table = provider.load(FIXTURES_DIR / "product_dim.yaml")

        assert isinstance(table, TableConfig)
        assert table.name == "product_dim"
        assert table.layer == "silver"
        assert table.natural_id == "product_key"

    def test_load_sales_fact_config(self):
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")
        table = provider.load(FIXTURES_DIR / "sales_fact.yaml")

        assert isinstance(table, TableConfig)
        assert table.name == "sales_fact"
        assert table.layer == "gold"
        assert len(table.stages) == 2

    def test_auto_discover_defaults(self):
        # Should automatically find defaults.yaml in same directory
        provider = YamlConfigProvider(TableConfig, auto_discover_defaults=True)
        table = provider.load(FIXTURES_DIR / "customer_dim.yaml")

        assert table.name == "customer_dim"
        # Verify macro was expanded from defaults
        assert table.policies.optimisation.load_mode == "merge"


class TestMacroExpansion:
    """Test macro expansion functionality."""

    def test_macro_expansion_in_policies(self):
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")
        table = provider.load(FIXTURES_DIR / "customer_dim.yaml")

        # Macros should be expanded from defaults
        assert table.policies.optimisation.load_mode == "merge"
        assert table.policies.creation.generic is True
        assert table.policies.orchestration.batch == 1
        assert table.policies.orchestration.active is True

    def test_macro_expansion_in_columns(self):
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")
        table = provider.load(FIXTURES_DIR / "customer_dim.yaml")

        # Find the created_at column
        created_at = next(col for col in table.iter_columns() if col.name == "created_at")

        # data_type should be expanded from defaults
        assert created_at.data_type == "timestamp"
        assert created_at.nullable is False

    def test_macro_expansion_preserves_types(self):
        """Macros should preserve the original type (bool, int, etc.)"""
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")
        table = provider.load(FIXTURES_DIR / "product_dim.yaml")

        # Find is_active column
        is_active = next(col for col in table.iter_columns() if col.name == "is_active")

        # Boolean should be preserved, not converted to string
        assert is_active.data_type == "boolean"
        assert is_active.nullable is False
        assert is_active.default is True


class TestDefaultsMerging:
    """Test defaults merging functionality."""

    def test_config_values_override_defaults(self):
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")
        table = provider.load(FIXTURES_DIR / "sales_fact.yaml")

        # sales_fact overrides the default load_mode
        assert table.policies.optimisation.load_mode == "append"
        # But uses default for generic
        assert table.policies.creation.generic is True

    def test_no_defaults_file_works(self):
        provider = YamlConfigProvider(TableConfig, auto_discover_defaults=False)

        # Create a minimal config without macros
        minimal_config = {
            "name": "test_table",
            "layer": "bronze",
            "stages": []
        }

        table = provider.load_dict(minimal_config)
        assert table.name == "test_table"
        assert table.layer == "bronze"


class TestColumnAttributes:
    """Test that column attributes are loaded correctly."""

    def test_natural_key_columns(self):
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")
        table = provider.load(FIXTURES_DIR / "customer_dim.yaml")

        natural_keys = table.get_natural_key_columns()
        assert len(natural_keys) == 2
        assert {col.name for col in natural_keys} == {"customer_id", "email"}

    def test_temporary_columns(self):
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")
        table = provider.load(FIXTURES_DIR / "customer_dim.yaml")

        temp_cols = table.temporary_columns
        assert "full_name" in temp_cols

    def test_extension_attributes(self):
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")
        table = provider.load(FIXTURES_DIR / "customer_dim.yaml")

        # Find email column with PII extension
        email = next(col for col in table.iter_columns() if col.name == "email")
        assert email.extensions.get("pii") is True
        assert email.extensions.get("masked") is True

    def test_column_inputs(self):
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")
        table = provider.load(FIXTURES_DIR / "customer_dim.yaml")

        # Check column with source mapping
        customer_id = next(col for col in table.iter_columns() if col.name == "customer_id")
        assert len(customer_id.inputs) == 1
        assert customer_id.inputs[0].source == "raw_customers"
        assert customer_id.inputs[0].column == "id"

        # Check column with expression
        full_name = next(col for col in table.iter_columns() if col.name == "full_name")
        assert len(full_name.inputs) == 1
        assert full_name.inputs[0].expression is not None
        assert "CONCAT" in full_name.inputs[0].expression


class TestMultipleStages:
    """Test configurations with multiple stages."""

    def test_sales_fact_has_two_stages(self):
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")
        table = provider.load(FIXTURES_DIR / "sales_fact.yaml")

        assert len(table.stages) == 2
        assert table.stages[0].name == "source_stage"
        assert table.stages[1].name == "aggregation_stage"

    def test_columns_across_stages(self):
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")
        table = provider.load(FIXTURES_DIR / "sales_fact.yaml")

        all_columns = list(table.iter_columns())
        assert len(all_columns) > 0

        # Check temp columns are from source_stage
        temp_cols = table.filter_columns(temporary=True)
        assert len(temp_cols) == 2
        assert {col.name for col in temp_cols} == {"temp_discount_pct", "temp_tax_amount"}


class TestSources:
    """Test source configuration loading."""

    def test_single_source(self):
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")
        table = provider.load(FIXTURES_DIR / "customer_dim.yaml")

        assert len(table.sources) == 1
        assert table.sources[0].name == "raw_customers"
        assert table.sources[0].path == "/lakehouse/bronze/customers"
        assert table.sources[0].format == "delta"

    def test_multiple_sources(self):
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")
        table = provider.load(FIXTURES_DIR / "product_dim.yaml")

        assert len(table.sources) == 2
        source_names = {s.name for s in table.sources}
        assert source_names == {"raw_products", "raw_categories"}

    def test_source_options(self):
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")
        table = provider.load(FIXTURES_DIR / "sales_fact.yaml")

        raw_tx_source = next(s for s in table.sources if s.name == "raw_transactions")
        assert raw_tx_source.options["merge_schema"] is True


class TestJoins:
    """Test join configuration loading."""

    def test_product_dim_has_join(self):
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")
        table = provider.load(FIXTURES_DIR / "product_dim.yaml")

        assert len(table.stages) == 1
        assert len(table.stages[0].joins) == 1

        join = table.stages[0].joins[0]
        assert join.source == "raw_categories"
        assert join.name == "cat"
        assert join.type == "left"


class TestTagsAndExtensions:
    """Test tags and extensions loading."""

    def test_table_tags(self):
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")
        table = provider.load(FIXTURES_DIR / "customer_dim.yaml")

        assert "dimension" in table.tags
        assert "scd2" in table.tags
        assert "customer" in table.tags

    def test_table_extensions(self):
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")
        table = provider.load(FIXTURES_DIR / "sales_fact.yaml")

        # Check data quality extension
        assert "data_quality" in table.extensions
        assert "rules" in table.extensions["data_quality"]
        assert len(table.extensions["data_quality"]["rules"]) == 2


class TestErrorHandling:
    """Test error handling and validation."""

    def test_missing_file_raises_error(self):
        provider = YamlConfigProvider(TableConfig)

        with pytest.raises(FileNotFoundError, match="not found"):
            provider.load("nonexistent.yaml")

    def test_invalid_yaml_raises_error(self):
        provider = YamlConfigProvider(TableConfig)

        # Create a temp file with invalid YAML
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("invalid: yaml: content: [unclosed")
            temp_path = f.name

        try:
            with pytest.raises(ValueError, match="Failed to parse YAML"):
                provider.load(temp_path)
        finally:
            Path(temp_path).unlink()

    def test_empty_yaml_raises_error(self):
        provider = YamlConfigProvider(TableConfig)

        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("")  # Empty file
            temp_path = f.name

        try:
            with pytest.raises(ValueError, match="empty"):
                provider.load(temp_path)
        finally:
            Path(temp_path).unlink()

    def test_missing_required_field_raises_validation_error(self):
        provider = YamlConfigProvider(TableConfig)

        # Config missing required 'name' field
        invalid_config = {
            "layer": "bronze",
            "stages": []
        }

        with pytest.raises(ValueError, match="validation failed"):
            provider.load_dict(invalid_config)

    def test_invalid_macro_raises_error(self):
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")

        config_with_bad_macro = {
            "name": "test",
            "layer": "${nonexistent.path.to.value}",
            "stages": []
        }

        with pytest.raises(ValueError, match="Could not resolve"):
            provider.load_dict(config_with_bad_macro)


class TestLoadDictMethod:
    """Test loading configs from dictionaries."""

    def test_load_dict_basic(self):
        provider = YamlConfigProvider(TableConfig)

        config = {
            "name": "test_table",
            "layer": "bronze",
            "stages": []
        }

        table = provider.load_dict(config)
        assert table.name == "test_table"
        assert table.layer == "bronze"

    def test_load_dict_with_validation_error(self):
        provider = YamlConfigProvider(TableConfig)

        config = {
            "name": 123,  # Should be string
            "layer": "bronze"
        }

        with pytest.raises(ValueError, match="validation failed"):
            provider.load_dict(config)
