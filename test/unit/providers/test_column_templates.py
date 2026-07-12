"""Unit tests for smart column template matching."""
from pathlib import Path

from deltagen.model import TableConfig
from deltagen.providers import YamlConfigProvider


# Path to test fixtures
FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "configs"


class TestSmartColumnTemplateMatching:
    """Test automatic column template merging."""

    def test_column_auto_inherits_from_defaults(self):
        """Column with name matching defaults.common_columns.X should auto-inherit."""
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")

        # Create config with minimal is_active column
        config = {
            "name": "test_table",
            "stages": [
                {
                    "name": "main",
                    "mode": "transformation",
                    "columns": [
                        {
                            "name": "is_active",
                            # Only specify what's different - rest comes from template
                            "temporary": False,
                        }
                    ],
                }
            ],
        }

        table = provider.load_dict(config)
        is_active = table.stages[0].columns[0]

        # Should have inherited from defaults.common_columns.is_active
        assert is_active.name == "is_active"
        assert is_active.data_type == "boolean"
        assert is_active.nullable is False
        assert is_active.default is True
        assert is_active.temporary is False  # From config, not template

    def test_column_config_overrides_template(self):
        """Config values should override template values."""
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")

        config = {
            "name": "test_table",
            "stages": [
                {
                    "name": "main",
                    "mode": "transformation",
                    "columns": [
                        {
                            "name": "is_active",
                            "data_type": "int",  # Override template's "boolean"
                            "nullable": True,  # Override template's False
                            "default": 0,  # Override template's True
                            "temporary": False,
                        }
                    ],
                }
            ],
        }

        table = provider.load_dict(config)
        is_active = table.stages[0].columns[0]

        # All values from config, not template
        assert is_active.data_type == "int"
        assert is_active.nullable is True
        assert is_active.default == 0

    def test_column_without_template_works_normally(self):
        """Columns without matching template should work as before."""
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")

        config = {
            "name": "test_table",
            "stages": [
                {
                    "name": "main",
                    "mode": "transformation",
                    "columns": [
                        {
                            "name": "custom_column",  # No template for this
                            "data_type": "varchar(100)",
                            "nullable": False,
                            "temporary": False,
                        }
                    ],
                }
            ],
        }

        table = provider.load_dict(config)
        col = table.stages[0].columns[0]

        assert col.name == "custom_column"
        assert col.data_type == "varchar(100)"
        assert col.nullable is False

    def test_multiple_columns_with_templates(self):
        """Multiple columns can use templates simultaneously."""
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")

        config = {
            "name": "test_table",
            "stages": [
                {
                    "name": "main",
                    "mode": "transformation",
                    "columns": [
                        {"name": "is_active", "temporary": False},
                        {"name": "created_at", "temporary": False},  # audit_created_at template
                        {"name": "updated_at", "temporary": False},  # audit_updated_at template
                    ],
                }
            ],
        }

        table = provider.load_dict(config)
        cols = {col.name: col for col in table.stages[0].columns}

        # is_active from template
        assert cols["is_active"].data_type == "boolean"

        # created_at should inherit from audit_created_at template
        # but the name in defaults is audit_created_at, not created_at
        # so this won't match - which is correct!
        assert "created_at" in cols
        assert cols["created_at"].data_type is None  # No template match

    def test_template_matching_respects_actual_name(self):
        """Template matching uses actual column name from defaults."""
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")

        config = {
            "name": "test_table",
            "stages": [
                {
                    "name": "main",
                    "mode": "transformation",
                    "columns": [
                        # Use exact name from defaults.common_columns
                        {"name": "audit_created_at", "temporary": False},
                    ],
                }
            ],
        }

        table = provider.load_dict(config)
        col = table.stages[0].columns[0]

        # Should match defaults.common_columns.audit_created_at
        assert col.data_type == "timestamp"
        assert col.nullable is False

    def test_templates_work_across_multiple_stages(self):
        """Templates should apply across all stages."""
        provider = YamlConfigProvider(TableConfig, defaults_path=FIXTURES_DIR / "defaults.yaml")

        config = {
            "name": "test_table",
            "stages": [
                {
                    "name": "stage1",
                    "mode": "transformation",
                    "columns": [{"name": "is_active", "temporary": False}],
                },
                {
                    "name": "stage2",
                    "mode": "transformation",
                    "columns": [{"name": "is_active", "temporary": False}],
                },
            ],
        }

        table = provider.load_dict(config)

        # Both stages should have template applied
        assert table.stages[0].columns[0].data_type == "boolean"
        assert table.stages[1].columns[0].data_type == "boolean"

    def test_no_defaults_no_templates(self):
        """Provider without defaults should work normally."""
        provider = YamlConfigProvider(TableConfig, auto_discover_defaults=False)

        config = {
            "name": "test_table",
            "stages": [
                {
                    "name": "main",
                    "mode": "transformation",
                    "columns": [
                        {
                            "name": "is_active",
                            "data_type": "int",
                            "nullable": True,
                            "temporary": False,
                        }
                    ],
                }
            ],
        }

        table = provider.load_dict(config)
        col = table.stages[0].columns[0]

        # No template applied, uses config values
        assert col.data_type == "int"
        assert col.nullable is True
