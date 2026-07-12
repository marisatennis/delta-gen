"""Unit tests for generic provider with different config types."""

from deltagen.model import EnvironmentConfig, TableConfig
from deltagen.providers import YamlConfigProvider


class TestGenericProviderWithTableConfig:
    """Test provider works with TableConfig."""

    def test_provider_loads_table_config(self):
        provider = YamlConfigProvider(TableConfig, auto_discover_defaults=False)

        config = {
            "name": "test_table",
            "stages": [
                {
                    "name": "main",
                    "mode": "transformation",
                    "columns": [
                        {"name": "id", "data_type": "int", "nullable": False, "natural": True}
                    ],
                }
            ],
        }

        table = provider.load_dict(config)

        assert isinstance(table, TableConfig)
        assert table.name == "test_table"
        assert len(table.stages) == 1


class TestGenericProviderWithEnvironmentConfig:
    """Test provider works with EnvironmentConfig."""

    def test_provider_loads_environment_config(self):
        provider = YamlConfigProvider(EnvironmentConfig, auto_discover_defaults=False)

        config = {
            "layers": [
                {
                    "name": "bronze",
                    "sources": [
                        {"name": "raw_events", "path": "/data/raw/events", "format": "parquet"}
                    ],
                },
                {
                    "name": "silver",
                    "sources": [
                        {"name": "clean_events", "path": "/data/clean/events", "format": "delta"}
                    ],
                },
            ]
        }

        env = provider.load_dict(config)

        assert isinstance(env, EnvironmentConfig)
        assert len(env.layers) == 2
        assert env.layers[0].name == "bronze"
        assert env.layers[1].name == "silver"

    def test_provider_from_yaml_file(self, tmp_path):
        """Test loading EnvironmentConfig from YAML file."""
        # Create a temporary YAML file
        yaml_file = tmp_path / "test_env.yaml"
        yaml_file.write_text("""
layers:
  - name: bronze
    sources:
      - name: raw_data
        path: /data/raw
        format: parquet
  - name: silver
    sources:
      - name: processed_data
        path: /data/processed
        format: delta
""")

        provider = YamlConfigProvider(EnvironmentConfig, auto_discover_defaults=False)
        env = provider.load(yaml_file)

        assert isinstance(env, EnvironmentConfig)
        assert len(env.layers) == 2
        assert env.layers[0].sources[0].name == "raw_data"
        assert env.layers[1].sources[0].format == "delta"


class TestGenericProviderTypeSafety:
    """Test that provider maintains type safety."""

    def test_table_provider_returns_table_config(self):
        provider = YamlConfigProvider(TableConfig, auto_discover_defaults=False)
        config = {"name": "test", "stages": []}

        result = provider.load_dict(config)

        # Type checker should know this is TableConfig
        assert hasattr(result, "stages")
        assert hasattr(result, "iter_columns")

    def test_environment_provider_returns_environment_config(self):
        provider = YamlConfigProvider(EnvironmentConfig, auto_discover_defaults=False)
        config = {"layers": []}

        result = provider.load_dict(config)

        # Type checker should know this is EnvironmentConfig
        assert hasattr(result, "layers")
        assert not hasattr(result, "stages")  # EnvironmentConfig doesn't have stages
