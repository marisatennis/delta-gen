#!/usr/bin/env python3
"""Demonstration of YamlConfigProvider with macro expansion."""

from pathlib import Path

from deltagen.model import TableConfig
from deltagen.providers import YamlConfigProvider

# Setup paths
CONFIGS_DIR = Path(__file__).parent.parent / "test" / "fixtures" / "configs"
DEFAULTS_PATH = CONFIGS_DIR / "defaults.yaml"


def example_1_basic_loading():
    """Example 1: Basic config loading with auto-discovery of defaults."""
    print("=" * 70)
    print("EXAMPLE 1: Basic Loading with Auto-Discovery")
    print("=" * 70)

    # Auto-discovers defaults.yaml in same directory
    provider = YamlConfigProvider(TableConfig)
    table = provider.load(CONFIGS_DIR / "customer_dim.yaml")

    print(f"\nLoaded table: {table.name}")
    print(f"Layer: {table.layer}")
    print(f"Natural ID: {table.natural_id}")
    print(f"Number of stages: {len(table.stages)}")
    print(f"Total columns: {len(list(table.iter_columns()))}")
    print(f"Persistent columns: {len(table.get_persistent_columns())}")
    print(f"Natural key columns: {len(table.get_natural_key_columns())}")


def example_2_macro_expansion():
    """Example 2: Macro expansion in action."""
    print("\n" + "=" * 70)
    print("EXAMPLE 2: Macro Expansion")
    print("=" * 70)

    provider = YamlConfigProvider(TableConfig, defaults_path=DEFAULTS_PATH)
    table = provider.load(CONFIGS_DIR / "product_dim.yaml")

    print(f"\nTable: {table.name}")
    print(f"\nMacro expansion in policies:")
    print(f"  load_mode (from macro): {table.policies.optimisation.load_mode}")

    # Find is_active column which has macro-expanded values
    is_active = next(col for col in table.iter_columns() if col.name == "is_active")
    print(f"\nMacro expansion in columns (is_active):")
    print(f"  data_type (from ${{defaults.common_columns.is_active.data_type}}): {is_active.data_type}")
    print(f"  nullable (from ${{defaults.common_columns.is_active.nullable}}): {is_active.nullable}")
    print(f"  default (from ${{defaults.common_columns.is_active.default}}): {is_active.default}")
    print(f"  Type preserved! nullable is {type(is_active.nullable).__name__}, not string")


def example_3_type_preservation():
    """Example 3: Type preservation during macro expansion."""
    print("\n" + "=" * 70)
    print("EXAMPLE 3: Type Preservation in Macros")
    print("=" * 70)

    provider = YamlConfigProvider(TableConfig, defaults_path=DEFAULTS_PATH)
    table = provider.load(CONFIGS_DIR / "customer_dim.yaml")

    print(f"\nTable: {table.name}")
    print(f"\nPolicy values (expanded from macros):")
    print(f"  generic: {table.policies.creation.generic} (type: {type(table.policies.creation.generic).__name__})")
    print(f"  active: {table.policies.orchestration.active} (type: {type(table.policies.orchestration.active).__name__})")
    print(f"  batch: {table.policies.orchestration.batch} (type: {type(table.policies.orchestration.batch).__name__})")
    print(f"\n✓ Booleans stay booleans, integers stay integers!")


def example_4_multi_stage_with_filtering():
    """Example 4: Multi-stage table with temporary column filtering."""
    print("\n" + "=" * 70)
    print("EXAMPLE 4: Multi-Stage Table with Column Filtering")
    print("=" * 70)

    provider = YamlConfigProvider(TableConfig, defaults_path=DEFAULTS_PATH)
    table = provider.load(CONFIGS_DIR / "sales_fact.yaml")

    print(f"\nTable: {table.name}")
    print(f"Number of stages: {len(table.stages)}")

    for stage in table.stages:
        print(f"\n  Stage: {stage.name}")
        print(f"    Total columns: {len(stage.columns)}")
        print(f"    Persistent columns: {len(stage.get_persistent_columns())}")
        print(f"    Temporary columns: {len(stage.filter_columns(temporary=True))}")

    # Show temporary vs persistent columns
    temp_cols = table.filter_columns(temporary=True)
    persistent_cols = table.get_persistent_columns()

    print(f"\n  Temporary column names (across all stages):")
    for col in temp_cols:
        print(f"    - {col.name}")

    print(f"\n  Persistent columns will be saved to Delta table:")
    print(f"    Total: {len(persistent_cols)} columns")


def example_5_joins_and_sources():
    """Example 5: Configuration with joins and multiple sources."""
    print("\n" + "=" * 70)
    print("EXAMPLE 5: Joins and Multiple Sources")
    print("=" * 70)

    provider = YamlConfigProvider(TableConfig, defaults_path=DEFAULTS_PATH)
    table = provider.load(CONFIGS_DIR / "product_dim.yaml")

    print(f"\nTable: {table.name}")
    print(f"\nSources ({len(table.sources)}):")
    for source in table.sources:
        print(f"  - {source.name}: {source.path} (format: {source.format})")

    print(f"\nJoins ({len(table.stages[0].joins)}):")
    for join in table.stages[0].joins:
        print(f"  - {join.name} ({join.type} join)")
        print(f"    Source: {join.source}")
        for cond in join.conditions:
            print(f"    Condition: {cond.left} {cond.operator} {cond.right}")


def example_6_load_from_dict():
    """Example 6: Loading from a dictionary with macro support."""
    print("\n" + "=" * 70)
    print("EXAMPLE 6: Loading from Dictionary with Macros")
    print("=" * 70)

    provider = YamlConfigProvider(TableConfig, defaults_path=DEFAULTS_PATH)

    config_dict = {
        "name": "test_table",
        "layer": "bronze",
        "policies": {
            "optimisation": {
                "load_mode": "${defaults.policies.optimisation.load_mode}",
            },
            "creation": {
                "generic": "${defaults.policies.creation.generic}",
            },
        },
        "stages": [
            {
                "name": "main",
                "mode": "transformation",
                "columns": [
                    {
                        "name": "id",
                        "data_type": "int",
                        "nullable": False,
                        "natural": True,
                    },
                    {
                        "name": "is_active",
                        "data_type": "${defaults.common_columns.is_active.data_type}",
                        "nullable": "${defaults.common_columns.is_active.nullable}",
                        "default": "${defaults.common_columns.is_active.default}",
                    },
                ],
            }
        ],
    }

    table = provider.load_dict(config_dict)

    print(f"\nLoaded from dictionary:")
    print(f"  Table: {table.name}")
    print(f"  Load mode (from macro): {table.policies.optimisation.load_mode}")
    print(f"  Generic flag (from macro): {table.policies.creation.generic}")

    is_active = next(col for col in table.iter_columns() if col.name == "is_active")
    print(f"  is_active data_type (from macro): {is_active.data_type}")
    print(f"  is_active nullable (from macro): {is_active.nullable} (type: {type(is_active.nullable).__name__})")


def example_7_typespec_integration():
    """Example 7: TypeSpec integration with loaded configs."""
    print("\n" + "=" * 70)
    print("EXAMPLE 7: TypeSpec Integration")
    print("=" * 70)

    provider = YamlConfigProvider(TableConfig, defaults_path=DEFAULTS_PATH)
    table = provider.load(CONFIGS_DIR / "product_dim.yaml")

    print(f"\nTable: {table.name}")
    print(f"\nColumn type specifications:")

    for col in table.iter_columns():
        typespec = col.get_typespec()
        if typespec:
            print(f"\n  {col.name}:")
            print(f"    Original: {col.data_type}")
            print(f"    Spark SQL: {typespec.to_spark_sql()}")
            print(f"    Standard SQL: {typespec.to_standard_sql()}")


def main():
    """Run all examples."""
    example_1_basic_loading()
    example_2_macro_expansion()
    example_3_type_preservation()
    example_4_multi_stage_with_filtering()
    example_5_joins_and_sources()
    example_6_load_from_dict()
    example_7_typespec_integration()

    print("\n" + "=" * 70)
    print("✓ All examples completed successfully!")
    print("=" * 70)


if __name__ == "__main__":
    main()
