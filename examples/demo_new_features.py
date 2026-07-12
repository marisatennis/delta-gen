#!/usr/bin/env python3
"""Demonstration of new YamlConfigProvider features: Generic types and smart column templates."""

from pathlib import Path

from deltagen.model import EnvironmentConfig, TableConfig
from deltagen.providers import YamlConfigProvider

# Setup paths
CONFIGS_DIR = Path(__file__).parent.parent / "test" / "fixtures" / "configs"
DEFAULTS_PATH = CONFIGS_DIR / "defaults.yaml"


def demo_1_smart_column_templates():
    """Demo 1: Smart column templates - no more verbose macros!"""
    print("=" * 80)
    print("DEMO 1: Smart Column Templates")
    print("=" * 80)

    provider = YamlConfigProvider(TableConfig, defaults_path=DEFAULTS_PATH)

    # OLD WAY: Had to specify every field with macros
    old_way = """
    columns:
      - name: is_active
        data_type: ${defaults.common_columns.is_active.data_type}
        nullable: ${defaults.common_columns.is_active.nullable}
        default: ${defaults.common_columns.is_active.default}
        temporary: false
    """

    # NEW WAY: Just specify the name, rest is auto-merged!
    new_way_config = {
        "name": "my_table",
        "stages": [
            {
                "name": "main",
                "mode": "transformation",
                "columns": [
                    {
                        "name": "is_active",  # ← System automatically finds defaults.common_columns.is_active
                        "temporary": False,  # ← Only override what you need!
                    },
                    {
                        "name": "audit_created_at",  # ← Auto-merges timestamp, nullable: false
                        "temporary": False,
                    },
                ],
            }
        ],
    }

    table = provider.load_dict(new_way_config)

    print("\n📝 OLD WAY (verbose):")
    print(old_way)

    print("\n✨ NEW WAY (concise):")
    print("""
    columns:
      - name: is_active        # ← Auto-inherits data_type, nullable, default from template!
        temporary: false
      - name: audit_created_at # ← Auto-inherits timestamp, nullable: false
        temporary: false
    """)

    print("\n✅ Results - both columns fully populated:")
    for col in table.iter_columns():
        print(f"  {col.name}:")
        print(f"    data_type: {col.data_type}")
        print(f"    nullable: {col.nullable}")
        if col.default is not None:
            print(f"    default: {col.default}")


def demo_2_column_template_overrides():
    """Demo 2: Override template values when needed."""
    print("\n" + "=" * 80)
    print("DEMO 2: Column Template Overrides")
    print("=" * 80)

    provider = YamlConfigProvider(TableConfig, defaults_path=DEFAULTS_PATH)

    config = {
        "name": "special_table",
        "stages": [
            {
                "name": "main",
                "mode": "transformation",
                "columns": [
                    {
                        "name": "is_active",
                        # Override template's boolean with int
                        "data_type": "int",
                        "nullable": True,  # Override template's False
                        "default": 0,  # Override template's True
                        "temporary": False,
                    }
                ],
            }
        ],
    }

    table = provider.load_dict(config)
    col = table.stages[0].columns[0]

    print("\n📋 Template says:")
    print("  data_type: boolean")
    print("  nullable: False")
    print("  default: True")

    print("\n🔧 Config overrides:")
    print("  data_type: int")
    print("  nullable: True")
    print("  default: 0")

    print(f"\n✅ Result - config wins:")
    print(f"  data_type: {col.data_type}")
    print(f"  nullable: {col.nullable}")
    print(f"  default: {col.default}")


def demo_3_generic_provider_with_table_config():
    """Demo 3: Generic provider with TableConfig."""
    print("\n" + "=" * 80)
    print("DEMO 3: Generic Provider with TableConfig")
    print("=" * 80)

    # Provider is now generic - specify the config class!
    provider = YamlConfigProvider(TableConfig, defaults_path=DEFAULTS_PATH)
    table = provider.load(CONFIGS_DIR / "customer_dim.yaml")

    print(f"\n✅ Loaded TableConfig:")
    print(f"  Type: {type(table).__name__}")
    print(f"  Name: {table.name}")
    print(f"  Stages: {len(table.stages)}")
    print(f"  Columns: {len(list(table.iter_columns()))}")
    print(f"  Has iter_columns(): {hasattr(table, 'iter_columns')}")


def demo_4_generic_provider_with_environment_config():
    """Demo 4: Generic provider with EnvironmentConfig."""
    print("\n" + "=" * 80)
    print("DEMO 4: Generic Provider with EnvironmentConfig")
    print("=" * 80)

    # Same provider class, different config type!
    provider = YamlConfigProvider(EnvironmentConfig, auto_discover_defaults=False)

    config = {
        "layers": [
            {
                "name": "bronze",
                "sources": [
                    {"name": "raw_events", "path": "/lakehouse/bronze/events", "format": "parquet"},
                    {"name": "raw_users", "path": "/lakehouse/bronze/users", "format": "json"},
                ],
            },
            {
                "name": "silver",
                "sources": [
                    {"name": "clean_events", "path": "/lakehouse/silver/events", "format": "delta"},
                    {"name": "clean_users", "path": "/lakehouse/silver/users", "format": "delta"},
                ],
            },
            {
                "name": "gold",
                "sources": [
                    {"name": "user_metrics", "path": "/lakehouse/gold/user_metrics", "format": "delta"}
                ],
            },
        ]
    }

    env = provider.load_dict(config)

    print(f"\n✅ Loaded EnvironmentConfig:")
    print(f"  Type: {type(env).__name__}")
    print(f"  Layers: {len(env.layers)}")
    print(f"  Has layers: {hasattr(env, 'layers')}")
    print(f"  Has stages: {hasattr(env, 'stages')}")  # False - different config!

    print("\n📊 Environment structure:")
    for layer in env.layers:
        print(f"\n  Layer: {layer.name}")
        for source in layer.sources:
            print(f"    - {source.name}: {source.path} ({source.format})")


def demo_5_one_provider_many_configs():
    """Demo 5: One provider, many config types!"""
    print("\n" + "=" * 80)
    print("DEMO 5: One Provider, Many Config Types")
    print("=" * 80)

    print("\n📦 Same YamlConfigProvider class can load:")

    # Load a table
    table_provider = YamlConfigProvider(TableConfig, defaults_path=DEFAULTS_PATH)
    table = table_provider.load(CONFIGS_DIR / "product_dim.yaml")
    print(f"\n  ✓ TableConfig: {table.name} ({len(list(table.iter_columns()))} columns)")

    # Load an environment
    env_provider = YamlConfigProvider(EnvironmentConfig)
    env_config = {"layers": [{"name": "dev", "sources": []}]}
    env = env_provider.load_dict(env_config)
    print(f"  ✓ EnvironmentConfig: {len(env.layers)} layers")

    print("\n💡 Tomorrow you add a new config type?")
    print("  No problem! Same provider works with any StrictBaseModel subclass.")


def demo_6_real_world_example():
    """Demo 6: Real-world example combining all features."""
    print("\n" + "=" * 80)
    print("DEMO 6: Real-World Example - Combining All Features")
    print("=" * 80)

    provider = YamlConfigProvider(TableConfig, defaults_path=DEFAULTS_PATH)

    # Realistic config using smart templates
    config = {
        "name": "user_activity_fact",
        "layer": "gold",
        "description": "Daily user activity metrics",
        "policies": {
            "optimisation": {
                "load_mode": "${defaults.policies.optimisation.load_mode}",  # Still use macros for non-column fields
                "partition_by": "activity_date",
            }
        },
        "stages": [
            {
                "name": "main",
                "mode": "transformation",
                "columns": [
                    {"name": "user_id", "data_type": "bigint", "nullable": False, "natural": True},
                    {"name": "activity_date", "data_type": "date", "nullable": False},
                    {"name": "page_views", "data_type": "int", "nullable": False},
                    {"name": "session_duration", "data_type": "decimal(18,2)", "nullable": True},
                    # Smart templates for audit columns!
                    {"name": "is_active", "temporary": False},  # ← Auto-gets boolean, nullable:false, default:true
                    {"name": "audit_created_at", "temporary": False},  # ← Auto-gets timestamp, nullable:false
                ],
            }
        ],
    }

    table = provider.load_dict(config)

    print("\n✨ Loaded config with:")
    print(f"  • {len(list(table.iter_columns()))} columns")
    print(f"  • Macros expanded for policies")
    print(f"  • Smart templates for audit columns")

    print("\n📋 Audit columns (from templates):")
    for col in table.iter_columns():
        if col.name in ("is_active", "audit_created_at"):
            print(f"  {col.name}: {col.data_type} (nullable={col.nullable})")


def main():
    """Run all demos."""
    demo_1_smart_column_templates()
    demo_2_column_template_overrides()
    demo_3_generic_provider_with_table_config()
    demo_4_generic_provider_with_environment_config()
    demo_5_one_provider_many_configs()
    demo_6_real_world_example()

    print("\n" + "=" * 80)
    print("🎉 All new features demonstrated!")
    print("=" * 80)
    print("\nKey Benefits:")
    print("  ✓ Less verbose configs (no more ${defaults.X.Y.Z} for every field)")
    print("  ✓ DRY principle (define once in defaults, use everywhere)")
    print("  ✓ Type-safe (one provider, works with any config class)")
    print("  ✓ Future-proof (add new config types without changing provider)")
    print("=" * 80)


if __name__ == "__main__":
    main()
