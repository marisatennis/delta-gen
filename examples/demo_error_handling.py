#!/usr/bin/env python3
"""Demonstration of YamlConfigProvider error handling."""

from pathlib import Path

from deltagen.model import TableConfig
from deltagen.providers import YamlConfigProvider

CONFIGS_DIR = Path(__file__).parent.parent / "test" / "fixtures" / "configs"


def demo_invalid_macro():
    """Show helpful error message for invalid macro."""
    print("=" * 70)
    print("DEMO: Invalid Macro Reference")
    print("=" * 70)

    provider = YamlConfigProvider(TableConfig, defaults_path=CONFIGS_DIR / "defaults.yaml")

    config = {
        "name": "test_table",
        "layer": "${nonexistent.path}",  # This macro doesn't exist
        "stages": [],
    }

    try:
        table = provider.load_dict(config)
        print(f"\nLoaded table (unexpected): {table.name}")
    except ValueError as e:
        print(f"\n✓ Got expected error:")
        print(f"  {e}\n")


def demo_validation_error():
    """Show helpful validation error messages."""
    print("=" * 70)
    print("DEMO: Pydantic Validation Error")
    print("=" * 70)

    provider = YamlConfigProvider(TableConfig)

    config = {
        "name": "test_table",
        "stages": [
            {
                # Missing required "name" field
                "mode": "transformation",
                "columns": [
                    {
                        # Missing required "name" field
                        "data_type": "int",
                    }
                ],
            }
        ],
    }

    try:
        table = provider.load_dict(config)
        print(f"\nLoaded table (unexpected): {table.name}")
    except ValueError as e:
        print(f"\n✓ Got expected validation errors:")
        print(f"  {e}\n")


def demo_available_macros():
    """Show which macro paths are available."""
    print("=" * 70)
    print("DEMO: Available Macro Paths")
    print("=" * 70)

    provider = YamlConfigProvider(TableConfig, defaults_path=CONFIGS_DIR / "defaults.yaml")

    config = {
        "name": "test",
        "layer": "${wrong.path.here}",
        "stages": [],
    }

    try:
        table = provider.load_dict(config)
        print(f"\nLoaded table (unexpected): {table.name}")
    except ValueError as e:
        error_msg = str(e)
        if "Available paths:" in error_msg:
            print(f"\n✓ Error shows available macro paths:")
            # Extract just the available paths part
            parts = error_msg.split("Available paths:")
            if len(parts) > 1:
                print(f"  Available paths:{parts[1]}\n")


def main():
    """Run error handling demos."""
    demo_invalid_macro()
    demo_validation_error()
    demo_available_macros()

    print("=" * 70)
    print("✓ Error handling works as expected!")
    print("=" * 70)


if __name__ == "__main__":
    main()
