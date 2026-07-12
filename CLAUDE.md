# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Delta-Gen v2 is a technology-neutral Data Lakehouse Configuration & Transformation Engine. It provides:
- Strict Pydantic v2 models for defining lakehouse tables, stages, columns, joins, and policies
- YAML-based configuration loading with macro expansion
- Plugin registry for extensible column/stage transformations
- PlanBuilder that compiles YAML configs into Spark DataFrame pipelines
- DeltaWriter with merge, overwrite, append, replace_by_partition, and SCD Type-2 modes
- Comprehensive metrics and observability system
- Microsoft Fabric integration layer (`deltagen.fabric`)

## Common Commands

```bash
# Install in development mode
pip install -e .
pip install -r requirements.txt

# Run all tests
pytest

# Run specific test file
pytest test/unit/model/test_models.py

# Run with verbose output
pytest -v

# Build distribution
python -m build
```

## Architecture

```
src/deltagen/
├── model/           # Pydantic models (TableConfig, SourceConfig, JoinConfig, etc.)
├── providers/       # ConfigProvider protocol + YamlConfigProvider + macro expansion
├── runner/          # PlanBuilder, DeltaWriter, join/union/aggregation/filter builders
├── plugins/         # Registry, context, metrics, default plugins
└── fabric/          # Microsoft Fabric integration (adapter, plugins, context)
```

### Key Architectural Patterns

1. **StrictBaseModel**: All models inherit from `StrictBaseModel` (frozen, extra="ignore") with an `extensions` dict for vendor-specific data.

2. **Plugin Registry**: Column and stage plugins registered via `@register_column`/`@register_stage` decorators, invoked via `extensions.transform` or `extensions.stage_plugin` in YAML.

3. **Writer Modes**: DeltaWriter supports `merge` (with strategies: update_changed, insert_only, scd2), `overwrite`, `append`, and `replace_by_partition`.

4. **Source Types**: Regular table sources, broadcast sources (for dimension lookups), and `generated` sources (synthetic DataFrames for expression-only tables like d_date).

5. **Incremental Loading**: Watermark-based and period-based filtering with source-level control (`source.incremental: false`), watermark overrides from tracking tables, and period expansion for replace_by_partition.

6. **Union Stages**: Support `column_map` per source for SQL expression remapping before union, and `allow_missing_columns` for flexible multi-source unions.

### Core Components

| Component | Location | Purpose |
|-----------|----------|---------|
| `TableConfig` | `model/table.py` | Root config with stages, policies, sources, natural_key |
| `SourceConfig` | `model/source.py` | Source table with generated, broadcast, incremental flags |
| `JoinConfig` | `model/join.py` | Join definition with alias, dedupe_by support |
| `StageConfig` | `model/stage.py` | Stage with UnionSource/column_map support |
| `PlanBuilder` | `runner/plan_builder.py` | Builds DataFrames from config with watermark overrides |
| `DeltaWriter` | `runner/writer.py` | Writes to Delta with merge/overwrite/replace_by_partition |
| `MetricsCollector` | `plugins/metrics.py` | Tracks source reads, stages, DQ, writes |
| `PluginContext` | `plugins/context.py` | Shared state, DQ logging with JSON schema |

### Default Plugins (plugins/defaults.py)

| Plugin | Type | Description |
|--------|------|-------------|
| `not_null` | column | Reject/warn/fill null values |
| `in_set` | column | Validate values against allowed set |
| `mask_email` | column | Mask email addresses |
| `dedupe_keep_last` | stage | Keep latest record per natural key |
| `distinct` | stage | Remove exact duplicate rows |
| `filter_latest_file_per_period` | stage | Keep only rows from most recent source file per period |

### Fabric Integration (fabric/)

| Component | Purpose |
|-----------|---------|
| `FabricMetricsAdapter` | Persists RunMetrics to 6 partitioned Delta tables |
| `create_fabric_context()` | One-line setup for Fabric PluginContext |
| `ensure_sentinels` | Union sentinel rows into dimension tables |
| `check_unresolved_fks` | Detect fact rows with NO_* FK sentinels |
| `schedule_fk_reresolution` | Schedule fact re-processing after dimension refresh |
| `self_join_previous_period` | Period-on-period flow calculation via FULL OUTER JOIN |
| `composite_period_replace` | Multi-column partition replacement |
| `log_nulls_to_table` / `log_invalid_to_table` | DQ violation logging to Delta tables |

## Testing Patterns

- Unit tests in `test/unit/` mirror `src/deltagen/` structure
- Test fixtures in `test/fixtures/configs/`
- Spark-dependent tests require PySpark but core model tests are pure Python/Pydantic

## Key Files for Understanding the Codebase

- `docs/architecture.md` - Architecture and design decisions
- `src/deltagen/plugins/README.md` - Plugin system documentation
- `docs/MERGE_STRATEGIES.md` - Merge strategy patterns
- `docs/JOIN_ALIAS_GUIDE.md` - Join aliases and chained joins
- `docs/FABRIC_INTEGRATION.md` - Fabric integration guide
