# Delta-Gen v2 Architecture

## Overview

Delta-Gen is a declarative, YAML-driven transformation engine for data lakehouses. It compiles table configuration files into Spark DataFrame pipelines with built-in incremental loading, data quality, and observability.

```
YAML Config  -->  YamlConfigProvider  -->  PlanBuilder  -->  DataFrame  -->  DeltaWriter  -->  Delta Table
                       |                       |                                  |
                  macro expansion          plugins                          merge/overwrite/
                  + validation          (transform,                        replace_by_partition
                                         DQ, etc.)
```

## Repository Layout

```
src/deltagen/
├── model/              # Pydantic v2 data models (configuration contract)
│   ├── base.py         # StrictBaseModel with extensions support
│   ├── table.py        # TableConfig (root model)
│   ├── stage.py        # StageConfig, UnionConfig, UnionSource, AggregationConfig
│   ├── column.py       # ColumnConfig, ColumnInput
│   ├── join.py         # JoinConfig with dedupe_by support
│   ├── source.py       # SourceConfig with generated/broadcast/incremental flags
│   ├── policies.py     # PoliciesConfig (load mode, merge strategy, orchestration)
│   ├── incremental.py  # IncrementalConfig, DQConfig
│   ├── typespec.py     # TypeSpec for cross-engine type normalization
│   └── environment.py  # EnvironmentConfig for layer/source resolution
│
├── providers/          # Configuration loading
│   ├── base.py         # ConfigProvider protocol
│   ├── yaml_provider.py# YamlConfigProvider with Pydantic validation
│   └── macros.py       # ${defaults.*} macro expansion
│
├── runner/             # Execution engine
│   ├── plan_builder.py # PlanBuilder - orchestrates the full pipeline
│   ├── source_loader.py# Load DataFrames from tables, paths, or generated sources
│   ├── column_builder.py# Resolve column expressions to Spark columns
│   ├── join_builder.py # Handle joins with alias dedup and source dedup
│   ├── union_builder.py# Union stages with column_map support
│   ├── aggregation_builder.py # GROUP BY aggregations
│   ├── filter_builder.py# WHERE clause generation
│   ├── sql_generator.py# Spark SQL expression building
│   ├── writer.py       # DeltaWriter (merge, overwrite, append, replace_by_partition, SCD2)
│   └── exceptions.py   # PlanBuilderError
│
├── plugins/            # Extensibility layer
│   ├── registry.py     # @register_column / @register_stage decorators
│   ├── context.py      # PluginContext - shared state, DQ logging
│   ├── defaults.py     # Built-in plugins (not_null, dedupe, distinct, etc.)
│   └── metrics.py      # MetricsCollector, RunMetrics
│
└── fabric/             # Microsoft Fabric integration
    ├── adapter.py      # FabricMetricsAdapter (auto-persist to Delta tables)
    ├── context.py      # create_fabric_context() helper
    └── plugins/        # Fabric-specific plugins (sentinels, DQ, flow, etc.)
```

## Core Concepts

### Configuration Flow

1. **YAML file** defines a table: name, sources, columns, stages, policies
2. **YamlConfigProvider** loads YAML, expands `${defaults.*}` macros, validates via Pydantic
3. **TableConfig** (root model) holds the validated, immutable configuration
4. **PlanBuilder** reads the config and builds a Spark DataFrame pipeline

### Stage Pipeline

PlanBuilder processes stages sequentially:

```
Load Sources  -->  Stage 1 (transform/union/aggregation)  -->  Stage 2  -->  ...  -->  Write
     |                          |
  incremental              join, filter,
  filtering               column plugins,
                          stage plugins
```

Each stage can be:
- **transformation**: Apply column expressions, joins, and filters
- **union**: Combine multiple sources (with optional column_map per source)
- **aggregation**: GROUP BY with aggregate functions

### Write Modes

| Mode | Behaviour |
|------|-----------|
| `merge` | Upsert on natural key. Strategies: `update_changed`, `update_all`, `insert_only`, `scd_type2` |
| `overwrite` | Full table replacement |
| `append` | Insert-only, no dedup |
| `replace_by_partition` | Delete matching partitions, then insert |

### Plugin System

Plugins are registered via decorators and invoked from YAML `extensions`:

```python
@register_column("my_transform")
def my_transform(df, column, ctx):
    # transform logic
    return df

@register_stage("my_stage")
def my_stage(df, stage, ctx):
    # stage logic
    return df
```

Referenced in YAML:
```yaml
columns:
  - name: email
    extensions:
      transform: my_transform

stages:
  - name: custom_step
    extensions:
      stage_plugin: my_stage
```

### Incremental Loading

Two modes for filtering source data:

- **Watermark**: Load records where `source_watermark > max(target.watermark_column)`
- **Period**: Load from the latest period in the target onwards

Advanced features:
- **Source-level control**: `source.incremental: false` skips filtering for reference tables
- **Watermark overrides**: Read from `log.watermark_overrides` table for FK re-resolution
- **Period expansion**: With `replace_by_partition` + `source_period_column`, identifies modified periods via watermark then loads full partitions

### Data Quality

- **Column plugins**: `not_null`, `in_set`, `log_nulls_to_table`, `log_invalid_to_table`
- **Stage plugins**: `check_unresolved_fks`, `check_duplicates`
- **DQ logging**: Rejected records written to DQ tables with fixed JSON schema
- **Metrics**: All DQ events recorded in MetricsCollector for observability

## Key Design Decisions

1. **Models are frozen**: All Pydantic models use `frozen=True`. No mutation after creation.
2. **Extensions escape hatch**: Every model has `extensions: dict` for vendor-specific data without polluting the core schema.
3. **Technology-neutral core**: The core engine has no Fabric dependencies. Fabric integration lives in `deltagen.fabric`.
4. **Plugins over inheritance**: Custom behaviour is added via the plugin registry, not by subclassing.
5. **Column pruning**: PlanBuilder only loads columns actually needed by the pipeline, reducing memory and I/O.
