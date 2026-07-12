# Delta-Gen v2

Delta-Gen v2 is a technology-neutral Data Lakehouse Configuration & Transformation Engine.
It provides a declarative YAML-driven approach to defining data pipelines across
Bronze, Silver, and Gold layers with built-in orchestration, data quality, and
incremental load capabilities.

## Features

### Core Engine
- **Strict Pydantic v2 models** with `extensions` for vendor-specific data
- **YAML configuration** with macro expansion (`${defaults.*}`)
- **PlanBuilder** orchestrates multi-stage transformations into Spark DataFrames
- **DeltaWriter** handles merge, overwrite, append, replace_by_partition, and SCD Type-2
- **Plugin registry** for extensible column/stage transformations via decorators

### Transformation Capabilities
- **Multi-source joins** with alias deduplication and chained join support
- **Join-source deduplication** (`dedupe_by`, `dedupe_order_by`) to prevent row explosion
- **Union stages** with `column_map` for per-source SQL expression mapping
- **Aggregation stages** with GROUP BY and aggregate functions
- **Generated sources** for synthetic tables (e.g., date dimensions)
- **Column pruning** - only loads columns actually needed by the pipeline

### Incremental Loading
- **Watermark-based filtering** on source tables
- **Period-based filtering** with `replace_by_partition` load mode
- **Watermark overrides** from tracking tables (for FK re-resolution)
- **Source-level incremental control** - skip filtering per source via `incremental: false`
- **Period expansion** for replace_by_partition - identifies modified periods, then loads full partitions

### Data Quality & Observability
- **MetricsCollector** tracks source reads, stage row counts, DQ issues, and write results
- **Built-in plugins**: `not_null`, `in_set`, `mask_email`, `dedupe_keep_last`, `distinct`, `filter_latest_file_per_period`
- **DQ logging** with fixed JSON schema (avoids Fabric catalog corruption)
- **Rejected/duplicate record tracking** to separate DQ tables

### Fabric Integration (`deltagen.fabric`)
- **FabricMetricsAdapter** - auto-persists metrics to 6 partitioned Delta tables
- **Fabric plugins** - dimension sentinels, DQ logging, FK re-resolution, flow calculation, composite partition replacement, write hooks
- **Context helpers** - `create_fabric_context()` for one-line Fabric setup

## Installation

```bash
python -m pip install -e .
```

## Quick Start

### YAML Configuration

```yaml
name: customer
layer: silver
target_schema: silver

sources:
  - name: src
    table: bronze.raw_customers

natural_key:
  - customer_id

incremental:
  filter_mode: watermark
  watermark_column: LastModifiedDate

policies:
  optimisation:
    load_mode: merge
    merge_strategy: update_changed
    hash_columns: [customer_name, email]

columns:
  - name: customer_id
    data_type: string
    nullable: false
    inputs:
      - source: src
        column: id

  - name: customer_name
    data_type: string
    nullable: false
    inputs:
      - source: src
        column: name
```

### Python Usage

```python
from deltagen.model import TableConfig
from deltagen.providers import YamlConfigProvider
from deltagen.runner import PlanBuilder
from deltagen.runner.writer import DeltaWriter

# Load config
provider = YamlConfigProvider(TableConfig, defaults_path="defaults.yaml")
config = provider.load("customer.yaml")

# Build transformation
builder = PlanBuilder(config)
df = builder.build(spark, debug=True)

# Write to Delta table
writer = DeltaWriter()
result = writer.write(spark, df, config)
```

### With Fabric Integration

```python
from deltagen.fabric import create_fabric_context

ctx = create_fabric_context(
    spark=spark,
    table_name="customer",
    config=config,
    load_id="batch_001",
)

df = builder.build(spark, context=ctx)
writer.write(spark, df, config, context=ctx)
ctx.metrics.complete()  # Auto-persists to Delta tables
```

## Architecture

```
src/deltagen/
├── model/           # Pydantic models (TableConfig, SourceConfig, JoinConfig, etc.)
├── providers/       # YAML config loading with macro expansion
├── runner/          # PlanBuilder, DeltaWriter, join/union/aggregation builders
├── plugins/         # Registry, context, metrics, default plugins
└── fabric/          # Microsoft Fabric integration (adapter, plugins, context)
```

## Tests

```bash
pytest
```

## Docs

- [Architecture](docs/architecture.md) - system design, layout, key concepts
- [Column inputs](docs/COLUMN_INPUTS.md) - three input forms: bare, source+column, expression
- [Join aliases](docs/JOIN_ALIAS_GUIDE.md) - alias deduplication and chained joins
- [Merge strategies](docs/MERGE_STRATEGIES.md) - update_changed, insert_only, SCD2, etc.
- [Duplicate column handling](docs/DUPLICATE_COLUMN_HANDLING.md) - multi-stage join resolution
- [Fabric integration](docs/FABRIC_INTEGRATION.md) - FabricMetricsAdapter, plugins, context
- [Plugin system](src/deltagen/plugins/README.md) - registry, decorators, built-in plugins

## License

MIT - see `LICENSE`.
