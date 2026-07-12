# Fabric Integration Guide

Delta-Gen includes a first-party Microsoft Fabric integration layer at `deltagen.fabric`. This module provides automatic metrics persistence, Fabric-specific plugins, and context helpers.

## Quick Start

```python
from deltagen.model import TableConfig
from deltagen.providers import YamlConfigProvider
from deltagen.runner import PlanBuilder
from deltagen.runner.writer import DeltaWriter
from deltagen.fabric import create_fabric_context

# Load config
provider = YamlConfigProvider(TableConfig, defaults_path="silver_defaults.yaml")
config = provider.load("customer.yaml")

# Create Fabric context with auto-persistence
ctx = create_fabric_context(
    spark=spark,
    table_name="silver_customer",
    config=config,
    load_id="batch_001",
    schema="log",           # Schema for metrics tables
    prefix="deltagen",      # Table prefix (e.g., log.deltagen_run_metrics)
    environment="fabric",
    full_load=False,
)

# Build and write
builder = PlanBuilder(config)
df = builder.build(spark, debug=True, context=ctx)

writer = DeltaWriter()
result = writer.write(spark, df, config, context=ctx)

# Finalize - metrics auto-persist to Delta tables
ctx.metrics.complete()
```

## FabricMetricsAdapter

Persists `RunMetrics` to 6 partitioned Delta tables:

| Table | Contents |
|-------|----------|
| `{schema}.{prefix}_run_metrics` | Run-level summary (duration, status, row counts) |
| `{schema}.{prefix}_source_metrics` | Per-source read stats (rows, columns, bytes, timing) |
| `{schema}.{prefix}_stage_metrics` | Per-stage row counts and timing |
| `{schema}.{prefix}_quality_metrics` | DQ issues (nulls, invalid values, unresolved FKs) |
| `{schema}.{prefix}_write_metrics` | Write results (inserted, updated, deleted counts) |
| `{schema}.{prefix}_plugin_metrics` | Plugin execution stats |

All tables are partitioned by `partition_year` / `partition_month`.

## Fabric Plugins

### Dimension Plugins

#### `ensure_sentinels`
Unions static sentinel rows (NO_*, NA_*) into dimension tables before write. Works with `insert_only` merge strategy for idempotent inserts.

```yaml
stages:
  - name: add_sentinels
    extensions:
      stage_plugin: ensure_sentinels
      records:
        - ProductID: "NO_PRODUCT"
          ProductName: "!! Unknown"
        - ProductID: "NA_PRODUCT"
          ProductName: "N/A"
```

#### `schedule_fk_reresolution`
Scans fact tables for unresolved FK sentinels and schedules re-processing via watermark overrides.

```yaml
stages:
  - name: schedule_reresolution
    extensions:
      stage_plugin: schedule_fk_reresolution
      sentinel: NO_PRODUCT
      inputs_path: /lakehouse/default/Files/inputs/gold/fact
      overrides_table: log.watermark_overrides
      lookback_days: 90
```

### DQ Plugins

#### `log_nulls_to_table`
Column plugin that validates not-null and logs violations to a Delta table.

```yaml
columns:
  - name: customer_id
    extensions:
      transform: log_nulls_to_table
      log_table: log.silver_dq_rejected
      on_null: reject  # reject | warn | fill
```

#### `log_invalid_to_table`
Column plugin that validates values against an allowed set.

```yaml
columns:
  - name: status
    extensions:
      transform: log_invalid_to_table
      log_table: log.silver_dq_rejected
      allowed_values: ["ACTIVE", "INACTIVE"]
      on_violation: reject
```

#### `check_unresolved_fks`
Stage plugin that detects fact rows with NO_* sentinel FK values and writes summary + records to DQ tables.

```yaml
stages:
  - name: check_fks
    extensions:
      stage_plugin: check_unresolved_fks
      summary_table: log.gold_dq_unresolved_fks
      records_table: log.gold_dq_unresolved_records
      checks:
        - column: FK_ProductID
          sentinel: NO_PRODUCT
        - column: FK_CustomerID
          sentinel: NO_CUSTOMER
```

### Flow Plugins

#### `self_join_previous_period`
Computes period-on-period flow using FULL OUTER JOIN instead of LAG. Captures both new and lost rows.

```yaml
stages:
  - name: calc_flow
    extensions:
      stage_plugin: self_join_previous_period
      period_column: source_period
      join_keys: [product, customer, region]
      fum_column: amount
```

### Partition Plugins

#### `composite_period_replace`
Replaces data by multiple partition columns. Issues separate DELETE per partition value.

```yaml
stages:
  - name: partition_load
    extensions:
      stage_plugin: composite_period_replace
      period_column: DateNaturalID
      partition_columns: [source_system]
```

### Write Hooks

```python
from deltagen.fabric.plugins.write_hooks import create_write_logging_hook

hook = create_write_logging_hook(spark, schema="log")
writer = DeltaWriter(post_write_hook=hook)
```

## Deployment Pattern

In Fabric notebooks, libraries are distributed via a zip file uploaded to the Lakehouse:

```
/lakehouse/default/Files/libs/
├── deltagen/          # Core engine
├── deltagen_helpers/  # Fabric helpers (or use deltagen.fabric)
├── orchestration/     # Notebook orchestration utilities
└── libs.zip           # Auto-generated zip for Spark distribution
```

The orchestrator notebook builds `libs.zip` at startup and distributes via `spark.sparkContext.addPyFile()`.
