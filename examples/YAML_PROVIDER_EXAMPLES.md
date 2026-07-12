# YamlConfigProvider - Working Examples

**NOTE:** This document covers the original DG-4 features and does not include the latest enhancements (Generic Provider + Smart Column Templates).

## Overview

The YamlConfigProvider is fully implemented and fully tested, with all tests currently passing. Here's what works:

## ✅ Key Features Working

### 1. **Macro Expansion with Type Preservation**

```yaml
# In defaults.yaml
defaults:
  policies:
    optimisation:
      load_mode: merge
  common_columns:
    is_active:
      data_type: boolean
      nullable: false
      default: true
```

```yaml
# In customer_dim.yaml - OLD WAY (still works!)
policies:
  optimisation:
    load_mode: ${defaults.policies.optimisation.load_mode}  # Expands to "merge"
columns:
  - name: is_active
    data_type: ${defaults.common_columns.is_active.data_type}    # boolean (not "boolean")
    nullable: ${defaults.common_columns.is_active.nullable}      # False (not "False")
    default: ${defaults.common_columns.is_active.default}        # True (not "True")

# NEW WAY - Smart Templates (automatic!)
columns:
  - name: is_active  # Auto-inherits data_type, nullable, default!
    temporary: false
```

**Result**: Types are preserved! Booleans stay `bool`, integers stay `int`.

### 2. **Auto-Discovery of Defaults**

```python
from deltagen.model import TableConfig
from deltagen.providers import YamlConfigProvider

# Automatically finds defaults.yaml in same directory
provider = YamlConfigProvider(TableConfig)
table = provider.load("configs/customer_dim.yaml")
```

### 3. **Explicit Defaults Path**

```python
# Use explicit path to defaults
provider = YamlConfigProvider(TableConfig, defaults_path="configs/defaults.yaml")
table = provider.load("configs/product_dim.yaml")
```

### 4. **Multi-Stage Tables with Temporary Columns**

```yaml
# sales_fact.yaml has 2 stages
stages:
  - name: source_stage
    columns:
      - name: temp_discount_pct
        temporary: true  # Won't be persisted
      - name: total_amount
        temporary: false  # Will be persisted

  - name: aggregation_stage
    columns:
      - name: net_amount
        temporary: false
```

```python
table = provider.load("configs/sales_fact.yaml")

# Filter temporary columns
temp_cols = table.filter_columns(temporary=True)
# Returns: [temp_discount_pct, temp_tax_amount]

# Get columns to persist
persistent = table.get_persistent_columns()
# Returns: 9 columns (temporary columns excluded)
```

### 5. **Joins Configuration**

```yaml
# product_dim.yaml
sources:
  - name: raw_products
    path: /lakehouse/bronze/products
  - name: raw_categories
    path: /lakehouse/bronze/categories

stages:
  - name: source_stage
    joins:
      - name: cat
        source: raw_categories
        type: left
        conditions:
          - left: category_id
            right: id
            operator: "="
```

```python
table = provider.load("configs/product_dim.yaml")
print(table.sources)  # 2 sources
print(table.stages[0].joins)  # 1 left join
```

### 6. **TypeSpec Integration**

```python
table = provider.load("configs/product_dim.yaml")

for col in table.iter_columns():
    typespec = col.get_typespec()
    if typespec:
        print(f"{col.name}:")
        print(f"  Original: {col.data_type}")
        print(f"  Spark SQL: {typespec.to_spark_sql()}")
        print(f"  Standard SQL: {typespec.to_standard_sql()}")
```

**Output**:
```
product_name:
  Original: varchar(255)
  Spark SQL: STRING
  Standard SQL: VARCHAR(255)

unit_price:
  Original: decimal(18,2)
  Spark SQL: DECIMAL(18,2)
  Standard SQL: DECIMAL(18,2)
```

### 7. **Load from Dictionary with Macros**

```python
provider = YamlConfigProvider(TableConfig, defaults_path="configs/defaults.yaml")

config_dict = {
    "name": "my_table",
    "layer": "bronze",
    "policies": {
        "optimisation": {
            "load_mode": "${defaults.policies.optimisation.load_mode}"
        }
    },
    "stages": [...]
}

table = provider.load_dict(config_dict)
# Macros are expanded automatically!
```

### 8. **Validation Error Formatting**

```python
# Missing required field
config = {
    "name": "test",
    # Missing "layer" field
    "stages": []
}

try:
    table = provider.load_dict(config)
except ValueError as e:
    print(e)
```

**Output**:
```
Configuration validation failed:
  • layer: Field required
```

### 9. **Extension Attributes Support**

```yaml
# sales_fact.yaml
extensions:
  data_quality:
    rules:
      - column: total_amount
        type: not_null
      - column: total_amount
        type: greater_than
        value: 0
```

```python
table = provider.load("configs/sales_fact.yaml")
print(table.extensions)
# {'data_quality': {'rules': [...]}}
```

### 10. **Column Filtering by Extensions**

```yaml
# In column definition
columns:
  - name: email
    data_type: varchar(255)
    extensions:
      pii: true
      masked: true
```

```python
# Filter by extension attributes
pii_columns = table.filter_columns(pii=True)
masked_columns = table.filter_columns(masked=True)
```

## 📊 Test Coverage

- **Total Tests**: 139 passing
- **Provider Tests**: 28
  - Basic loading: 4 tests
  - Macro expansion: 3 tests
  - Defaults merging: 2 tests
  - Column attributes: 4 tests
  - Multi-stage: 2 tests
  - Sources: 3 tests
  - Joins: 1 test
  - Tags/Extensions: 2 tests
  - Error handling: 6 tests
  - Load dict: 2 tests

## 🎯 Real-World Usage Pattern

```python
from deltagen.providers import YamlConfigProvider
from pathlib import Path

# Setup
configs_dir = Path("configs")
provider = YamlConfigProvider(TableConfig, defaults_path=configs_dir / "defaults.yaml")

# Load configuration
table = provider.load(configs_dir / "sales_fact.yaml")

# Use in your pipeline
print(f"Processing table: {table.name}")
print(f"Layer: {table.layer}")
print(f"Load mode: {table.policies.optimisation.load_mode}")

# Get columns to write to Delta
persistent_cols = table.get_persistent_columns()
print(f"Writing {len(persistent_cols)} columns to Delta table")

# Use TypeSpec for SQL generation
for col in persistent_cols:
    typespec = col.get_typespec()
    if typespec:
        sql_type = typespec.to_spark_sql()
        print(f"{col.name} {sql_type}")
```

### 11. **Union Multiple Sources**

Combine multiple DataFrames in a stage using `unions`:

```yaml
# Combine sales from multiple years
stages:
  - name: combine_years
    unions:
      sources: [sales_2023, sales_2024]  # Minimum 2 sources
      mode: by_name                       # or 'by_position'
      distinct: false                     # UNION ALL (default) vs UNION DISTINCT
      allow_missing_columns: false        # Fill missing columns with NULL if true

    # Source filters applied BEFORE union (performance optimization)
    source_filters:
      sales_2023:
        - "is_valid = true"
      sales_2024:
        - "is_valid = true"

    columns:
      - name: sale_id
        inputs:
          - column: transaction_id
      # ... more columns
```

**Union Modes:**
- `by_name` (default): Match columns by name across sources
- `by_position`: Match columns by index (all sources must have same column count)

### 12. **Group By and Aggregations**

Aggregate data with `group_by`:

```yaml
stages:
  - name: aggregate_sales
    # Filters applied BEFORE group_by (WHERE clause)
    filters:
      - "sale_amount > 0"

    group_by:
      columns:
        - region_code
        - product_category

      aggregations:
        # SUM
        - column: sale_amount
          function: sum
          alias: total_sales

        # COUNT
        - column: sale_id
          function: count
          alias: transaction_count

        # COUNT DISTINCT
        - column: customer_id
          function: count
          alias: unique_customers
          distinct: true

        # AVG, MIN, MAX
        - column: sale_amount
          function: avg
          alias: avg_sale

        # FIRST, LAST
        - column: sale_date
          function: first
          alias: first_sale_date

        # COLLECT_LIST, COLLECT_SET
        - column: product_id
          function: collect_set
          alias: unique_products

      # HAVING clause - filters AFTER aggregation
      having:
        - "total_sales > 1000"
        - "transaction_count >= 10"
```

**Supported Aggregation Functions:**
- `sum`, `avg`, `mean`, `min`, `max`, `count`
- `first`, `last`
- `collect_list`, `collect_set`
- `stddev`, `stddev_pop`, `stddev_samp`
- `variance`, `var_pop`, `var_samp`
- `approx_count_distinct`, `count_distinct`

**Processing Order (Optimized for Performance):**
```
1. Source filters (BEFORE union/joins - reduces data early)
2. Unions
3. Joins
4. Columns
5. Filters (WHERE - BEFORE group_by)
6. Group by (HAVING clause applied internally)
7. Stage plugin
```

### 13. **Complete Union + Group By Example**

See `examples/configs/sales_aggregation.yaml` for a complete example combining:
- Union of multiple year sources
- Dimension joins
- Aggregation with HAVING clause

```python
from deltagen.providers import YamlConfigProvider
from deltagen.runner import PlanBuilder

provider = YamlConfigProvider(TableConfig)
config = provider.load("examples/configs/sales_aggregation.yaml")

builder = PlanBuilder(config)
df = builder.build(spark, debug=True)  # Shows step-by-step processing
```

## 🚀 Next Steps

With YamlConfigProvider complete, you can now:

1. **DG-5**: Build XmlConfigAdapter for backwards compatibility
2. **DG-6**: Implement PlanBuilder for transformation engine
3. **DG-7**: Create Writer interface (will use `get_persistent_columns()`)

All the foundation is in place! 🎉
