# Automatic Duplicate Column Handling in Joins

## Overview

DeltaGen now automatically handles duplicate column names when joining DataFrames. This is particularly useful for multi-stage transformations where natural key columns appear in both the previous stage and dimension tables.

## Problem Statement

When joining two DataFrames that have columns with the same name, Spark creates ambiguous column references:

```python
# Before enhancement - would fail with AMBIGUOUS_REFERENCE error
df1 = stage1_output  # has ProductNaturalID
df2 = dim_product    # also has ProductNaturalID
result = df1.join(df2, df1.ProductNaturalID == df2.ProductNaturalID)
# result now has TWO columns named ProductNaturalID - ambiguous!
```

## Solution

DeltaGen's join builder now automatically:
1. Detects when both DataFrames have columns with the same name
2. Keeps the column from the **left DataFrame** (the one being joined to)
3. Drops the duplicate column from the **right DataFrame** (the one being joined)

This follows standard ETL framework behavior and SQL semantics.

## Example

### Multi-Stage Join with Duplicate Columns

```yaml
stages:
  # Stage 1: Aggregate fact data
  - name: aggregate_facts
    mode: transformation
    group_by:
      columns: [ProductNaturalID, DateNaturalID]
      aggregations:
        - column: amount
          function: sum
          alias: TotalAmount
    columns:
      - name: ProductNaturalID
        data_type: string
        nullable: false
        inputs:
          - source: raw_sales
            column: product_code

      - name: DateNaturalID
        data_type: date
        nullable: false
        inputs:
          - source: raw_sales
            column: sale_date

  # Stage 2: Join with dimensions (ProductNaturalID exists in both!)
  - name: enrich_with_dims
    mode: transformation
    joins:
      - name: product_dim
        type: left
        source: d_product
        conditions:
          - left: ProductNaturalID  # From previous stage
            right: d_product.ProductNaturalID  # From dimension
            operator: "="

    columns:
      - name: ProductNaturalID
        data_type: string
        nullable: false
        inputs:
          - column: ProductNaturalID  # ✅ No ambiguity - uses previous stage

      - name: FK_ProductID
        data_type: string
        nullable: false
        inputs:
          - source: d_product
            column: ProductID  # Gets the surrogate key
```

**Before this feature:** Would fail with `[AMBIGUOUS_REFERENCE]` error
**After this feature:** Works seamlessly - keeps `ProductNaturalID` from stage 1

## Debug Output

When running with `debug=True`, you'll see which columns are being deduplicated:

```
JOINS:
  - LEFT JOIN d_product
    ON ProductNaturalID = d_product.ProductNaturalID
    Duplicate columns detected: ['ProductNaturalID']
    Keeping from left, dropping from d_product
```

## Behavior

- **Default behavior:** Automatically enabled for all joins
- **Column preference:** Always keeps left DataFrame column
- **Performance:** Minimal overhead - only applies when duplicates detected
- **Transparency:** Debug mode shows which columns are handled

## Benefits

1. **No workarounds needed:** No need to rename columns to avoid conflicts
2. **Cleaner YAML:** Natural column names throughout
3. **Standard behavior:** Matches SQL and other ETL frameworks
4. **Efficient:** Multi-stage aggregations work as expected
5. **Backwards compatible:** Doesn't affect joins without duplicate columns

## Migration Guide

If you previously worked around this limitation by renaming columns:

**Before (workaround):**
```yaml
# Stage 1 - renamed to avoid conflict
columns:
  - name: Fact_ProductNaturalID  # 👎 Workaround naming

# Stage 2 - rename back
columns:
  - name: ProductNaturalID
    inputs:
      - column: Fact_ProductNaturalID
```

**After (clean):**
```yaml
# Stage 1 - natural names
columns:
  - name: ProductNaturalID  # ✅ Clean naming

# Stage 2 - no renaming needed
columns:
  - name: ProductNaturalID
    inputs:
      - column: ProductNaturalID  # Just works!
```

## Technical Details

Implementation: [`src/deltagen/runner/join_builder.py`](../src/deltagen/runner/join_builder.py)
Tests: [`test/unit/runner/test_join_builder.py`](../test/unit/runner/test_join_builder.py)

The enhancement detects duplicate columns by comparing the column sets of both DataFrames before joining, then uses Spark's `select()` with explicit column references to disambiguate after the join.
