# Column Input Forms

Every column in a deltagen stage is backed by one or more `ColumnInput` entries under the `inputs:` key. There are three mutually exclusive forms.

---

## Form 1 — Bare `column`

References a column that already exists in the **current accumulated DataFrame** — i.e. the output of any preceding union, filter, or join in the same stage.

```yaml
columns:
  - name: contact
    data_type: string
    inputs:
      - column: contact
```

This is equivalent to `F.col("contact")` in PySpark. Use it when:

- The column was produced by an earlier union and passes straight through.
- The column was introduced by a preceding join and is unambiguous in the accumulated DataFrame (e.g. no alias prefix needed).

---

## Form 2 — `source` + `column`

References a column from a **named source DataFrame** (loaded before the stage runs).

```yaml
columns:
  - name: customer_id
    data_type: int
    nullable: false
    natural: true
    inputs:
      - source: raw_orders
        column: cust_id
```

This is equivalent to `F.col("raw_orders.cust_id")` in PySpark. Use it when you want to be explicit about which loaded source a column comes from (useful when the same column name exists in multiple sources).

---

## Form 3 — `expression`

Evaluates an arbitrary SQL expression via `F.expr(...)`. Supports COALESCE, CASE WHEN, arithmetic, function calls, and `alias.column` references to joined tables.

```yaml
columns:
  - name: fca_number
    data_type: string
    inputs:
      - expression: "COALESCE(fca_number_ifa, mapping_ifa.fca_number)"
```

### `alias.column` references in expressions

When a join has an `alias` set, its columns are accessible as `alias.column` in expressions. If the column was a duplicate (present on both sides of the join), deltagen renames it to `alias__column` internally — but you can still write `alias.column` in the YAML and deltagen rewrites it automatically.

```yaml
joins:
  - name: ifa_mapping_join
    type: left
    source: mapping_ifa_pmps
    alias: mapping_ifa
    conditions:
      - left: ifa
        right: firm_name_in_fum_file

columns:
  - name: ifa
    inputs:
      - expression: "COALESCE(mapping_ifa.ifa, ifa)"
```

---

## Choosing the right form

| Situation | Form |
|---|---|
| Pass-through column from union or earlier join (no ambiguity) | `column` |
| Column from a specific loaded source, before any joins | `source + column` |
| Computed value, COALESCE, CASE WHEN, or cross-join reference | `expression` |

---

## Join conditions: left-side column references

The left side of a join condition also supports dot notation to reference a column introduced by a **preceding join** in the same stage:

```yaml
joins:
  - name: mapping_product_join
    type: left
    source: mapping_product_pmps
    alias: mapping_product
    conditions:
      - left: model          # bare → comes from the initial stage DataFrame
        right: title

  - name: master_lookup_join
    type: left
    source: master_lookup_platform_fum_models
    alias: master_lookup
    conditions:
      - left: mapping_product.bm_model_id   # alias.column → from preceding join
        right: bm_model_id
```

- **Bare** left-side (`model`) — column already on the initial stage DataFrame.
- **`alias.column`** left-side (`mapping_product.bm_model_id`) — column introduced by a join that ran earlier in the same stage.

This notation also ensures the column is correctly included in column pruning so Spark can resolve it at join time.

---

## Technical details

- Implementation: [src/deltagen/model/column.py](../src/deltagen/model/column.py), [src/deltagen/runner/column_builder.py](../src/deltagen/runner/column_builder.py)
- Join condition resolution: [src/deltagen/runner/join_builder.py](../src/deltagen/runner/join_builder.py)
- Tests: [test/unit/runner/test_column_builder.py](../test/unit/runner/test_column_builder.py), [test/unit/runner/test_join_builder.py](../test/unit/runner/test_join_builder.py)
