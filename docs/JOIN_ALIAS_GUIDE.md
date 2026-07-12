# Join Aliases and Chained Joins in Delta-Gen

This guide covers the join alias feature and how to write join conditions that reference columns introduced by earlier joins in the same stage.

---

## The problem: duplicate column names

When you join two tables that share column names (e.g. both have a `title` column), Spark raises an `AnalysisException` because the reference is ambiguous. Delta-Gen solves this with **join aliases**.

When a join is given an `alias`, any column that already exists in the left-hand DataFrame is automatically renamed to `{alias}__{column}` in the joined DataFrame before the join condition is evaluated. Non-duplicate columns are kept as-is.

---

## Syntax

```yaml
joins:
  - name: my_join
    type: left
    source: my_lookup_table      # source name as declared in the sources: block
    alias: my_lookup             # short alias for this join
    conditions:
      - left: <left-side column>
        right: <right-side column>
```

### Left-side column rules

| What you write | Meaning |
|---|---|
| `model` | A bare column — must already exist in the stage DataFrame (from the union or a previous stage). |
| `my_lookup.bm_model_id` | A column introduced by the join named `my_lookup`. Delta-Gen resolves this to the correct Spark column name at runtime. |

### Right-side column rules

The right side always refers to a column in `source`. Write the plain column name (`bm_model_id`), not prefixed with the alias.

---

## Example 1 — Simple alias join (deduplicating column names)

Both `mapping_ifa_pmps` and the stage DataFrame have a column called `ifa`. Using an alias prevents the ambiguity.

```yaml
sources:
  - name: mapping_ifa_pmps
    catalog: silver
    schema: sharepoint
    table: mapping_ifa_pmps

stages:
  - name: enrich_ifa
    mode: transformation
    joins:
      - name: ifa_mapping_join
        type: left
        source: mapping_ifa_pmps
        alias: mapping_ifa             # <-- alias declared here
        conditions:
          - left: ifa                  # bare column — already in stage DF
            right: firm_name_in_fum_file

    columns:
      - name: ifa
        data_type: string
        inputs:
          # Use alias.column notation in expressions to read from the joined table.
          # Delta-Gen rewrites this to the correct Spark column name automatically.
          - expression: "COALESCE(mapping_ifa.ifa, ifa)"

      - name: fca_number
        data_type: string
        inputs:
          - expression: "COALESCE(fca_number_ifa, mapping_ifa.fca_number)"
```

**What happens internally:**
- `mapping_ifa_pmps` has `ifa`, which also exists in the stage DF → it is renamed to `mapping_ifa__ifa` before the join.
- In the `columns` block, `mapping_ifa.ifa` is rewritten by Delta-Gen to `mapping_ifa__ifa` before being passed to Spark.
- Non-duplicate columns from `mapping_ifa_pmps` (e.g. `fca_number`) are kept as plain `fca_number` and `mapping_ifa.fca_number` rewrites to just `fca_number`.

---

## Example 2 — Chained joins (using a column from join 1 to drive join 2)

This is the key new capability. Join 3 needs a column (`bm_model_id`) that doesn't exist in the original data — it was introduced by join 2. Use `alias.column` notation on the left side of the condition.

```yaml
sources:
  - name: mapping_ifa_pmps
    catalog: silver
    schema: sharepoint
    table: mapping_ifa_pmps

  - name: mapping_product_pmps
    catalog: silver
    schema: sharepoint
    table: mapping_product_pmps

  - name: master_lookup_platform_fum_models
    catalog: silver
    schema: sharepoint
    table: master_lookup_platform_fum_models

stages:
  - name: enrich_details
    mode: transformation
    joins:
      # Join 1: enrich IFA/FCA info
      - name: ifa_mapping_join
        type: left
        source: mapping_ifa_pmps
        alias: mapping_ifa
        conditions:
          - left: ifa                        # already in stage DF
            right: firm_name_in_fum_file

      # Join 2: look up the product row by model name → this adds bm_model_id
      - name: mapping_product_join
        type: left
        source: mapping_product_pmps
        alias: mapping_product
        conditions:
          - left: model                      # already in stage DF
            right: title

      # Join 3: use bm_model_id from join 2 to look up the canonical BM model name
      - name: master_lookup_platform_fum_join
        type: left
        source: master_lookup_platform_fum_models
        alias: master_lookup_platform_fum_models
        conditions:
          - left: mapping_product.bm_model_id   # <-- column introduced by join 2
            right: bm_model_id

    columns:
      - name: model
        data_type: string
        inputs:
          - expression: "COALESCE(master_lookup_platform_fum_models.bm_model, model)"

      - name: bm_model
        data_type: string
        inputs:
          - expression: "master_lookup_platform_fum_models.bm_model"
```

**What happens internally:**

1. After join 1: `mapping_ifa_pmps` columns are in the DataFrame. Any duplicates were renamed to `mapping_ifa__<col>`.
2. After join 2: `mapping_product_pmps` columns (including `bm_model_id`) are in the DataFrame. Delta-Gen records that `mapping_product.bm_model_id` → plain `bm_model_id` (no rename needed since no duplicate).
3. Join 3 condition: `mapping_product.bm_model_id` is resolved via the alias map to `bm_model_id`, which correctly references the `bm_model_id` column added by join 2. Spark can resolve it.

---

## Example 3 — No alias (simple joins without duplicate columns)

If the join source shares no column names with the stage DataFrame, you can omit `alias` entirely.

```yaml
joins:
  - name: currency_join
    type: left
    source: dim_currency
    # no alias — dim_currency has no columns that clash with the stage DF
    conditions:
      - left: currency_code
        right: code
```

No renaming happens. If duplicates exist but no alias is set, Delta-Gen resolves ambiguity by keeping the left-side value and dropping the right-side duplicate after the join.

---

## Example 4 — Multi-condition join

Multiple conditions are AND-ed together.

```yaml
joins:
  - name: fx_rates_join
    type: left
    source: dim_fx_rates
    alias: fx
    conditions:
      - left: currency_code
        right: from_currency
      - left: report_date
        right: effective_date
```

---

## Example 5 — Broadcast hint

Small lookup tables can be broadcast to avoid a shuffle. Set it on the join or on the source.

```yaml
# Option A: on the join itself (overrides source setting)
joins:
  - name: country_join
    type: left
    source: dim_country
    alias: country
    broadcast: true
    conditions:
      - left: country_code
        right: iso_code

# Option B: on the source declaration (applies to all joins against this source)
sources:
  - name: dim_country
    catalog: silver
    schema: reference
    table: dim_country
    broadcast: true
```

---

## Column expressions with alias references

Anywhere you write an `expression`, you can use `alias.column` notation to reference a column from a joined table. Delta-Gen preprocesses these before passing them to Spark's `expr()`.

```yaml
columns:
  # Reading a renamed duplicate (col existed in stage DF and in join source)
  - name: canonical_name
    inputs:
      - expression: "COALESCE(mapping_ifa.name, name)"
      # mapping_ifa.name → mapping_ifa__name  (was a duplicate, got renamed)

  # Reading a non-duplicate column from a join source
  - name: bm_model
    inputs:
      - expression: "master_lookup.bm_model"
      # master_lookup.bm_model → bm_model  (no duplicate, kept as-is)

  # Mixing multiple aliases and raw columns in one expression
  - name: display_label
    inputs:
      - expression: "CONCAT(mapping_ifa.ifa, ' (', master_lookup.bm_model, ')')"
```

---

## Column pruning — how it interacts with aliases

Delta-Gen automatically prunes source columns to only load what's needed (reducing Spark scan cost). The pruning logic understands alias notation:

- `mapping_product.bm_model_id` in a join condition → loads `bm_model_id` from `mapping_product_pmps`
- `mapping_ifa.fca_number` in a column expression → loads `fca_number` from `mapping_ifa_pmps`
- A bare left-side condition column (e.g. `left: model`) comes from the stage DataFrame — no pruning action needed, it's already in memory.

You do **not** need to do anything special — this is fully automatic.

---

## Quick reference

| Scenario | What to write |
|---|---|
| Join on a column already in the stage DF | `left: column_name` (bare) |
| Join on a column added by a preceding join | `left: alias.column_name` |
| Reference a joined column in an expression | `alias.column_name` inside the expression string |
| Avoid duplicate column ambiguity | Set `alias:` on the join |
| Broadcast a small lookup | `broadcast: true` on the join or source |
