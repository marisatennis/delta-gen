# Merge Strategies Guide

This guide explains the different merge strategies available in DeltaGen's Writer module and when to use each one.

## Overview

When using `load_mode: merge`, you can specify a `merge_strategy` to control how records are matched and updated. Each strategy is designed for specific data warehouse patterns.

| Strategy | Pattern | Updates Existing? | Tracks History? | Use Case |
|----------|---------|-------------------|-----------------|----------|
| `update_all` | Type 1 SCD | Yes (all columns) | No | Simple dimensions |
| `update_changed` | Type 1 SCD + CDC | Yes (if changed) | No | Change tracking |
| `insert_only` | Deduplication | No | No | Transaction facts |
| `scd_type2` | Type 2 SCD | Expires old | Yes | Historical dimensions |
| `accumulating` | Accumulating Snapshot | Yes (milestones) | No | Order fulfillment |
| `soft_delete` | Soft Delete | Yes + delete flag | No | Audit trails |

---

## Strategy Details

### 1. `update_all` (Default)

**Type 1 SCD - Overwrite on Match**

The simplest strategy. When a record matches by natural key, all columns are overwritten with the new values. New records are inserted.

**When to Use:**
- Dimensions where you only need current values
- Reference data that changes infrequently
- Lookup tables

**Example Config:**
```yaml
name: product_dim
layer: silver
policies:
  optimisation:
    load_mode: merge
    merge_strategy: update_all  # This is the default
stages:
  - name: transform
    columns:
      - name: product_id
        natural: true
      - name: product_name
      - name: category
      - name: price
```

**Behavior:**
```
Source: {product_id: 1, name: "Widget", price: 10.00}
Target: {product_id: 1, name: "Widget", price: 9.00}
Result: {product_id: 1, name: "Widget", price: 10.00}  # Price updated
```

---

### 2. `update_changed`

**Type 1 SCD with Change Detection**

Only updates records when data has actually changed. Useful when you need accurate `last_modified` timestamps or want to reduce write amplification.

**When to Use:**
- Dimensions where you track when records actually changed
- Systems where downstream processes react to modifications
- Reducing unnecessary writes in large tables

**Example Config:**
```yaml
name: customer_dim
layer: silver
policies:
  optimisation:
    load_mode: merge
    merge_strategy: update_changed
    hash_columns: [name, email, address, phone]  # Columns to compare
stages:
  - name: transform
    columns:
      - name: customer_id
        natural: true
      - name: name
      - name: email
      - name: address
      - name: phone
      - name: last_modified
        inputs:
          - expression: "current_timestamp()"
```

**Alternative - Pre-computed Hash:**
```yaml
policies:
  optimisation:
    load_mode: merge
    merge_strategy: update_changed
    # No hash_columns - expects row_hash column in DataFrame
stages:
  - name: transform
    columns:
      - name: customer_id
        natural: true
      - name: name
      - name: email
      - name: row_hash
        temporary: true  # Used for comparison, not persisted
        inputs:
          - expression: "md5(concat(name, '|', email))"
```

**Behavior:**
```
Source: {id: 1, name: "John", email: "john@new.com"}
Target: {id: 1, name: "John", email: "john@old.com"}
Result: Updated (email changed)

Source: {id: 1, name: "John", email: "john@old.com"}
Target: {id: 1, name: "John", email: "john@old.com"}
Result: No update (no change detected)
```

---

### 3. `insert_only`

**Deduplication - Never Update Existing**

Only inserts records that don't already exist. Existing records are never modified.

**When to Use:**
- Transaction/event fact tables (immutable events)
- Preventing duplicate ingestion
- Append-only data with potential re-delivery

**Example Config:**
```yaml
name: order_transactions
layer: gold
policies:
  optimisation:
    load_mode: merge
    merge_strategy: insert_only
stages:
  - name: transform
    columns:
      - name: transaction_id
        natural: true
      - name: order_id
      - name: amount
      - name: transaction_date
```

**Behavior:**
```
Source: {txn_id: 100, amount: 50.00}
Target: {txn_id: 100, amount: 50.00}  # Already exists
Result: No change (record skipped)

Source: {txn_id: 101, amount: 75.00}
Target: (not found)
Result: Inserted
```

---

### 4. `scd_type2`

**Type 2 SCD - Full History Tracking**

Maintains complete history of changes. When a record changes, the current version is "expired" (end_date set, is_current=false) and a new version is inserted.

**When to Use:**
- Customer dimensions (track address history)
- Product dimensions (track price history)
- Any dimension where you need point-in-time queries
- Regulatory/compliance requirements for history

**Required Columns:**
- `effective_date_col` (default: `effective_date`) - When record became active
- `end_date_col` (default: `end_date`) - When record was superseded (null if current)
- `current_flag_col` (default: `is_current`) - Boolean flag for current record

**How It Works:**
The source DataFrame must have `is_current=true` and appropriate `effective_date` values set by the transformation layer. The writer will:
1. Compare source records against only current target records (`is_current=true`)
2. Expire (set end_date, is_current=false) target records that have changed
3. Insert new versions for changed records (since they no longer match after expiring)
4. Insert completely new records

**Example Config:**
```yaml
name: customer_dim
layer: silver
policies:
  optimisation:
    load_mode: merge
    merge_strategy: scd_type2
    effective_date_col: valid_from
    end_date_col: valid_to
    current_flag_col: is_current
stages:
  - name: transform
    columns:
      - name: customer_id
        natural: true
      - name: name
      - name: address
      - name: valid_from
        inputs:
          - expression: "current_timestamp()"
      - name: valid_to
        inputs:
          - expression: "null"
      - name: is_current
        inputs:
          - expression: "true"
```

**Behavior:**
```
# Initial load
Target: {customer_id: 1, name: "John", address: "123 Main", valid_from: "2024-01-01", valid_to: null, is_current: true}

# After address change
Target:
  {customer_id: 1, name: "John", address: "123 Main", valid_from: "2024-01-01", valid_to: "2024-06-15", is_current: false}
  {customer_id: 1, name: "John", address: "456 Oak", valid_from: "2024-06-15", valid_to: null, is_current: true}
```

**Querying Historical Data:**
```sql
-- Current state
SELECT * FROM customer_dim WHERE is_current = true

-- Point-in-time (what was the address on March 1st?)
SELECT * FROM customer_dim
WHERE customer_id = 1
  AND valid_from <= '2024-03-01'
  AND (valid_to > '2024-03-01' OR valid_to IS NULL)
```

---

### 5. `accumulating`

**Accumulating Snapshot - Progressive Updates**

Updates specific "milestone" columns as they become available, preserving existing values. Used for tracking entities through a process with multiple stages.

**When to Use:**
- Order fulfillment tracking (ordered -> shipped -> delivered)
- Loan processing (applied -> approved -> funded -> closed)
- Any multi-stage process where stages complete at different times

**Required Config:**
- `milestone_columns` - List of columns that get progressively populated

**Example Config:**
```yaml
name: order_fulfillment_fact
layer: gold
policies:
  optimisation:
    load_mode: merge
    merge_strategy: accumulating
    milestone_columns:
      - payment_date
      - shipped_date
      - delivered_date
      - returned_date
stages:
  - name: transform
    columns:
      - name: order_id
        natural: true
      - name: customer_id
      - name: order_date
      - name: payment_date      # Milestone 1
      - name: shipped_date      # Milestone 2
      - name: delivered_date    # Milestone 3
      - name: returned_date     # Milestone 4 (optional)
      - name: current_status
```

**Behavior:**
```
# Day 1: Order placed
Target: {order_id: 1, order_date: "Jan 1", payment_date: null, shipped_date: null, delivered_date: null}

# Day 2: Payment received (only payment_date updated)
Source: {order_id: 1, payment_date: "Jan 2", shipped_date: null, ...}
Target: {order_id: 1, order_date: "Jan 1", payment_date: "Jan 2", shipped_date: null, delivered_date: null}

# Day 5: Shipped (only shipped_date updated, payment_date preserved)
Source: {order_id: 1, payment_date: null, shipped_date: "Jan 5", ...}  # Note: payment_date null in source
Target: {order_id: 1, order_date: "Jan 1", payment_date: "Jan 2", shipped_date: "Jan 5", delivered_date: null}
```

---

### 6. `soft_delete`

**Soft Delete - Mark as Deleted**

Instead of physically removing records, marks them with a deleted flag. Maintains referential integrity and audit trails.

**When to Use:**
- Systems with foreign key relationships
- Audit/compliance requirements
- Ability to "undelete" records
- Tracking what was deleted and when

**Required Config:**
- `deleted_flag_col` (default: `is_deleted`) - Boolean column for delete flag
- Source DataFrame must include this column

**Example Config:**
```yaml
name: customer_dim
layer: silver
policies:
  optimisation:
    load_mode: merge
    merge_strategy: soft_delete
    deleted_flag_col: is_deleted
stages:
  - name: transform
    columns:
      - name: customer_id
        natural: true
      - name: name
      - name: email
      - name: is_deleted
        inputs:
          - expression: "CASE WHEN source_status = 'DELETED' THEN true ELSE false END"
      - name: deleted_at
        inputs:
          - expression: "CASE WHEN source_status = 'DELETED' THEN current_timestamp() ELSE null END"
```

**Behavior:**
```
# Customer marked as deleted in source
Source: {customer_id: 1, name: "John", is_deleted: true}
Target: {customer_id: 1, name: "John", is_deleted: false}
Result: {customer_id: 1, name: "John", is_deleted: true}  # Soft deleted

# Querying active customers
SELECT * FROM customer_dim WHERE is_deleted = false
```

---

## Decision Tree

```
Need to track historical changes?
├─ Yes → scd_type2
└─ No
   ├─ Should existing records be updated?
   │  ├─ No → insert_only (deduplication)
   │  └─ Yes
   │     ├─ Is this a multi-stage process?
   │     │  ├─ Yes → accumulating
   │     │  └─ No
   │     │     ├─ Need to track deletions without removing?
   │     │     │  ├─ Yes → soft_delete
   │     │     │  └─ No
   │     │     │     ├─ Need to detect actual changes?
   │     │     │     │  ├─ Yes → update_changed
   │     │     │     │  └─ No → update_all (default)
```

---

## Common Patterns by Table Type

### Dimension Tables

| Dimension Type | Recommended Strategy | Notes |
|----------------|---------------------|-------|
| Type 1 (overwrite) | `update_all` | Simple, no history |
| Type 1 (with audit) | `update_changed` | Track last_modified accurately |
| Type 2 (history) | `scd_type2` | Full change history |
| Type 3 (previous value) | Custom (use Option 2 builder) | Needs custom logic |

### Fact Tables

| Fact Type | Recommended Strategy | Notes |
|-----------|---------------------|-------|
| Transaction | `insert_only` or `append` | Immutable events |
| Periodic Snapshot | `append` | Regular snapshots |
| Accumulating Snapshot | `accumulating` | Multi-stage processes |
| Factless | `insert_only` | Event markers |

---

## Performance Considerations

1. **`update_all`** - Fastest for small-medium tables
2. **`update_changed`** - Reduces write amplification for large tables with few changes
3. **`insert_only`** - Very fast, minimal overhead
4. **`scd_type2`** - More complex, consider partitioning by effective_date
5. **`accumulating`** - Efficient for sparse updates
6. **`soft_delete`** - Consider periodic hard deletes for very old soft-deleted records
