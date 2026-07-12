"""Flow calculation plugins for Delta-Gen.

Stage plugins for computing period-on-period flow using a self-join approach
instead of LAG window functions. This captures both "new" and "lost" rows,
where transfers between entities net to zero.

Usage:
    stages:
      - name: self_join_previous
        extensions:
          stage_plugin: self_join_previous_period
          period_column: source_period
          join_keys: [product, customer, region]
          fum_column: amount
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from deltagen.plugins.registry import register_stage

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@register_stage(
    "self_join_previous_period",
    description="FULL OUTER JOIN each period to its previous period for flow calculation with lost-key detection",
    version="2.1.0",
    author="deltagen_helpers",
    tags={"flow", "netflow"},
)
def self_join_previous_period(
    df: "DataFrame", stage: "StageConfig", ctx: "PluginContext"
) -> "DataFrame":
    """Self-join each period to its previous period using FULL OUTER JOIN.

    Replaces LAG-based flow calculation. The LAG approach misses "lost" rows
    (keys that existed in the previous period but not the current one), which
    inflates flow because transfers between entities only show the positive side.

    Approach:
    1. Collect sorted distinct periods (the loaded slice, optionally unioned with
       an anchor source — see ``previous_period_source``)
    2. Build a period->prev_period mapping (each period paired with the one before it)
    3. Create a "previous" copy, shifting each row's period forward by one step
       (so Jan data gets tagged as "previous for Feb")
    4. FULL OUTER JOIN the original to the shifted copy on (join_keys + period)
    5. Lost rows (in previous but not current) get fum=0, is_lost_key=True
    6. The earliest period has no previous — gets last_month_fum=NULL (correct)

    Single join operation — no looping over periods.

    Args:
        df: Input DataFrame from the aggregate stage.
        stage: Stage configuration with extensions:
            - period_column: Column identifying the period (required)
            - join_keys: List of columns to join on (required)
            - fum_column: Column containing the amount/value (optional, defaults to 'fum')
            - previous_period_source: Table (schema.table) to read the previous
              period's value from when it is not present in the loaded slice (optional).
              Required for incremental loads that process a single period — without it
              a single-period slice has no predecessor and last_month_fum is NULL for
              every row. The prior period feeds the join only; it is never written.
        ctx: Plugin context for logging and metrics.

    Returns:
        DataFrame with all loaded periods enriched with last_month_fum and is_lost_key.

    YAML Config:
        stages:
          - name: self_join_previous
            extensions:
              stage_plugin: self_join_previous_period
              period_column: source_period
              join_keys: [product, customer, region]
              fum_column: amount
              # optional, for single-period incremental loads:
              previous_period_source: silver.consolidated_fum
    """
    from pyspark.sql import functions as F, Row

    extensions = stage.extensions or {}
    period_col = extensions.get("period_column")
    join_keys = extensions.get("join_keys")
    fum_col = extensions.get("fum_column", "fum")

    if not period_col:
        ctx.log_error("self_join_previous_period requires 'period_column' in stage extensions")
        return df
    if not join_keys:
        ctx.log_error("self_join_previous_period requires 'join_keys' in stage extensions")
        return df

    spark = df.sparkSession

    # Where the "previous period" comes from.
    # By default it is taken from the loaded slice itself (df). Under an incremental
    # load the slice often contains only ONE period, so the self-join finds no
    # predecessor and every row gets last_month_fum=NULL -> net flow collapses to
    # == fum. When `previous_period_source` is set, the previous period is read from
    # that table (e.g. the stage's own consolidated source) instead. Only
    # join_keys + fum (+ the optional carry-forward flag) are read, aggregated to
    # the join-key grain; the prior period's rows ONLY feed the join's right side
    # and are NEVER emitted, so the write still touches the loaded period(s) only.
    prev_source_tbl = extensions.get("previous_period_source")
    if prev_source_tbl:
        src = spark.table(prev_source_tbl)
        agg_exprs = [F.sum(F.col(fum_col)).alias(fum_col)]
        if "is_arrears" in src.columns:
            agg_exprs.append(F.max(F.col("is_arrears")).alias("is_arrears"))
        prev_base = src.groupBy(*join_keys, period_col).agg(*agg_exprs)
        ctx.log_info(f"self_join_previous_period: previous period sourced from {prev_source_tbl}")
    else:
        prev_base = df

    # Periods present in the current slice (what we enrich and write).
    loaded_periods = sorted([row[0] for row in df.select(period_col).distinct().collect()])
    # Full period universe (slice ∪ anchor source) used to find each loaded period's predecessor.
    universe = sorted(
        {row[0] for row in prev_base.select(period_col).distinct().collect()} | set(loaded_periods)
    )

    # Map each loaded period to its immediate predecessor in the universe.
    map_rows = [
        Row(prev_period=max(preds), next_period=lp)
        for lp in loaded_periods
        for preds in [[p for p in universe if p < lp]]
        if preds
    ]

    if not map_rows:
        ctx.log_info(
            f"self_join_previous_period: no predecessor period for {loaded_periods} — "
            "emitting NULL last_month_fum (correct for the earliest period only)"
        )
        return (
            df.withColumn("last_month_fum", F.lit(None).cast("decimal"))
            .withColumn("is_lost_key", F.lit(False))
        )

    ctx.log_info(
        f"self_join_previous_period: {len(loaded_periods)} loaded period(s) "
        f"({loaded_periods[0]} to {loaded_periods[-1]}), "
        f"anchors={[r.prev_period for r in map_rows]}, keys={join_keys}"
    )
    period_map_df = spark.createDataFrame(map_rows)

    # Replace NULLs in join keys with a sentinel for the join only.
    # NULL != NULL in SQL joins, so rows whose join keys are NULL (e.g. optional
    # dimensions that are absent for some sources) would never match their
    # previous-period counterpart. NULLs are restored after the join so any
    # downstream sentinel routing that keys off NULL is preserved.
    _SENTINEL = "__NULL__"
    for k in join_keys:
        df = df.withColumn(k, F.coalesce(F.col(k), F.lit(_SENTINEL)))
    if prev_source_tbl:
        for k in join_keys:
            prev_base = prev_base.withColumn(k, F.coalesce(F.col(k), F.lit(_SENTINEL)))
    else:
        prev_base = df  # reuse the (now sentinel-coalesced) slice as its own previous

    # Build the "previous" side: each row tagged with the NEXT period it provides prev FUM for.
    # Carry an optional flag column (``is_arrears``) forward, when present, so
    # lost-key rows can inherit it from the previous period.
    prev_cols = [*join_keys, period_col, F.col(fum_col).alias("last_month_fum")]
    if "is_arrears" in prev_base.columns:
        prev_cols.append(F.col("is_arrears").alias("_prev_is_arrears"))
    prev_proj = prev_base.select(*prev_cols)
    prev_side = (
        prev_proj
        .join(period_map_df, prev_proj[period_col] == period_map_df["prev_period"], how="inner")
        .drop("prev_period", period_col)
        .withColumnRenamed("next_period", period_col)
    )

    # FULL OUTER JOIN: current rows ←→ previous rows on (join_keys + period)
    join_on = join_keys + [period_col]
    joined = df.join(prev_side, on=join_on, how="full_outer")

    # Restore NULLs from sentinel
    for k in join_keys:
        joined = joined.withColumn(
            k, F.when(F.col(k) == _SENTINEL, F.lit(None)).otherwise(F.col(k))
        )

    # For lost rows: current-side columns are NULL. Fill period and fum.
    joined = joined.withColumn(
        fum_col,
        F.coalesce(F.col(fum_col), F.lit(0).cast("decimal")),
    )
    joined = joined.withColumn(
        "is_lost_key",
        F.when(
            (F.col(fum_col) == 0) & F.col("last_month_fum").isNotNull(),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )

    # For lost-key rows the current side is NULL; fill the optional carry-forward
    # flag from the previous side so downstream filters can still see it.
    if "_prev_is_arrears" in joined.columns:
        joined = joined.withColumn(
            "is_arrears",
            F.coalesce(F.col("is_arrears"), F.col("_prev_is_arrears"), F.lit(False)),
        ).drop("_prev_is_arrears")

    ctx.log_info(f"self_join_previous_period: complete")

    return joined
