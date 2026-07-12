"""
Demo: Using PlanBuilder with YAML configurations

This example shows how to use PlanBuilder in a notebook-style workflow,
demonstrating both simple and complex transformations.
"""

# =============================================================================
# SIMPLE EXAMPLE: Basic customer dimension
# =============================================================================

def demo_simple_customer(spark):
    """
    Simple example: Load a basic customer dimension config and transform.
    """
    from deltagen.providers import YamlConfigProvider
    from deltagen.runner import PlanBuilder
    from deltagen.model import TableConfig

    # Load the YAML config
    provider = YamlConfigProvider(TableConfig)
    config = provider.load("examples/configs/simple_customer.yaml")

    # Create the builder
    builder = PlanBuilder(config)

    # -------------------------------------------------------------------------
    # Option 1: See what will be built (without executing)
    # -------------------------------------------------------------------------
    print("=" * 60)
    print("PLAN EXPLANATION:")
    print("=" * 60)
    print(builder.explain())

    # -------------------------------------------------------------------------
    # Option 2: View the SQL that will be generated
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("GENERATED SQL:")
    print("=" * 60)

    # Note: to_sql() works best after build() populates the SQL parts
    # For preview, use explain() or build with debug=True

    # -------------------------------------------------------------------------
    # Option 3: Build all stages at once with debug output
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("BUILDING WITH DEBUG:")
    print("=" * 60)
    df = builder.build(spark, debug=True)

    # -------------------------------------------------------------------------
    # Option 4: View the generated SQL after build
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("SQL AFTER BUILD:")
    print("=" * 60)
    print(builder.to_sql())

    return df


# =============================================================================
# COMPLEX EXAMPLE: Multi-stage sales fact with joins
# =============================================================================

def demo_complex_sales(spark):
    """
    Complex example: Multi-stage transformation with joins and lookups.
    Demonstrates stage-by-stage building for debugging.
    """
    from deltagen.providers import YamlConfigProvider
    from deltagen.runner import PlanBuilder
    from deltagen.model import TableConfig

    # Load the YAML config
    provider = YamlConfigProvider(TableConfig)
    config = provider.load("examples/configs/complex_sales_fact.yaml")

    # Create the builder
    builder = PlanBuilder(config)

    # -------------------------------------------------------------------------
    # View the full plan
    # -------------------------------------------------------------------------
    print("=" * 60)
    print("COMPLEX PLAN EXPLANATION:")
    print("=" * 60)
    print(builder.explain())

    # -------------------------------------------------------------------------
    # Stage-by-stage building (useful for debugging)
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STAGE-BY-STAGE BUILD:")
    print("=" * 60)

    # First, load all sources
    sources = builder.load_sources(spark)
    print(f"Loaded sources: {list(sources.keys())}")

    # Build Stage 1: Cast stage
    print("\n--- Stage 1: cast_stage ---")
    df_stage1 = builder.build_stage(
        spark,
        config.stages[0],  # cast_stage
        sources,
        input_df=None,     # First stage, no input
        debug=True
    )

    # Inspect intermediate result
    print(f"\nStage 1 columns: {df_stage1.columns}")
    # display(df_stage1.limit(5))  # Uncomment in notebook

    # View SQL for stage 1
    print(f"\nSQL for cast_stage:")
    print(builder.to_sql("cast_stage"))

    # Build Stage 2: Enrich stage (with joins)
    print("\n--- Stage 2: enrich_stage ---")
    df_stage2 = builder.build_stage(
        spark,
        config.stages[1],  # enrich_stage
        sources,
        input_df=df_stage1,  # Chain from stage 1
        debug=True
    )

    # Inspect final result
    print(f"\nStage 2 columns: {df_stage2.columns}")
    # display(df_stage2.limit(5))  # Uncomment in notebook

    # -------------------------------------------------------------------------
    # Or build everything at once
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("FULL BUILD (ALL STAGES):")
    print("=" * 60)
    df_final = builder.build(spark, debug=True)

    return df_final


# =============================================================================
# NOTEBOOK TEMPLATE PATTERN
# =============================================================================

def notebook_template(spark, config_path: str):
    """
    Template pattern for notebooks.

    Most tables follow this pattern. For custom logic, insert
    your transformations after builder.build() or between stages.
    """
    from deltagen.providers import YamlConfigProvider
    from deltagen.runner import PlanBuilder
    from deltagen.model import TableConfig

    # =========================================================================
    # 1. LOAD CONFIG
    # =========================================================================
    provider = YamlConfigProvider(TableConfig)
    config = provider.load(config_path)
    builder = PlanBuilder(config)

    # =========================================================================
    # 2. BUILD TRANSFORMATION
    # =========================================================================
    df = builder.build(spark, debug=True)

    # =========================================================================
    # 3. CUSTOM LOGIC (optional - add your modifications here)
    # =========================================================================
    # Example: Add a custom calculated column
    # from pyspark.sql import functions as F
    # df = df.withColumn("custom_flag", F.when(F.col("amount") > 1000, "high").otherwise("low"))

    # =========================================================================
    # 4. DATA QUALITY CHECKS (would be in lakehouseutils)
    # =========================================================================
    # log_nulls(df, natural_keys)
    # log_duplicates(df, natural_keys)

    # =========================================================================
    # 5. WRITE (DG-7 Writer - not implemented yet)
    # =========================================================================
    # writer.merge(df, target_table)

    return df


# =============================================================================
# INLINE CONFIG EXAMPLE (no YAML file needed)
# =============================================================================

def demo_inline_config(spark):
    """
    Create config programmatically without YAML file.
    Useful for dynamic table generation.
    """
    from deltagen.runner import PlanBuilder
    from deltagen.model import TableConfig
    from deltagen.model.stage import StageConfig
    from deltagen.model.column import ColumnConfig, ColumnInput
    from deltagen.model.source import SourceConfig

    # Build config in code
    config = TableConfig(
        name="inline_example",
        layer="silver",
        sources=[
            SourceConfig(
                name="raw_data",
                path="/lakehouse/bronze/data",
                format="delta"
            )
        ],
        stages=[
            StageConfig(
                name="transform",
                columns=[
                    ColumnConfig(
                        name="id",
                        data_type="int",
                        natural=True,
                        inputs=[ColumnInput(source="raw_data", column="id")]
                    ),
                    ColumnConfig(
                        name="computed_value",
                        data_type="decimal(18,2)",
                        inputs=[ColumnInput(expression="amount * quantity")]
                    ),
                    ColumnConfig(
                        name="created_at",
                        data_type="timestamp",
                        inputs=[ColumnInput(expression="current_timestamp()")]
                    )
                ],
                # Filters use SQL expression strings - supports full Spark SQL syntax:
                # Simple: "status = 'active'"
                # Complex: "((a = 1 OR b = 2) AND c = 3) OR d = 4"
                # Functions: "YEAR(created_date) = 2024"
                # Multiple filters are combined with AND
                filters=["status = 'active'"]
            )
        ]
    )

    # Use it the same way
    builder = PlanBuilder(config)
    print(builder.explain())

    # Build when ready
    # df = builder.build(spark, debug=True)
    # return df


# =============================================================================
# RUN DEMOS
# =============================================================================

if __name__ == "__main__":
    print("Run these functions in a Spark-enabled notebook:")
    print("  - demo_simple_customer(spark)")
    print("  - demo_complex_sales(spark)")
    print("  - notebook_template(spark, 'path/to/config.yaml')")
    print("  - demo_inline_config(spark)")

    # Demo the explain() without Spark
    print("\n" + "=" * 60)
    print("DEMO: Inline config explanation (no Spark needed)")
    print("=" * 60)
    demo_inline_config(None)
