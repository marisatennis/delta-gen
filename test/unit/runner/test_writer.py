"""Unit tests for Writer module.

Tests cover:
- WriterError exception formatting
- drop_temporary_columns helper function
- DeltaWriter._resolve_target for various config patterns
- Append mode calls correct DataFrame methods
- Merge mode requires natural keys
- Merge mode builds correct merge condition
"""
import pytest
from unittest.mock import MagicMock, patch

from deltagen.runner.writer import (
    DeltaWriter,
    Writer,
    drop_temporary_columns,
)
from deltagen.runner.exceptions import WriterError
from deltagen.model import TableConfig
from deltagen.model.stage import StageConfig
from deltagen.model.column import ColumnConfig
from deltagen.model.policies import PoliciesConfig, OptimisationPolicy


class TestWriterError:
    """Tests for WriterError exception class."""

    def test_basic_message(self):
        """Test error with just a message."""
        error = WriterError("Something went wrong")
        msg = str(error)
        assert "Something went wrong" in msg
        assert "WriterError" in msg

    def test_error_with_target(self):
        """Test error message includes target info."""
        error = WriterError("Write failed", target="silver.customer_dim")
        msg = str(error)
        assert "Write failed" in msg
        assert "silver.customer_dim" in msg
        assert "Target: silver.customer_dim" in msg

    def test_error_with_mode(self):
        """Test error message includes mode info."""
        error = WriterError("Write failed", mode="merge")
        msg = str(error)
        assert "Write failed" in msg
        assert "Mode: merge" in msg

    def test_error_with_detail(self):
        """Test error message includes detail."""
        error = WriterError("Write failed", detail="Natural keys required")
        msg = str(error)
        assert "Write failed" in msg
        assert "Natural keys required" in msg

    def test_error_with_target_and_mode(self):
        """Test error with both target and mode."""
        error = WriterError(
            "Failed to write",
            target="gold.fact_sales",
            mode="append"
        )
        msg = str(error)
        assert "Failed to write" in msg
        assert "gold.fact_sales" in msg
        assert "append" in msg

    def test_error_full_context(self):
        """Test error with all fields populated."""
        error = WriterError(
            "Failed to persist",
            target="silver.customer",
            mode="merge",
            detail="No natural key columns defined"
        )
        msg = str(error)
        assert "Failed to persist" in msg
        assert "silver.customer" in msg
        assert "merge" in msg
        assert "No natural key columns defined" in msg

    def test_error_is_exception(self):
        """Test that WriterError is a proper exception."""
        error = WriterError("Test")
        assert isinstance(error, Exception)

    def test_error_can_be_raised(self):
        """Test that error can be raised and caught."""
        with pytest.raises(WriterError) as exc_info:
            raise WriterError("Intentional error", target="test_table")
        assert "Intentional error" in str(exc_info.value)
        assert "test_table" in str(exc_info.value)

    def test_error_attributes(self):
        """Test that error attributes are accessible."""
        error = WriterError(
            "Test",
            target="my_target",
            mode="my_mode",
            detail="my_detail"
        )
        assert error.target == "my_target"
        assert error.mode == "my_mode"
        assert error.detail == "my_detail"


class TestDropTemporaryColumns:
    """Tests for drop_temporary_columns helper function."""

    def test_drops_single_temporary_column(self):
        """Test dropping a single temporary column."""
        mock_df = MagicMock()
        mock_df.columns = ["id", "name", "temp_calc"]
        mock_df.drop.return_value = MagicMock()

        config = TableConfig(
            name="test",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", temporary=False, inputs=[]),
                        ColumnConfig(name="name", temporary=False, inputs=[]),
                        ColumnConfig(name="temp_calc", temporary=True, inputs=[]),
                    ]
                )
            ]
        )

        drop_temporary_columns(mock_df, config)

        mock_df.drop.assert_called_once_with("temp_calc")

    def test_drops_multiple_temporary_columns(self):
        """Test dropping multiple temporary columns."""
        mock_df = MagicMock()
        mock_df.columns = ["id", "temp1", "name", "temp2"]
        mock_df.drop.return_value = MagicMock()

        config = TableConfig(
            name="test",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", temporary=False, inputs=[]),
                        ColumnConfig(name="temp1", temporary=True, inputs=[]),
                        ColumnConfig(name="name", temporary=False, inputs=[]),
                        ColumnConfig(name="temp2", temporary=True, inputs=[]),
                    ]
                )
            ]
        )

        drop_temporary_columns(mock_df, config)

        mock_df.drop.assert_called_once_with("temp1", "temp2")

    def test_no_temporary_columns_returns_original_df(self):
        """Test that DataFrame is returned unchanged if no temp columns."""
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]

        config = TableConfig(
            name="test",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", temporary=False, inputs=[]),
                        ColumnConfig(name="name", temporary=False, inputs=[]),
                    ]
                )
            ]
        )

        result = drop_temporary_columns(mock_df, config)

        assert result is mock_df
        mock_df.drop.assert_not_called()

    def test_skips_nonexistent_temporary_columns(self):
        """Test that temp columns not in DataFrame are skipped."""
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]  # No temp_calc column
        mock_df.drop.return_value = MagicMock()

        config = TableConfig(
            name="test",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", temporary=False, inputs=[]),
                        ColumnConfig(name="name", temporary=False, inputs=[]),
                        ColumnConfig(name="temp_calc", temporary=True, inputs=[]),
                    ]
                )
            ]
        )

        result = drop_temporary_columns(mock_df, config)

        # Should return original df since temp_calc isn't in columns
        assert result is mock_df
        mock_df.drop.assert_not_called()

    def test_empty_stages_returns_original_df(self):
        """Test with no stages defined."""
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]

        config = TableConfig(name="test", stages=[])

        result = drop_temporary_columns(mock_df, config)

        assert result is mock_df


class TestDeltaWriterResolveTarget:
    """Tests for DeltaWriter._resolve_target method."""

    def test_resolve_with_target_schema_and_name(self):
        """Test target resolution with target_schema prefix."""
        writer = DeltaWriter()
        config = TableConfig(name="customer_dim", target_schema="silver")

        target = writer._resolve_target(config)

        assert target == "silver.customer_dim"

    def test_resolve_with_name_only(self):
        """Test target resolution without target_schema."""
        writer = DeltaWriter()
        config = TableConfig(name="customer_dim")

        target = writer._resolve_target(config)

        assert target == "customer_dim"

    def test_resolve_with_sharepoint_target_schema(self):
        """Test target resolution with sharepoint target_schema."""
        writer = DeltaWriter()
        config = TableConfig(name="fact_sales", target_schema="sharepoint")

        target = writer._resolve_target(config)

        assert target == "sharepoint.fact_sales"

    def test_resolve_with_layer_does_not_affect_target(self):
        """Test that layer is semantic only and does not affect target path."""
        writer = DeltaWriter()
        config = TableConfig(name="raw_orders", layer="bronze")

        target = writer._resolve_target(config)

        # Layer is semantic, so target should be just the name
        assert target == "raw_orders"

    def test_resolve_with_both_layer_and_target_schema(self):
        """Test that target_schema is used for target when both layer and target_schema are set."""
        writer = DeltaWriter()
        config = TableConfig(name="customer_dim", layer="silver", target_schema="sharepoint")

        target = writer._resolve_target(config)

        # target_schema is used for physical path, layer is semantic only
        assert target == "sharepoint.customer_dim"


class TestDeltaWriterAppendMode:
    """Tests for DeltaWriter append mode."""

    def test_append_mode_calls_saveAsTable(self):
        """Test that append mode calls saveAsTable with correct mode."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]
        mock_writer = MagicMock()
        mock_df.write.format.return_value.mode.return_value = mock_writer

        config = TableConfig(
            name="customer",
            target_schema="silver",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", inputs=[]),
                        ColumnConfig(name="name", inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(load_mode="append")
            )
        )

        writer = DeltaWriter()
        writer.write(mock_spark, mock_df, config)

        mock_df.write.format.assert_called_with("delta")
        mock_df.write.format().mode.assert_called_with("append")
        mock_writer.saveAsTable.assert_called_with("silver.customer")

    def test_append_mode_with_partition_scheme(self):
        """Test append mode applies partition scheme."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.columns = ["id", "name", "date_key"]
        mock_writer = MagicMock()
        mock_partitioned_writer = MagicMock()
        mock_df.write.format.return_value.mode.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_partitioned_writer

        config = TableConfig(
            name="orders",
            target_schema="silver",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", inputs=[]),
                        ColumnConfig(name="name", inputs=[]),
                        ColumnConfig(name="date_key", inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(
                    load_mode="append",
                    partition_scheme="date_key"
                )
            )
        )

        writer = DeltaWriter()
        writer.write(mock_spark, mock_df, config)

        mock_writer.partitionBy.assert_called_with("date_key")
        mock_partitioned_writer.saveAsTable.assert_called_with("silver.orders")

    def test_append_mode_with_multiple_partition_columns(self):
        """Test append mode with comma-separated partition columns."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.columns = ["id", "year", "month"]
        mock_writer = MagicMock()
        mock_partitioned_writer = MagicMock()
        mock_df.write.format.return_value.mode.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_partitioned_writer

        config = TableConfig(
            name="events",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", inputs=[]),
                        ColumnConfig(name="year", inputs=[]),
                        ColumnConfig(name="month", inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(
                    load_mode="append",
                    partition_scheme="year, month"
                )
            )
        )

        writer = DeltaWriter()
        writer.write(mock_spark, mock_df, config)

        mock_writer.partitionBy.assert_called_with("year", "month")

    def test_append_mode_drops_temporary_columns(self):
        """Test that temporary columns are dropped before append."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_clean_df = MagicMock()
        mock_df.columns = ["id", "name", "temp_val"]
        mock_df.drop.return_value = mock_clean_df
        mock_writer = MagicMock()
        mock_clean_df.write.format.return_value.mode.return_value = mock_writer

        config = TableConfig(
            name="customer",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", inputs=[]),
                        ColumnConfig(name="name", inputs=[]),
                        ColumnConfig(name="temp_val", temporary=True, inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(load_mode="append")
            )
        )

        writer = DeltaWriter()
        writer.write(mock_spark, mock_df, config)

        mock_df.drop.assert_called_once_with("temp_val")
        mock_clean_df.write.format.assert_called_with("delta")


class TestDeltaWriterMergeMode:
    """Tests for DeltaWriter merge mode."""

    def test_merge_mode_requires_natural_keys(self):
        """Test that merge mode raises error without natural keys."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]

        config = TableConfig(
            name="customer",
            target_schema="silver",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", natural=False, inputs=[]),
                        ColumnConfig(name="name", natural=False, inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(load_mode="merge")
            )
        )

        writer = DeltaWriter()

        # Mock the delta.tables module since it's not installed
        mock_delta_tables = MagicMock()
        with patch.dict("sys.modules", {"delta": MagicMock(), "delta.tables": mock_delta_tables}):
            with pytest.raises(WriterError) as exc_info:
                writer.write(mock_spark, mock_df, config)

        assert "natural key" in str(exc_info.value).lower()
        assert "merge" in str(exc_info.value).lower()

    def test_merge_mode_builds_correct_condition_single_key(self):
        """Test merge condition with single natural key."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]
        mock_df.alias.return_value = mock_df

        mock_delta_table = MagicMock()
        mock_merge_builder = MagicMock()
        mock_delta_table.alias.return_value.merge.return_value = mock_merge_builder
        mock_merge_builder.whenMatchedUpdateAll.return_value = mock_merge_builder
        mock_merge_builder.whenNotMatchedInsertAll.return_value = mock_merge_builder

        mock_delta_tables = MagicMock()
        mock_delta_tables.DeltaTable.forName.return_value = mock_delta_table

        config = TableConfig(
            name="customer",
            target_schema="silver",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", natural=True, inputs=[]),
                        ColumnConfig(name="name", natural=False, inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(load_mode="merge")
            )
        )

        writer = DeltaWriter()

        with patch.dict("sys.modules", {"delta": MagicMock(), "delta.tables": mock_delta_tables}):
            writer.write(mock_spark, mock_df, config)

        # Verify merge was called with correct condition
        mock_delta_table.alias.assert_called_with("target")
        mock_delta_table.alias().merge.assert_called()
        call_args = mock_delta_table.alias().merge.call_args
        merge_condition = call_args[0][1]  # Second positional arg is the condition
        assert "target.id = source.id" in merge_condition

    def test_merge_mode_builds_correct_condition_composite_key(self):
        """Test merge condition with composite natural key."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        mock_df = MagicMock()
        mock_df.columns = ["customer_id", "product_id", "quantity"]
        mock_df.alias.return_value = mock_df

        mock_delta_table = MagicMock()
        mock_merge_builder = MagicMock()
        mock_delta_table.alias.return_value.merge.return_value = mock_merge_builder
        mock_merge_builder.whenMatchedUpdateAll.return_value = mock_merge_builder
        mock_merge_builder.whenNotMatchedInsertAll.return_value = mock_merge_builder

        mock_delta_tables = MagicMock()
        mock_delta_tables.DeltaTable.forName.return_value = mock_delta_table

        config = TableConfig(
            name="order_line",
            target_schema="silver",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="customer_id", natural=True, inputs=[]),
                        ColumnConfig(name="product_id", natural=True, inputs=[]),
                        ColumnConfig(name="quantity", natural=False, inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(load_mode="merge")
            )
        )

        writer = DeltaWriter()

        with patch.dict("sys.modules", {"delta": MagicMock(), "delta.tables": mock_delta_tables}):
            writer.write(mock_spark, mock_df, config)

        call_args = mock_delta_table.alias().merge.call_args
        merge_condition = call_args[0][1]
        assert "target.customer_id = source.customer_id" in merge_condition
        assert "target.product_id = source.product_id" in merge_condition
        assert " AND " in merge_condition

    def test_merge_mode_calls_whenMatchedUpdateAll(self):
        """Test that merge calls whenMatchedUpdateAll."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]
        mock_df.alias.return_value = mock_df

        mock_delta_table = MagicMock()
        mock_merge_builder = MagicMock()
        mock_delta_table.alias.return_value.merge.return_value = mock_merge_builder
        mock_merge_builder.whenMatchedUpdateAll.return_value = mock_merge_builder
        mock_merge_builder.whenNotMatchedInsertAll.return_value = mock_merge_builder

        mock_delta_tables = MagicMock()
        mock_delta_tables.DeltaTable.forName.return_value = mock_delta_table

        config = TableConfig(
            name="customer",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", natural=True, inputs=[]),
                        ColumnConfig(name="name", inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(load_mode="merge")
            )
        )

        writer = DeltaWriter()

        with patch.dict("sys.modules", {"delta": MagicMock(), "delta.tables": mock_delta_tables}):
            writer.write(mock_spark, mock_df, config)

        mock_merge_builder.whenMatchedUpdateAll.assert_called_once()

    def test_merge_mode_calls_whenNotMatchedInsertAll(self):
        """Test that merge calls whenNotMatchedInsertAll."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]
        mock_df.alias.return_value = mock_df

        mock_delta_table = MagicMock()
        mock_merge_builder = MagicMock()
        mock_delta_table.alias.return_value.merge.return_value = mock_merge_builder
        mock_merge_builder.whenMatchedUpdateAll.return_value = mock_merge_builder
        mock_merge_builder.whenNotMatchedInsertAll.return_value = mock_merge_builder

        mock_delta_tables = MagicMock()
        mock_delta_tables.DeltaTable.forName.return_value = mock_delta_table

        config = TableConfig(
            name="customer",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", natural=True, inputs=[]),
                        ColumnConfig(name="name", inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(load_mode="merge")
            )
        )

        writer = DeltaWriter()

        with patch.dict("sys.modules", {"delta": MagicMock(), "delta.tables": mock_delta_tables}):
            writer.write(mock_spark, mock_df, config)

        mock_merge_builder.whenNotMatchedInsertAll.assert_called_once()

    def test_merge_mode_calls_execute(self):
        """Test that merge calls execute to run the merge."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]
        mock_df.alias.return_value = mock_df

        mock_delta_table = MagicMock()
        mock_merge_builder = MagicMock()
        mock_delta_table.alias.return_value.merge.return_value = mock_merge_builder
        mock_merge_builder.whenMatchedUpdateAll.return_value = mock_merge_builder
        mock_merge_builder.whenNotMatchedInsertAll.return_value = mock_merge_builder

        mock_delta_tables = MagicMock()
        mock_delta_tables.DeltaTable.forName.return_value = mock_delta_table

        config = TableConfig(
            name="customer",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", natural=True, inputs=[]),
                        ColumnConfig(name="name", inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(load_mode="merge")
            )
        )

        writer = DeltaWriter()

        with patch.dict("sys.modules", {"delta": MagicMock(), "delta.tables": mock_delta_tables}):
            writer.write(mock_spark, mock_df, config)

        mock_merge_builder.execute.assert_called_once()

    def test_merge_mode_creates_table_if_not_exists(self):
        """Test that merge mode creates table on first write."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = False
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]
        mock_writer = MagicMock()
        mock_df.write.format.return_value.mode.return_value = mock_writer

        mock_delta_tables = MagicMock()

        config = TableConfig(
            name="customer",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", natural=True, inputs=[]),
                        ColumnConfig(name="name", inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(load_mode="merge")
            )
        )

        writer = DeltaWriter()

        with patch.dict("sys.modules", {"delta": MagicMock(), "delta.tables": mock_delta_tables}):
            writer.write(mock_spark, mock_df, config)

        # Should fall back to append for initial creation
        mock_df.write.format.assert_called_with("delta")
        mock_writer.saveAsTable.assert_called_with("customer")


class TestDeltaWriterProtocol:
    """Tests for Writer protocol compliance."""

    def test_delta_writer_implements_protocol(self):
        """Test that DeltaWriter implements Writer protocol."""
        writer = DeltaWriter()
        assert isinstance(writer, Writer)

    def test_protocol_has_write_method(self):
        """Test that protocol defines write method."""
        assert hasattr(Writer, "write")


class TestDeltaWriterDebugOutput:
    """Tests for debug output in DeltaWriter."""

    def test_debug_prints_target_info(self, capsys):
        """Test that debug=True prints target information."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]
        mock_writer = MagicMock()
        mock_df.write.format.return_value.mode.return_value = mock_writer

        config = TableConfig(
            name="customer",
            target_schema="silver",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", inputs=[]),
                        ColumnConfig(name="name", inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(load_mode="append")
            )
        )

        writer = DeltaWriter()
        writer.write(mock_spark, mock_df, config, debug=True)

        captured = capsys.readouterr()
        assert "silver.customer" in captured.out
        assert "append" in captured.out

    def test_debug_prints_column_info(self, capsys):
        """Test that debug=True prints column information."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_clean_df = MagicMock()
        mock_df.columns = ["id", "name", "temp_val"]
        mock_clean_df.columns = ["id", "name"]
        mock_df.drop.return_value = mock_clean_df
        mock_writer = MagicMock()
        mock_clean_df.write.format.return_value.mode.return_value = mock_writer

        config = TableConfig(
            name="customer",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", inputs=[]),
                        ColumnConfig(name="name", inputs=[]),
                        ColumnConfig(name="temp_val", temporary=True, inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(load_mode="append")
            )
        )

        writer = DeltaWriter()
        writer.write(mock_spark, mock_df, config, debug=True)

        captured = capsys.readouterr()
        assert "temp_val" in captured.out


class TestDeltaWriterErrorHandling:
    """Tests for error handling in DeltaWriter."""

    def test_delta_library_import_error_is_wrapped(self):
        """Test that missing delta library raises helpful WriterError."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]

        config = TableConfig(
            name="customer",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", natural=True, inputs=[]),
                        ColumnConfig(name="name", inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(load_mode="merge")
            )
        )

        writer = DeltaWriter()

        # Simulate ImportError by not mocking delta.tables
        # The module should raise WriterError when delta is not available
        import sys
        # Remove delta from sys.modules if it exists
        delta_modules = [k for k in sys.modules.keys() if k.startswith("delta")]
        for mod in delta_modules:
            sys.modules.pop(mod, None)

        with pytest.raises(WriterError) as exc_info:
            writer.write(mock_spark, mock_df, config)

        assert "Delta Lake library" in str(exc_info.value)
        assert "merge" in str(exc_info.value)


class TestMergeStrategies:
    """Tests for different merge strategies."""

    def test_update_all_strategy_calls_correct_methods(self):
        """Test update_all strategy calls whenMatchedUpdateAll and whenNotMatchedInsertAll."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]
        mock_df.alias.return_value = mock_df

        mock_delta_table = MagicMock()
        mock_merge_builder = MagicMock()
        mock_delta_table.alias.return_value.merge.return_value = mock_merge_builder
        mock_merge_builder.whenMatchedUpdateAll.return_value = mock_merge_builder
        mock_merge_builder.whenNotMatchedInsertAll.return_value = mock_merge_builder

        mock_delta_tables = MagicMock()
        mock_delta_tables.DeltaTable.forName.return_value = mock_delta_table

        config = TableConfig(
            name="customer",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", natural=True, inputs=[]),
                        ColumnConfig(name="name", inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(
                    load_mode="merge",
                    merge_strategy="update_all"
                )
            )
        )

        writer = DeltaWriter()

        with patch.dict("sys.modules", {"delta": MagicMock(), "delta.tables": mock_delta_tables}):
            writer.write(mock_spark, mock_df, config)

        mock_merge_builder.whenMatchedUpdateAll.assert_called_once()
        mock_merge_builder.whenNotMatchedInsertAll.assert_called_once()

    def test_insert_only_strategy_skips_update(self):
        """Test insert_only strategy only calls whenNotMatchedInsertAll."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]
        mock_df.alias.return_value = mock_df

        mock_delta_table = MagicMock()
        mock_merge_builder = MagicMock()
        mock_delta_table.alias.return_value.merge.return_value = mock_merge_builder
        mock_merge_builder.whenNotMatchedInsertAll.return_value = mock_merge_builder

        mock_delta_tables = MagicMock()
        mock_delta_tables.DeltaTable.forName.return_value = mock_delta_table

        config = TableConfig(
            name="transactions",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", natural=True, inputs=[]),
                        ColumnConfig(name="name", inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(
                    load_mode="merge",
                    merge_strategy="insert_only"
                )
            )
        )

        writer = DeltaWriter()

        with patch.dict("sys.modules", {"delta": MagicMock(), "delta.tables": mock_delta_tables}):
            writer.write(mock_spark, mock_df, config)

        # insert_only should NOT call whenMatchedUpdateAll
        mock_merge_builder.whenMatchedUpdateAll.assert_not_called()
        mock_merge_builder.whenNotMatchedInsertAll.assert_called_once()

    def test_update_changed_strategy_with_hash_columns(self):
        """Test update_changed strategy uses hash_columns for change detection."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        mock_df = MagicMock()
        mock_df.columns = ["id", "name", "email"]
        mock_df.alias.return_value = mock_df

        mock_delta_table = MagicMock()
        mock_merge_builder = MagicMock()
        mock_delta_table.alias.return_value.merge.return_value = mock_merge_builder
        mock_merge_builder.whenMatchedUpdate.return_value = mock_merge_builder
        mock_merge_builder.whenNotMatchedInsertAll.return_value = mock_merge_builder

        mock_delta_tables = MagicMock()
        mock_delta_tables.DeltaTable.forName.return_value = mock_delta_table

        config = TableConfig(
            name="customer",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", natural=True, inputs=[]),
                        ColumnConfig(name="name", inputs=[]),
                        ColumnConfig(name="email", inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(
                    load_mode="merge",
                    merge_strategy="update_changed",
                    hash_columns=["name", "email"]
                )
            )
        )

        writer = DeltaWriter()

        with patch.dict("sys.modules", {"delta": MagicMock(), "delta.tables": mock_delta_tables}):
            writer.write(mock_spark, mock_df, config)

        # update_changed uses whenMatchedUpdate with condition
        mock_merge_builder.whenMatchedUpdate.assert_called_once()
        call_kwargs = mock_merge_builder.whenMatchedUpdate.call_args[1]
        assert "condition" in call_kwargs
        assert "md5" in call_kwargs["condition"]

    def test_accumulating_strategy_requires_milestone_columns(self):
        """Test accumulating strategy raises error without milestone_columns."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        mock_df = MagicMock()
        mock_df.columns = ["id", "status"]

        mock_delta_table = MagicMock()
        mock_delta_tables = MagicMock()
        mock_delta_tables.DeltaTable.forName.return_value = mock_delta_table

        config = TableConfig(
            name="orders",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", natural=True, inputs=[]),
                        ColumnConfig(name="status", inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(
                    load_mode="merge",
                    merge_strategy="accumulating"
                    # milestone_columns not set
                )
            )
        )

        writer = DeltaWriter()

        with patch.dict("sys.modules", {"delta": MagicMock(), "delta.tables": mock_delta_tables}):
            with pytest.raises(WriterError) as exc_info:
                writer.write(mock_spark, mock_df, config)

        assert "milestone_columns" in str(exc_info.value)

    def test_accumulating_strategy_with_milestone_columns(self):
        """Test accumulating strategy uses COALESCE for milestone columns."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        mock_df = MagicMock()
        mock_df.columns = ["order_id", "shipped_date", "delivered_date"]
        mock_df.alias.return_value = mock_df

        mock_delta_table = MagicMock()
        mock_merge_builder = MagicMock()
        mock_delta_table.alias.return_value.merge.return_value = mock_merge_builder
        mock_merge_builder.whenMatchedUpdate.return_value = mock_merge_builder
        mock_merge_builder.whenNotMatchedInsertAll.return_value = mock_merge_builder

        mock_delta_tables = MagicMock()
        mock_delta_tables.DeltaTable.forName.return_value = mock_delta_table

        config = TableConfig(
            name="order_fulfillment",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="order_id", natural=True, inputs=[]),
                        ColumnConfig(name="shipped_date", inputs=[]),
                        ColumnConfig(name="delivered_date", inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(
                    load_mode="merge",
                    merge_strategy="accumulating",
                    milestone_columns=["shipped_date", "delivered_date"]
                )
            )
        )

        writer = DeltaWriter()

        with patch.dict("sys.modules", {"delta": MagicMock(), "delta.tables": mock_delta_tables}):
            writer.write(mock_spark, mock_df, config)

        mock_merge_builder.whenMatchedUpdate.assert_called_once()
        call_kwargs = mock_merge_builder.whenMatchedUpdate.call_args[1]
        assert "set" in call_kwargs
        update_set = call_kwargs["set"]
        # Milestone columns should use COALESCE
        assert "COALESCE" in update_set["shipped_date"]
        assert "COALESCE" in update_set["delivered_date"]

    def test_soft_delete_strategy_requires_deleted_column(self):
        """Test soft_delete strategy raises error if deleted column missing from DataFrame."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]  # Missing is_deleted column

        mock_delta_table = MagicMock()
        mock_delta_tables = MagicMock()
        mock_delta_tables.DeltaTable.forName.return_value = mock_delta_table

        config = TableConfig(
            name="customer",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", natural=True, inputs=[]),
                        ColumnConfig(name="name", inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(
                    load_mode="merge",
                    merge_strategy="soft_delete"
                )
            )
        )

        writer = DeltaWriter()

        with patch.dict("sys.modules", {"delta": MagicMock(), "delta.tables": mock_delta_tables}):
            with pytest.raises(WriterError) as exc_info:
                writer.write(mock_spark, mock_df, config)

        assert "is_deleted" in str(exc_info.value)

    def test_soft_delete_strategy_with_deleted_column(self):
        """Test soft_delete strategy works when deleted column is present."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        mock_df = MagicMock()
        mock_df.columns = ["id", "name", "is_deleted"]
        mock_df.alias.return_value = mock_df

        mock_delta_table = MagicMock()
        mock_merge_builder = MagicMock()
        mock_delta_table.alias.return_value.merge.return_value = mock_merge_builder
        mock_merge_builder.whenMatchedUpdateAll.return_value = mock_merge_builder
        mock_merge_builder.whenNotMatchedInsertAll.return_value = mock_merge_builder

        mock_delta_tables = MagicMock()
        mock_delta_tables.DeltaTable.forName.return_value = mock_delta_table

        config = TableConfig(
            name="customer",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", natural=True, inputs=[]),
                        ColumnConfig(name="name", inputs=[]),
                        ColumnConfig(name="is_deleted", inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(
                    load_mode="merge",
                    merge_strategy="soft_delete"
                )
            )
        )

        writer = DeltaWriter()

        with patch.dict("sys.modules", {"delta": MagicMock(), "delta.tables": mock_delta_tables}):
            writer.write(mock_spark, mock_df, config)

        mock_merge_builder.whenMatchedUpdateAll.assert_called_once()
        mock_merge_builder.whenNotMatchedInsertAll.assert_called_once()

    def test_scd_type2_strategy_uses_configured_columns(self):
        """Test scd_type2 strategy uses configured effective/end/current columns."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        mock_df = MagicMock()
        mock_df.columns = ["customer_id", "name", "valid_from", "valid_to", "is_current"]
        mock_df.alias.return_value = mock_df

        mock_delta_table = MagicMock()
        mock_merge_builder = MagicMock()
        mock_delta_table.alias.return_value.merge.return_value = mock_merge_builder
        mock_merge_builder.whenMatchedUpdate.return_value = mock_merge_builder
        mock_merge_builder.whenNotMatchedInsertAll.return_value = mock_merge_builder

        mock_delta_tables = MagicMock()
        mock_delta_tables.DeltaTable.forName.return_value = mock_delta_table

        # Mock pyspark.sql.functions
        mock_pyspark = MagicMock()
        mock_pyspark_sql = MagicMock()

        config = TableConfig(
            name="customer_history",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="customer_id", natural=True, inputs=[]),
                        ColumnConfig(name="name", inputs=[]),
                        ColumnConfig(name="valid_from", inputs=[]),
                        ColumnConfig(name="valid_to", inputs=[]),
                        ColumnConfig(name="is_current", inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(
                    load_mode="merge",
                    merge_strategy="scd_type2",
                    effective_date_col="valid_from",
                    end_date_col="valid_to",
                    current_flag_col="is_current"
                )
            )
        )

        writer = DeltaWriter()

        with patch.dict("sys.modules", {
            "delta": MagicMock(),
            "delta.tables": mock_delta_tables,
            "pyspark": mock_pyspark,
            "pyspark.sql": mock_pyspark_sql,
            "pyspark.sql.functions": MagicMock()
        }):
            writer.write(mock_spark, mock_df, config)

        # SCD Type 2 should call whenMatchedUpdate to expire records
        mock_merge_builder.whenMatchedUpdate.assert_called_once()
        call_kwargs = mock_merge_builder.whenMatchedUpdate.call_args[1]
        assert "set" in call_kwargs
        update_set = call_kwargs["set"]
        # Should set end date and current flag
        assert "valid_to" in update_set
        assert "is_current" in update_set

    def test_debug_output_shows_strategy(self, capsys):
        """Test that debug=True shows the merge strategy."""
        mock_spark = MagicMock()
        mock_spark.catalog.tableExists.return_value = True
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]
        mock_df.alias.return_value = mock_df

        mock_delta_table = MagicMock()
        mock_merge_builder = MagicMock()
        mock_delta_table.alias.return_value.merge.return_value = mock_merge_builder
        mock_merge_builder.whenMatchedUpdateAll.return_value = mock_merge_builder
        mock_merge_builder.whenNotMatchedInsertAll.return_value = mock_merge_builder

        mock_delta_tables = MagicMock()
        mock_delta_tables.DeltaTable.forName.return_value = mock_delta_table

        config = TableConfig(
            name="customer",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", natural=True, inputs=[]),
                        ColumnConfig(name="name", inputs=[]),
                    ]
                )
            ],
            policies=PoliciesConfig(
                optimisation=OptimisationPolicy(
                    load_mode="merge",
                    merge_strategy="update_all"
                )
            )
        )

        writer = DeltaWriter()

        with patch.dict("sys.modules", {"delta": MagicMock(), "delta.tables": mock_delta_tables}):
            writer.write(mock_spark, mock_df, config, debug=True)

        captured = capsys.readouterr()
        assert "update_all" in captured.out
        assert "Merge strategy" in captured.out
