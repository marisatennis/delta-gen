"""Tests for new features backported from production.

Covers:
- SourceConfig: generated, load_all_columns, incremental, row_count fields
- JoinConfig: dedupe_by, dedupe_order_by, dedupe_order_desc fields
- StageConfig/UnionConfig: UnionSource with column_map
- PoliciesConfig: overwrite and replace_by_partition load modes
- IncrementalConfig: source_period_column
"""
import pytest
from pydantic import ValidationError

from deltagen.model.source import SourceConfig
from deltagen.model.join import JoinConfig
from deltagen.model.stage import StageConfig, UnionSource
from deltagen.model.policies import PoliciesConfig
from deltagen.model.incremental import IncrementalConfig


class TestSourceConfigNewFields:
    """Tests for new SourceConfig fields."""

    def test_generated_default_false(self):
        config = SourceConfig(name="src")
        assert config.generated is False

    def test_generated_source(self):
        config = SourceConfig(name="date_spine", generated=True)
        assert config.generated is True

    def test_generated_with_row_count(self):
        config = SourceConfig(name="date_spine", generated=True, row_count=365)
        assert config.row_count == 365

    def test_row_count_default_one(self):
        config = SourceConfig(name="src")
        assert config.row_count == 1

    def test_load_all_columns_default_false(self):
        config = SourceConfig(name="src")
        assert config.load_all_columns is False

    def test_load_all_columns_true(self):
        config = SourceConfig(name="src", load_all_columns=True)
        assert config.load_all_columns is True

    def test_incremental_default_true(self):
        config = SourceConfig(name="src")
        assert config.incremental is True

    def test_incremental_false(self):
        config = SourceConfig(name="src", incremental=False)
        assert config.incremental is False

    def test_broadcast_field(self):
        config = SourceConfig(name="dim", broadcast=True)
        assert config.broadcast is True


class TestJoinConfigNewFields:
    """Tests for new JoinConfig deduplication fields."""

    def test_dedupe_by_default_none(self):
        config = JoinConfig(
            name="join1",
            source="dim",
            type="left",
            conditions=[{"left": "id", "right": "id"}],
        )
        assert config.dedupe_by is None

    def test_dedupe_by_set(self):
        config = JoinConfig(
            name="join1",
            source="dim",
            type="left",
            conditions=[{"left": "id", "right": "id"}],
            dedupe_by=["customer_id", "effective_date"],
        )
        assert config.dedupe_by == ["customer_id", "effective_date"]

    def test_dedupe_order_by_default_none(self):
        config = JoinConfig(
            name="join1",
            source="dim",
            type="left",
            conditions=[{"left": "id", "right": "id"}],
        )
        assert config.dedupe_order_by is None

    def test_dedupe_order_by_set(self):
        config = JoinConfig(
            name="join1",
            source="dim",
            type="left",
            conditions=[{"left": "id", "right": "id"}],
            dedupe_by=["customer_id"],
            dedupe_order_by="modified_date",
        )
        assert config.dedupe_order_by == "modified_date"

    def test_dedupe_order_desc_default_true(self):
        config = JoinConfig(
            name="join1",
            source="dim",
            type="left",
            conditions=[{"left": "id", "right": "id"}],
        )
        assert config.dedupe_order_desc is True

    def test_dedupe_order_desc_false(self):
        config = JoinConfig(
            name="join1",
            source="dim",
            type="left",
            conditions=[{"left": "id", "right": "id"}],
            dedupe_by=["customer_id"],
            dedupe_order_desc=False,
        )
        assert config.dedupe_order_desc is False


class TestUnionSourceModel:
    """Tests for UnionSource and column_map support."""

    def test_union_source_basic(self):
        src = UnionSource(name="source_a")
        assert src.name == "source_a"
        assert src.column_map is None

    def test_union_source_with_column_map(self):
        src = UnionSource(
            name="source_a",
            column_map={
                "total_amount": "quantity * unit_price",
                "source_system": "'SYSTEM_A'",
            },
        )
        assert src.column_map["total_amount"] == "quantity * unit_price"
        assert src.column_map["source_system"] == "'SYSTEM_A'"

    def test_union_config_string_coercion(self):
        """UnionConfig should accept plain strings and coerce to UnionSource."""
        stage = StageConfig(
            name="combine",

            unions={"mode": "by_name", "sources": ["source_a", "source_b"]},
        )
        assert len(stage.unions.sources) == 2
        assert stage.unions.sources[0].name == "source_a"
        assert stage.unions.sources[1].name == "source_b"

    def test_union_config_dict_sources(self):
        """UnionConfig should accept dict sources with column_map."""
        stage = StageConfig(
            name="combine",

            unions={
                "mode": "by_name",
                "sources": [
                    {"name": "source_a", "column_map": {"amount": "value * 100"}},
                    {"name": "source_b"},
                ],
            },
        )
        assert len(stage.unions.sources) == 2
        assert stage.unions.sources[0].column_map == {"amount": "value * 100"}
        assert stage.unions.sources[1].column_map is None

    def test_union_config_mixed_sources(self):
        """UnionConfig should accept a mix of strings and dicts."""
        stage = StageConfig(
            name="combine",

            unions={
                "mode": "by_name",
                "sources": [
                    "simple_source",
                    {"name": "mapped_source", "column_map": {"x": "y"}},
                ],
            },
        )
        assert stage.unions.sources[0].name == "simple_source"
        assert stage.unions.sources[1].name == "mapped_source"
        assert stage.unions.sources[1].column_map == {"x": "y"}


class TestPoliciesNewLoadModes:
    """Tests for new load modes in PoliciesConfig."""

    def test_overwrite_load_mode(self):
        config = PoliciesConfig(
            optimisation={"load_mode": "overwrite"}
        )
        assert config.optimisation.load_mode == "overwrite"

    def test_replace_by_partition_load_mode(self):
        config = PoliciesConfig(
            optimisation={"load_mode": "replace_by_partition"}
        )
        assert config.optimisation.load_mode == "replace_by_partition"

    def test_merge_load_mode(self):
        config = PoliciesConfig(
            optimisation={"load_mode": "merge"}
        )
        assert config.optimisation.load_mode == "merge"

    def test_append_load_mode(self):
        config = PoliciesConfig(
            optimisation={"load_mode": "append"}
        )
        assert config.optimisation.load_mode == "append"


class TestIncrementalSourcePeriodColumn:
    """Tests for source_period_column in IncrementalConfig."""

    def test_source_period_column_default_none(self):
        config = IncrementalConfig()
        assert config.source_period_column is None

    def test_source_period_column_set(self):
        config = IncrementalConfig(
            filter_mode="watermark",
            watermark_column="modified_date",
            source_period_column="report_month",
        )
        assert config.source_period_column == "report_month"

    def test_source_period_column_with_period_mode(self):
        config = IncrementalConfig(
            filter_mode="period",
            period_column="report_month",
            source_period_column="report_month",
        )
        assert config.source_period_column == "report_month"
