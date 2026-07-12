"""Tests for self_join_previous_period flow plugin.

Focus: the ``previous_period_source`` extension that fixes single-period
incremental loads. Without it, a slice containing only ONE period has no
predecessor to self-join against, so every row gets last_month_fum=NULL and
net flow collapses to == fum. With it, the previous period is sourced from an
anchor table (join-side only, never written) so flow is computed correctly.

Spark-dependent: skipped where PySpark is not installed.
"""
from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from deltagen.fabric.plugins.flow_plugins import self_join_previous_period  # noqa: E402
from deltagen.model.stage import StageConfig  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_flow_plugins")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


class _Ctx:
    """Minimal PluginContext stand-in that records log calls."""

    def __init__(self) -> None:
        self.infos: list[str] = []
        self.errors: list[str] = []

    def log_info(self, message: str) -> None:
        self.infos.append(message)

    def log_warning(self, message: str) -> None:  # pragma: no cover - unused here
        pass

    def log_error(self, message: str) -> None:
        self.errors.append(message)


def _rows_by_key(df, period_col="source_period", key="entity"):
    """Collect into {(entity, period): row_dict} for easy assertions."""
    return {(r[key], r[period_col]): r.asDict() for r in df.collect()}


def _stage(**extensions):
    return StageConfig(name="self_join_previous", extensions=extensions)


def test_single_period_slice_with_previous_period_source(spark):
    """The fix: a single loaded period pulls its predecessor from the anchor source."""
    # Anchor source has two periods; loaded slice will be only the latest.
    source = spark.createDataFrame(
        [
            ("A", "202401", 100.0),
            ("B", "202401", 50.0),   # B exists in Jan but not Feb -> lost key
            ("A", "202402", 150.0),
        ],
        ["entity", "source_period", "fum"],
    )
    source.createOrReplaceTempView("anchor_fum")

    # Loaded slice: only Feb (single period -> no in-slice predecessor).
    loaded = spark.createDataFrame(
        [("A", "202402", 150.0)],
        ["entity", "source_period", "fum"],
    )

    ctx = _Ctx()
    out = self_join_previous_period(
        loaded,
        _stage(
            period_column="source_period",
            join_keys=["entity"],
            fum_column="fum",
            previous_period_source="anchor_fum",
        ),
        ctx,
    )
    rows = _rows_by_key(out)

    # A in Feb now carries Jan's 100 -> net flow = 150-100 = 50 (was NULL before the fix).
    assert ("A", "202402") in rows
    assert float(rows[("A", "202402")]["last_month_fum"]) == 100.0
    assert rows[("A", "202402")]["is_lost_key"] is False

    # B was in Jan but not Feb -> emitted as a lost-key row on the loaded period.
    assert ("B", "202402") in rows
    assert float(rows[("B", "202402")]["fum"]) == 0.0
    assert float(rows[("B", "202402")]["last_month_fum"]) == 50.0
    assert rows[("B", "202402")]["is_lost_key"] is True

    # Only the loaded period (202402) is emitted; the anchor's Jan rows are never written.
    assert {p for _, p in rows} == {"202402"}
    assert not ctx.errors


def test_single_period_slice_without_source_is_null(spark):
    """Documents the pre-fix behaviour: no source + single period -> NULL last_month_fum."""
    loaded = spark.createDataFrame(
        [("A", "202402", 150.0)],
        ["entity", "source_period", "fum"],
    )
    ctx = _Ctx()
    out = self_join_previous_period(
        loaded,
        _stage(period_column="source_period", join_keys=["entity"], fum_column="fum"),
        ctx,
    )
    rows = _rows_by_key(out)
    assert rows[("A", "202402")]["last_month_fum"] is None
    assert rows[("A", "202402")]["is_lost_key"] is False


def test_multi_period_self_join_no_source(spark):
    """Baseline self-join across two loaded periods (no external source needed)."""
    loaded = spark.createDataFrame(
        [
            ("A", "202401", 100.0),
            ("B", "202401", 50.0),
            ("A", "202402", 150.0),
        ],
        ["entity", "source_period", "fum"],
    )
    ctx = _Ctx()
    out = self_join_previous_period(
        loaded,
        _stage(period_column="source_period", join_keys=["entity"], fum_column="fum"),
        ctx,
    )
    rows = _rows_by_key(out)

    # Earliest period has no predecessor.
    assert rows[("A", "202401")]["last_month_fum"] is None
    # Feb A carries Jan A's value.
    assert float(rows[("A", "202402")]["last_month_fum"]) == 100.0
    # B dropped in Feb -> lost-key ghost row with prior value.
    assert ("B", "202402") in rows
    assert float(rows[("B", "202402")]["fum"]) == 0.0
    assert rows[("B", "202402")]["is_lost_key"] is True


def test_missing_required_extensions_returns_input(spark):
    """Missing period_column/join_keys logs an error and returns df unchanged."""
    loaded = spark.createDataFrame([("A", "202402", 1.0)], ["entity", "source_period", "fum"])
    ctx = _Ctx()
    out = self_join_previous_period(loaded, _stage(join_keys=["entity"]), ctx)
    assert out is loaded
    assert ctx.errors  # error was logged
