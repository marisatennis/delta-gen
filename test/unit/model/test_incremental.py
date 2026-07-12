"""Unit tests for IncrementalConfig model."""
import pytest
from pydantic import ValidationError

from deltagen.model.incremental import IncrementalConfig, SourceFilterMode


class TestIncrementalConfig:
    """Tests for IncrementalConfig validation and defaults."""

    def test_defaults(self):
        config = IncrementalConfig()
        assert config.filter_mode == SourceFilterMode.NONE
        assert config.is_enabled is False

    def test_period_mode_requires_period_column(self):
        with pytest.raises(ValidationError, match="period_column"):
            IncrementalConfig(filter_mode="period")

    def test_watermark_mode_requires_watermark_column(self):
        with pytest.raises(ValidationError, match="watermark_column"):
            IncrementalConfig(filter_mode="watermark")

    def test_period_mode_valid(self):
        config = IncrementalConfig(
            filter_mode="period",
            period_column="report_date",
        )
        assert config.filter_mode == SourceFilterMode.PERIOD
        assert config.period_column == "report_date"
        assert config.is_enabled is True

    def test_watermark_mode_valid(self):
        config = IncrementalConfig(
            filter_mode="watermark",
            watermark_column="modified_date",
        )
        assert config.filter_mode == SourceFilterMode.WATERMARK
        assert config.effective_source_watermark == "modified_date"

    def test_effective_source_watermark_override(self):
        config = IncrementalConfig(
            filter_mode="watermark",
            watermark_column="target_col",
            source_watermark_column="source_col",
        )
        assert config.effective_source_watermark == "source_col"

    def test_source_period_column(self):
        """Test source_period_column for replace_by_partition expansion."""
        config = IncrementalConfig(
            filter_mode="watermark",
            watermark_column="modified_date",
            source_period_column="report_month",
        )
        assert config.source_period_column == "report_month"

    def test_source_period_column_default_none(self):
        config = IncrementalConfig(
            filter_mode="watermark",
            watermark_column="modified_date",
        )
        assert config.source_period_column is None
