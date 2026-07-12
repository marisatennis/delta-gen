"""Unit tests for union_builder module."""
import pytest
from unittest.mock import MagicMock, patch

from deltagen.model.stage import StageConfig, UnionConfig
from deltagen.runner.union_builder import apply_unions, _union_by_name, _union_by_position
from deltagen.runner.exceptions import PlanBuilderError


class TestApplyUnions:
    """Tests for apply_unions function."""

    @patch("deltagen.runner.union_builder._get_spark_functions")
    def test_apply_unions_basic(self, mock_get_f):
        """Test basic union of two sources."""
        mock_F = MagicMock()
        mock_get_f.return_value = mock_F
        mock_F.col.return_value = MagicMock()
        mock_F.lit.return_value = MagicMock()

        mock_df1 = MagicMock()
        mock_df2 = MagicMock()
        mock_df1.columns = ["a", "b", "c"]
        mock_df2.columns = ["a", "b", "c"]
        mock_df1.select.return_value = mock_df1
        mock_df2.select.return_value = mock_df2
        mock_df1.union.return_value = mock_df1

        stage = StageConfig(
            name="union_stage",
            unions=UnionConfig(
                sources=["source1", "source2"],
            )
        )

        sources = {
            "source1": mock_df1,
            "source2": mock_df2,
        }
        sql_parts = {"union_stage": {"from": None}}

        result = apply_unions(stage, sources, sql_parts, debug=False)

        assert result is not None
        assert sql_parts["union_stage"]["from"] == "UNION ALL (source1, source2)"

    def test_apply_unions_missing_config(self):
        """Test that apply_unions raises error when no union config defined."""
        stage = StageConfig(name="no_union_stage")
        sources = {}
        sql_parts = {}

        with pytest.raises(PlanBuilderError, match="no union config defined"):
            apply_unions(stage, sources, sql_parts)

    def test_apply_unions_missing_source(self):
        """Test that apply_unions raises error for missing source."""
        stage = StageConfig(
            name="union_stage",
            unions=UnionConfig(
                sources=["source1", "missing_source"],
            )
        )

        sources = {"source1": MagicMock()}
        sql_parts = {"union_stage": {"from": None}}

        with pytest.raises(PlanBuilderError, match="Union source 'missing_source' not found"):
            apply_unions(stage, sources, sql_parts)

    @patch("deltagen.runner.union_builder._get_spark_functions")
    def test_apply_unions_with_distinct(self, mock_get_f):
        """Test union with distinct=True."""
        mock_F = MagicMock()
        mock_get_f.return_value = mock_F
        mock_F.col.return_value = MagicMock()
        mock_F.lit.return_value = MagicMock()

        mock_df1 = MagicMock()
        mock_df2 = MagicMock()
        mock_df1.columns = ["a", "b"]
        mock_df2.columns = ["a", "b"]
        mock_df1.select.return_value = mock_df1
        mock_df2.select.return_value = mock_df2
        mock_df1.union.return_value = mock_df1
        mock_df1.distinct.return_value = mock_df1

        stage = StageConfig(
            name="union_stage",
            unions=UnionConfig(
                sources=["source1", "source2"],
                distinct=True,
            )
        )

        sources = {"source1": mock_df1, "source2": mock_df2}
        sql_parts = {"union_stage": {"from": None}}

        result = apply_unions(stage, sources, sql_parts, debug=False)

        mock_df1.distinct.assert_called_once()
        assert sql_parts["union_stage"]["from"] == "UNION (source1, source2)"


class TestUnionByName:
    """Tests for _union_by_name function."""

    @patch("deltagen.runner.union_builder._get_spark_functions")
    def test_single_df_returns_unchanged(self, mock_get_f):
        """Test that single DataFrame is returned unchanged."""
        mock_df = MagicMock()
        mock_df.columns = ["a", "b"]

        config = UnionConfig(sources=["s1", "s2"])  # Not used in this test

        result = _union_by_name([mock_df], config, "test_stage")

        assert result is mock_df

    def test_empty_dfs_raises_error(self):
        """Test that empty list raises error."""
        config = UnionConfig(sources=["s1", "s2"])

        with pytest.raises(PlanBuilderError, match="No DataFrames to union"):
            _union_by_name([], config, "test_stage")

    @patch("deltagen.runner.union_builder._get_spark_functions")
    def test_mismatched_columns_raises_error(self, mock_get_f):
        """Test that mismatched columns raise error when allow_missing_columns=False."""
        mock_F = MagicMock()
        mock_get_f.return_value = mock_F

        mock_df1 = MagicMock()
        mock_df2 = MagicMock()
        mock_df1.columns = ["a", "b", "c"]
        mock_df2.columns = ["a", "b", "d"]  # Different column

        config = UnionConfig(
            sources=["s1", "s2"],
            allow_missing_columns=False,
        )

        with pytest.raises(PlanBuilderError, match="Union sources have different columns"):
            _union_by_name([mock_df1, mock_df2], config, "test_stage")

    @patch("deltagen.runner.union_builder._get_spark_functions")
    def test_allow_missing_columns_fills_nulls(self, mock_get_f):
        """Test that missing columns are filled with NULL when allowed."""
        mock_F = MagicMock()
        mock_get_f.return_value = mock_F
        mock_col = MagicMock()
        mock_lit = MagicMock()
        mock_F.col.return_value = mock_col
        mock_F.lit.return_value = mock_lit
        mock_lit.alias.return_value = mock_lit

        mock_df1 = MagicMock()
        mock_df2 = MagicMock()
        mock_df1.columns = ["a", "b"]
        mock_df2.columns = ["a", "c"]  # Different second column

        mock_df1.select.return_value = mock_df1
        mock_df2.select.return_value = mock_df2
        mock_df1.union.return_value = mock_df1

        config = UnionConfig(
            sources=["s1", "s2"],
            allow_missing_columns=True,
        )

        result = _union_by_name([mock_df1, mock_df2], config, "test_stage")

        # Verify lit(None) was called for missing columns
        mock_F.lit.assert_called_with(None)
        assert result is not None


class TestUnionByPosition:
    """Tests for _union_by_position function."""

    def test_single_df_returns_unchanged(self):
        """Test that single DataFrame is returned unchanged."""
        mock_df = MagicMock()
        mock_df.columns = ["a", "b"]

        config = UnionConfig(sources=["s1", "s2"], mode="by_position")

        result = _union_by_position([mock_df], config, "test_stage")

        assert result is mock_df

    def test_empty_dfs_raises_error(self):
        """Test that empty list raises error."""
        config = UnionConfig(sources=["s1", "s2"], mode="by_position")

        with pytest.raises(PlanBuilderError, match="No DataFrames to union"):
            _union_by_position([], config, "test_stage")

    def test_different_column_count_raises_error(self):
        """Test that different column counts raise error."""
        mock_df1 = MagicMock()
        mock_df2 = MagicMock()
        mock_df1.columns = ["a", "b", "c"]
        mock_df2.columns = ["x", "y"]  # Different count

        config = UnionConfig(sources=["s1", "s2"], mode="by_position")

        with pytest.raises(PlanBuilderError, match="requires same column count"):
            _union_by_position([mock_df1, mock_df2], config, "test_stage")

    def test_union_by_position_chains_union(self):
        """Test that union by position chains DataFrames correctly."""
        mock_df1 = MagicMock()
        mock_df2 = MagicMock()
        mock_df3 = MagicMock()
        mock_df1.columns = ["a", "b"]
        mock_df2.columns = ["x", "y"]
        mock_df3.columns = ["p", "q"]

        mock_df1.union.return_value = mock_df1

        config = UnionConfig(
            sources=["s1", "s2", "s3"],
            mode="by_position",
        )

        result = _union_by_position([mock_df1, mock_df2, mock_df3], config, "test_stage")

        # Should have called union twice (chaining)
        assert mock_df1.union.call_count == 2


class TestUnionConfigModel:
    """Tests for UnionConfig model validation."""

    def test_union_config_requires_min_two_sources(self):
        """Test that UnionConfig requires at least 2 sources."""
        with pytest.raises(ValueError):
            UnionConfig(sources=["only_one"])

    def test_union_config_defaults(self):
        """Test UnionConfig default values."""
        config = UnionConfig(sources=["s1", "s2"])

        assert config.mode == "by_name"
        assert config.distinct is False
        assert config.allow_missing_columns is False

    def test_union_config_custom_values(self):
        """Test UnionConfig with custom values."""
        config = UnionConfig(
            sources=["a", "b", "c"],
            mode="by_position",
            distinct=True,
            allow_missing_columns=True,
        )

        assert len(config.sources) == 3
        assert config.sources[0].name == "a"
        assert config.sources[1].name == "b"
        assert config.sources[2].name == "c"
        assert config.mode == "by_position"
        assert config.distinct is True
        assert config.allow_missing_columns is True


class TestStageConfigWithUnion:
    """Tests for StageConfig with union configuration."""

    def test_stage_with_union_config(self):
        """Test creating a stage with union configuration."""
        stage = StageConfig(
            name="union_stage",
            unions=UnionConfig(
                sources=["sales_2023", "sales_2024"],
                mode="by_name",
            )
        )

        assert stage.unions is not None
        assert len(stage.unions.sources) == 2
        assert stage.unions.sources[0].name == "sales_2023"
        assert stage.unions.sources[1].name == "sales_2024"

    def test_stage_without_union_config(self):
        """Test that stage without union config has None."""
        stage = StageConfig(name="no_union")

        assert stage.unions is None
