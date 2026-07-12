"""Unit tests for filter_builder module.

Tests cover SQL expression string filtering including complex boolean expressions.
"""
from unittest.mock import MagicMock

from deltagen.runner.filter_builder import (
    apply_filters,
    extract_source_references,
    analyze_filter_pushdown,
)
from deltagen.model.stage import StageConfig


class TestApplyFilters:
    """Tests for apply_filters function."""

    def test_no_filters_returns_original_df(self):
        """Test that stage with no filters returns original DataFrame."""
        mock_df = MagicMock()
        stage = StageConfig(name="no_filters", filters=[])
        sql_parts = {"no_filters": {"where": []}}

        result = apply_filters(mock_df, stage, sql_parts)

        assert result == mock_df
        mock_df.filter.assert_not_called()

    def test_single_filter_applied(self):
        """Test single filter is applied to DataFrame."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        stage = StageConfig(
            name="single_filter",
            filters=["status = 'active'"]
        )
        sql_parts = {"single_filter": {"where": []}}

        apply_filters(mock_df, stage, sql_parts)

        mock_df.filter.assert_called_once_with("status = 'active'")

    def test_multiple_filters_applied_sequentially(self):
        """Test multiple filters are applied in order (AND logic)."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        stage = StageConfig(
            name="multi_filter",
            filters=[
                "status = 'active'",
                "created_date >= '2020-01-01'"
            ]
        )
        sql_parts = {"multi_filter": {"where": []}}

        apply_filters(mock_df, stage, sql_parts)

        # Should be called twice
        assert mock_df.filter.call_count == 2
        # First call
        mock_df.filter.assert_any_call("status = 'active'")
        # Second call
        mock_df.filter.assert_any_call("created_date >= '2020-01-01'")

    def test_sql_parts_updated(self):
        """Test that SQL parts dictionary is updated with filters."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        stage = StageConfig(
            name="track_filters",
            filters=["amount > 100", "is_cancelled = false"]
        )
        sql_parts = {"track_filters": {"where": []}}

        apply_filters(mock_df, stage, sql_parts)

        assert len(sql_parts["track_filters"]["where"]) == 2
        assert "amount > 100" in sql_parts["track_filters"]["where"]
        assert "is_cancelled = false" in sql_parts["track_filters"]["where"]


class TestComplexFilters:
    """Tests for complex SQL expression filters.

    These tests document that filters support full Spark SQL syntax.
    """

    def test_simple_comparison(self):
        """Test simple comparison filter."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        stage = StageConfig(
            name="simple",
            filters=["price > 100"]
        )
        sql_parts = {"simple": {"where": []}}

        apply_filters(mock_df, stage, sql_parts)
        mock_df.filter.assert_called_with("price > 100")

    def test_and_condition(self):
        """Test AND condition in single filter."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        stage = StageConfig(
            name="and_filter",
            filters=["status = 'active' AND amount > 0"]
        )
        sql_parts = {"and_filter": {"where": []}}

        apply_filters(mock_df, stage, sql_parts)
        mock_df.filter.assert_called_with("status = 'active' AND amount > 0")

    def test_or_condition(self):
        """Test OR condition in single filter."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        stage = StageConfig(
            name="or_filter",
            filters=["region = 'US' OR region = 'CA'"]
        )
        sql_parts = {"or_filter": {"where": []}}

        apply_filters(mock_df, stage, sql_parts)
        mock_df.filter.assert_called_with("region = 'US' OR region = 'CA'")

    def test_complex_nested_boolean(self):
        """Test complex nested boolean expression."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        # Complex: ((x= 1 OR y = 2) AND ( B=1 OR C =3) ) OR (g= 2 AND j= 5)
        complex_filter = "((x = 1 OR y = 2) AND (b = 1 OR c = 3)) OR (g = 2 AND j = 5)"
        stage = StageConfig(
            name="complex_bool",
            filters=[complex_filter]
        )
        sql_parts = {"complex_bool": {"where": []}}

        apply_filters(mock_df, stage, sql_parts)
        mock_df.filter.assert_called_with(complex_filter)

    def test_in_operator(self):
        """Test IN operator in filter."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        stage = StageConfig(
            name="in_filter",
            filters=["category IN ('A', 'B', 'C')"]
        )
        sql_parts = {"in_filter": {"where": []}}

        apply_filters(mock_df, stage, sql_parts)
        mock_df.filter.assert_called_with("category IN ('A', 'B', 'C')")

    def test_not_in_operator(self):
        """Test NOT IN operator in filter."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        stage = StageConfig(
            name="not_in_filter",
            filters=["status NOT IN ('deleted', 'archived')"]
        )
        sql_parts = {"not_in_filter": {"where": []}}

        apply_filters(mock_df, stage, sql_parts)
        mock_df.filter.assert_called_with("status NOT IN ('deleted', 'archived')")

    def test_is_null_check(self):
        """Test IS NULL check."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        stage = StageConfig(
            name="null_check",
            filters=["deleted_at IS NULL"]
        )
        sql_parts = {"null_check": {"where": []}}

        apply_filters(mock_df, stage, sql_parts)
        mock_df.filter.assert_called_with("deleted_at IS NULL")

    def test_is_not_null_check(self):
        """Test IS NOT NULL check."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        stage = StageConfig(
            name="not_null_check",
            filters=["email IS NOT NULL"]
        )
        sql_parts = {"not_null_check": {"where": []}}

        apply_filters(mock_df, stage, sql_parts)
        mock_df.filter.assert_called_with("email IS NOT NULL")

    def test_between_operator(self):
        """Test BETWEEN operator."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        stage = StageConfig(
            name="between_filter",
            filters=["amount BETWEEN 100 AND 500"]
        )
        sql_parts = {"between_filter": {"where": []}}

        apply_filters(mock_df, stage, sql_parts)
        mock_df.filter.assert_called_with("amount BETWEEN 100 AND 500")

    def test_like_operator(self):
        """Test LIKE operator."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        stage = StageConfig(
            name="like_filter",
            filters=["name LIKE 'John%'"]
        )
        sql_parts = {"like_filter": {"where": []}}

        apply_filters(mock_df, stage, sql_parts)
        mock_df.filter.assert_called_with("name LIKE 'John%'")

    def test_coalesce_function(self):
        """Test COALESCE function in filter."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        stage = StageConfig(
            name="coalesce_filter",
            filters=["COALESCE(status, 'unknown') != 'deleted'"]
        )
        sql_parts = {"coalesce_filter": {"where": []}}

        apply_filters(mock_df, stage, sql_parts)
        mock_df.filter.assert_called_with("COALESCE(status, 'unknown') != 'deleted'")

    def test_date_functions(self):
        """Test date functions in filter."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        stage = StageConfig(
            name="date_filter",
            filters=["YEAR(created_date) = 2024"]
        )
        sql_parts = {"date_filter": {"where": []}}

        apply_filters(mock_df, stage, sql_parts)
        mock_df.filter.assert_called_with("YEAR(created_date) = 2024")

    def test_string_functions(self):
        """Test string functions in filter."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        stage = StageConfig(
            name="string_filter",
            filters=["LOWER(country_code) = 'us'"]
        )
        sql_parts = {"string_filter": {"where": []}}

        apply_filters(mock_df, stage, sql_parts)
        mock_df.filter.assert_called_with("LOWER(country_code) = 'us'")

    def test_length_function(self):
        """Test LENGTH function in filter."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        stage = StageConfig(
            name="length_filter",
            filters=["LENGTH(description) > 10"]
        )
        sql_parts = {"length_filter": {"where": []}}

        apply_filters(mock_df, stage, sql_parts)
        mock_df.filter.assert_called_with("LENGTH(description) > 10")

    def test_case_expression(self):
        """Test CASE expression in filter."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        stage = StageConfig(
            name="case_filter",
            filters=["CASE WHEN amount > 1000 THEN 'high' ELSE 'low' END = 'high'"]
        )
        sql_parts = {"case_filter": {"where": []}}

        apply_filters(mock_df, stage, sql_parts)
        mock_df.filter.assert_called_with(
            "CASE WHEN amount > 1000 THEN 'high' ELSE 'low' END = 'high'"
        )


class TestDebugOutput:
    """Tests for debug output."""

    def test_debug_prints_filters(self, capsys):
        """Test that debug mode prints filter information."""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        stage = StageConfig(
            name="debug_test",
            filters=["status = 'active'"]
        )
        sql_parts = {"debug_test": {"where": []}}

        apply_filters(mock_df, stage, sql_parts, debug=True)

        captured = capsys.readouterr()
        assert "FILTERS" in captured.out
        assert "status = 'active'" in captured.out


class TestExtractSourceReferences:
    """Tests for extract_source_references function."""

    def test_single_source_reference(self):
        """Test extracting single source reference."""
        refs = extract_source_references("src.order_date >= '2024-01-01'")
        assert refs == {"src"}

    def test_multiple_source_references(self):
        """Test extracting multiple source references."""
        refs = extract_source_references("src.id = cust.order_id")
        assert refs == {"src", "cust"}

    def test_no_source_reference(self):
        """Test filter without source prefix."""
        refs = extract_source_references("amount > 100")
        assert refs == set()

    def test_complex_filter_single_source(self):
        """Test complex filter with single source."""
        refs = extract_source_references(
            "src.status = 'active' AND src.amount > 100"
        )
        assert refs == {"src"}

    def test_filter_with_alias(self):
        """Test filter using table alias."""
        refs = extract_source_references("t1.field_name = 'value'")
        assert refs == {"t1"}

    def test_excludes_sql_function_patterns(self):
        """Test that SQL functions are not mistaken for source refs."""
        # These look like source.column but are actually SQL functions
        refs = extract_source_references("CAST.value = 1")  # Edge case
        # CAST is excluded, so this should be empty
        assert refs == set()

    def test_filter_with_coalesce(self):
        """Test filter with COALESCE doesn't extract it as source."""
        refs = extract_source_references("COALESCE(src.value, 0) > 10")
        # src is a valid source, COALESCE is not
        assert refs == {"src"}

    def test_underscore_in_source_name(self):
        """Test source name with underscores."""
        refs = extract_source_references("dim_customer.is_active = true")
        assert refs == {"dim_customer"}

    def test_multiple_columns_same_source(self):
        """Test multiple columns from same source."""
        refs = extract_source_references(
            "orders.status = 'active' AND orders.amount > 0 AND orders.date >= '2024-01-01'"
        )
        assert refs == {"orders"}

    def test_mixed_sources_complex(self):
        """Test complex filter with multiple sources."""
        refs = extract_source_references(
            "src.customer_id = cust.id AND prod.is_active = true"
        )
        assert refs == {"src", "cust", "prod"}


class TestAnalyzeFilterPushdown:
    """Tests for analyze_filter_pushdown function."""

    def test_single_source_pushdown(self):
        """Test filter with single source can be pushed down."""
        filters = ["src.order_date >= '2024-01-01'"]
        known_sources = {"src", "cust"}

        pushdown, post_join = analyze_filter_pushdown(filters, known_sources)

        assert pushdown == {"src": ["src.order_date >= '2024-01-01'"]}
        assert post_join == []

    def test_multi_source_filter_not_pushed(self):
        """Test filter with multiple sources is not pushed down."""
        filters = ["src.id = cust.customer_id"]
        known_sources = {"src", "cust"}

        pushdown, post_join = analyze_filter_pushdown(filters, known_sources)

        assert pushdown == {}
        assert post_join == ["src.id = cust.customer_id"]

    def test_no_prefix_filter_not_pushed(self):
        """Test filter without prefix is not pushed down."""
        filters = ["amount > 100"]
        known_sources = {"src", "cust"}

        pushdown, post_join = analyze_filter_pushdown(filters, known_sources)

        assert pushdown == {}
        assert post_join == ["amount > 100"]

    def test_unknown_source_filter_not_pushed(self):
        """Test filter with unknown source is not pushed down."""
        filters = ["unknown.field = 'value'"]
        known_sources = {"src", "cust"}

        pushdown, post_join = analyze_filter_pushdown(filters, known_sources)

        assert pushdown == {}
        assert post_join == ["unknown.field = 'value'"]

    def test_mixed_filters(self):
        """Test mix of pushable and non-pushable filters."""
        filters = [
            "src.date >= '2024-01-01'",  # Pushable to src
            "cust.is_active = true",     # Pushable to cust
            "src.id = cust.order_id",    # Not pushable (multiple sources)
            "total_amount > 100",        # Not pushable (no prefix)
        ]
        known_sources = {"src", "cust"}

        pushdown, post_join = analyze_filter_pushdown(filters, known_sources)

        assert pushdown == {
            "src": ["src.date >= '2024-01-01'"],
            "cust": ["cust.is_active = true"],
        }
        assert post_join == ["src.id = cust.order_id", "total_amount > 100"]

    def test_multiple_filters_same_source(self):
        """Test multiple filters for the same source are grouped."""
        filters = [
            "src.status = 'active'",
            "src.amount > 0",
            "src.date >= '2024-01-01'",
        ]
        known_sources = {"src"}

        pushdown, post_join = analyze_filter_pushdown(filters, known_sources)

        assert pushdown == {
            "src": [
                "src.status = 'active'",
                "src.amount > 0",
                "src.date >= '2024-01-01'",
            ]
        }
        assert post_join == []

    def test_empty_filters(self):
        """Test empty filter list."""
        pushdown, post_join = analyze_filter_pushdown([], {"src"})

        assert pushdown == {}
        assert post_join == []

    def test_empty_known_sources(self):
        """Test with no known sources - all go to post-join."""
        filters = ["src.status = 'active'"]

        pushdown, post_join = analyze_filter_pushdown(filters, set())

        assert pushdown == {}
        assert post_join == ["src.status = 'active'"]

    def test_alias_matching(self):
        """Test that aliases are properly matched."""
        filters = ["s.order_date >= '2024-01-01'"]
        known_sources = {"raw_orders", "s", "dim_customer", "c"}

        pushdown, post_join = analyze_filter_pushdown(filters, known_sources)

        assert pushdown == {"s": ["s.order_date >= '2024-01-01'"]}
        assert post_join == []
