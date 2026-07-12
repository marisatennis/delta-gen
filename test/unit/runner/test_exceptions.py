"""Unit tests for PlanBuilderError exception."""
import pytest

from deltagen.runner.exceptions import PlanBuilderError


class TestPlanBuilderError:
    """Tests for PlanBuilderError exception class."""

    def test_basic_message(self):
        """Test error with just a message."""
        error = PlanBuilderError("Something went wrong")
        msg = str(error)
        assert "Something went wrong" in msg
        assert "PlanBuilderError" in msg

    def test_error_with_stage(self):
        """Test error message includes stage info."""
        error = PlanBuilderError("Test error", stage="transform_stage")
        msg = str(error)
        assert "Test error" in msg
        assert "transform_stage" in msg
        assert "Stage: transform_stage" in msg

    def test_error_with_column(self):
        """Test error message includes column info."""
        error = PlanBuilderError("Test error", column="customer_id")
        msg = str(error)
        assert "Test error" in msg
        assert "customer_id" in msg
        assert "Column: customer_id" in msg

    def test_error_with_detail(self):
        """Test error message includes detail."""
        error = PlanBuilderError("Test error", detail="Column not found in source")
        msg = str(error)
        assert "Test error" in msg
        assert "Column not found in source" in msg

    def test_error_with_stage_and_column(self):
        """Test error with both stage and column."""
        error = PlanBuilderError(
            "Failed to build column",
            stage="enrich",
            column="fk_customer"
        )
        msg = str(error)
        assert "Failed to build column" in msg
        assert "enrich" in msg
        assert "fk_customer" in msg

    def test_error_full_context(self):
        """Test error with all fields populated."""
        error = PlanBuilderError(
            "Failed to build",
            stage="transform",
            column="customer_id",
            detail="Source 'raw_data' not found in available sources"
        )
        msg = str(error)
        assert "Failed to build" in msg
        assert "transform" in msg
        assert "customer_id" in msg
        assert "Source 'raw_data' not found" in msg

    def test_error_is_exception(self):
        """Test that PlanBuilderError is a proper exception."""
        error = PlanBuilderError("Test")
        assert isinstance(error, Exception)

    def test_error_can_be_raised(self):
        """Test that error can be raised and caught."""
        with pytest.raises(PlanBuilderError) as exc_info:
            raise PlanBuilderError("Intentional error", stage="test_stage")
        assert "Intentional error" in str(exc_info.value)
        assert "test_stage" in str(exc_info.value)

    def test_error_attributes(self):
        """Test that error attributes are accessible."""
        error = PlanBuilderError(
            "Test",
            stage="my_stage",
            column="my_column",
            detail="my_detail"
        )
        # Attributes should be stored (if we add them)
        # For now, just verify the string representation
        msg = str(error)
        assert "my_stage" in msg
        assert "my_column" in msg
        assert "my_detail" in msg
