"""
Test cases for datapy.mod_manager.result module.

Tests the ModResult class and convenience functions for standardized result handling
across all DataPy framework components.
"""

import sys
from pathlib import Path

# Add project root to Python path for testing
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pytest
import time
from unittest.mock import patch, MagicMock

from datapy.mod_manager.result import (
    ModResult, 
    validation_error, 
    runtime_error,
    SUCCESS,
    SUCCESS_WITH_WARNINGS, 
    VALIDATION_ERROR,
    RUNTIME_ERROR,
    TIMEOUT,
    CANCELED
)


class TestModResult:
    """Test cases for ModResult class."""
    
    def test_init_valid_parameters(self):
        """Test ModResult initialization with valid parameters."""
        result = ModResult("csv_reader", "test_mod")
        
        assert result.mod_type == "csv_reader"
        assert result.mod_name == "test_mod"
        assert isinstance(result.start_time, float)
        assert result.run_id.startswith("csv_reader_")
        # run_id format: mod_type + "_" + first 8 chars of uuid hex (32 chars)
        run_id_parts = result.run_id.split('_')
        assert len(run_id_parts) >= 2  # Could be more if mod_type contains underscores
        assert result.warnings == []
        assert result.errors == []
        assert result.metrics == {}
        assert result.artifacts == {}
        assert result.globals == {}
    
    def test_init_strips_whitespace(self):
        """Test that initialization strips whitespace from parameters."""
        result = ModResult("  csv_reader  ", "  test_mod  ")
        
        assert result.mod_type == "csv_reader"
        assert result.mod_name == "test_mod"
    
    def test_init_empty_mod_type_raises_error(self):
        """Test that empty mod_type raises ValueError."""
        with pytest.raises(ValueError, match="mod_type must be a non-empty string"):
            ModResult("", "test_mod")
        
        with pytest.raises(ValueError, match="mod_type must be a non-empty string"):
            ModResult("   ", "test_mod")
        
        with pytest.raises(ValueError, match="mod_type must be a non-empty string"):
            ModResult(None, "test_mod")
    
    def test_init_empty_mod_name_raises_error(self):
        """Test that empty mod_name raises ValueError."""
        with pytest.raises(ValueError, match="mod_name must be a non-empty string"):
            ModResult("csv_reader", "")
        
        with pytest.raises(ValueError, match="mod_name must be a non-empty string"):
            ModResult("csv_reader", "   ")
        
        with pytest.raises(ValueError, match="mod_name must be a non-empty string"):
            ModResult("csv_reader", None)
    
    def test_init_invalid_types_raises_error(self):
        """Test that non-string types raise ValueError."""
        with pytest.raises(ValueError, match="mod_type must be a non-empty string"):
            ModResult(123, "test_mod")
        
        with pytest.raises(ValueError, match="mod_name must be a non-empty string"):
            ModResult("csv_reader", 123)
    
    @patch('uuid.uuid4')
    def test_run_id_generation(self, mock_uuid):
        """Test run_id is generated correctly."""
        mock_uuid.return_value.hex = "abcd1234" * 4  # 32 chars
        
        result = ModResult("csv_reader", "test_mod")
        
        assert result.run_id == "csv_reader_abcd1234"
        mock_uuid.assert_called_once()


class TestAddWarning:
    """Test cases for add_warning method."""
    
    def test_add_warning_valid(self):
        """Test adding valid warning message."""
        result = ModResult("csv_reader", "test_mod")
        
        with patch('time.time', return_value=1234567890.5):
            result.add_warning("Test warning message")
        
        assert len(result.warnings) == 1
        warning = result.warnings[0]
        assert warning["message"] == "Test warning message"
        assert warning["warning_code"] == SUCCESS_WITH_WARNINGS
        assert warning["timestamp"] == 1234567890.5
    
    def test_add_warning_custom_code(self):
        """Test adding warning with custom warning code."""
        result = ModResult("csv_reader", "test_mod")
        
        result.add_warning("Custom warning", 15)
        
        warning = result.warnings[0]
        assert warning["message"] == "Custom warning"
        assert warning["warning_code"] == 15
    
    def test_add_warning_strips_whitespace(self):
        """Test that warning message whitespace is stripped."""
        result = ModResult("csv_reader", "test_mod")
        
        result.add_warning("  Test warning  ")
        
        assert result.warnings[0]["message"] == "Test warning"
    
    def test_add_warning_multiple(self):
        """Test adding multiple warnings."""
        result = ModResult("csv_reader", "test_mod")
        
        result.add_warning("Warning 1")
        result.add_warning("Warning 2")
        
        assert len(result.warnings) == 2
        assert result.warnings[0]["message"] == "Warning 1"
        assert result.warnings[1]["message"] == "Warning 2"
    
    def test_add_warning_empty_message_behavior(self):
        """Test the actual behavior of add_warning with empty messages."""
        result = ModResult("csv_reader", "test_mod")
        
        # Test what actually happens with empty string
        try:
            result.add_warning("")
            # If no exception, record what happened
            assert False, f"Expected exception but got warnings: {result.warnings}"
        except ValueError as e:
            # This is what we expect
            assert "warning message cannot be empty" in str(e)
        except Exception as e:
            assert False, f"Got unexpected exception: {type(e).__name__}: {e}"
    
    def test_add_warning_non_string_message_raises_error(self):
        """Test that non-string warning message raises ValueError."""
        result = ModResult("csv_reader", "test_mod")
        
        # The actual code does isinstance(message, str) check
        with pytest.raises(ValueError, match="warning message cannot be empty"):
            result.add_warning(123)


class TestAddError:
    """Test cases for add_error method."""
    
    def test_add_error_valid(self):
        """Test adding valid error message."""
        result = ModResult("csv_reader", "test_mod")
        
        with patch('time.time', return_value=1234567890.5):
            result.add_error("Test error message")
        
        assert len(result.errors) == 1
        error = result.errors[0]
        assert error["message"] == "Test error message"
        assert error["error_code"] == RUNTIME_ERROR
        assert error["timestamp"] == 1234567890.5
    
    def test_add_error_custom_code(self):
        """Test adding error with custom error code."""
        result = ModResult("csv_reader", "test_mod")
        
        result.add_error("Validation failed", VALIDATION_ERROR)
        
        error = result.errors[0]
        assert error["message"] == "Validation failed"
        assert error["error_code"] == VALIDATION_ERROR
    
    def test_add_error_strips_whitespace(self):
        """Test that error message whitespace is stripped."""
        result = ModResult("csv_reader", "test_mod")
        
        result.add_error("  Test error  ")
        
        assert result.errors[0]["message"] == "Test error"
    
    def test_add_error_multiple(self):
        """Test adding multiple errors."""
        result = ModResult("csv_reader", "test_mod")
        
        result.add_error("Error 1")
        result.add_error("Error 2")
        
        assert len(result.errors) == 2
        assert result.errors[0]["message"] == "Error 1"
        assert result.errors[1]["message"] == "Error 2"
    
    def test_add_error_empty_message_raises_error(self):
        """Test that empty error message raises ValueError."""
        result = ModResult("csv_reader", "test_mod")
        
        with pytest.raises(ValueError, match="error message cannot be empty"):
            result.add_error("")
        
        with pytest.raises(ValueError, match="error message cannot be empty"):
            result.add_error(None)


class TestAddMetric:
    """Test cases for add_metric method."""
    
    def test_add_metric_valid(self):
        """Test adding valid metrics."""
        result = ModResult("csv_reader", "test_mod")
        
        result.add_metric("rows_processed", 100)
        result.add_metric("processing_time", 1.5)
        result.add_metric("success_rate", 0.95)
        
        assert result.metrics["rows_processed"] == 100
        assert result.metrics["processing_time"] == 1.5
        assert result.metrics["success_rate"] == 0.95
    
    def test_add_metric_strips_key_whitespace(self):
        """Test that metric key whitespace is stripped."""
        result = ModResult("csv_reader", "test_mod")
        
        result.add_metric("  test_metric  ", "value")
        
        assert "test_metric" in result.metrics
        assert "  test_metric  " not in result.metrics
    
    def test_add_metric_overwrites_existing(self):
        """Test that adding metric with same key overwrites."""
        result = ModResult("csv_reader", "test_mod")
        
        result.add_metric("test_key", "old_value")
        result.add_metric("test_key", "new_value")
        
        assert result.metrics["test_key"] == "new_value"
    
    def test_add_metric_empty_key_raises_error(self):
        """Test that empty metric key raises ValueError."""
        result = ModResult("csv_reader", "test_mod")
        
        with pytest.raises(ValueError, match="metric key cannot be empty"):
            result.add_metric("", "value")
        
        with pytest.raises(ValueError, match="metric key cannot be empty"):
            result.add_metric("   ", "value")
        
        with pytest.raises(ValueError, match="metric key cannot be empty"):
            result.add_metric(None, "value")
    
    def test_add_metric_non_string_key_raises_error(self):
        """Test that non-string metric key raises ValueError."""
        result = ModResult("csv_reader", "test_mod")
        
        with pytest.raises(ValueError, match="metric key cannot be empty"):
            result.add_metric(123, "value")


class TestAddArtifact:
    """Test cases for add_artifact method."""
    
    def test_add_artifact_valid_types(self):
        """Test adding artifacts of various types."""
        result = ModResult("csv_reader", "test_mod")
        
        # Test different artifact types
        test_df = MagicMock()  # Mock DataFrame
        test_list = [1, 2, 3]
        test_dict = {"key": "value"}
        test_string = "/path/to/file.csv"
        
        result.add_artifact("dataframe", test_df)
        result.add_artifact("list_data", test_list)
        result.add_artifact("dict_data", test_dict)
        result.add_artifact("file_path", test_string)
        
        assert result.artifacts["dataframe"] is test_df
        assert result.artifacts["list_data"] == test_list
        assert result.artifacts["dict_data"] == test_dict
        assert result.artifacts["file_path"] == test_string
    
    def test_add_artifact_strips_key_whitespace(self):
        """Test that artifact key whitespace is stripped."""
        result = ModResult("csv_reader", "test_mod")
        
        result.add_artifact("  test_artifact  ", "value")
        
        assert "test_artifact" in result.artifacts
        assert "  test_artifact  " not in result.artifacts
    
    def test_add_artifact_overwrites_existing(self):
        """Test that adding artifact with same key overwrites."""
        result = ModResult("csv_reader", "test_mod")
        
        result.add_artifact("test_key", "old_value")
        result.add_artifact("test_key", "new_value")
        
        assert result.artifacts["test_key"] == "new_value"
    
    def test_add_artifact_empty_key_raises_error(self):
        """Test that empty artifact key raises ValueError."""
        result = ModResult("csv_reader", "test_mod")
        
        with pytest.raises(ValueError, match="artifact key cannot be empty"):
            result.add_artifact("", "value")
        
        with pytest.raises(ValueError, match="artifact key cannot be empty"):
            result.add_artifact(None, "value")


class TestAddGlobal:
    """Test cases for add_global method."""
    
    def test_add_global_valid(self):
        """Test adding valid global variables."""
        result = ModResult("csv_reader", "test_mod")
        
        result.add_global("row_count", 100)
        result.add_global("file_path", "/data/input.csv")
        result.add_global("processed", True)
        
        assert result.globals["row_count"] == 100
        assert result.globals["file_path"] == "/data/input.csv"
        assert result.globals["processed"] is True
    
    def test_add_global_strips_key_whitespace(self):
        """Test that global key whitespace is stripped."""
        result = ModResult("csv_reader", "test_mod")
        
        result.add_global("  test_global  ", "value")
        
        assert "test_global" in result.globals
        assert "  test_global  " not in result.globals
    
    def test_add_global_empty_key_raises_error(self):
        """Test that empty global key raises ValueError."""
        result = ModResult("csv_reader", "test_mod")
        
        with pytest.raises(ValueError, match="global key cannot be empty"):
            result.add_global("", "value")
        
        with pytest.raises(ValueError, match="global key cannot be empty"):
            result.add_global(None, "value")


class TestResultBuilders:
    """Test cases for success(), warning(), error() methods."""
    
    @patch('time.time')
    def test_success_result(self, mock_time):
        """Test building success result."""
        mock_time.side_effect = [1000.0, 1001.5]  # start_time, end_time
        
        result = ModResult("csv_reader", "test_mod")
        result.add_metric("rows", 100)
        result.add_artifact("data", "test_data")
        result.add_global("count", 100)
        
        success_result = result.success()
        
        assert success_result["status"] == "success"
        assert success_result["exit_code"] == SUCCESS
        assert success_result["execution_time"] == 1.5
        assert success_result["metrics"] == {"rows": 100}
        assert success_result["artifacts"] == {"data": "test_data"}
        assert success_result["globals"] == {"count": 100}
        assert success_result["warnings"] == []
        assert success_result["errors"] == []
        assert success_result["logs"]["mod_type"] == "csv_reader"
        assert success_result["logs"]["mod_name"] == "test_mod"
        assert "run_id" in success_result["logs"]
    
    @patch('time.time')
    def test_warning_result(self, mock_time):
        """Test building warning result."""
        mock_time.return_value = 1000.0  # Single return value for start time
        result = ModResult("csv_reader", "test_mod")
        result.add_warning("Test warning")
        result.add_metric("rows", 50)
        
        # Set different time for end calculation
        mock_time.return_value = 1002.0
        warning_result = result.warning()
        
        assert warning_result["status"] == "warning"
        assert warning_result["exit_code"] == SUCCESS_WITH_WARNINGS
        assert warning_result["execution_time"] == 2.0
        assert len(warning_result["warnings"]) == 1
        assert warning_result["warnings"][0]["message"] == "Test warning"
    
    @patch('time.time')
    def test_error_result_default_code(self, mock_time):
        """Test building error result with default error code."""
        mock_time.return_value = 1000.0  # Single return for start time
        result = ModResult("csv_reader", "test_mod")
        result.add_error("Test error")
        
        # Set different time for end calculation
        mock_time.return_value = 1001.0
        error_result = result.error()
        
        assert error_result["status"] == "error"
        assert error_result["exit_code"] == RUNTIME_ERROR
        assert error_result["execution_time"] == 1.0
        assert len(error_result["errors"]) == 1
        assert error_result["errors"][0]["message"] == "Test error"
    
    @patch('time.time')
    def test_error_result_custom_code(self, mock_time):
        """Test building error result with custom error code."""
        mock_time.return_value = 1000.0  # Single return for start time
        result = ModResult("csv_reader", "test_mod")
        result.add_error("Validation failed")
        
        # Set different time for end calculation  
        mock_time.return_value = 1001.0
        error_result = result.error(VALIDATION_ERROR)
        
        assert error_result["status"] == "error"
        assert error_result["exit_code"] == VALIDATION_ERROR
    
    def test_execution_time_calculation(self):
        """Test execution time is calculated correctly."""
        with patch('time.time') as mock_time:
            mock_time.return_value = 1000.0
            result = ModResult("csv_reader", "test_mod")
            
            mock_time.return_value = 1003.456
            success_result = result.success()
            
            assert success_result["execution_time"] == 3.456


class TestBuildResult:
    """Test cases for _build_result internal method."""
    
    def test_build_result_invalid_status_raises_error(self):
        """Test that invalid status raises ValueError."""
        result = ModResult("csv_reader", "test_mod")
        
        with pytest.raises(ValueError, match="Invalid status: invalid"):
            result._build_result("invalid", SUCCESS)
    
    def test_build_result_invalid_exit_code_raises_error(self):
        """Test that invalid exit_code raises ValueError."""
        result = ModResult("csv_reader", "test_mod")
        
        with pytest.raises(ValueError, match="Invalid exit_code: -1"):
            result._build_result("success", -1)
        
        with pytest.raises(ValueError, match="Invalid exit_code: not_int"):
            result._build_result("success", "not_int")
    
    def test_build_result_copies_collections(self):
        """Test that collections are shallow copied."""
        result = ModResult("csv_reader", "test_mod")
        
        # Add some data
        result.add_metric("test", "value")
        result.add_artifact("test", "value") 
        result.add_global("test", "value")
        result.add_warning("test warning")
        result.add_error("test error")
        
        success_result = result.success()
        
        # Test that top-level collections are copied (modifying result doesn't affect success_result)
        original_metrics = success_result["metrics"].copy()
        original_artifacts = success_result["artifacts"].copy()
        original_globals = success_result["globals"].copy()
        
        # Modify original collections - these should not affect the result
        result.metrics["test"] = "modified"
        result.artifacts["test"] = "modified"
        result.globals["test"] = "modified"
        
        # Top-level collections should be unchanged
        assert success_result["metrics"] == original_metrics
        assert success_result["artifacts"] == original_artifacts  
        assert success_result["globals"] == original_globals
        
        # But note: .copy() is shallow, so nested objects are still referenced
        # This is expected behavior - deep copy would be expensive for large DataFrames
        # The important thing is that top-level collections are independent


class TestConvenienceFunctions:
    """Test cases for convenience functions."""
    
    def test_validation_error_function(self):
        """Test validation_error convenience function."""
        result = validation_error("test_mod", "Invalid parameter")
        
        assert result["status"] == "error"
        assert result["exit_code"] == VALIDATION_ERROR
        assert result["logs"]["mod_name"] == "test_mod"
        assert result["logs"]["mod_type"] == "unknown"
        assert len(result["errors"]) == 1
        assert result["errors"][0]["message"] == "Invalid parameter"
        assert result["errors"][0]["error_code"] == VALIDATION_ERROR
    
    def test_runtime_error_function(self):
        """Test runtime_error convenience function."""
        result = runtime_error("test_mod", "Execution failed")
        
        assert result["status"] == "error"
        assert result["exit_code"] == RUNTIME_ERROR
        assert result["logs"]["mod_name"] == "test_mod"
        assert result["logs"]["mod_type"] == "unknown"
        assert len(result["errors"]) == 1
        assert result["errors"][0]["message"] == "Execution failed"
        assert result["errors"][0]["error_code"] == RUNTIME_ERROR
    
    def test_convenience_functions_empty_inputs_raise_error(self):
        """Test convenience functions raise error for empty inputs."""
        with pytest.raises(ValueError, match="mod_name and message cannot be empty"):
            validation_error("", "message")
        
        with pytest.raises(ValueError, match="mod_name and message cannot be empty"):
            validation_error("mod_name", "")
        
        with pytest.raises(ValueError, match="mod_name and message cannot be empty"):
            runtime_error(None, "message")
        
        with pytest.raises(ValueError, match="mod_name and message cannot be empty"):
            runtime_error("mod_name", None)


class TestExitCodes:
    """Test cases for exit code constants."""
    
    def test_exit_code_constants_exist(self):
        """Test that all required exit code constants are defined."""
        assert SUCCESS == 0
        assert SUCCESS_WITH_WARNINGS == 10
        assert VALIDATION_ERROR == 20
        assert RUNTIME_ERROR == 30
        assert TIMEOUT == 40
        assert CANCELED == 50
    
    def test_exit_code_constants_are_integers(self):
        """Test that exit code constants are integers."""
        exit_codes = [SUCCESS, SUCCESS_WITH_WARNINGS, VALIDATION_ERROR, 
                     RUNTIME_ERROR, TIMEOUT, CANCELED]
        
        for code in exit_codes:
            assert isinstance(code, int)
            assert code >= 0


class TestIntegrationScenarios:
    """Integration test cases for complete mod result workflows."""
    
    def test_successful_mod_execution_flow(self):
        """Test complete successful mod execution result flow."""
        result = ModResult("csv_reader", "extract_customers")
        
        # Simulate successful execution
        result.add_metric("rows_read", 1000)
        result.add_metric("file_size_mb", 2.5)
        result.add_artifact("data", MagicMock())
        result.add_artifact("file_path", "/data/customers.csv")
        result.add_global("row_count", 1000)
        result.add_global("last_processed", "2024-01-15")
        
        final_result = result.success()
        
        # Verify complete result structure
        assert final_result["status"] == "success"
        assert final_result["exit_code"] == SUCCESS
        assert isinstance(final_result["execution_time"], float)
        assert final_result["metrics"]["rows_read"] == 1000
        assert final_result["artifacts"]["file_path"] == "/data/customers.csv"
        assert final_result["globals"]["row_count"] == 1000
        assert final_result["warnings"] == []
        assert final_result["errors"] == []
        assert final_result["logs"]["mod_type"] == "csv_reader"
        assert final_result["logs"]["mod_name"] == "extract_customers"
    
    def test_mod_execution_with_warnings_flow(self):
        """Test mod execution with warnings result flow."""
        result = ModResult("csv_filter", "filter_customers")
        
        # Simulate execution with warnings
        result.add_warning("Found 5 duplicate records")
        result.add_warning("Missing values in optional column 'phone'")
        result.add_metric("original_rows", 1000)
        result.add_metric("filtered_rows", 950)
        result.add_artifact("filtered_data", MagicMock())
        result.add_global("filter_rate", 0.05)
        
        final_result = result.warning()
        
        # Verify warning result structure
        assert final_result["status"] == "warning"
        assert final_result["exit_code"] == SUCCESS_WITH_WARNINGS
        assert len(final_result["warnings"]) == 2
        assert "duplicate records" in final_result["warnings"][0]["message"]
        assert final_result["metrics"]["filtered_rows"] == 950
    
    def test_mod_execution_failure_flow(self):
        """Test mod execution failure result flow."""
        result = ModResult("csv_writer", "save_results")
        
        # Simulate execution failure
        result.add_error("Permission denied: cannot write to /protected/output.csv")
        result.add_error("Disk space insufficient")
        result.add_metric("attempted_rows", 1000)
        result.add_metric("written_rows", 0)
        
        final_result = result.error(RUNTIME_ERROR)
        
        # Verify error result structure
        assert final_result["status"] == "error"
        assert final_result["exit_code"] == RUNTIME_ERROR
        assert len(final_result["errors"]) == 2
        assert "Permission denied" in final_result["errors"][0]["message"]
        assert final_result["metrics"]["written_rows"] == 0
        assert final_result["warnings"] == []  # No warnings in failure case