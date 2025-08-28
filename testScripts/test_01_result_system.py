"""
Test 01: Result System
Tests the ModResult class and result handling functionality.
"""

import sys
import json
from pathlib import Path

# Add project root to path (tests folder is in root)
sys.path.insert(0, str(Path(__file__).parent.parent))

from datapy.mod_manager.result import (
    ModResult, validation_error, runtime_error,
    SUCCESS, SUCCESS_WITH_WARNINGS, VALIDATION_ERROR, RUNTIME_ERROR
)

def test_basic_result_creation():
    """Test basic ModResult creation and validation."""
    print("=== Test: Basic Result Creation ===")
    
    # Valid creation
    result = ModResult("csv_reader", "test_extract")
    assert result.mod_type == "csv_reader"
    assert result.mod_name == "test_extract"
    assert result.run_id.startswith("csv_reader_")
    assert len(result.warnings) == 0
    assert len(result.errors) == 0
    
    print("PASS: Basic creation successful")
    
    # Invalid creation tests
    try:
        ModResult("", "test")
        assert False, "Should fail with empty mod_type"
    except ValueError:
        print("PASS: Empty mod_type validation works")
    
    try:
        ModResult("test", "")
        assert False, "Should fail with empty mod_name"
    except ValueError:
        print("PASS: Empty mod_name validation works")


def test_warnings_and_errors():
    """Test warning and error handling."""
    print("\n=== Test: Warnings and Errors ===")
    
    result = ModResult("test_mod", "test_instance")
    
    # Add warnings
    result.add_warning("Test warning message")
    result.add_warning("Second warning", 15)
    
    assert len(result.warnings) == 2
    assert result.warnings[0]["message"] == "Test warning message"
    assert result.warnings[0]["warning_code"] == SUCCESS_WITH_WARNINGS
    assert result.warnings[1]["warning_code"] == 15
    
    print("PASS: Warnings added successfully")
    
    # Add errors  
    result.add_error("Test error message")
    result.add_error("Second error", VALIDATION_ERROR)
    
    assert len(result.errors) == 2
    assert result.errors[0]["message"] == "Test error message"
    assert result.errors[0]["error_code"] == RUNTIME_ERROR
    assert result.errors[1]["error_code"] == VALIDATION_ERROR
    
    print("PASS: Errors added successfully")


def test_metrics_and_artifacts():
    """Test metrics and artifacts handling."""
    print("\n=== Test: Metrics and Artifacts ===")
    
    result = ModResult("test_mod", "test_instance")
    
    # Add metrics
    result.add_metric("rows_processed", 1000)
    result.add_metric("processing_time", 5.2)
    result.add_metric("status", "completed")
    
    assert result.metrics["rows_processed"] == 1000
    assert result.metrics["processing_time"] == 5.2
    assert result.metrics["status"] == "completed"
    
    print("PASS: Metrics added successfully")
    
    # Add artifacts
    test_data = {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
    result.add_artifact("data", test_data)
    result.add_artifact("file_path", "/path/to/file.csv")
    
    assert result.artifacts["data"] == test_data
    assert result.artifacts["file_path"] == "/path/to/file.csv"
    
    print("PASS: Artifacts added successfully")


def test_globals():
    """Test global variables handling."""
    print("\n=== Test: Global Variables ===")
    
    result = ModResult("test_mod", "test_instance")
    
    # Add globals
    result.add_global("total_records", 5000)
    result.add_global("batch_id", "batch_20241201_001")
    result.add_global("success_rate", 98.5)
    
    assert result.globals["total_records"] == 5000
    assert result.globals["batch_id"] == "batch_20241201_001"
    assert result.globals["success_rate"] == 98.5
    
    print("PASS: Globals added successfully")


def test_result_states():
    """Test success, warning, and error result states."""
    print("\n=== Test: Result States ===")
    
    result = ModResult("test_mod", "test_instance")
    
    # Test success result
    result.add_metric("processed", 100)
    success_result = result.success()
    
    assert success_result["status"] == "success"
    assert success_result["exit_code"] == SUCCESS
    assert "execution_time" in success_result
    assert success_result["metrics"]["processed"] == 100
    
    print("PASS: Success result created correctly")
    
    # Test warning result
    result_warn = ModResult("test_mod", "test_instance_warn")
    result_warn.add_warning("Minor issue occurred")
    warning_result = result_warn.warning()
    
    assert warning_result["status"] == "warning"
    assert warning_result["exit_code"] == SUCCESS_WITH_WARNINGS
    assert len(warning_result["warnings"]) == 1
    
    print("PASS: Warning result created correctly")
    
    # Test error result
    result_err = ModResult("test_mod", "test_instance_err")
    result_err.add_error("Critical failure")
    error_result = result_err.error()
    
    assert error_result["status"] == "error"
    assert error_result["exit_code"] == RUNTIME_ERROR
    assert len(error_result["errors"]) == 1
    
    print("PASS: Error result created correctly")


def test_convenience_functions():
    """Test validation_error and runtime_error convenience functions."""
    print("\n=== Test: Convenience Functions ===")
    
    # Test validation error
    val_error = validation_error("test_mod", "Invalid parameter")
    assert val_error["status"] == "error"
    assert val_error["exit_code"] == VALIDATION_ERROR
    assert len(val_error["errors"]) == 1
    assert val_error["errors"][0]["message"] == "Invalid parameter"
    
    print("PASS: validation_error function works")
    
    # Test runtime error
    run_error = runtime_error("test_mod", "Execution failed")
    assert run_error["status"] == "error" 
    assert run_error["exit_code"] == RUNTIME_ERROR
    assert len(run_error["errors"]) == 1
    assert run_error["errors"][0]["message"] == "Execution failed"
    
    print("PASS: runtime_error function works")


def test_result_serialization():
    """Test that results can be serialized to JSON."""
    print("\n=== Test: Result Serialization ===")
    
    result = ModResult("test_mod", "test_instance")
    result.add_metric("count", 42)
    result.add_artifact("data", [1, 2, 3])
    result.add_global("status", "complete")
    result.add_warning("Test warning")
    
    success_result = result.success()
    
    # Test JSON serialization
    try:
        json_str = json.dumps(success_result, default=str)
        parsed_back = json.loads(json_str)
        
        assert parsed_back["status"] == "success"
        assert parsed_back["metrics"]["count"] == 42
        assert parsed_back["artifacts"]["data"] == [1, 2, 3]
        
        print("PASS: Result serializes to JSON correctly")
        
    except Exception as e:
        print(f"FAIL: JSON serialization failed: {e}")
        raise


def main():
    """Run all result system tests."""
    print("Starting Result System Tests...")
    print("=" * 50)
    
    try:
        test_basic_result_creation()
        test_warnings_and_errors() 
        test_metrics_and_artifacts()
        test_globals()
        test_result_states()
        test_convenience_functions()
        test_result_serialization()
        
        print("\n" + "=" * 50)
        print("ALL RESULT SYSTEM TESTS PASSED")
        return True
        
    except Exception as e:
        print(f"\nFAIL: Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)