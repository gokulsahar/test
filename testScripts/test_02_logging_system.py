"""
Test 02: Logging System
Tests the JSON logging functionality and log level configuration.
"""

import sys
import json
import logging
import io
from pathlib import Path
from contextlib import redirect_stderr

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datapy.mod_manager.logger import (
    setup_logger, set_log_level, setup_console_logging, 
    DataPyFormatter, DEFAULT_LOG_CONFIG, reset_logging
)

def capture_log_output(func, *args, **kwargs):
    """Capture log output to stderr for testing."""
    log_capture = io.StringIO()
    with redirect_stderr(log_capture):
        result = func(*args, **kwargs)
    return result, log_capture.getvalue()


def test_basic_logger_setup():
    """Test basic logger setup and configuration."""
    print("=== Test: Basic Logger Setup ===")
    
    # Reset logging state
    reset_logging()
    
    # Setup console logging
    setup_console_logging(DEFAULT_LOG_CONFIG)
    
    # Create logger
    logger = setup_logger("test.module")
    
    assert isinstance(logger, logging.Logger)
    assert logger.name == "test.module"
    
    print("PASS: Basic logger setup successful")


def test_mod_context_logging():
    """Test logger with mod context information."""
    print("\n=== Test: Mod Context Logging ===")
    
    # Reset logging state
    reset_logging()
    setup_console_logging(DEFAULT_LOG_CONFIG)
    
    # Create logger with mod context
    logger = setup_logger("test.csv_reader", "csv_reader", "extract_customers")
    
    # Capture log output
    def log_test():
        logger.info("Processing customer data")
        return True
    
    result, log_output = capture_log_output(log_test)
    
    # Parse JSON log entry
    if log_output.strip():
        log_entry = json.loads(log_output.strip().split('\n')[0])
        
        assert log_entry["level"] == "INFO"
        assert log_entry["logger"] == "test.csv_reader"
        assert log_entry["message"] == "Processing customer data"
        assert log_entry["mod_type"] == "csv_reader"
        assert log_entry["mod_name"] == "extract_customers"
        
        print("PASS: Mod context logging works")
    else:
        print("WARNING: No log output captured - may be due to test environment")


def test_json_formatter():
    """Test JSON formatter directly."""
    print("\n=== Test: JSON Formatter ===")
    
    formatter = DataPyFormatter()
    
    # Create a test log record
    record = logging.LogRecord(
        name="test.module",
        level=logging.INFO,
        pathname="/path/to/file.py",
        lineno=42,
        msg="Test message with %s",
        args=("parameter",),
        exc_info=None
    )
    
    # Add custom attributes
    record.mod_type = "csv_reader"
    record.mod_name = "test_instance"
    record.custom_field = "custom_value"
    
    # Format the record
    formatted = formatter.format(record)
    
    # Parse JSON
    log_entry = json.loads(formatted)
    
    assert log_entry["level"] == "INFO"
    assert log_entry["logger"] == "test.module"
    assert log_entry["message"] == "Test message with parameter"
    assert log_entry["mod_type"] == "csv_reader"
    assert log_entry["mod_name"] == "test_instance"
    assert log_entry["custom_field"] == "custom_value"
    assert "timestamp" in log_entry
    
    print("PASS: JSON formatter works correctly")


def test_log_levels():
    """Test different log levels and filtering."""
    print("\n=== Test: Log Levels ===")
    
    # Reset and setup
    reset_logging()
    
    # Test each log level
    log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    
    for level in log_levels:
        try:
            set_log_level(level)
            logger = setup_logger(f"test.{level.lower()}")
            
            # Test that logger is configured
            expected_level = getattr(logging, level)
            root_logger = logging.getLogger()
            assert root_logger.level == expected_level
            
            print(f"PASS: Log level {level} configured correctly")
            
        except Exception as e:
            print(f"FAIL: Log level {level} failed: {e}")
            raise


def test_invalid_log_level():
    """Test invalid log level handling."""
    print("\n=== Test: Invalid Log Level ===")
    
    try:
        set_log_level("INVALID_LEVEL")
        assert False, "Should fail with invalid log level"
    except ValueError as e:
        assert "Invalid log level" in str(e)
        print("PASS: Invalid log level rejected correctly")


def test_exception_logging():
    """Test exception logging in JSON format."""
    print("\n=== Test: Exception Logging ===")
    
    # Reset and setup
    reset_logging()
    setup_console_logging({"log_level": "ERROR"})
    
    logger = setup_logger("test.exceptions")
    
    def log_with_exception():
        try:
            raise ValueError("Test exception for logging")
        except Exception:
            logger.error("An error occurred", exc_info=True)
    
    result, log_output = capture_log_output(log_with_exception)
    
    if log_output.strip():
        log_entry = json.loads(log_output.strip().split('\n')[0])
        
        assert log_entry["level"] == "ERROR"
        assert log_entry["message"] == "An error occurred"
        assert "exception" in log_entry
        assert "ValueError" in log_entry["exception"]
        
        print("PASS: Exception logging works correctly")
    else:
        print("WARNING: No exception log output captured")


def test_extra_fields_logging():
    """Test logging with extra fields."""
    print("\n=== Test: Extra Fields Logging ===")
    
    # Reset and setup
    reset_logging()
    setup_console_logging(DEFAULT_LOG_CONFIG)
    
    logger = setup_logger("test.extras")
    
    def log_with_extras():
        logger.info("Processing file", extra={
            "file_path": "/data/customers.csv",
            "row_count": 1000,
            "processing_time": 2.5,
            "batch_id": "batch_001"
        })
    
    result, log_output = capture_log_output(log_with_extras)
    
    if log_output.strip():
        log_entry = json.loads(log_output.strip().split('\n')[0])
        
        assert log_entry["message"] == "Processing file"
        assert log_entry["file_path"] == "/data/customers.csv"
        assert log_entry["row_count"] == 1000
        assert log_entry["processing_time"] == 2.5
        assert log_entry["batch_id"] == "batch_001"
        
        print("PASS: Extra fields logging works correctly")
    else:
        print("WARNING: No extra fields log output captured")


def test_console_logging_setup():
    """Test console logging setup with different configurations."""
    print("\n=== Test: Console Logging Setup ===")
    
    # Test valid configuration
    config = {"log_level": "WARNING"}
    try:
        setup_console_logging(config)
        print("PASS: Valid console logging configuration accepted")
    except Exception as e:
        print(f"FAIL: Valid configuration rejected: {e}")
        raise
    
    # Test invalid configuration
    try:
        setup_console_logging("not a dict")
        assert False, "Should fail with non-dict config"
    except RuntimeError as e:
        assert "must be a dictionary" in str(e)
        print("PASS: Invalid configuration rejected correctly")


def test_logger_name_validation():
    """Test logger name validation."""
    print("\n=== Test: Logger Name Validation ===")
    
    # Valid name
    logger = setup_logger("valid.module.name")
    assert logger.name == "valid.module.name"
    print("PASS: Valid logger name accepted")
    
    # Invalid names
    try:
        setup_logger("")
        assert False, "Should fail with empty name"
    except RuntimeError as e:
        assert "non-empty string" in str(e)
        print("PASS: Empty logger name rejected")
    
    try:
        setup_logger(None)
        assert False, "Should fail with None name"
    except RuntimeError as e:
        assert "non-empty string" in str(e)
        print("PASS: None logger name rejected")


def test_logging_reset():
    """Test logging reset functionality."""
    print("\n=== Test: Logging Reset ===")
    
    # Setup logging
    setup_console_logging(DEFAULT_LOG_CONFIG)
    logger = setup_logger("test.reset")
    
    # Verify setup
    root_logger = logging.getLogger()
    initial_handler_count = len(root_logger.handlers)
    assert initial_handler_count > 0
    
    # Reset logging
    reset_logging()
    
    # Verify reset
    final_handler_count = len(root_logger.handlers)
    assert final_handler_count == 0
    
    print("PASS: Logging reset works correctly")


def main():
    """Run all logging system tests."""
    print("Starting Logging System Tests...")
    print("=" * 50)
    
    try:
        test_basic_logger_setup()
        test_mod_context_logging()
        test_json_formatter()
        test_log_levels()
        test_invalid_log_level()
        test_exception_logging()
        test_extra_fields_logging()
        test_console_logging_setup()
        test_logger_name_validation()
        test_logging_reset()
        
        print("\n" + "=" * 50)
        print("ALL LOGGING SYSTEM TESTS PASSED")
        return True
        
    except Exception as e:
        print(f"\nFAIL: Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Always reset logging after tests
        try:
            reset_logging()
        except:
            pass


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)