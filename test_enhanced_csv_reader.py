"""
Enhanced test script for DataPy SDK with rich metadata support.
"""

from datapy.mod_manager.sdk import set_global_config, run_mod
import json

def test_basic_csv_reading():
    """Test basic CSV reading functionality."""
    print("=== Basic CSV Reading Test ===")
    
    # Set global configuration
    set_global_config({
        "log_level": "INFO",
        "log_path": "test_logs"
    })
    
    # Test with minimal parameters (required only)
    result = run_mod("csv_reader", {
        "file_path": "test_data\customers.csv"
    }, "test_basic_reader")
    
    print(f"Status: {result['status']}")
    print(f"Exit Code: {result['exit_code']}")
    print(f"Execution Time: {result['execution_time']}s")
    
    # Check metrics
    metrics = result['metrics']
    print(f"\nMetrics:")
    print(f"  Rows read: {metrics['rows_read']}")
    print(f"  Columns read: {metrics['columns_read']}")
    print(f"  File size: {metrics['file_size_bytes']} bytes")
    print(f"  Missing values: {metrics['missing_values']}")
    print(f"  Duplicate rows: {metrics['duplicate_rows']}")
    
    # Check artifacts
    artifacts = result['artifacts']
    print(f"\nArtifacts:")
    print(f"  Data shape: {artifacts['data'].shape}")
    print(f"  Columns: {artifacts['column_names']}")
    print(f"  Source file: {artifacts['source_file']}")
    
    # Check globals
    globals_produced = result['globals']
    print(f"\nGlobals Produced:")
    print(f"  Row count: {globals_produced['row_count']}")
    print(f"  File size: {globals_produced['file_size']}")
    # Note: source_encoding removed from globals (passed as parameters instead)
    
    # Check warnings
    if result['warnings']:
        print(f"\nWarnings ({len(result['warnings'])}):")
        for warning in result['warnings']:
            print(f"  - {warning['message']}")
    
    return result

def test_advanced_csv_reading():
    """Test CSV reading with advanced parameters."""
    print("\n=== Advanced CSV Reading Test ===")
    
    # Test with all optional parameters
    result = run_mod("csv_reader", {
        "file_path": "test_data/customers.csv",
        "encoding": "utf-8",
        "delimiter": ",",
        "header_row": 0,
        "skip_rows": 0
    }, "test_advanced_reader")
    
    print(f"Status: {result['status']}")
    print(f"All parameters processed successfully")
    
    # Check file info artifact
    file_info = result['artifacts']['file_info']
    print(f"\nFile Info:")
    print(f"  Path: {file_info['path']}")
    print(f"  Encoding: {file_info['encoding']}")
    print(f"  Delimiter: {file_info['delimiter']}")
    print(f"  Shape: {file_info['shape']}")
    
    return result

def test_error_handling():
    """Test error handling for invalid inputs."""
    print("\n=== Error Handling Test ===")
    
    # Test missing file
    result = run_mod("csv_reader", {
        "file_path": "test_data/nonexistent.csv"
    }, "test_error_reader")
    
    print(f"Status: {result['status']}")
    print(f"Exit Code: {result['exit_code']}")
    
    if result['errors']:
        print(f"Errors ({len(result['errors'])}):")
        for error in result['errors']:
            print(f"  - {error['message']}")
    
    return result

def main():
    print(" DataPy Enhanced CSV Reader Test Suite")
    print("=" * 50)
    
    # Run all tests
    basic_result = test_basic_csv_reading()
    advanced_result = test_advanced_csv_reading()
    error_result = test_error_handling()
    
    # Summary
    print("\n" + "=" * 50)
    print(" Test Suite Complete!")
    print(f"Basic Test: {basic_result['status']}")
    print(f"Advanced Test: {advanced_result['status']}")
    print(f"Error Test: {error_result['status']}")

if __name__ == "__main__":
    main()