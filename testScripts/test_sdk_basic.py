# test_sdk_basic.py - Basic SDK functionality
"""Basic SDK functionality tests."""

from datapy.mod_manager.sdk import set_global_config, run_mod, get_global_config, clear_global_config

def test_global_config():
    """Test global configuration management."""
    print("=== Testing Global Config ===")
    
    # Test setting config
    set_global_config({"log_level": "DEBUG", "test_var": "test_value"})
    config = get_global_config()
    print(f"Config set: {config}")
    
    # Test updating config
    set_global_config({"log_level": "INFO", "another_var": "another_value"})
    config = get_global_config()
    print(f"Config updated: {config}")
    
    # Test clearing config
    clear_global_config()
    config = get_global_config()
    print(f"Config cleared: {config}")

def test_mod_execution_auto_name():
    """Test mod execution with auto-generated name."""
    print("\n=== Testing Auto-Generated Mod Name ===")
    
    set_global_config({"log_level": "INFO"})
    
    # Test with auto-generated mod_name
    result = run_mod("csv_reader", {"file_path": "test_data/customers.csv"})
    
    print(f"Status: {result['status']}")
    print(f"Execution time: {result['execution_time']}s")
    print(f"Auto-generated mod_name: {result['logs']['mod_name']}")
    print(f"Rows read: {result['metrics']['rows_read']}")
    print(f"File size: {result['metrics']['file_size_bytes']}")
    
    return result

def test_mod_execution_explicit_name():
    """Test mod execution with explicit name."""
    print("\n=== Testing Explicit Mod Name ===")
    
    result = run_mod("csv_reader", {
        "file_path": "test_data/customers.csv",
        "encoding": "utf-8",
        "delimiter": ","
    }, "my_custom_reader")
    
    print(f"Status: {result['status']}")
    print(f"Explicit mod_name: {result['logs']['mod_name']}")
    print(f"Columns: {result['artifacts']['column_names']}")
    print(f"Warnings: {len(result['warnings'])}")
    
    if result['warnings']:
        for warning in result['warnings']:
            print(f"  Warning: {warning['message']}")
    
    return result

if __name__ == "__main__":
    test_global_config()
    result1 = test_mod_execution_auto_name()
    result2 = test_mod_execution_explicit_name()
    
    print(f"\n=== Summary ===")
    print(f"Auto-name test: {result1['status']}")
    print(f"Explicit name test: {result2['status']}")
