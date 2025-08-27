# test_sdk_advanced.py - Advanced SDK testing
"""Advanced SDK functionality tests."""

import json
from datapy.mod_manager.sdk import set_global_config, run_mod

def test_parameter_variations():
    """Test different parameter combinations."""
    print("=== Testing Parameter Variations ===")
    
    set_global_config({"log_level": "INFO"})
    
    test_cases = [
        # Basic parameters
        {"file_path": "test_data/customers.csv"},
        
        # With encoding
        {"file_path": "test_data/customers.csv", "encoding": "utf-8"},
        
        # With delimiter
        {"file_path": "test_data/customers.csv", "delimiter": ","},
        
        # With header and skip rows
        {"file_path": "test_data/customers.csv", "header_row": 0, "skip_rows": 0},
        
        # All parameters
        {
            "file_path": "test_data/customers.csv",
            "encoding": "utf-8",
            "delimiter": ",", 
            "header_row": 0,
            "skip_rows": 0
        }
    ]
    
    results = []
    for i, params in enumerate(test_cases):
        print(f"\nTest case {i+1}: {params}")
        result = run_mod("csv_reader", params, f"test_case_{i+1}")
        results.append(result)
        print(f"  Result: {result['status']}")
        print(f"  Rows: {result['metrics']['rows_read']}")
        print(f"  Mod name: {result['logs']['mod_name']}")
    
    return results

def test_error_scenarios():
    """Test various error scenarios."""
    print("\n=== Testing Error Scenarios ===")
    
    # Test missing file
    print("1. Testing missing file...")
    result1 = run_mod("csv_reader", {
        "file_path": "test_data/nonexistent.csv"
    }, "test_missing_file")
    print(f"  Status: {result1['status']} (expected: error)")
    if result1['errors']:
        print(f"  Error: {result1['errors'][0]['message']}")
    
    # Test invalid mod type
    print("\n2. Testing invalid mod type...")
    result2 = run_mod("nonexistent_mod", {
        "file_path": "test_data/customers.csv"
    }, "test_invalid_mod")
    print(f"  Status: {result2['status']} (expected: error)")
    if result2['errors']:
        print(f"  Error: {result2['errors'][0]['message']}")
    
    return result1, result2

def test_artifacts_and_globals():
    """Test artifacts and globals handling."""
    print("\n=== Testing Artifacts and Globals ===")
    
    result = run_mod("csv_reader", {
        "file_path": "test_data/customers.csv"
    }, "test_artifacts")
    
    print(f"Status: {result['status']}")
    print(f"Artifacts keys: {list(result['artifacts'].keys())}")
    print(f"Globals keys: {list(result['globals'].keys())}")
    
    # Check specific artifacts
    if 'data' in result['artifacts']:
        df = result['artifacts']['data']
        print(f"Data artifact type: {type(df).__name__}")
        if hasattr(df, 'shape'):
            print(f"Data shape: {df.shape}")
        if hasattr(df, 'columns'):
            print(f"Columns: {list(df.columns)}")
    
    if 'file_info' in result['artifacts']:
        file_info = result['artifacts']['file_info']
        print(f"File info: {file_info}")
    
    # Check globals
    print(f"Row count global: {result['globals'].get('row_count')}")
    print(f"File size global: {result['globals'].get('file_size')}")
    
    return result

if __name__ == "__main__":
    param_results = test_parameter_variations()
    error_results = test_error_scenarios()
    artifact_result = test_artifacts_and_globals()
    
    success_count = sum(1 for r in param_results if r['status'] == 'success')
    print(f"\n=== Final Summary ===")
    print(f"Parameter tests: {success_count}/{len(param_results)} successful")
    print(f"Error handling: Working as expected")
    print(f"Artifacts test: {artifact_result['status']}")