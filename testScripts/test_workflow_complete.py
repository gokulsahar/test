"""Complete workflow testing with multiple mod executions."""

from datapy.mod_manager.sdk import set_global_config, run_mod

def test_sequential_workflow():
    """Test sequential mod executions in a workflow."""
    print("=== Complete Workflow Test ===")
    
    # Setup global configuration
    set_global_config({
        "log_level": "INFO",
        "base_path": "test_data",
        "default_encoding": "utf-8"
    })
    
    workflow_results = []
    
    # Step 1: Read customers data
    print("Step 1: Reading customers data...")
    customers_result = run_mod("csv_reader", {
        "file_path": "test_data/customers.csv"
    }, "extract_customers")
    
    workflow_results.append(("extract_customers", customers_result))
    
    if customers_result['status'] != 'success':
        print(f"Step 1 failed: {customers_result['errors']}")
        return False
    
    print(f"  Success: {customers_result['metrics']['rows_read']} rows extracted")
    
    # Step 2: Read same data with different name (simulate multiple sources)
    print("Step 2: Reading backup customer data...")
    backup_result = run_mod("csv_reader", {
        "file_path": "test_data/customers.csv",
        "encoding": "utf-8"
    }, "extract_customers_backup")
    
    workflow_results.append(("extract_customers_backup", backup_result))
    
    print(f"  Success: {backup_result['metrics']['rows_read']} rows extracted")
    
    # Step 3: Demonstrate data access and globals usage
    print("Step 3: Processing extracted data...")
    customers_data = customers_result['artifacts']['data']
    backup_data = backup_result['artifacts']['data']
    
    # Use globals from previous steps
    total_rows = customers_result['globals']['row_count'] + backup_result['globals']['row_count']
    total_size = customers_result['globals']['file_size'] + backup_result['globals']['file_size']
    
    print(f"  Combined processing:")
    print(f"    Total rows: {total_rows}")
    print(f"    Total file size: {total_size} bytes")
    print(f"    Customers shape: {customers_data.shape}")
    print(f"    Backup shape: {backup_data.shape}")
    
    # Step 4: Generate summary
    print("Step 4: Workflow summary...")
    successful_steps = sum(1 for name, result in workflow_results if result['status'] == 'success')
    total_execution_time = sum(result['execution_time'] for name, result in workflow_results)
    
    print(f"  Successful steps: {successful_steps}/{len(workflow_results)}")
    print(f"  Total execution time: {total_execution_time:.3f}s")
    
    return True

def test_multiple_auto_names():
    """Test multiple executions with auto-generated names."""
    print("\n=== Testing Multiple Auto-Generated Names ===")
    
    set_global_config({"log_level": "WARNING"})  # Reduce noise
    
    results = []
    for i in range(5):
        result = run_mod("csv_reader", {
            "file_path": "test_data/customers.csv"
        })  # No explicit mod_name - should auto-generate
        results.append(result)
        print(f"Execution {i+1}: {result['logs']['mod_name']} - {result['status']}")
    
    # Verify all names are unique
    mod_names = [r['logs']['mod_name'] for r in results]
    unique_names = set(mod_names)
    print(f"Generated {len(unique_names)} unique names out of {len(mod_names)} executions")
    
    return results

if __name__ == "__main__":
    workflow_success = test_sequential_workflow()
    auto_name_results = test_multiple_auto_names()
    
    print(f"\n=== Final Results ===")
    print(f"Workflow test: {'SUCCESS' if workflow_success else 'FAILED'}")
    print(f"Auto-name test: {len(auto_name_results)} executions completed")