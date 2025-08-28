#!/usr/bin/env python3
"""
DataPy Framework Demo - Python SDK Pipeline

This script demonstrates the complete DataPy workflow:
1. Reading CSV data
2. Filtering and transforming data
3. Writing results to output

Features demonstrated:
- SDK mod execution
- Context variable substitution
- Parameter chaining between mods
- Error handling and logging
- Result validation
"""

import sys
import os
from pathlib import Path

# Add project root to Python path (for development)
# Go up 3 levels: filter_job -> demo_project -> projects -> datapy_root
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from datapy.mod_manager.sdk import run_mod, set_context, set_log_level
from datapy.mod_manager.logger import setup_logger

def setup_demo_environment():
    """Set up the demo environment and logging."""
    print("=== DataPy Framework Demo - Python SDK ===")
    
    # Set up logging
    set_log_level("INFO")
    logger = setup_logger(__name__)
    
    # Set context for variable substitution (relative to this script)
    context_file = Path(__file__).parent / "context.json"
    set_context(str(context_file))
    
    logger.info("Demo environment initialized", extra={
        "context_file": str(context_file),
        "working_directory": str(Path.cwd())
    })
    
    return logger

def execute_pipeline():
    """Execute the complete data processing pipeline."""
    logger = setup_logger(__name__)
    
    try:
        print("\n--- Step 1: Reading CSV Data ---")
        
        # Step 1: Read CSV data (relative to demo_project root)
        read_result = run_mod("csv_reader", {
            "file_path": "../data/customers.csv",  # Up one level from filter_job
            "encoding": "utf-8",
            "delimiter": ",",
            "header": 0
        }, "read_customers")
        
        if read_result["status"] != "success":
            print(f"ERROR: Failed to read CSV data: {read_result['errors']}")
            return False
        
        customer_data = read_result["artifacts"]["data"]
        print(f" Read {read_result['metrics']['rows_read']} customers from CSV")
        print(f"   Columns: {list(customer_data.columns)}")
        
        print("\n--- Step 2: Filtering Data ---")
        
        # Step 2: Filter data (adults in major cities)
        filter_result = run_mod("csv_filter", {
            "data": customer_data,
            "filter_conditions": {
                "age": {"gte": 25},  # Adults 25+
                "city": {"in": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]}
            },
            "keep_columns": ["name", "age", "city", "email"],
            "drop_duplicates": True,
            "sort_by": "age"
        }, "filter_young_customers")
        
        if filter_result["status"] != "success":
            print(f"ERROR: Failed to filter data: {filter_result['errors']}")
            return False
        
        filtered_data = filter_result["artifacts"]["filtered_data"]
        filter_rate = filter_result["metrics"]["filter_rate"]
        print(f" Filtered to {filter_result['metrics']['filtered_rows']} customers")
        print(f"   Filter rate: {filter_rate:.2%} removed")
        
        # Show sample of filtered data
        print("\n   Sample filtered data:")
        if not filtered_data.empty:
            print(filtered_data.head().to_string(index=False))
        else:
            print("   (No data matched filter criteria)")
        
        print("\n--- Step 3: Writing Results ---")
        
        # Step 3: Write filtered data to output (relative to demo_project root)
        write_result = run_mod("csv_writer", {
            "data": filtered_data,
            "output_path": "../output/filtered_customers.csv",  # Up one level from filter_job
            "encoding": "utf-8",
            "include_header": True,
            "create_directories": True,
            "backup_existing": True
        }, "write_filtered_data")
        
        if write_result["status"] != "success":
            print(f"ERROR: Failed to write output: {write_result['errors']}")
            return False
        
        output_path = write_result["artifacts"]["output_path"]
        file_size = write_result["metrics"]["file_size_bytes"]
        print(f" Wrote {write_result['metrics']['rows_written']} rows to {output_path}")
        print(f"   File size: {file_size} bytes")
        
        print("\n--- Pipeline Summary ---")
        print(f" Original rows: {read_result['metrics']['rows_read']}")
        print(f" Filtered rows: {filter_result['metrics']['filtered_rows']}")
        print(f" Rows written: {write_result['metrics']['rows_written']}")
        print(f" Processing efficiency: {(1-filter_rate):.2%} data retained")
        
        return True
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        print(f"\n Pipeline failed: {e}")
        return False

def demonstrate_context_variables():
    """Demonstrate context variable substitution."""
    print("\n=== Context Variable Substitution Demo ===")
    
    logger = setup_logger(__name__)
    
    try:
        # Use context variables in mod execution
        result = run_mod("csv_reader", {
            "file_path": "${env.data_path}/customers.csv",  # From context
            "encoding": "utf-8"
        }, "context_demo")
        
        if result["status"] == "success":
            print(" Context variable substitution working")
            print(f"   Resolved path: {result['artifacts']['file_path']}")
            return True
        else:
            print(" Context variable substitution failed")
            return False
            
    except Exception as e:
        print(f" Context demo failed: {e}")
        return False

def demonstrate_error_handling():
    """Demonstrate error handling capabilities."""
    print("\n=== Error Handling Demo ===")
    
    # Try to read non-existent file
    result = run_mod("csv_reader", {
        "file_path": "../data/nonexistent.csv"
    }, "error_demo")
    
    if result["status"] == "error":
        print(" Error handling working correctly")
        print(f"   Error: {result['errors'][0]['message']}")
        print(f"   Exit code: {result['exit_code']}")
        return True
    else:
        print(" Expected error but got success")
        return False

def main():
    """Main demo execution."""
    try:
        # Setup environment
        logger = setup_demo_environment()
        
        # Check if required files exist (relative to this script's directory)
        required_files = [
            Path("../data/customers.csv"),  # Up one level to demo_project/data/
            Path("./context.json")          # Same directory as script
        ]
        
        missing_files = []
        for f in required_files:
            full_path = Path(__file__).parent / f
            if not full_path.exists():
                missing_files.append(str(f))
        
        if missing_files:
            print(f"\n Missing required files: {missing_files}")
            print("Please ensure demo data files are in place.")
            print(f"Current working directory: {Path.cwd()}")
            print(f"Script directory: {Path(__file__).parent}")
            return False
        
        # Run pipeline
        pipeline_success = execute_pipeline()
        
        if pipeline_success:
            print("\n Pipeline completed successfully!")
        else:
            print("\n Pipeline failed!")
            return False
        
        # Additional demos
        context_success = demonstrate_context_variables()
        error_success = demonstrate_error_handling()
        
        # Final summary
        print("\n" + "="*60)
        print("DEMO SUMMARY")
        print("="*60)
        print(f" Main Pipeline: {'PASSED' if pipeline_success else 'FAILED'}")
        print(f" Context Variables: {'PASSED' if context_success else 'FAILED'}")
        print(f" Error Handling: {'PASSED' if error_success else 'FAILED'}")
        
        if all([pipeline_success, context_success, error_success]):
            print("\n All DataPy framework features working correctly!")
            return True
        else:
            print("\n Some demo components failed.")
            return False
        
    except Exception as e:
        print(f"\n Demo failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)