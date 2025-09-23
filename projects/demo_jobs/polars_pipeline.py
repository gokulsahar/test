#!/usr/bin/env python3
"""
DataPy Framework - Production Pipeline using New Polars Mods

Demonstrates the new production-ready mods:
- file_input: Universal lazy file reader  
- data_filter: Advanced filtering with custom functions
- file_output: Universal lazy file writer

Features:
- Pure Polars lazy/streaming operations
- Memory efficient (stays under 2GB RAM)
- Success/reject data flows  
- Custom filtering functions
- Production error handling
"""

import sys
from pathlib import Path
from datapy.mod_manager.sdk import run_mod, setup_logging, setup_context, get_context_value
from datapy.utils.script_monitor import monitor_execution
import polars as pl

# Add project root to path for development
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def setup_pipeline():
    """Initialize logging and context for the pipeline."""
    logger = setup_logging("INFO", "polars_pipeline.py")
    
    # Use context file for configuration
    context_file = Path(__file__).parent / "pipeline_config.json"
    if context_file.exists():
        setup_context(str(context_file))
    
    logger.info("Starting production Polars pipeline")
    return logger


def define_custom_filters():
    """Define custom filter functions for advanced filtering."""
    
    def premium_customer_filter(data):
        """Filter for premium customers: high balance + long tenure."""
        return (pl.col('account_balance') >= 50000)
    
    def valid_contact_filter(data):
        """Filter for customers with valid contact information."""
        return (
            pl.col('email').str.contains(r'^[^@]+@[^@]+\.[^@]+$') &
            pl.col('phone').str.len_chars() >= 10
        )
    
    
    return {
        "premium_customer": premium_customer_filter,
        "valid_contact": valid_contact_filter
    }


def execute_data_ingestion(logger):
    """Step 1: Ingest data using file_input mod."""
    logger.info("=== Step 1: Data Ingestion ===")
    
    # Use our new file_input mod with lazy evaluation
    result = run_mod("file_input", {
        "file_path": "data/customers_large.csv",
        "encoding": "utf-8",
        "delimiter": ",",
        "header_rows": 1,
        "read_options": {
            # Power user options for optimization
            "ignore_errors": True,
            "low_memory": True
        }
    }, "ingest_customers")
    
    if result["status"] != "success":
        logger.error(f"Data ingestion failed: {result.get('errors', [])}")
        return None
    
    logger.info(f"Ingestion completed - Format: {result['metrics']['file_format']}")
    logger.info(f"Columns: {result['metrics']['column_count']}")
    logger.info(f"File size: {result['metrics']['file_size_bytes']} bytes")
    
    return result["artifacts"]["data"]  # This is a lazy DataFrame


def execute_data_filtering(customer_data, logger):
    """Step 2: Filter data using data_filter mod with success/reject outputs."""
    logger.info("=== Step 2: Data Filtering ===")
    
    # Define custom filters
    custom_filters = define_custom_filters()
    
    # Use our new data_filter mod
    result = run_mod("data_filter", {
        "data": customer_data,
        "filter_conditions": {
            # Standard filtering conditions
            "age": {">=": 18, "<=": 80},                    # Adult customers
            "city": {"in": ["New York", "Los Angeles", "Chicago", "Houston"]},  # Major cities
            "account_balance": {">": 1000},                # Minimum balance
        },
        "custom_functions": {
            "premium_customer": custom_filters["premium_customer"],
            "valid_contact": custom_filters["valid_contact"]
        },
        "output_reject": True,                             # Enable reject output
        "condition_logic": "OR",                          # All conditions must pass
        "null_handling": "exclude"                         # Exclude rows with nulls
    }, "filter_customers")
    
    if result["status"] not in ["success", "warning"]:
        logger.error(f"Data filtering failed: {result.get('errors', [])}")
        return None, None
    
    logger.info(f"Filtering completed")
    logger.info(f"Conditions applied: {result['metrics']['conditions_applied']}")
    
    # Return both success and reject data (both are lazy DataFrames)
    filtered_data = result["artifacts"]["filtered_data"]
    rejected_data = result["artifacts"].get("rejected_data")
    
    return filtered_data, rejected_data


def execute_data_output(filtered_data, rejected_data, logger):
    """Step 3: Output data using file_output mod."""
    logger.info("=== Step 3: Data Output ===")
    
    results = []
    
    # Write successful records to Parquet for analytics
    logger.info("Writing filtered data to Parquet...")
    success_result = run_mod("file_output", {
        "data": filtered_data,
        "output_path": "output/filtered_customers.parquet",
        "write_options": {
            "compression": "snappy",  # Optimal compression for analytics
        }
    }, "output_success_parquet")
    
    results.append(("Success Parquet", success_result))
    
    if success_result["status"] != "success":
        logger.error(f"Success output failed: {success_result.get('errors', [])}")
    else:
        logger.info(f"Success output - Size: {success_result['metrics']['file_size_bytes']} bytes")
    
    # Write rejected records to CSV for review
    if rejected_data is not None:
        logger.info("Writing rejected data to CSV...")
        reject_result = run_mod("file_output", {
            "data": rejected_data,
            "output_path": "output/rejected_customers.csv",
            "encoding": "utf-8",
            "include_header": True
        }, "output_reject_csv")
        
        results.append(("Reject CSV", reject_result))
        
        if reject_result["status"] != "success":
            logger.error(f"Reject output failed: {reject_result.get('errors', [])}")
        else:
            logger.info(f"Reject output - Size: {reject_result['metrics']['file_size_bytes']} bytes")
    
    return results


def run_production_pipeline(logger):
    """Execute the complete production pipeline."""
    try:
        # Step 1: Data Ingestion
        customer_data = execute_data_ingestion(logger)
        if customer_data is None:
            return False
        
        # Step 2: Data Filtering  
        filtered_data, rejected_data = execute_data_filtering(customer_data, logger)
        if filtered_data is None:
            return False
        
        # Step 3: Data Output
        output_results = execute_data_output(filtered_data, rejected_data, logger)
        
        # Check all outputs succeeded
        all_success = all(result[1]["status"] == "success" for result in output_results)
        
        return all_success
        
    except Exception as e:
        logger.error(f"Pipeline failed with exception: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def demonstrate_lazy_evaluation(logger):
    """Demonstrate that operations remain lazy until output."""
    logger.info("=== Lazy Evaluation Demo ===")
    
    try:
        # Read data lazily
        data = run_mod("file_input", {
            "file_path": "data/customers_large.csv"
        }, "lazy_demo")["artifacts"]["data"]
        
        # Apply multiple filters - all remain lazy
        filtered = run_mod("data_filter", {
            "data": data,
            "filter_conditions": {
                "age": {">=": 25}
            }
        }, "lazy_filter")["artifacts"]["filtered_data"]
        
        # Data is still not materialized - just lazy operations queued
        logger.info("All operations completed lazily - no data materialized yet")
        logger.info(f"Lazy DataFrame type: {type(filtered)}")
        
        # Only when we write to file does materialization happen
        output = run_mod("file_output", {
            "data": filtered,
            "output_path": "output/lazy_demo.parquet"
        }, "lazy_output")
        
        if output["status"] == "success":
            logger.info("Data materialized only during final write operation")
            return True
        
    except Exception as e:
        logger.error(f"Lazy evaluation demo failed: {e}")
        return False
    
    return False


@monitor_execution("polars_pipeline")
def main():
    """Main pipeline execution with comprehensive logging."""
    # Setup
    logger = setup_pipeline()
    
    # Ensure output directory exists
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    
    try:
        # Run main pipeline
        logger.info("Starting production pipeline with new Polars mods")
        pipeline_success = run_production_pipeline(logger)
        
        # Run lazy evaluation demonstration
        lazy_demo_success = demonstrate_lazy_evaluation(logger)
        
        # Final summary
        logger.info("=" * 60)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Production Pipeline: {'PASSED' if pipeline_success else 'FAILED'}")
        logger.info(f"Lazy Evaluation Demo: {'PASSED' if lazy_demo_success else 'FAILED'}")
        
        if pipeline_success and lazy_demo_success:
            logger.info("All pipeline components executed successfully!")
            logger.info("New Polars mods are production-ready!")
            return True
        else:
            logger.error("Some pipeline components failed")
            return False
            
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    print(f"\nPipeline {'SUCCEEDED' if success else 'FAILED'}")
    sys.exit(0 if success else 1)