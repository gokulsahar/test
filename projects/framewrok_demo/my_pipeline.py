#!/usr/bin/env python3
"""
DataPy Framework Demo - Clean Pipeline Structure
"""

from datapy.mod_manager.sdk import run_mod, setup_logging, setup_context, get_context_value


def pre_run():
    """Setup logging and context."""
    logger = setup_logging("INFO", "my_piepeline.py")  
    setup_context("bank_config.json")
    logger.info("Starting client processing pipeline")
    return logger


def run_pipeline(logger):
    """Main pipeline execution."""
    clients = run_mod("csv_reader", {"file_path": "${data.input_path}/clients.csv"})
    if clients["status"] != "success":
        return clients

    logger.info(f"clientresult ::\n{clients}")
    # Use context to control whether filtering is needed
    enable_filtering = get_context_value("pipeline.enable_filtering")
    if enable_filtering:
        logger.info("Filtering enabled - applying business rules")
        filtered = run_mod("csv_filter", { "data": clients["artifacts"]["data"],"filter_conditions": {"age": {"gte": "${business_rules.adult_age}"},"city": {"in": "${filters.target_cities}"},"account_balance": {"gte": "${business_rules.min_balance}"} },"keep_columns": ["client_id", "name", "age", "city", "account_balance", "account_type"],"sort_by": "account_balance"})        
        if filtered["status"] not in ["success", "warning"]:
            return filtered
        data_for_output = filtered["artifacts"]["filtered_data"]
    else:
        logger.info("Filtering disabled - using all client data")
        data_for_output = clients["artifacts"]["data"]

    # Use context to control output format
    output_format = get_context_value("pipeline.output_format")
    if output_format == "excel":
        report = run_mod("excel_writer", {"data": data_for_output,"output_path": "${data.output_path}/client_report.xlsx","add_title": "${reporting.title}","sheet_name": "${reporting.sheet_name}"})
    else:
        report = run_mod("csv_writer", {"data": data_for_output,"output_path": "${data.output_path}/client_report.csv"})
    
    return report


def post_run(logger, result):
    """Final reporting."""
    if result["status"] in ["success", "warning"]:
        logger.info("Pipeline completed successfully!")
    else:
        logger.error(f"Pipeline failed: {result.get('errors', [])}")


def main():
    """Execute complete pipeline."""
    logger = pre_run()
    result = run_pipeline(logger)
    post_run(logger, result)
    return result["status"] in ["success", "warning"]


if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)