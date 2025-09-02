from datapy.mod_manager.sdk import run_mod, setup_logging, setup_context

logger = setup_logging("INFO", "my_pipeline.py")  
setup_context("bank_config.json")

logger.info("Starting client processing pipeline")


clients = run_mod("csv_reader", {"file_path": "${data.input_path}/clients.csv"})

filtered = run_mod("csv_filter", {
    "data": clients["artifacts"]["data"], 
    "filter_conditions": {
        "age": {"gte": "${business_rules.adult_age}"},
        "city": {"in": "${filters.target_cities}"},
        "account_balance": {"gte": "${business_rules.min_balance}"}
    },
    "keep_columns": ["client_id", "name", "age", "city", "account_balance", "account_type"],
    "sort_by": "account_balance"
})

report = run_mod("excel_writer", {
    "data": filtered["artifacts"]["filtered_data"],
    "output_path": "${data.output_path}/client_report.xlsx",
    "add_title": "${reporting.title}",
    "sheet_name": "${reporting.sheet_name}"
})

print(f"\n=== Results ===")
print(f"Original clients: {clients['metrics']['rows_read']} \n Filtered clients: {filtered['metrics']['filtered_rows']} \n Excel report: {report['artifacts']['output_filename']} \n  Total time: {clients['execution_time'] + filtered['execution_time'] + report['execution_time']:.2f}s")

logger.info("Pipeline completed successfully!")

