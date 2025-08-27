import subprocess
import json
from datapy.mod_manager.sdk import set_global_config, run_mod

print("=== Integration Test Debug ===")

# Test SDK execution
print("1. Testing SDK...")
set_global_config({'log_level': 'ERROR'})
sdk_result = run_mod('csv_reader', {'file_path': 'test_data/customers.csv'}, 'integration_test_sdk')
print(f"SDK result: {sdk_result['status']} - {sdk_result['metrics']['rows_read']} rows")

# Test CLI execution and capture both stdout and stderr
print("\n2. Testing CLI...")
cli_process = subprocess.run([
    'python', '-m', 'datapy', 'run-mod', 'extract_customers', 
    '--params', 'test_basic_job.yaml', '--log-level', 'ERROR'
], capture_output=True, text=True)

print("CLI return code:", cli_process.returncode)
print("CLI stdout length:", len(cli_process.stdout))
print("CLI stderr length:", len(cli_process.stderr))

print("\n--- CLI stdout ---")
print(repr(cli_process.stdout))  # Use repr to see exact content

print("\n--- CLI stderr ---")
print(repr(cli_process.stderr))

# Try to parse CLI output
try:
    if cli_process.stdout.strip():
        cli_result = json.loads(cli_process.stdout)
        print(f"\n✓ CLI JSON parsed successfully")
        print(f"CLI result: {cli_result['status']} - {cli_result['metrics']['rows_read']} rows")
        
        # Compare results
        if sdk_result['metrics']['rows_read'] == cli_result['metrics']['rows_read']:
            print("✓ Results match!")
        else:
            print("✗ Results don't match")
    else:
        print("\n✗ CLI stdout is empty")
        
except json.JSONDecodeError as e:
    print(f"\n✗ CLI JSON parsing failed: {e}")
    print("Raw stdout content:", cli_process.stdout)