# quick_test.py - Quick individual test runner
"""Quick test runner for DataPy framework components."""

import sys
import subprocess
from datapy.mod_manager.sdk import set_global_config, run_mod

def test_registry():
    """Test registry functionality."""
    print(" Testing Registry...")
    
    try:
        # Register mod
        result = subprocess.run([
            "python", "-m", "datapy", "register-mod", "datapy.mods.sources.csv_reader"
        ], capture_output=True, text=True)
        
        # List registry
        result = subprocess.run([
            "python", "-m", "datapy", "list-registry"
        ], capture_output=True, text=True)
        
        if "csv_reader" in result.stdout:
            print(" Registry working - csv_reader found")
            return True
        else:
            print(" Registry issue - csv_reader not found")
            return False
            
    except Exception as e:
        print(f" Registry error: {e}")
        return False

def test_sdk():
    """Test SDK functionality."""
    print(" Testing SDK...")
    
    try:
        set_global_config({"log_level": "ERROR"})  # Quiet mode
        
        # Test basic execution
        result = run_mod("csv_reader", {"file_path": "test_data/customers.csv"})
        
        if result['status'] == 'success':
            rows = result['metrics']['rows_read']
            print(f" SDK working - read {rows} rows")
            return True
        else:
            print(f" SDK error: {result['errors']}")
            return False
            
    except Exception as e:
        print(f" SDK error: {e}")
        return False

def test_cli():
    """Test CLI functionality.""" 
    print(" Testing CLI...")
    
    try:
        result = subprocess.run([
            "python", "-m", "datapy", "run-mod", "extract_customers", 
            "--params", "test_basic_job.yaml", "-q"
        ], capture_output=True, text=True)
        
        if result.returncode == 0 and "success" in result.stdout:
            print(" CLI working - mod executed successfully")
            return True
        else:
            print(f" CLI error: {result.stderr}")
            return False
            
    except Exception as e:
        print(f" CLI error: {e}")
        return False

def test_logging():
    """Test logging functionality."""
    print(" Testing Logging...")
    
    try:
        # Run with log capture
        result = subprocess.run([
            "python", "-m", "datapy", "run-mod", "extract_customers", 
            "--params", "test_basic_job.yaml"
        ], capture_output=True, text=True)
        
        # Check for JSON structure in stderr (where logs go)
        stderr_lines = result.stderr.strip().split('\n')
        json_lines = [line for line in stderr_lines if line.strip().startswith('{')]
        
        if len(json_lines) > 0:
            import json
            # Try to parse first JSON line
            log_entry = json.loads(json_lines[0])
            if 'timestamp' in log_entry and 'level' in log_entry:
                print("Logging working - JSON structure valid")
                return True
        
        print(" Logging issue - no valid JSON logs found")
        return False
        
    except Exception as e:
        print(f" Logging error: {e}")
        return False

def run_all_tests():
    """Run all quick tests."""
    print(" DataPy Framework - Quick Test Suite")
    print("=" * 50)
    
    tests = [
        ("Registry", test_registry),
        ("SDK", test_sdk), 
        ("CLI", test_cli),
        ("Logging", test_logging)
    ]
    
    results = []
    for name, test_func in tests:
        try:
            success = test_func()
            results.append((name, success))
        except Exception as e:
            print(f" {name} crashed: {e}")
            results.append((name, False))
        print()
    
    # Summary
    print("=" * 50)
    print(" Test Summary:")
    passed = sum(1 for name, success in results if success)
    total = len(results)
    
    for name, success in results:
        status = " PASS" if success else "❌ FAIL"
        print(f"  {name:10} {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print(" All tests passed! Framework is working correctly.")
        return True
    else:
        print("  Some tests failed. Check the output above.")
        return False

def setup_test_environment():
    """Setup minimal test environment."""
    import os
    
    # Create test data if it doesn't exist
    if not os.path.exists("test_data/customers.csv"):
        os.makedirs("test_data", exist_ok=True)
        with open("test_data/customers.csv", "w") as f:
            f.write("id,name,email,age,city\n")
            f.write("1,John Doe,john@email.com,25,New York\n")
            f.write("2,Jane Smith,jane@email.com,30,\n")
            f.write("3,Bob Johnson,bob@email.com,35,Chicago\n")
        print(" Created test_data/customers.csv")
    
    # Create basic YAML config if it doesn't exist
    if not os.path.exists("test_basic_job.yaml"):
        with open("test_basic_job.yaml", "w") as f:
            f.write("globals:\n")
            f.write("  log_level: \"INFO\"\n")
            f.write("mods:\n")
            f.write("  extract_customers:\n")
            f.write("    _type: csv_reader\n")
            f.write("    file_path: \"test_data/customers.csv\"\n")
        print(" Created test_basic_job.yaml")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        test_name = sys.argv[1].lower()
        
        if test_name == "setup":
            setup_test_environment()
        elif test_name == "registry":
            test_registry()
        elif test_name == "sdk":
            setup_test_environment()
            test_sdk()
        elif test_name == "cli":
            setup_test_environment()
            test_cli()
        elif test_name == "logging":
            setup_test_environment() 
            test_logging()
        else:
            print(f"Unknown test: {test_name}")
            print("Available tests: setup, registry, sdk, cli, logging")
    else:
        # Run all tests
        setup_test_environment()
        success = run_all_tests()
        sys.exit(0 if success else 1)