"""
Test 10: Integration Tests
Comprehensive end-to-end testing of DataPy framework components working together.
"""

import sys
import json
import os
import subprocess
import yaml
import pandas as pd
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
import shutil

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datapy.mod_manager.sdk import run_mod, set_context, clear_context, set_log_level
from datapy.mod_manager.registry import get_registry
from datapy.mod_manager.result import SUCCESS, SUCCESS_WITH_WARNINGS, VALIDATION_ERROR, RUNTIME_ERROR


class IntegrationTestSuite:
    """Integration test suite with setup/teardown for DataPy framework."""
    
    def __init__(self):
        self.temp_files = []
        self.original_registry_state = None
        self.project_root = Path(__file__).parent.parent
        
    def setup(self):
        """Set up test environment."""
        print("=== Integration Test Setup ===")
        
        # Save original registry state
        try:
            registry = get_registry()
            self.original_registry_state = list(registry.list_available_mods())
            print(f"Original registry has {len(self.original_registry_state)} mods")
        except Exception as e:
            print(f"Registry setup warning: {e}")
            self.original_registry_state = []
        
        # Set up logging
        set_log_level("INFO")
        
        # Clear any existing context
        clear_context()
        
        print("PASS: Test environment setup complete")
        
    def teardown(self):
        """Clean up test environment."""
        print("\n=== Integration Test Teardown ===")
        
        # Clean up temporary files
        for file_path in self.temp_files:
            try:
                if os.path.exists(file_path):
                    os.unlink(file_path)
            except:
                pass
        
        # Clear context
        clear_context()
        
        print("PASS: Test environment cleaned up")
    
    def create_temp_csv(self, content_rows: list) -> str:
        """Create temporary CSV file with given rows."""
        with NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            # Write header
            f.write("name,age,city\n")
            # Write data rows
            for row in content_rows:
                f.write(f"{row[0]},{row[1]},{row[2]}\n")
            
            temp_path = f.name
            self.temp_files.append(temp_path)
            return temp_path
    
    def create_temp_yaml(self, content: dict) -> str:
        """Create temporary YAML file."""
        with NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8') as f:
            yaml.dump(content, f, default_flow_style=False)
            temp_path = f.name
            self.temp_files.append(temp_path)
            return temp_path
    
    def create_temp_json(self, content: dict) -> str:
        """Create temporary JSON file."""
        with NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
            json.dump(content, f, indent=2)
            temp_path = f.name
            self.temp_files.append(temp_path)
            return temp_path
    
    def run_cli_command(self, *args) -> dict:
        """Run DataPy CLI command and return result."""
        cli_script = self.project_root / "datapy" / "__main__.py"
        
        env = os.environ.copy()
        env['PYTHONPATH'] = str(self.project_root)
        
        cmd = [sys.executable, str(cli_script)] + list(args)
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
            cwd=self.project_root
        )
        
        return {
            'exit_code': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'success': result.returncode == 0
        }


def test_csv_reader_mod_availability():
    """Test that csv_reader mod is available or can be registered."""
    print("\n=== Test: CSV Reader Mod Availability ===")
    
    try:
        registry = get_registry()
        available_mods = registry.list_available_mods()
        
        if 'csv_reader' in available_mods:
            print("PASS: csv_reader mod already registered")
            return True
        
        # Try to register csv_reader mod
        print("Attempting to register csv_reader mod...")
        success = registry.register_mod('datapy.mods.sources.csv_reader')
        
        if success:
            print("PASS: csv_reader mod registered successfully")
            return True
        else:
            print("WARNING: csv_reader mod registration failed")
            return False
            
    except Exception as e:
        print(f"ERROR: csv_reader mod availability check failed: {e}")
        return False


def test_basic_sdk_execution(suite: IntegrationTestSuite):
    """Test basic mod execution via SDK."""
    print("\n=== Test: Basic SDK Execution ===")
    
    # Create test CSV data
    csv_data = [
        ("John", 25, "New York"),
        ("Jane", 30, "Los Angeles"), 
        ("Bob", 35, "Chicago")
    ]
    csv_file = suite.create_temp_csv(csv_data)
    
    try:
        # Execute csv_reader mod via SDK
        result = run_mod("csv_reader", {
            "file_path": csv_file,
            "encoding": "utf-8"
        }, "test_basic_execution")
        
        # Validate result structure
        assert result["status"] == "success", f"Expected success, got {result['status']}"
        assert result["exit_code"] == SUCCESS, f"Expected exit code {SUCCESS}, got {result['exit_code']}"
        
        # Validate metrics
        assert result["metrics"]["rows_read"] == 3, f"Expected 3 rows, got {result['metrics']['rows_read']}"
        assert result["metrics"]["columns_read"] == 3, f"Expected 3 columns, got {result['metrics']['columns_read']}"
        
        # Validate artifacts
        assert "data" in result["artifacts"], "Expected 'data' artifact"
        data = result["artifacts"]["data"]
        assert isinstance(data, pd.DataFrame), f"Expected DataFrame, got {type(data)}"
        assert len(data) == 3, f"Expected 3 rows in DataFrame, got {len(data)}"
        
        # Validate globals
        assert result["globals"]["row_count"] == 3, f"Expected row_count=3, got {result['globals']['row_count']}"
        assert result["globals"]["column_count"] == 3, f"Expected column_count=3, got {result['globals']['column_count']}"
        
        print("PASS: Basic SDK execution works correctly")
        return True
        
    except Exception as e:
        print(f"FAIL: Basic SDK execution failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_parameter_resolution_chain(suite: IntegrationTestSuite):
    """Test parameter resolution with project defaults, job params, and context."""
    print("\n=== Test: Parameter Resolution Chain ===")
    
    try:
        # Create context file with environment variables
        context_data = {
            "env": {
                "data_path": "/tmp/test_data",
                "encoding": "utf-8"
            },
            "settings": {
                "delimiter": ",",
                "header_row": 0
            }
        }
        context_file = suite.create_temp_json(context_data)
        
        # Create test CSV
        csv_data = [("Alice", 28, "Boston"), ("Charlie", 32, "Seattle")]
        csv_file = suite.create_temp_csv(csv_data)
        
        # Set context
        set_context(context_file)
        
        # Test parameter resolution with context variables
        result = run_mod("csv_reader", {
            "file_path": csv_file,  # Direct value (highest priority)
            "encoding": "${env.encoding}",  # From context
            "delimiter": "${settings.delimiter}",  # From context  
            "header": "${settings.header_row}"  # From context
        }, "test_param_resolution")
        
        assert result["status"] == "success", f"Expected success, got {result['status']}"
        assert result["metrics"]["encoding_used"] == "utf-8", f"Context encoding not resolved"
        assert result["metrics"]["delimiter_used"] == ",", f"Context delimiter not resolved"
        
        print("PASS: Parameter resolution chain works correctly")
        return True
        
    except Exception as e:
        print(f"FAIL: Parameter resolution test failed: {e}")
        return False
    finally:
        clear_context()


def test_cli_integration(suite: IntegrationTestSuite):
    """Test CLI integration with YAML configuration."""
    print("\n=== Test: CLI Integration ===")
    
    try:
        # Create test data
        csv_data = [("David", 29, "Phoenix"), ("Eve", 31, "Denver")]
        csv_file = suite.create_temp_csv(csv_data)
        
        # Create YAML job configuration
        yaml_config = {
            "globals": {
                "log_level": "INFO"
            },
            "mods": {
                "extract_test_data": {
                    "_type": "csv_reader",
                    "file_path": csv_file,
                    "encoding": "utf-8",
                    "delimiter": ","
                }
            }
        }
        yaml_file = suite.create_temp_yaml(yaml_config)
        
        # Execute via CLI
        cli_result = suite.run_cli_command("run-mod", "extract_test_data", "--params", yaml_file)
        
        assert cli_result["success"], f"CLI command failed: {cli_result['stderr']}"
        
        # Parse JSON output from CLI - handle indented JSON and log lines
        output_lines = cli_result["stdout"].strip().split('\n')
        json_output = None
        
        # Strategy 1: Look for a single line with complete JSON
        for line in output_lines:
            line = line.strip()
            if line.startswith('{"result"'):
                try:
                    json_output = json.loads(line)
                    break
                except json.JSONDecodeError:
                    continue
        
        # Strategy 2: Look for multi-line JSON (indented format)
        if json_output is None:
            # Find the start of JSON block (line that starts with just '{')
            json_start = -1
            for i, line in enumerate(output_lines):
                if line.strip() == '{':
                    # Check if next line contains "result"
                    if i + 1 < len(output_lines) and '"result"' in output_lines[i + 1]:
                        json_start = i
                        break
            
            if json_start >= 0:
                # Find the end of JSON block (line that starts with just '}')
                json_end = -1
                brace_count = 0
                for i in range(json_start, len(output_lines)):
                    line = output_lines[i].strip()
                    brace_count += line.count('{') - line.count('}')
                    if brace_count == 0 and i > json_start:
                        json_end = i
                        break
                
                if json_end > json_start:
                    # Combine JSON lines
                    json_lines = output_lines[json_start:json_end + 1]
                    json_text = '\n'.join(json_lines)
                    try:
                        json_output = json.loads(json_text)
                    except json.JSONDecodeError:
                        pass
        
        assert json_output is not None, f"No JSON output found in CLI result. Output lines: {output_lines[:10]}"
        
        result = json_output["result"]
        assert result["status"] == "success", f"CLI execution failed: {result}"
        assert result["metrics"]["rows_read"] == 2, f"Expected 2 rows, got {result['metrics']['rows_read']}"
        
        print("PASS: CLI integration works correctly")
        return True
        
    except Exception as e:
        print(f"FAIL: CLI integration test failed: {e}")
        return False


def test_context_substitution_integration(suite: IntegrationTestSuite):
    """Test context variable substitution in full workflow."""
    print("\n=== Test: Context Substitution Integration ===")
    
    try:
        # Create context with nested variables
        context_data = {
            "database": {
                "host": "localhost", 
                "port": 5432,
                "ssl_enabled": True
            },
            "files": {
                "input_path": "/data/input",
                "encoding": "utf-8"
            },
            "processing": {
                "batch_size": 1000,
                "enable_validation": False
            }
        }
        context_file = suite.create_temp_json(context_data)
        
        # Create test CSV
        csv_data = [("Frank", 27, "Miami"), ("Grace", 33, "Portland")]
        csv_file = suite.create_temp_csv(csv_data)
        
        # Create YAML with context variables
        yaml_config = {
            "globals": {
                "log_level": "INFO"
            },
            "mods": {
                "extract_with_context": {
                    "_type": "csv_reader",
                    "file_path": csv_file,  # Direct file path
                    "encoding": "${files.encoding}",  # From context - string
                    "header": 0,  # Direct value - int
                    "max_rows": "${processing.batch_size}"  # From context - int (should preserve type)
                }
            }
        }
        yaml_file = suite.create_temp_yaml(yaml_config)
        
        # Execute via CLI with context
        cli_result = suite.run_cli_command("run-mod", "extract_with_context", 
                                         "--params", yaml_file, 
                                         "--context", context_file)
        
        assert cli_result["success"], f"CLI with context failed: {cli_result['stderr']}"
        
        # Validate context substitution worked - handle indented JSON and log lines
        output_lines = cli_result["stdout"].strip().split('\n')
        json_output = None
        
        # Strategy 1: Look for single-line JSON
        for line in output_lines:
            line = line.strip()
            if line.startswith('{"result"'):
                try:
                    json_output = json.loads(line)
                    break
                except json.JSONDecodeError:
                    continue
        
        # Strategy 2: Look for multi-line JSON (indented format)
        if json_output is None:
            # Find JSON block starting with '{' and containing "result"
            json_start = -1
            for i, line in enumerate(output_lines):
                if line.strip() == '{':
                    # Check if this is the result JSON block
                    if i + 1 < len(output_lines) and '"result"' in output_lines[i + 1]:
                        json_start = i
                        break
            
            if json_start >= 0:
                # Find the end of JSON block
                brace_count = 0
                json_end = -1
                for i in range(json_start, len(output_lines)):
                    line = output_lines[i].strip()
                    brace_count += line.count('{') - line.count('}')
                    if brace_count == 0 and i > json_start:
                        json_end = i
                        break
                
                if json_end > json_start:
                    json_lines = output_lines[json_start:json_end + 1]
                    json_text = '\n'.join(json_lines)
                    try:
                        json_output = json.loads(json_text)
                    except json.JSONDecodeError:
                        pass
        
        assert json_output is not None, f"No JSON output found. First 10 lines: {output_lines[:10]}"
        result = json_output["result"]
        
        assert result["status"] == "success", f"Context substitution execution failed"
        assert result["metrics"]["encoding_used"] == "utf-8", "Context encoding not substituted"
        
        print("PASS: Context substitution integration works correctly")
        return True
        
    except Exception as e:
        print(f"FAIL: Context substitution integration failed: {e}")
        return False


def test_error_handling_integration(suite: IntegrationTestSuite):
    """Test error handling across components."""
    print("\n=== Test: Error Handling Integration ===")
    
    try:
        # Test 1: File not found error
        result = run_mod("csv_reader", {
            "file_path": "/nonexistent/file.csv"
        }, "test_error_handling")
        
        assert result["status"] == "error", f"Expected error status, got {result['status']}"
        assert result["exit_code"] == RUNTIME_ERROR, f"Expected exit code {RUNTIME_ERROR}"
        assert len(result["errors"]) > 0, "Expected error messages"
        assert "not found" in result["errors"][0]["message"].lower(), "Expected file not found error"
        
        # Test 2: Invalid YAML via CLI
        invalid_yaml_content = "invalid: yaml: content: {"
        with NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8') as f:
            f.write(invalid_yaml_content)
            invalid_yaml = f.name
            suite.temp_files.append(invalid_yaml)
        
        cli_result = suite.run_cli_command("run-mod", "test_mod", "--params", invalid_yaml)
        assert not cli_result["success"], "Expected CLI failure with invalid YAML"
        
        # Test 3: Missing mod in YAML
        valid_yaml_missing_mod = {
            "mods": {
                "other_mod": {"_type": "csv_reader", "file_path": "/test.csv"}
            }
        }
        yaml_file = suite.create_temp_yaml(valid_yaml_missing_mod)
        
        cli_result = suite.run_cli_command("run-mod", "missing_mod", "--params", yaml_file)
        assert not cli_result["success"], "Expected CLI failure with missing mod"
        
        print("PASS: Error handling integration works correctly")
        return True
        
    except Exception as e:
        print(f"FAIL: Error handling integration failed: {e}")
        return False


def test_registry_integration(suite: IntegrationTestSuite):
    """Test registry operations integration."""
    print("\n=== Test: Registry Integration ===")
    
    try:
        # Test registry CLI commands
        cli_result = suite.run_cli_command("list-registry")
        assert cli_result["success"], f"list-registry failed: {cli_result['stderr']}"
        
        cli_result = suite.run_cli_command("validate-registry") 
        assert cli_result["success"], f"validate-registry failed: {cli_result['stderr']}"
        
        # Test mod-info for csv_reader (if registered)
        cli_result = suite.run_cli_command("mod-info", "csv_reader")
        if cli_result["success"]:
            assert "csv_reader" in cli_result["stdout"], "mod-info should show csv_reader details"
            print("PASS: mod-info works for csv_reader")
        else:
            print("INFO: csv_reader not registered, skipping mod-info test")
        
        print("PASS: Registry integration works correctly")
        return True
        
    except Exception as e:
        print(f"FAIL: Registry integration failed: {e}")
        return False


def test_logging_integration(suite: IntegrationTestSuite):
    """Test logging integration across components."""
    print("\n=== Test: Logging Integration ===")
    
    try:
        # Test different log levels
        original_level = "INFO"
        
        for level in ["DEBUG", "INFO", "WARNING", "ERROR"]:
            set_log_level(level)
            
            # Execute a simple mod
            csv_data = [("Test", 1, "City")]
            csv_file = suite.create_temp_csv(csv_data)
            
            result = run_mod("csv_reader", {
                "file_path": csv_file
            }, f"test_logging_{level.lower()}")
            
            assert result["status"] == "success", f"Logging test failed at level {level}"
            assert "mod_type" in result["logs"], "Expected mod_type in logs"
            assert "mod_name" in result["logs"], "Expected mod_name in logs"
        
        # Reset to original level
        set_log_level(original_level)
        
        print("PASS: Logging integration works correctly")
        return True
        
    except Exception as e:
        print(f"FAIL: Logging integration failed: {e}")
        return False


def test_cross_platform_compatibility(suite: IntegrationTestSuite):
    """Test cross-platform file handling."""
    print("\n=== Test: Cross-Platform Compatibility ===")
    
    try:
        # Test with different path formats
        csv_data = [("Platform", 1, "Test")]
        csv_file = suite.create_temp_csv(csv_data)
        
        # Convert to Path object for cross-platform compatibility
        csv_path = Path(csv_file)
        
        result = run_mod("csv_reader", {
            "file_path": str(csv_path),
            "encoding": "utf-8"
        }, "test_cross_platform")
        
        assert result["status"] == "success", "Cross-platform path handling failed"
        
        # Test CLI with cross-platform paths
        yaml_config = {
            "mods": {
                "platform_test": {
                    "_type": "csv_reader",
                    "file_path": str(csv_path)
                }
            }
        }
        yaml_file = suite.create_temp_yaml(yaml_config)
        
        cli_result = suite.run_cli_command("run-mod", "platform_test", "--params", yaml_file)
        assert cli_result["success"], "Cross-platform CLI execution failed"
        
        print("PASS: Cross-platform compatibility works correctly")
        return True
        
    except Exception as e:
        print(f"FAIL: Cross-platform compatibility failed: {e}")
        return False


def main():
    """Run all integration tests."""
    print("Starting DataPy Framework Integration Tests")
    print("=" * 60)
    
    suite = IntegrationTestSuite()
    
    try:
        # Setup test environment
        suite.setup()
        
        # Run integration tests in dependency order
        tests = [
            test_csv_reader_mod_availability,
            lambda: test_basic_sdk_execution(suite),
            lambda: test_parameter_resolution_chain(suite),
            lambda: test_cli_integration(suite),
            lambda: test_context_substitution_integration(suite),
            lambda: test_error_handling_integration(suite),
            lambda: test_registry_integration(suite),
            lambda: test_logging_integration(suite),
            lambda: test_cross_platform_compatibility(suite)
        ]
        
        results = []
        for test in tests:
            try:
                result = test()
                results.append(result)
            except Exception as e:
                print(f"FAIL: Test failed with exception: {e}")
                results.append(False)
        
        # Summary
        passed = sum(results)
        total = len(results)
        
        print("\n" + "=" * 60)
        print("INTEGRATION TEST SUMMARY")
        print("=" * 60)
        print(f"Tests passed: {passed}/{total}")
        
        if passed == total:
            print("SUCCESS: All integration tests passed!")
            print("\nDataPy Framework is working correctly end-to-end.")
            return True
        else:
            print(f"FAILURE: {total - passed} integration tests failed.")
            return False
        
    except Exception as e:
        print(f"FAIL: Integration test suite failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Always clean up
        suite.teardown()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)