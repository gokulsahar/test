"""
Test 08: SDK System
Tests the Python SDK functionality including mod execution, parameter resolution,
context integration, and result handling.
"""

import sys
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory, NamedTemporaryFile

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datapy.mod_manager.sdk import (
    run_mod, set_context, clear_context, set_log_level
)
from datapy.mod_manager.registry import ModRegistry
from datapy.mod_manager.result import SUCCESS, SUCCESS_WITH_WARNINGS, VALIDATION_ERROR, RUNTIME_ERROR
from datapy.mod_manager.logger import reset_logging


def create_test_mod_file(temp_dir: str, mod_name: str, mod_behavior: str = "success") -> str:
    """Create a temporary test mod file with specified behavior."""
    
    if mod_behavior == "success":
        run_function = '''
    mod_name = params.get("_mod_name", "test_mod")
    result = ModResult("test_mod", mod_name)
    
    # Process parameters
    test_param = params.get("test_param", "default")
    optional_param = params.get("optional_param", "default_optional")
    
    # Add metrics and artifacts
    result.add_metric("test_metric", 42)
    result.add_metric("param_value", test_param)
    result.add_artifact("test_data", [1, 2, 3])
    result.add_artifact("processed_param", test_param)
    result.add_global("test_global", "global_value")
    
    return result.success()
'''
    elif mod_behavior == "warning":
        run_function = '''
    mod_name = params.get("_mod_name", "test_mod")
    result = ModResult("test_mod", mod_name)
    
    result.add_warning("Test warning message")
    result.add_metric("warning_count", 1)
    
    return result.warning()
'''
    elif mod_behavior == "error":
        run_function = '''
    mod_name = params.get("_mod_name", "test_mod")
    result = ModResult("test_mod", mod_name)
    
    result.add_error("Test error message")
    
    return result.error()
'''
    elif mod_behavior == "invalid_result":
        run_function = '''
    return "not a dict"  # Invalid result
'''
    else:
        run_function = '''
    return {"invalid": "result_structure"}  # Missing required fields
'''
    
    mod_content = f'''"""
Test mod for SDK testing.
"""

from datapy.mod_manager.result import ModResult
from datapy.mod_manager.base import ModMetadata, ConfigSchema

METADATA = ModMetadata(
    type="{mod_name}",
    version="1.0.0",
    description="Test mod for SDK system testing",
    category="solo",
    input_ports=[],
    output_ports=["result"],
    globals=["test_global"],
    packages=[],
    python_version=">=3.8"
)

CONFIG_SCHEMA = ConfigSchema(
    required={{
        "test_param": {{
            "type": "str",
            "description": "Test required parameter"
        }}
    }},
    optional={{
        "optional_param": {{
            "type": "str",
            "default": "default_optional",
            "description": "Test optional parameter"
        }}
    }}
)

def run(params):
    """Test run function."""
    {run_function}
'''
    
    mod_file = Path(temp_dir) / f"{mod_name}.py"
    with open(mod_file, 'w') as f:
        f.write(mod_content)
    
    return str(mod_file)


def create_test_registry_with_mods(mod_specs: list) -> str:
    """Create test registry with specified mods."""
    registry_data = {
        "_metadata": {
            "version": "1.0.0",
            "created": "2024-01-01",
            "description": "Test Registry for SDK",
            "last_updated": "2024-01-01"
        },
        "mods": {}
    }
    
    for mod_name, module_path in mod_specs:
        registry_data["mods"][mod_name] = {
            "module_path": module_path,
            "type": mod_name,
            "version": "1.0.0",
            "description": f"Test mod {mod_name}",
            "category": "solo",
            "input_ports": [],
            "output_ports": ["result"],
            "globals": ["test_global"],
            "packages": [],
            "python_version": ">=3.8",
            "config_schema": {
                "required": {
                    "test_param": {
                        "type": "str",
                        "description": "Test required parameter"
                    }
                },
                "optional": {
                    "optional_param": {
                        "type": "str",
                        "default": "default_optional",
                        "description": "Test optional parameter"
                    }
                }
            },
            "registered_at": "2024-01-01T00:00:00"
        }
    
    with NamedTemporaryFile(mode='w', suffix='_registry.json', delete=False, encoding='utf-8') as f:
        json.dump(registry_data, f, indent=2)
        return f.name


def create_test_context_file(context_data: dict) -> str:
    """Create temporary context file."""
    with NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
        json.dump(context_data, f, indent=2)
        return f.name


def cleanup_file(file_path: str) -> None:
    """Clean up temporary file."""
    try:
        if os.path.exists(file_path):
            os.unlink(file_path)
    except:
        pass


def test_basic_mod_execution():
    """Test basic mod execution with auto-generated name."""
    print("=== Test: Basic Mod Execution ===")
    
    with TemporaryDirectory() as temp_dir:
        sys.path.insert(0, temp_dir)
        
        try:
            # Create test mod
            create_test_mod_file(temp_dir, "basic_test_mod", "success")
            
            # Create registry with test mod
            registry_file = create_test_registry_with_mods([("basic_test_mod", "basic_test_mod")])
            
            try:
                # Temporarily override global registry
                from datapy.mod_manager import registry
                original_registry = registry._global_registry
                registry._global_registry = ModRegistry(registry_file)
                
                # Execute mod with auto-generated name
                result = run_mod("basic_test_mod", {"test_param": "test_value"})
                
                # Validate result structure
                assert result["status"] == "success"
                assert result["exit_code"] == SUCCESS
                assert "execution_time" in result
                assert isinstance(result["execution_time"], (int, float))
                
                # Validate result contents
                assert "metrics" in result
                assert "artifacts" in result
                assert "globals" in result
                assert "warnings" in result
                assert "errors" in result
                assert "logs" in result
                
                # Validate specific data
                assert result["metrics"]["test_metric"] == 42
                assert result["metrics"]["param_value"] == "test_value"
                assert result["artifacts"]["test_data"] == [1, 2, 3]
                assert result["artifacts"]["processed_param"] == "test_value"
                assert result["globals"]["test_global"] == "global_value"
                
                # Validate logs
                assert result["logs"]["mod_type"] == "basic_test_mod"
                assert result["logs"]["mod_name"].startswith("basic_test_mod_")
                assert "run_id" in result["logs"]
                
                print("PASS: Basic mod execution successful")
                
            finally:
                registry._global_registry = original_registry
                cleanup_file(registry_file)
                
        finally:
            sys.path.remove(temp_dir)


def test_explicit_mod_name_execution():
    """Test mod execution with explicit mod name."""
    print("\n=== Test: Explicit Mod Name Execution ===")
    
    with TemporaryDirectory() as temp_dir:
        sys.path.insert(0, temp_dir)
        
        try:
            create_test_mod_file(temp_dir, "named_test_mod", "success")
            registry_file = create_test_registry_with_mods([("named_test_mod", "named_test_mod")])
            
            try:
                from datapy.mod_manager import registry
                original_registry = registry._global_registry
                registry._global_registry = ModRegistry(registry_file)
                
                # Execute with explicit name
                result = run_mod("named_test_mod", {"test_param": "explicit_test"}, "my_custom_name")
                
                assert result["status"] == "success"
                assert result["logs"]["mod_name"] == "my_custom_name"
                assert result["logs"]["mod_type"] == "named_test_mod"
                
                print("PASS: Explicit mod name execution works")
                
            finally:
                registry._global_registry = original_registry
                cleanup_file(registry_file)
                
        finally:
            sys.path.remove(temp_dir)


def test_mod_execution_with_warnings():
    """Test mod execution that returns warnings."""
    print("\n=== Test: Mod Execution with Warnings ===")
    
    with TemporaryDirectory() as temp_dir:
        sys.path.insert(0, temp_dir)
        
        try:
            create_test_mod_file(temp_dir, "warning_test_mod", "warning")
            registry_file = create_test_registry_with_mods([("warning_test_mod", "warning_test_mod")])
            
            try:
                from datapy.mod_manager import registry
                original_registry = registry._global_registry
                registry._global_registry = ModRegistry(registry_file)
                
                result = run_mod("warning_test_mod", {"test_param": "warning_test"})
                
                assert result["status"] == "warning"
                assert result["exit_code"] == SUCCESS_WITH_WARNINGS
                assert len(result["warnings"]) == 1
                assert "Test warning message" in result["warnings"][0]["message"]
                assert result["metrics"]["warning_count"] == 1
                
                print("PASS: Mod execution with warnings works")
                
            finally:
                registry._global_registry = original_registry
                cleanup_file(registry_file)
                
        finally:
            sys.path.remove(temp_dir)


def test_mod_execution_with_errors():
    """Test mod execution that returns errors."""
    print("\n=== Test: Mod Execution with Errors ===")
    
    with TemporaryDirectory() as temp_dir:
        sys.path.insert(0, temp_dir)
        
        try:
            create_test_mod_file(temp_dir, "error_test_mod", "error")
            registry_file = create_test_registry_with_mods([("error_test_mod", "error_test_mod")])
            
            try:
                from datapy.mod_manager import registry
                original_registry = registry._global_registry
                registry._global_registry = ModRegistry(registry_file)
                
                result = run_mod("error_test_mod", {"test_param": "error_test"})
                
                assert result["status"] == "error"
                assert result["exit_code"] == RUNTIME_ERROR
                assert len(result["errors"]) == 1
                assert "Test error message" in result["errors"][0]["message"]
                
                print("PASS: Mod execution with errors works")
                
            finally:
                registry._global_registry = original_registry
                cleanup_file(registry_file)
                
        finally:
            sys.path.remove(temp_dir)


def test_mod_not_found_error():
    """Test execution of non-existent mod."""
    print("\n=== Test: Mod Not Found Error ===")
    
    # Try to execute non-existent mod
    result = run_mod("nonexistent_mod", {"test_param": "value"})
    
    assert result["status"] == "error"
    assert result["exit_code"] == VALIDATION_ERROR
    assert len(result["errors"]) == 1
    error_message = result["errors"][0]["message"]
    assert "not found in registry" in error_message
    assert "Register it with:" in error_message
    
    print("PASS: Mod not found error handled correctly")


def test_parameter_validation_errors():
    """Test parameter validation error handling."""
    print("\n=== Test: Parameter Validation Errors ===")
    
    with TemporaryDirectory() as temp_dir:
        sys.path.insert(0, temp_dir)
        
        try:
            create_test_mod_file(temp_dir, "validation_test_mod", "success")
            registry_file = create_test_registry_with_mods([("validation_test_mod", "validation_test_mod")])
            
            try:
                from datapy.mod_manager import registry
                original_registry = registry._global_registry
                registry._global_registry = ModRegistry(registry_file)
                
                # Test missing required parameter
                result = run_mod("validation_test_mod", {})  # Missing test_param
                
                assert result["status"] == "error"
                assert result["exit_code"] == VALIDATION_ERROR
                assert len(result["errors"]) == 1
                assert "Missing required parameters" in result["errors"][0]["message"]
                assert "test_param" in result["errors"][0]["message"]
                
                print("PASS: Parameter validation errors handled correctly")
                
            finally:
                registry._global_registry = original_registry
                cleanup_file(registry_file)
                
        finally:
            sys.path.remove(temp_dir)


def test_context_integration():
    """Test SDK integration with context variable substitution."""
    print("\n=== Test: Context Integration ===")
    
    # Create test context
    context_data = {
        "config": {
            "test_value": "from_context",
            "number_value": 123,
            "bool_value": True
        },
        "paths": {
            "base": "/data"
        }
    }
    
    context_file = create_test_context_file(context_data)
    
    with TemporaryDirectory() as temp_dir:
        sys.path.insert(0, temp_dir)
        
        try:
            create_test_mod_file(temp_dir, "context_test_mod", "success")
            registry_file = create_test_registry_with_mods([("context_test_mod", "context_test_mod")])
            
            try:
                from datapy.mod_manager import registry
                original_registry = registry._global_registry
                registry._global_registry = ModRegistry(registry_file)
                
                # Set context
                set_context(context_file)
                
                # Execute with context variables
                result = run_mod("context_test_mod", {
                    "test_param": "${config.test_value}",
                    "optional_param": "path_${paths.base}"
                })
                
                assert result["status"] == "success"
                assert result["metrics"]["param_value"] == "from_context"
                assert result["artifacts"]["processed_param"] == "from_context"
                
                print("PASS: Context integration works")
                
                # Clear context
                clear_context()
                
                # Test without context - variables should remain unchanged
                result = run_mod("context_test_mod", {
                    "test_param": "${config.test_value}",
                    "optional_param": "unchanged"
                })
                
                assert result["status"] == "success"
                assert result["metrics"]["param_value"] == "${config.test_value}"
                
                print("PASS: Context clearing works")
                
            finally:
                registry._global_registry = original_registry
                cleanup_file(registry_file)
                clear_context()
                
        finally:
            sys.path.remove(temp_dir)
            cleanup_file(context_file)


def test_invalid_mod_structure():
    """Test handling of mods with invalid structure."""
    print("\n=== Test: Invalid Mod Structure ===")
    
    with TemporaryDirectory() as temp_dir:
        sys.path.insert(0, temp_dir)
        
        try:
            # Create mod with invalid return value
            create_test_mod_file(temp_dir, "invalid_result_mod", "invalid_result")
            registry_file = create_test_registry_with_mods([("invalid_result_mod", "invalid_result_mod")])
            
            try:
                from datapy.mod_manager import registry
                original_registry = registry._global_registry
                registry._global_registry = ModRegistry(registry_file)
                
                result = run_mod("invalid_result_mod", {"test_param": "test"})
                
                assert result["status"] == "error"
                assert result["exit_code"] == RUNTIME_ERROR
                assert "must return a dictionary" in result["errors"][0]["message"]
                
                print("PASS: Invalid mod return type handled")
                
                # Test mod with missing result fields
                create_test_mod_file(temp_dir, "incomplete_result_mod", "incomplete")
                registry_file2 = create_test_registry_with_mods([("incomplete_result_mod", "incomplete_result_mod")])
                
                registry._global_registry = ModRegistry(registry_file2)
                
                result = run_mod("incomplete_result_mod", {"test_param": "test"})
                
                assert result["status"] == "error"
                assert "missing required fields" in result["errors"][0]["message"]
                
                print("PASS: Incomplete mod result handled")
                
            finally:
                registry._global_registry = original_registry
                cleanup_file(registry_file)
                
        finally:
            sys.path.remove(temp_dir)


def test_input_validation():
    """Test SDK input validation."""
    print("\n=== Test: SDK Input Validation ===")
    
    # Test invalid mod_type
    try:
        run_mod("", {"param": "value"})
        assert False, "Should fail with empty mod_type"
    except:
        # Should return error result, not raise exception
        result = run_mod("", {"param": "value"})
        assert result["status"] == "error"
        print("PASS: Empty mod_type validation works")
    
    # Test invalid params
    try:
        run_mod("test_mod", "not a dict")
        assert False, "Should fail with non-dict params"
    except:
        # Should return error result, not raise exception
        result = run_mod("test_mod", "not a dict")
        assert result["status"] == "error"
        print("PASS: Non-dict params validation works")
    
    # Test invalid mod_name
    try:
        run_mod("test_mod", {}, "")
        assert False, "Should fail with empty mod_name"
    except:
        # Should return error result, not raise exception
        result = run_mod("test_mod", {}, "")
        assert result["status"] == "error"
        print("PASS: Empty mod_name validation works")


def test_log_level_configuration():
    """Test log level configuration via SDK."""
    print("\n=== Test: Log Level Configuration ===")
    
    try:
        # Test valid log levels
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        
        for level in valid_levels:
            set_log_level(level)
            print(f"PASS: Log level {level} set successfully")
        
        # Test invalid log level
        try:
            set_log_level("INVALID_LEVEL")
            assert False, "Should fail with invalid log level"
        except ValueError as e:
            assert "Invalid log level" in str(e)
            print("PASS: Invalid log level rejected")
        
        # Test empty log level
        try:
            set_log_level("")
            assert False, "Should fail with empty log level"
        except ValueError as e:
            assert "must be a non-empty string" in str(e)
            print("PASS: Empty log level rejected")
            
    finally:
        # Reset logging to clean state
        try:
            reset_logging()
        except:
            pass


def test_context_api_functions():
    """Test context API functions."""
    print("\n=== Test: Context API Functions ===")
    
    # Test setting context
    context_data = {"test": {"value": "context_test"}}
    context_file = create_test_context_file(context_data)
    
    try:
        set_context(context_file)
        print("PASS: Context file set successfully")
        
        # Test clearing context
        clear_context()
        print("PASS: Context cleared successfully")
        
        # Test invalid context file path
        try:
            set_context("")
            assert False, "Should fail with empty context path"
        except ValueError as e:
            assert "must be a non-empty string" in str(e)
            print("PASS: Empty context path rejected")
        
    finally:
        cleanup_file(context_file)
        clear_context()


def test_auto_generated_names():
    """Test auto-generated mod name uniqueness."""
    print("\n=== Test: Auto-generated Names ===")
    
    with TemporaryDirectory() as temp_dir:
        sys.path.insert(0, temp_dir)
        
        try:
            create_test_mod_file(temp_dir, "name_test_mod", "success")
            registry_file = create_test_registry_with_mods([("name_test_mod", "name_test_mod")])
            
            try:
                from datapy.mod_manager import registry
                original_registry = registry._global_registry
                registry._global_registry = ModRegistry(registry_file)
                
                # Execute multiple times to test name uniqueness
                result1 = run_mod("name_test_mod", {"test_param": "test1"})
                result2 = run_mod("name_test_mod", {"test_param": "test2"})
                
                assert result1["status"] == "success"
                assert result2["status"] == "success"
                
                name1 = result1["logs"]["mod_name"]
                name2 = result2["logs"]["mod_name"]
                
                assert name1 != name2
                assert name1.startswith("name_test_mod_")
                assert name2.startswith("name_test_mod_")
                
                print("PASS: Auto-generated names are unique")
                
            finally:
                registry._global_registry = original_registry
                cleanup_file(registry_file)
                
        finally:
            sys.path.remove(temp_dir)


def main():
    """Run all SDK system tests."""
    print("Starting SDK System Tests...")
    print("=" * 50)
    
    try:
        test_basic_mod_execution()
        test_explicit_mod_name_execution()
        test_mod_execution_with_warnings()
        test_mod_execution_with_errors()
        test_mod_not_found_error()
        test_parameter_validation_errors()
        test_context_integration()
        test_invalid_mod_structure()
        test_input_validation()
        test_log_level_configuration()
        test_context_api_functions()
        test_auto_generated_names()
        
        print("\n" + "=" * 50)
        print("ALL SDK SYSTEM TESTS PASSED")
        return True
        
    except Exception as e:
        print(f"\nFAIL: Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)