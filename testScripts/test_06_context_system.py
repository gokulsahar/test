"""
Test 06: Context System
Tests the context file management and variable substitution functionality with type preservation.
"""

import sys
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datapy.mod_manager.context import (
   set_context, clear_context, substitute_context_variables, get_context_info
)


def create_test_context_file(context_data: dict) -> str:
   """Create temporary context file with given data."""
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


def test_context_file_management():
   """Test setting and clearing context files."""
   print("=== Test: Context File Management ===")
   
   # Test setting context
   test_context_path = "/path/to/context.json"
   set_context(test_context_path)
   
   context_info = get_context_info()
   assert context_info["context_file"] == test_context_path
   assert context_info["context_loaded"] is False
   
   print("PASS: Context file path set correctly")
   
   # Test clearing context
   clear_context()
   
   context_info = get_context_info()
   assert context_info["context_file"] is None
   assert context_info["context_loaded"] is False
   
   print("PASS: Context cleared correctly")
   
   # Test invalid context path
   try:
       set_context("")
       assert False, "Should fail with empty context path"
   except ValueError as e:
       assert "must be a non-empty string" in str(e)
       print("PASS: Empty context path rejected")


def test_basic_variable_substitution():
   """Test basic variable substitution with type preservation."""
   print("\n=== Test: Basic Variable Substitution ===")
   
   # Create test context with various types
   context_data = {
       "database": {
           "host": "localhost",
           "port": 5432,
           "name": "testdb",
           "ssl_enabled": True,
           "timeout": 30.5
       },
       "paths": {
           "input": "/data/input",
           "output": "/data/output"
       },
       "settings": {
           "batch_size": 1000,
           "debug": False,
           "rate_limit": None
       }
   }
   
   context_file = create_test_context_file(context_data)
   
   try:
       set_context(context_file)
       
       # Test pure variable substitution (should preserve types)
       params = {
           "db_host": "${database.host}",
           "db_port": "${database.port}",
           "ssl_enabled": "${database.ssl_enabled}",
           "timeout": "${database.timeout}",
           "input_path": "${paths.input}",
           "batch_size": "${settings.batch_size}",
           "debug_mode": "${settings.debug}",
           "rate_limit": "${settings.rate_limit}"
       }
       
       result = substitute_context_variables(params)
       
       # Verify types are preserved
       assert result["db_host"] == "localhost" and isinstance(result["db_host"], str)
       assert result["db_port"] == 5432 and isinstance(result["db_port"], int)
       assert result["ssl_enabled"] is True and isinstance(result["ssl_enabled"], bool)
       assert result["timeout"] == 30.5 and isinstance(result["timeout"], float)
       assert result["input_path"] == "/data/input" and isinstance(result["input_path"], str)
       assert result["batch_size"] == 1000 and isinstance(result["batch_size"], int)
       assert result["debug_mode"] is False and isinstance(result["debug_mode"], bool)
       assert result["rate_limit"] is None
       
       print("PASS: Basic variable substitution preserves types correctly")
       
   finally:
       cleanup_file(context_file)
       clear_context()


def test_pure_vs_mixed_substitution():
   """Test difference between pure variable substitution and mixed content."""
   print("\n=== Test: Pure vs Mixed Substitution ===")
   
   context_data = {
       "app": {
           "name": "myapp",
           "version": "2.1.0",
           "port": 8080,
           "debug": True
       }
   }
   
   context_file = create_test_context_file(context_data)
   
   try:
       set_context(context_file)
       
       params = {
           # Pure substitutions - preserve original types
           "app_name": "${app.name}",
           "app_port": "${app.port}",
           "debug_flag": "${app.debug}",
           
           # Mixed content - convert to strings
           "docker_image": "${app.name}:${app.version}",
           "log_path": "/var/log/${app.name}.log",
           "service_url": "http://localhost:${app.port}/api",
           "debug_message": "Debug mode: ${app.debug}",
           
           # Multiple variables - convert to strings
           "full_version": "${app.name}-${app.version}",
           "config_info": "${app.name} v${app.version} on port ${app.port}",
           
           # Edge cases with whitespace
           "spaced": " ${app.port} ",
           "prefixed": "port-${app.port}",
           "suffixed": "${app.port}-suffix"
       }
       
       result = substitute_context_variables(params)
       
       # Pure substitutions preserve types
       assert result["app_name"] == "myapp" and isinstance(result["app_name"], str)
       assert result["app_port"] == 8080 and isinstance(result["app_port"], int)
       assert result["debug_flag"] is True and isinstance(result["debug_flag"], bool)
       
       # Mixed content becomes strings
       assert result["docker_image"] == "myapp:2.1.0" and isinstance(result["docker_image"], str)
       assert result["log_path"] == "/var/log/myapp.log" and isinstance(result["log_path"], str)
       assert result["service_url"] == "http://localhost:8080/api" and isinstance(result["service_url"], str)
       assert result["debug_message"] == "Debug mode: True" and isinstance(result["debug_message"], str)
       
       # Multiple variables become strings
       assert result["full_version"] == "myapp-2.1.0" and isinstance(result["full_version"], str)
       assert result["config_info"] == "myapp v2.1.0 on port 8080" and isinstance(result["config_info"], str)
       
       # Whitespace/prefix/suffix cases become strings
       assert result["spaced"] == " 8080 " and isinstance(result["spaced"], str)
       assert result["prefixed"] == "port-8080" and isinstance(result["prefixed"], str)
       assert result["suffixed"] == "8080-suffix" and isinstance(result["suffixed"], str)
       
       print("PASS: Pure vs mixed substitution handled correctly")
       
   finally:
       cleanup_file(context_file)
       clear_context()


def test_nested_variable_substitution():
   """Test nested dictionary and list variable substitution."""
   print("\n=== Test: Nested Variable Substitution ===")
   
   context_data = {
       "env": {
           "name": "production",
           "region": "us-east-1"
       },
       "config": {
           "url": "https://api.example.com",
           "version": "v1",
           "port": 443,
           "secure": True
       }
   }
   
   context_file = create_test_context_file(context_data)
   
   try:
       set_context(context_file)
       
       # Test nested structures with mixed pure and mixed substitutions
       params = {
           "database_config": {
               "host": "${env.name}-db.${env.region}.rds.amazonaws.com",  # Mixed - string
               "port": "${config.port}",  # Pure - int
               "ssl": "${config.secure}",  # Pure - bool
               "timeout": 30  # Unchanged - int
           },
           "api_endpoints": [
               "${config.url}/${config.version}/users",  # Mixed - string
               "${config.port}",  # Pure - int  
               "https://static.example.com/assets"  # Unchanged - string
           ],
           "environment": "${env.name}",  # Pure - string
           "region": "${env.region}",  # Pure - string
           "api_port": "${config.port}"  # Pure - int
       }
       
       result = substitute_context_variables(params)
       
       # Nested dict - mixed types
       assert result["database_config"]["host"] == "production-db.us-east-1.rds.amazonaws.com"
       assert result["database_config"]["port"] == 443 and isinstance(result["database_config"]["port"], int)
       assert result["database_config"]["ssl"] is True and isinstance(result["database_config"]["ssl"], bool)
       assert result["database_config"]["timeout"] == 30  # Unchanged
       
       # Nested list - mixed types
       assert result["api_endpoints"][0] == "https://api.example.com/v1/users"
       assert result["api_endpoints"][1] == 443 and isinstance(result["api_endpoints"][1], int)
       assert result["api_endpoints"][2] == "https://static.example.com/assets"  # Unchanged
       
       # Top level - preserve types
       assert result["environment"] == "production" and isinstance(result["environment"], str)
       assert result["region"] == "us-east-1" and isinstance(result["region"], str)
       assert result["api_port"] == 443 and isinstance(result["api_port"], int)
       
       print("PASS: Nested variable substitution with type preservation works")
       
   finally:
       cleanup_file(context_file)
       clear_context()


def test_no_context_substitution():
   """Test substitution when no context is set."""
   print("\n=== Test: No Context Substitution ===")
   
   clear_context()
   
   params = {
       "normal_key": "normal_value",
       "with_variable": "${some.variable}",
       "nested": {
           "key": "${another.variable}"
       }
   }
   
   # Should return unchanged when no context set
   result = substitute_context_variables(params)
   
   assert result == params
   assert result["normal_key"] == "normal_value"
   assert result["with_variable"] == "${some.variable}"
   assert result["nested"]["key"] == "${another.variable}"
   
   print("PASS: No context substitution returns unchanged params")


def test_no_variables_substitution():
   """Test substitution when no variables are present."""
   print("\n=== Test: No Variables Substitution ===")
   
   context_data = {"test": {"value": "unused"}}
   context_file = create_test_context_file(context_data)
   
   try:
       set_context(context_file)
       
       params = {
           "normal_key": "normal_value",
           "number": 42,
           "boolean": True,
           "list": [1, 2, 3],
           "nested": {
               "inner": "value"
           }
       }
       
       result = substitute_context_variables(params)
       
       # Should return unchanged copy
       assert result == params
       assert result is not params  # Different object
       
       print("PASS: No variables substitution works efficiently")
       
   finally:
       cleanup_file(context_file)
       clear_context()


def test_missing_variable_error():
   """Test error handling for missing variables."""
   print("\n=== Test: Missing Variable Error ===")
   
   context_data = {
       "existing": {
           "key": "value"
       }
   }
   
   context_file = create_test_context_file(context_data)
   
   try:
       set_context(context_file)
       
       # Test missing variable in pure substitution
       params = {
           "valid": "${existing.key}",
           "invalid": "${nonexistent.key}"
       }
       
       try:
           substitute_context_variables(params)
           assert False, "Should fail with missing variable"
       except ValueError as e:
           assert "Context variable not found" in str(e)
           assert "${nonexistent.key}" in str(e)
           print("PASS: Missing variable error handled correctly")
       
       # Test missing variable in mixed content
       mixed_params = {
           "valid": "${existing.key}",
           "invalid_mixed": "prefix-${nonexistent.key}-suffix"
       }
       
       try:
           substitute_context_variables(mixed_params)
           assert False, "Should fail with missing variable in mixed content"
       except ValueError as e:
           assert "Context variable not found" in str(e)
           assert "${nonexistent.key}" in str(e)
           print("PASS: Missing variable in mixed content error handled correctly")
       
   finally:
       cleanup_file(context_file)
       clear_context()


def test_invalid_context_file():
   """Test handling of invalid context files."""
   print("\n=== Test: Invalid Context File ===")
   
   # Test non-existent file
   set_context("/path/that/does/not/exist.json")
   
   params = {"test": "${some.variable}"}
   
   try:
       substitute_context_variables(params)
       assert False, "Should fail with non-existent file"
   except RuntimeError as e:
       assert "Context file not found" in str(e)
       print("PASS: Non-existent context file error handled")
   
   # Test invalid JSON file
   with NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
       f.write("invalid json content {")
       invalid_json_file = f.name
   
   try:
       set_context(invalid_json_file)
       
       try:
           substitute_context_variables(params)
           assert False, "Should fail with invalid JSON"
       except RuntimeError as e:
           assert "Invalid JSON" in str(e)
           print("PASS: Invalid JSON file error handled")
       
   finally:
       cleanup_file(invalid_json_file)
       clear_context()
   
   # Test non-dictionary JSON
   with NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
       json.dump(["not", "a", "dictionary"], f)
       non_dict_file = f.name
   
   try:
       set_context(non_dict_file)
       
       try:
           substitute_context_variables(params)
           assert False, "Should fail with non-dictionary JSON"
       except RuntimeError as e:
           assert "must contain a JSON dictionary" in str(e)
           print("PASS: Non-dictionary JSON error handled")
       
   finally:
       cleanup_file(non_dict_file)
       clear_context()


def test_complex_variable_paths():
   """Test complex variable path resolution with type preservation."""
   print("\n=== Test: Complex Variable Paths ===")
   
   context_data = {
       "level1": {
           "level2": {
               "level3": {
                   "deep_value": "found_it",
                   "deep_number": 999,
                   "deep_bool": False
               }
           }
       },
       "array_like_key": {
           "0": "first_item",
           "1": 42,
           "2": True
       },
       "special-chars": {
           "under_score": "underscore_value",
           "dash-key": "dash_value",
           "number-key": 123
       }
   }
   
   context_file = create_test_context_file(context_data)
   
   try:
       set_context(context_file)
       
       params = {
           "deep_string": "${level1.level2.level3.deep_value}",
           "deep_number": "${level1.level2.level3.deep_number}",
           "deep_bool": "${level1.level2.level3.deep_bool}",
           "array_string": "${array_like_key.0}",
           "array_number": "${array_like_key.1}",
           "array_bool": "${array_like_key.2}",
           "underscore": "${special-chars.under_score}",
           "dash": "${special-chars.dash-key}",
           "number_key": "${special-chars.number-key}",
           # Mixed content with deep paths
           "mixed_deep": "Value is ${level1.level2.level3.deep_number}"
       }
       
       result = substitute_context_variables(params)
       
       # Pure substitutions preserve types
       assert result["deep_string"] == "found_it" and isinstance(result["deep_string"], str)
       assert result["deep_number"] == 999 and isinstance(result["deep_number"], int)
       assert result["deep_bool"] is False and isinstance(result["deep_bool"], bool)
       assert result["array_string"] == "first_item" and isinstance(result["array_string"], str)
       assert result["array_number"] == 42 and isinstance(result["array_number"], int)
       assert result["array_bool"] is True and isinstance(result["array_bool"], bool)
       assert result["underscore"] == "underscore_value" and isinstance(result["underscore"], str)
       assert result["dash"] == "dash_value" and isinstance(result["dash"], str)
       assert result["number_key"] == 123 and isinstance(result["number_key"], int)
       
       # Mixed content becomes string
       assert result["mixed_deep"] == "Value is 999" and isinstance(result["mixed_deep"], str)
       
       print("PASS: Complex variable paths with type preservation work")
       
   finally:
       cleanup_file(context_file)
       clear_context()


def test_type_preservation_comprehensive():
   """Test comprehensive type preservation for all JSON types."""
   print("\n=== Test: Comprehensive Type Preservation ===")
   
   context_data = {
       "types": {
           "string": "hello",
           "integer": 42,
           "float": 3.14159,
           "boolean_true": True,
           "boolean_false": False,
           "null_value": None,
           "empty_string": "",
           "zero": 0,
           "negative": -123,
           "large_number": 9999999999
       }
   }
   
   context_file = create_test_context_file(context_data)
   
   try:
       set_context(context_file)
       
       params = {
           # Pure substitutions - should preserve exact types
           "str_val": "${types.string}",
           "int_val": "${types.integer}",
           "float_val": "${types.float}",
           "bool_true": "${types.boolean_true}",
           "bool_false": "${types.boolean_false}",
           "null_val": "${types.null_value}",
           "empty_str": "${types.empty_string}",
           "zero_val": "${types.zero}",
           "negative_val": "${types.negative}",
           "large_val": "${types.large_number}",
           
           # Mixed content - should become strings
           "mixed_int": "The value is ${types.integer}",
           "mixed_bool": "Enabled: ${types.boolean_true}",
           "mixed_null": "Value: ${types.null_value}",
           
           # Unchanged values
           "unchanged_str": "static",
           "unchanged_int": 999,
           "unchanged_bool": True
       }
       
       result = substitute_context_variables(params)
       
       # Verify pure substitutions preserve exact types and values
       assert result["str_val"] == "hello" and isinstance(result["str_val"], str)
       assert result["int_val"] == 42 and isinstance(result["int_val"], int)
       assert result["float_val"] == 3.14159 and isinstance(result["float_val"], float)
       assert result["bool_true"] is True and isinstance(result["bool_true"], bool)
       assert result["bool_false"] is False and isinstance(result["bool_false"], bool)
       assert result["null_val"] is None
       assert result["empty_str"] == "" and isinstance(result["empty_str"], str)
       assert result["zero_val"] == 0 and isinstance(result["zero_val"], int)
       assert result["negative_val"] == -123 and isinstance(result["negative_val"], int)
       assert result["large_val"] == 9999999999 and isinstance(result["large_val"], int)
       
       # Verify mixed content becomes strings
       assert result["mixed_int"] == "The value is 42" and isinstance(result["mixed_int"], str)
       assert result["mixed_bool"] == "Enabled: True" and isinstance(result["mixed_bool"], str)
       assert result["mixed_null"] == "Value: None" and isinstance(result["mixed_null"], str)
       
       # Verify unchanged values remain unchanged
       assert result["unchanged_str"] == "static" and isinstance(result["unchanged_str"], str)
       assert result["unchanged_int"] == 999 and isinstance(result["unchanged_int"], int)
       assert result["unchanged_bool"] is True and isinstance(result["unchanged_bool"], bool)
       
       print("PASS: Comprehensive type preservation works correctly")
       
   finally:
       cleanup_file(context_file)
       clear_context()


def test_context_info():
   """Test context information retrieval."""
   print("\n=== Test: Context Info ===")
   
   # Test with no context
   clear_context()
   info = get_context_info()
   
   assert info["context_file"] is None
   assert info["context_loaded"] is False
   assert info["context_keys"] is None
   
   print("PASS: Context info with no context")
   
   # Test with context file set but not loaded
   context_data = {"key1": "value1", "key2": {"nested": "value"}}
   context_file = create_test_context_file(context_data)
   
   try:
       set_context(context_file)
       info = get_context_info()
       
       assert info["context_file"] == context_file
       assert info["context_loaded"] is False
       assert info["context_keys"] is None
       
       print("PASS: Context info with file set but not loaded")
       
       # Trigger loading by doing substitution
       substitute_context_variables({"test": "${key1}"})
       
       info = get_context_info()
       assert info["context_loaded"] is True
       assert set(info["context_keys"]) == {"key1", "key2"}
       
       print("PASS: Context info after loading")
       
   finally:
       cleanup_file(context_file)
       clear_context()


def main():
   """Run all context system tests."""
   print("Starting Context System Tests...")
   print("=" * 50)
   
   try:
       test_context_file_management()
       test_basic_variable_substitution()
       test_pure_vs_mixed_substitution()
       test_nested_variable_substitution()
       test_no_context_substitution()
       test_no_variables_substitution()
       test_missing_variable_error()
       test_invalid_context_file()
       test_complex_variable_paths()
       test_type_preservation_comprehensive()
       test_context_info()
       
       print("\n" + "=" * 50)
       print("ALL CONTEXT SYSTEM TESTS PASSED")
       return True
       
   except Exception as e:
       print(f"\nFAIL: Test failed with error: {e}")
       import traceback
       traceback.print_exc()
       return False


if __name__ == "__main__":
   success = main()
   sys.exit(0 if success else 1)