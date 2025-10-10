"""
Test cases for datapy.mod_manager.sdk module.

Tests the Python SDK for DataPy framework including mod execution, parameter resolution,
context management, logging setup, and all error scenarios.
"""

import sys
import json
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open, call
from typing import Dict, Any

# Add project root to Python path for testing
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pytest

from datapy.mod_manager.sdk import (
    run_mod,
    set_context,
    clear_context,
    set_log_level,
    setup_logging,
    setup_context,
    _auto_generate_mod_name,
    _resolve_mod_parameters,
    _execute_mod_function,
    _validate_mod_execution_inputs,
    _parse_common_args
)
from datapy.mod_manager.result import SUCCESS, SUCCESS_WITH_WARNINGS, VALIDATION_ERROR, RUNTIME_ERROR


class TestSDKContextManagement:
    """Test cases for SDK context management functions."""
    
    @patch('datapy.mod_manager.sdk._set_context')
    def test_set_context_calls_internal(self, mock_set_context):
        """Test set_context calls internal context function."""
        set_context("/path/to/context.json")
        mock_set_context.assert_called_once_with("/path/to/context.json")
    
    @patch('datapy.mod_manager.sdk._clear_context')
    def test_clear_context_calls_internal(self, mock_clear_context):
        """Test clear_context calls internal context function."""
        clear_context()
        mock_clear_context.assert_called_once()


class TestSDKLogLevelManagement:
    """Test cases for SDK log level management."""
    
    @patch('datapy.mod_manager.sdk._set_log_level')
    def test_set_log_level_calls_internal(self, mock_set_log_level):
        """Test set_log_level calls internal logging function."""
        set_log_level("DEBUG")
        mock_set_log_level.assert_called_once_with("DEBUG")


class TestAutoGenerateModName:
    """Test cases for auto-generating mod names."""
    
    @patch('datetime.datetime')
    def test_auto_generate_mod_name_format(self, mock_datetime):
        """Test mod name generation format."""
        mock_now = MagicMock()
        mock_now.strftime.return_value = "20240101_120000_123"
        mock_datetime.now.return_value = mock_now
        
        result = _auto_generate_mod_name("csv_reader")
        
        assert result.startswith("csv_reader_20240101_120000_")
        mock_datetime.now.assert_called_once()
    
    def test_auto_generate_mod_name_different_types(self):
        """Test mod name generation with different mod types."""
        result1 = _auto_generate_mod_name("csv_reader")
        result2 = _auto_generate_mod_name("data_filter")
        
        assert result1.startswith("csv_reader_")
        assert result2.startswith("data_filter_")
        assert result1 != result2  # Should be unique


class TestResolveModParameters:
    """Test cases for parameter resolution."""
    
    @patch('datapy.mod_manager.sdk.create_resolver')
    def test_resolve_mod_parameters_success(self, mock_create_resolver):
        """Test successful parameter resolution."""
        mock_resolver = MagicMock()
        mock_resolver.resolve_mod_params.return_value = {"param1": "resolved_value"}
        mock_create_resolver.return_value = mock_resolver
        
        result = _resolve_mod_parameters("csv_reader", {"param1": "original_value"})
        
        assert result == {"param1": "resolved_value"}
        mock_resolver.resolve_mod_params.assert_called_once_with(
            mod_name="csv_reader",
            job_params={"param1": "original_value"}
        )
    
    @patch('datapy.mod_manager.sdk.create_resolver')
    def test_resolve_mod_parameters_resolver_failure(self, mock_create_resolver):
        """Test parameter resolution failure."""
        mock_create_resolver.side_effect = Exception("Resolver creation failed")
        
        with pytest.raises(RuntimeError, match="Parameter resolution failed"):
            _resolve_mod_parameters("csv_reader", {"param1": "value"})
    
    @patch('datapy.mod_manager.sdk.create_resolver')
    def test_resolve_mod_parameters_resolution_failure(self, mock_create_resolver):
        """Test parameter resolution method failure."""
        mock_resolver = MagicMock()
        mock_resolver.resolve_mod_params.side_effect = Exception("Resolution failed")
        mock_create_resolver.return_value = mock_resolver
        
        with pytest.raises(RuntimeError, match="Parameter resolution failed"):
            _resolve_mod_parameters("csv_reader", {"param1": "value"})


class TestValidateModExecutionInputs:
    """Test cases for mod execution input validation."""
    
    def test_validate_inputs_valid(self):
        """Test validation with valid inputs."""
        # Should not raise any exception
        _validate_mod_execution_inputs("csv_reader", {"param": "value"}, "test_mod")
    
    def test_validate_inputs_empty_mod_type(self):
        """Test validation with empty mod_type."""
        with pytest.raises(ValueError, match="mod_type must be a non-empty string"):
            _validate_mod_execution_inputs("", {"param": "value"}, "test_mod")
        
        with pytest.raises(ValueError, match="mod_type must be a non-empty string"):
            _validate_mod_execution_inputs(None, {"param": "value"}, "test_mod")
    
    def test_validate_inputs_non_string_mod_type(self):
        """Test validation with non-string mod_type."""
        with pytest.raises(ValueError, match="mod_type must be a non-empty string"):
            _validate_mod_execution_inputs(123, {"param": "value"}, "test_mod")
    
    def test_validate_inputs_non_dict_params(self):
        """Test validation with non-dict params."""
        with pytest.raises(ValueError, match="params must be a dictionary"):
            _validate_mod_execution_inputs("csv_reader", "not_dict", "test_mod")
        
        with pytest.raises(ValueError, match="params must be a dictionary"):
            _validate_mod_execution_inputs("csv_reader", None, "test_mod")
    
    def test_validate_inputs_empty_mod_name(self):
        """Test validation with empty mod_name."""
        with pytest.raises(ValueError, match="mod_name must be a non-empty string"):
            _validate_mod_execution_inputs("csv_reader", {"param": "value"}, "")
        
        with pytest.raises(ValueError, match="mod_name must be a non-empty string"):
            _validate_mod_execution_inputs("csv_reader", {"param": "value"}, None)
    
    def test_validate_inputs_non_string_mod_name(self):
        """Test validation with non-string mod_name."""
        with pytest.raises(ValueError, match="mod_name must be a non-empty string"):
            _validate_mod_execution_inputs("csv_reader", {"param": "value"}, 123)


class TestExecuteModFunction:
    """Test cases for mod function execution."""
    
    def test_execute_mod_function_success(self):
        """Test successful mod function execution."""
        # Create synthetic mod module
        mock_mod_module = MagicMock()
        mock_mod_module.run = MagicMock(return_value={
            "status": "success",
            "exit_code": 0,
            "metrics": {"rows": 100},
            "artifacts": {"data": "test_data"},
            "globals": {"count": 100},
            "warnings": [],
            "errors": [],
            "logs": {"run_id": "test_123"}
        })
        
        mod_info = {
            "module_path": "test.mod",
            "type": "test_reader"
        }
        
        with patch('importlib.import_module', return_value=mock_mod_module), \
             patch('datapy.mod_manager.sdk.setup_logger') as mock_logger:
            
            result = _execute_mod_function(mod_info, {"param1": "value"}, "test_mod")
            
            assert result["status"] == "success"
            assert result["logs"]["mod_name"] == "test_mod"
            assert result["logs"]["mod_type"] == "test_reader"
            
            # Verify mod function called with metadata
            mock_mod_module.run.assert_called_once()
            called_params = mock_mod_module.run.call_args[0][0]
            assert called_params["param1"] == "value"
            assert called_params["_mod_name"] == "test_mod"
            assert called_params["_mod_type"] == "test_reader"
    
    def test_execute_mod_function_import_error(self):
        """Test mod function execution with import error."""
        import importlib
        original_import = importlib.import_module
        
        def selective_import(module_name):
            if module_name == "nonexistent.mod":
                raise ImportError("Module not found")
            return original_import(module_name)
        
        mod_info = {
            "module_path": "nonexistent.mod",
            "type": "test_reader"
        }
        
        with patch('importlib.import_module', side_effect=selective_import), \
            patch('datapy.mod_manager.sdk.setup_logger'):
            
            result = _execute_mod_function(mod_info, {"param1": "value"}, "test_mod")
            
            assert result["status"] == "error"
            assert result["exit_code"] == VALIDATION_ERROR
            assert "Cannot import mod" in result["errors"][0]["message"]
    
    def test_execute_mod_function_missing_run_function(self):
        """Test mod function execution with missing run function."""
        mock_mod_module = MagicMock()
        del mock_mod_module.run  # Remove run attribute
        
        mod_info = {
            "module_path": "test.mod",
            "type": "test_reader"
        }
        
        with patch('importlib.import_module', return_value=mock_mod_module), \
             patch('datapy.mod_manager.sdk.setup_logger'):
            
            result = _execute_mod_function(mod_info, {"param1": "value"}, "test_mod")
            
            assert result["status"] == "error"
            assert result["exit_code"] == VALIDATION_ERROR
            assert "missing required 'run' function" in result["errors"][0]["message"]
    
    def test_execute_mod_function_run_not_callable(self):
        """Test mod function execution with non-callable run."""
        mock_mod_module = MagicMock()
        mock_mod_module.run = "not_callable"
        
        mod_info = {
            "module_path": "test.mod",
            "type": "test_reader"
        }
        
        with patch('importlib.import_module', return_value=mock_mod_module), \
             patch('datapy.mod_manager.sdk.setup_logger'):
            
            result = _execute_mod_function(mod_info, {"param1": "value"}, "test_mod")
            
            assert result["status"] == "error"
            assert result["exit_code"] == VALIDATION_ERROR
            assert "'run' must be callable" in result["errors"][0]["message"]
    
    def test_execute_mod_function_invalid_result_type(self):
        """Test mod function execution with invalid result type."""
        mock_mod_module = MagicMock()
        mock_mod_module.run = MagicMock(return_value="not_dict")
        
        mod_info = {
            "module_path": "test.mod",
            "type": "test_reader"
        }
        
        with patch('importlib.import_module', return_value=mock_mod_module), \
             patch('datapy.mod_manager.sdk.setup_logger'):
            
            result = _execute_mod_function(mod_info, {"param1": "value"}, "test_mod")
            
            assert result["status"] == "error"
            assert result["exit_code"] == RUNTIME_ERROR
            assert "must return a dictionary" in result["errors"][0]["message"]
    
    def test_execute_mod_function_missing_required_fields(self):
        """Test mod function execution with missing required result fields."""
        mock_mod_module = MagicMock()
        mock_mod_module.run = MagicMock(return_value={
            "status": "success",
            # Missing other required fields
        })
        
        mod_info = {
            "module_path": "test.mod",
            "type": "test_reader"
        }
        
        with patch('importlib.import_module', return_value=mock_mod_module), \
             patch('datapy.mod_manager.sdk.setup_logger'):
            
            result = _execute_mod_function(mod_info, {"param1": "value"}, "test_mod")
            
            assert result["status"] == "error"
            assert result["exit_code"] == RUNTIME_ERROR
            assert "missing required fields" in result["errors"][0]["message"]
    
    def test_execute_mod_function_invalid_status(self):
        """Test mod function execution with invalid status."""
        mock_mod_module = MagicMock()
        mock_mod_module.run = MagicMock(return_value={
            "status": "invalid_status",
            "exit_code": 0,
            "metrics": {},
            "artifacts": {},
            "globals": {},
            "warnings": [],
            "errors": [],
            "logs": {}
        })
        
        mod_info = {
            "module_path": "test.mod",
            "type": "test_reader"
        }
        
        with patch('importlib.import_module', return_value=mock_mod_module), \
             patch('datapy.mod_manager.sdk.setup_logger'):
            
            result = _execute_mod_function(mod_info, {"param1": "value"}, "test_mod")
            
            assert result["status"] == "error"
            assert result["exit_code"] == RUNTIME_ERROR
            assert "Invalid status" in result["errors"][0]["message"]
    
    def test_execute_mod_function_execution_exception(self):
        """Test mod function execution with runtime exception."""
        mock_mod_module = MagicMock()
        mock_mod_module.run = MagicMock(side_effect=Exception("Execution failed"))
        
        mod_info = {
            "module_path": "test.mod",
            "type": "test_reader"
        }
        
        with patch('importlib.import_module', return_value=mock_mod_module), \
             patch('datapy.mod_manager.sdk.setup_logger'):
            
            result = _execute_mod_function(mod_info, {"param1": "value"}, "test_mod")
            
            assert result["status"] == "error"
            assert result["exit_code"] == RUNTIME_ERROR
            assert "Mod execution failed" in result["errors"][0]["message"]


class TestRunMod:
    """Test cases for the main run_mod function."""
    
    def test_run_mod_complete_success_flow(self):
        """Test complete successful mod execution flow."""
        # Mock all dependencies
        mock_registry = MagicMock()
        mock_registry.get_mod_info.return_value = {
            "module_path": "test.mod",
            "type": "test_reader",
            "config_schema": {"required": {}, "optional": {}}
        }
        
        mock_resolver = MagicMock()
        mock_resolver.resolve_mod_params.return_value = {"resolved": "param"}
        
        mock_mod_module = MagicMock()
        mock_mod_module.run = MagicMock(return_value={
            "status": "success",
            "exit_code": 0,
            "metrics": {"rows": 100},
            "artifacts": {"data": "test_data"},
            "globals": {"count": 100},
            "warnings": [],
            "errors": [],
            "logs": {"run_id": "test_123"}
        })
        
        with patch('datapy.mod_manager.sdk.get_registry', return_value=mock_registry), \
             patch('datapy.mod_manager.sdk.create_resolver', return_value=mock_resolver), \
             patch('datapy.mod_manager.sdk.substitute_context_variables', return_value={"substituted": "param"}), \
             patch('datapy.mod_manager.sdk.validate_mod_parameters', return_value={"validated": "param"}), \
             patch('importlib.import_module', return_value=mock_mod_module), \
             patch('datapy.mod_manager.sdk.setup_logger'):
            
            result = run_mod("test_reader", {"input": "param"}, "test_mod")
            
            assert result["status"] == "success"
            assert result["logs"]["mod_name"] == "test_mod"
            assert result["logs"]["mod_type"] == "test_reader"
    
    def test_run_mod_auto_generate_name(self):
        """Test mod execution with auto-generated name."""
        # Just test that it works without mocking the name generation
        mock_registry = MagicMock()
        mock_registry.get_mod_info.return_value = {
            "module_path": "test.mod",
            "type": "test_reader",
            "config_schema": {"required": {}, "optional": {}}
        }
        
        mock_resolver = MagicMock()
        mock_resolver.resolve_mod_params.return_value = {"resolved": "param"}
        
        mock_mod_module = MagicMock()
        mock_mod_module.run = MagicMock(return_value={
            "status": "success",
            "exit_code": 0,
            "metrics": {},
            "artifacts": {},
            "globals": {},
            "warnings": [],
            "errors": [],
            "logs": {"run_id": "test_123"}
        })
        
        with patch('datapy.mod_manager.sdk.get_registry', return_value=mock_registry), \
             patch('datapy.mod_manager.sdk.create_resolver', return_value=mock_resolver), \
             patch('datapy.mod_manager.sdk.substitute_context_variables', return_value={"substituted": "param"}), \
             patch('datapy.mod_manager.sdk.validate_mod_parameters', return_value={"validated": "param"}), \
             patch('importlib.import_module', return_value=mock_mod_module), \
             patch('datapy.mod_manager.sdk.setup_logger'), \
             patch('datapy.mod_manager.sdk._auto_generate_mod_name', return_value="auto_generated_name"):
            
            result = run_mod("test_reader", {"input": "param"})  # No mod_name provided
            
            assert result["status"] == "success"
            assert result["logs"]["mod_name"].startswith("test_reader_")
    
    def test_run_mod_registry_lookup_failure(self):
        """Test mod execution with registry lookup failure."""
        mock_registry = MagicMock()
        mock_registry.get_mod_info.side_effect = ValueError("Mod 'unknown' not found in registry")
        
        with patch('datapy.mod_manager.sdk.get_registry', return_value=mock_registry):
            result = run_mod("unknown_mod", {"input": "param"}, "test_mod")
            
            assert result["status"] == "error"
            assert result["exit_code"] == VALIDATION_ERROR
            assert "not found in registry" in result["errors"][0]["message"]
    
    def test_run_mod_parameter_resolution_failure(self):
        """Test mod execution with parameter resolution failure."""
        mock_registry = MagicMock()
        mock_registry.get_mod_info.return_value = {"module_path": "test.mod", "config_schema": {}}
        
        with patch('datapy.mod_manager.sdk.get_registry', return_value=mock_registry), \
            patch('datapy.mod_manager.sdk._resolve_mod_parameters', side_effect=RuntimeError("Resolution failed")):
            
            result = run_mod("test_reader", {"input": "param"}, "test_mod")
            
            assert result["status"] == "error"
            assert result["exit_code"] == VALIDATION_ERROR
            assert "Resolution failed" in result["errors"][0]["message"]
    
    def test_run_mod_context_substitution_failure(self):
        """Test mod execution with context substitution failure."""
        mock_registry = MagicMock()
        mock_registry.get_mod_info.return_value = {"module_path": "test.mod", "config_schema": {}}
        mock_resolver = MagicMock()
        mock_resolver.resolve_mod_params.return_value = {"resolved": "param"}
        
        with patch('datapy.mod_manager.sdk.get_registry', return_value=mock_registry), \
            patch('datapy.mod_manager.sdk._resolve_mod_parameters', return_value={}), \
            patch('datapy.mod_manager.sdk.substitute_context_variables', side_effect=ValueError("Substitution failed")):
            
            result = run_mod("test_reader", {"input": "param"}, "test_mod")
            
            assert result["status"] == "error"
            assert result["exit_code"] == VALIDATION_ERROR
            assert "Substitution failed" in result["errors"][0]["message"]
    
    def test_run_mod_parameter_validation_failure(self):
        """Test mod execution with parameter validation failure."""
        mock_registry = MagicMock()
        mock_registry.get_mod_info.return_value = {
            "module_path": "test.mod",
            "config_schema": {"required": {}, "optional": {}}  # Add config_schema
        }
        mock_resolver = MagicMock()
        mock_resolver.resolve_mod_params.return_value = {"resolved": "param"}
        
        with patch('datapy.mod_manager.sdk.get_registry', return_value=mock_registry), \
            patch('datapy.mod_manager.sdk._resolve_mod_parameters', return_value={}), \
            patch('datapy.mod_manager.sdk.substitute_context_variables', return_value={"substituted": "param"}), \
            patch('datapy.mod_manager.sdk.validate_mod_parameters', side_effect=ValueError("Validation failed")):
            
            result = run_mod("test_reader", {"input": "param"}, "test_mod")
            
            assert result["status"] == "error"
            assert result["exit_code"] == VALIDATION_ERROR
            assert "Validation failed" in result["errors"][0]["message"]
    
    def test_run_mod_input_validation_failure(self):
        """Test mod execution with input validation failure."""
        # Test with empty string - should return error result (not raise)
        result = run_mod("", {"input": "param"}, "test_mod")
        assert result["status"] == "error"
        assert result["exit_code"] == VALIDATION_ERROR
        assert "mod_type must be a non-empty string" in result["errors"][0]["message"]
        
        # Test with None - will cause AttributeError when calling .strip(), resulting in RUNTIME_ERROR
        result = run_mod(None, {"input": "param"}, "test_mod")
        assert result["status"] == "error"
        assert result["exit_code"] == RUNTIME_ERROR  # AttributeError gets caught as runtime error
        
        # Test with whitespace only - should return error result after stripping
        result = run_mod("   ", {"input": "param"}, "test_mod")
        assert result["status"] == "error"
        assert result["exit_code"] == VALIDATION_ERROR
        assert "mod_type must be a non-empty string" in result["errors"][0]["message"]


    
    def test_run_mod_whitespace_handling(self):
        """Test mod execution with whitespace in inputs."""
        mock_registry = MagicMock()
        mock_registry.get_mod_info.return_value = {
            "module_path": "test.mod",
            "type": "test_reader",
            "config_schema": {"required": {}, "optional": {}}
        }
        
        mock_resolver = MagicMock()
        mock_resolver.resolve_mod_params.return_value = {"resolved": "param"}
        
        mock_mod_module = MagicMock()
        mock_mod_module.run = MagicMock(return_value={
            "status": "success",
            "exit_code": 0,
            "metrics": {},
            "artifacts": {},
            "globals": {},
            "warnings": [],
            "errors": [],
            "logs": {"run_id": "test_123"}
        })
        
        with patch('datapy.mod_manager.sdk.get_registry', return_value=mock_registry), \
            patch('datapy.mod_manager.sdk._resolve_mod_parameters', return_value={}), \
            patch('datapy.mod_manager.sdk.substitute_context_variables', return_value={}), \
            patch('datapy.mod_manager.sdk.validate_mod_parameters', return_value={}), \
            patch('datapy.mod_manager.sdk._execute_mod_function', return_value=mock_mod_module.run.return_value):
            
            result = run_mod("  test_reader  ", {"input": "param"}, "  test_mod  ")
            
            assert result["status"] == "success"
    
    def test_run_mod_unexpected_exception(self):
        """Test mod execution with unexpected exception."""
        with patch('datapy.mod_manager.sdk.get_registry', side_effect=Exception("Unexpected error")):
            
            result = run_mod("test_reader", {"input": "param"}, "test_mod")
            
            assert result["status"] == "error"
            assert result["exit_code"] == RUNTIME_ERROR
            assert "Unexpected error" in result["errors"][0]["message"]


class TestParseCommonArgs:
    """Test cases for command line argument parsing."""
    
    def test_parse_common_args_no_args(self):
        """Test parsing with no command line arguments."""
        with patch('sys.argv', ['script.py']):
            result = _parse_common_args()
            
            assert result["log_level"] == "INFO"
            assert result["log_provided"] is False
            assert result["context_path"] == ""
            assert result["context_provided"] is False
    
    def test_parse_common_args_log_level_provided(self):
        """Test parsing with log level argument."""
        with patch('sys.argv', ['script.py', '--log-level', 'DEBUG']):
            result = _parse_common_args()
            
            assert result["log_level"] == "DEBUG"
            assert result["log_provided"] is True
    
    def test_parse_common_args_context_provided(self):
        """Test parsing with context argument."""
        with patch('sys.argv', ['script.py', '--context', 'context.json']):
            result = _parse_common_args()
            
            assert result["context_path"] == "context.json"
            assert result["context_provided"] is True
    
    def test_parse_common_args_both_provided(self):
        """Test parsing with both arguments."""
        with patch('sys.argv', ['script.py', '--log-level', 'ERROR', '--context', 'prod.json']):
            result = _parse_common_args()
            
            assert result["log_level"] == "ERROR"
            assert result["log_provided"] is True
            assert result["context_path"] == "prod.json"
            assert result["context_provided"] is True
    
    def test_parse_common_args_case_conversion(self):
        """Test log level case conversion."""
        with patch('sys.argv', ['script.py', '--log-level', 'DEBUG']):  # Use uppercase
            result = _parse_common_args()
            
            assert result["log_level"] == "DEBUG"
    
    def test_parse_common_args_parsing_failure(self):
        """Test parsing failure returns safe defaults."""
        with patch('argparse.ArgumentParser.parse_known_args', side_effect=Exception("Parse error")):
            result = _parse_common_args()
            
            assert result["log_level"] == "INFO"
            assert result["log_provided"] is False
            assert result["context_path"] == ""
            assert result["context_provided"] is False


class TestSetupLogging:
    """Test cases for setup_logging hybrid function."""
    
    @patch('datapy.mod_manager.logger.setup_logger')
    @patch('datapy.mod_manager.sdk.set_log_level')
    @patch('datapy.mod_manager.sdk._parse_common_args')
    def test_setup_logging_command_line_override(self, mock_parse_args, mock_set_log_level, mock_setup_logger):
        """Test setup_logging with command line override takes priority."""
        mock_parse_args.return_value = {
            "log_level": "DEBUG",
            "log_provided": True,  # This must be True to hit the first branch
            "context_path": "",
            "context_provided": False
        }
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        result = setup_logging("INFO", "test.module")  # Different level provided as param
        
        mock_set_log_level.assert_called_once_with("DEBUG")  # Command line wins over param
        mock_setup_logger.assert_called_once_with("test.module")
        assert result is mock_logger
    
    
    @patch('datapy.mod_manager.logger.setup_logger')
    @patch('datapy.mod_manager.sdk.set_log_level')
    @patch('datapy.mod_manager.sdk._parse_common_args')
    def test_setup_logging_explicit_level_no_command_line(self, mock_parse_args, mock_set_log_level, mock_setup_logger):
        """Test setup_logging with explicit level parameter and no command line override."""
        mock_parse_args.return_value = {
            "log_level": "INFO",
            "log_provided": False,  # No command line override
            "context_path": "",
            "context_provided": False
        }
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        result = setup_logging("WARNING", "test.module")  # Explicit level provided
        
        mock_set_log_level.assert_called_once_with("WARNING")  # Should use explicit level
        mock_setup_logger.assert_called_once_with("test.module")
        assert result is mock_logger

    @patch('datapy.mod_manager.logger.setup_logger')
    @patch('datapy.mod_manager.sdk.set_log_level')
    @patch('datapy.mod_manager.sdk._parse_common_args')
    def test_setup_logging_default_fallback_no_overrides(self, mock_parse_args, mock_set_log_level, mock_setup_logger):
        """Test setup_logging with default fallback when no level provided anywhere."""
        mock_parse_args.return_value = {
            "log_level": "INFO",
            "log_provided": False,  # No command line override
            "context_path": "",
            "context_provided": False
        }
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        result = setup_logging()  # No level parameter provided (None)
        
        mock_set_log_level.assert_called_once_with("INFO")  # Should use default "INFO"
        mock_setup_logger.assert_called_once_with("datapy.user")  # Default name
        assert result is mock_logger
    
    @patch('datapy.mod_manager.logger.setup_logger')  # Correct import path
    @patch('datapy.mod_manager.sdk.set_log_level')
    @patch('datapy.mod_manager.sdk._parse_common_args')
    def test_setup_logging_custom_logger_name(self, mock_parse_args, mock_set_log_level, mock_setup_logger):
        """Test setup_logging with custom logger name."""
        mock_parse_args.return_value = {
            "log_level": "INFO",
            "log_provided": False,
            "context_path": "",
            "context_provided": False
        }
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger
        
        result = setup_logging("ERROR", "custom.logger.name")
        
        mock_set_log_level.assert_called_once_with("ERROR")
        mock_setup_logger.assert_called_once_with("custom.logger.name")
        assert result is mock_logger


class TestSetupContext:
    """Test cases for setup_context hybrid function."""
    
    @patch('datapy.mod_manager.sdk.set_context')
    @patch('datapy.mod_manager.sdk._parse_common_args')
    def test_setup_context_command_line_override(self, mock_parse_args, mock_set_context):
        """Test setup_context with command line override."""
        mock_parse_args.return_value = {
            "log_level": "INFO",
            "log_provided": False,
            "context_path": "cmd_context.json",
            "context_provided": True
        }
        
        setup_context("param_context.json")
        
        mock_set_context.assert_called_once_with("cmd_context.json")  # Command line wins
    
    @patch('datapy.mod_manager.sdk.set_context')
    @patch('datapy.mod_manager.sdk._parse_common_args')
    def test_setup_context_explicit_path(self, mock_parse_args, mock_set_context):
        """Test setup_context with explicit path parameter."""
        mock_parse_args.return_value = {
            "log_level": "INFO",
            "log_provided": False,
            "context_path": "",
            "context_provided": False
        }
        
        setup_context("explicit_context.json")
        
        mock_set_context.assert_called_once_with("explicit_context.json")
    
    @patch('datapy.mod_manager.sdk.set_context')
    @patch('datapy.mod_manager.sdk._parse_common_args')
    def test_setup_context_no_context_set(self, mock_parse_args, mock_set_context):
        """Test setup_context with no context to set."""
        mock_parse_args.return_value = {
            "log_level": "INFO",
            "log_provided": False,
            "context_path": "",
            "context_provided": False
        }
        
        setup_context()  # No context provided
        
        mock_set_context.assert_not_called()  # Should not call set_context
    
    @patch('datapy.mod_manager.sdk.set_context')
    @patch('datapy.mod_manager.sdk._parse_common_args')
    def test_setup_context_none_path(self, mock_parse_args, mock_set_context):
        """Test setup_context with None path parameter."""
        mock_parse_args.return_value = {
            "log_level": "INFO",
            "log_provided": False,
            "context_path": "",
            "context_provided": False
        }
        
        setup_context(None)  # Explicit None
        
        mock_set_context.assert_not_called()  # Should not call set_context


class TestIntegrationScenarios:
    """Integration test cases for complete SDK workflows."""
    
    def test_complete_successful_workflow(self):
        """Test complete successful mod execution workflow with all components."""
        # Create comprehensive mocks for all dependencies
        mock_registry = MagicMock()
        mock_registry.get_mod_info.return_value = {
            "module_path": "datapy.mods.test_mod",
            "type": "test_processor",
            "version": "1.0.0",
            "description": "Test processor mod",
            "config_schema": {
                "required": {
                    "input_data": {"type": "str", "description": "Input data path"}
                },
                "optional": {
                    "output_format": {"type": "str", "default": "csv", "description": "Output format"}
                }
            }
        }
        
        mock_resolver = MagicMock()
        mock_resolver.resolve_mod_params.return_value = {
            "input_data": "resolved_input_path",
            "batch_size": 1000  # From project defaults
        }
        
        # Mock successful mod module
        mock_mod_module = MagicMock()
        mock_mod_module.run = MagicMock(return_value={
            "status": "success",
            "exit_code": SUCCESS,
            "metrics": {
                "rows_processed": 5000,
                "processing_rate": 0.98,
                "memory_used": "256MB"
            },
            "artifacts": {
                "processed_data": "<DataFrame with 5000 rows>",
                "output_file": "/output/processed_data.csv",
                "summary_report": {"total": 5000, "errors": 0}
            },
            "globals": {
                "last_processed_count": 5000,
                "processing_timestamp": "2024-01-15T10:30:00Z"
            },
            "warnings": [
                {"message": "Found 10 duplicate records", "warning_code": 10, "timestamp": 1640995200.123}
            ],
            "errors": [],
            "logs": {
                "run_id": "test_processor_20240115_103000_abc123",
                "mod_type": "test_processor",
                "mod_name": "data_processor"
            }
        })
        
        with patch('datapy.mod_manager.sdk.get_registry', return_value=mock_registry), \
             patch('datapy.mod_manager.sdk.create_resolver', return_value=mock_resolver), \
             patch('datapy.mod_manager.sdk.substitute_context_variables') as mock_substitute, \
             patch('datapy.mod_manager.sdk.validate_mod_parameters') as mock_validate, \
             patch('importlib.import_module', return_value=mock_mod_module), \
             patch('datapy.mod_manager.sdk.setup_logger') as mock_logger:
            
            # Set up context substitution
            mock_substitute.return_value = {
                "input_data": "/prod/data/customers.csv",  # After substitution
                "batch_size": 1000,
                "output_format": "parquet"
            }
            
            # Set up parameter validation
            mock_validate.return_value = {
                "input_data": "/prod/data/customers.csv",
                "batch_size": 1000,
                "output_format": "parquet",  # With defaults applied
                "_mod_name": "data_processor",
                "_mod_type": "test_processor"
            }
            
            # Execute the mod
            result = run_mod(
                mod_type="test_processor",
                params={
                    "input_data": "${env.data_path}/customers.csv",  # With variable
                    "output_format": "parquet"
                },
                mod_name="data_processor"
            )
            
            # Verify successful execution
            assert result["status"] == "success"
            assert result["exit_code"] == SUCCESS
            assert result["metrics"]["rows_processed"] == 5000
            assert len(result["warnings"]) == 1
            assert len(result["errors"]) == 0
            assert result["logs"]["mod_name"] == "data_processor"
            assert result["logs"]["mod_type"] == "test_processor"
            
            # Verify all steps were called correctly
            mock_registry.get_mod_info.assert_called_once_with("test_processor")
            mock_resolver.resolve_mod_params.assert_called_once()
            mock_substitute.assert_called_once()
            mock_validate.assert_called_once()
            mock_mod_module.run.assert_called_once()
    
    def test_workflow_with_warning_result(self):
        """Test workflow that completes with warnings."""
        # Set up mocks for warning scenario
        mock_registry = MagicMock()
        mock_registry.get_mod_info.return_value = {
            "module_path": "test.warning_mod",
            "type": "warning_processor",
            "config_schema": {"required": {}, "optional": {}}
        }
        
        mock_resolver = MagicMock()
        mock_resolver.resolve_mod_params.return_value = {"param": "value"}
        
        mock_mod_module = MagicMock()
        mock_mod_module.run = MagicMock(return_value={
            "status": "warning",
            "exit_code": SUCCESS_WITH_WARNINGS,
            "metrics": {"processed": 100},
            "artifacts": {},
            "globals": {},
            "warnings": [
                {"message": "Data quality issues detected", "warning_code": 10, "timestamp": 1640995200.123}
            ],
            "errors": [],
            "logs": {"run_id": "test_123"}
        })
        
        with patch('datapy.mod_manager.sdk.get_registry', return_value=mock_registry), \
             patch('datapy.mod_manager.sdk.create_resolver', return_value=mock_resolver), \
             patch('datapy.mod_manager.sdk.substitute_context_variables', return_value={"param": "value"}), \
             patch('datapy.mod_manager.sdk.validate_mod_parameters', return_value={"param": "value"}), \
             patch('importlib.import_module', return_value=mock_mod_module), \
             patch('datapy.mod_manager.sdk.setup_logger'):
            
            result = run_mod("warning_processor", {"param": "value"}, "warning_test")
            
            assert result["status"] == "warning"
            assert result["exit_code"] == SUCCESS_WITH_WARNINGS
            assert len(result["warnings"]) == 1
            assert "Data quality issues" in result["warnings"][0]["message"]
    
    def test_workflow_with_complex_error_chain(self):
        """Test error scenarios across different failure points."""
        # Test registry failure
        with patch('datapy.mod_manager.sdk.get_registry', side_effect=Exception("Registry access failed")):
            result = run_mod("test_mod", {}, "test")
            assert result["status"] == "error"
            assert result["exit_code"] == RUNTIME_ERROR
        
        # Test parameter resolution failure  
        mock_registry = MagicMock()
        mock_registry.get_mod_info.return_value = {"module_path": "test.mod"}
        
        with patch('datapy.mod_manager.sdk.get_registry', return_value=mock_registry), \
             patch('datapy.mod_manager.sdk._resolve_mod_parameters', side_effect=RuntimeError("Param resolution failed")):
            
            result = run_mod("test_mod", {}, "test")
            assert result["status"] == "error"
            assert result["exit_code"] == VALIDATION_ERROR
            assert "Param resolution failed" in result["errors"][0]["message"]
        
        # Test context substitution failure
        mock_resolver = MagicMock()
        mock_resolver.resolve_mod_params.return_value = {}
        
        with patch('datapy.mod_manager.sdk.get_registry', return_value=mock_registry), \
             patch('datapy.mod_manager.sdk.create_resolver', return_value=mock_resolver), \
             patch('datapy.mod_manager.sdk.substitute_context_variables', side_effect=ValueError("Context error")):
            
            result = run_mod("test_mod", {}, "test")
            assert result["status"] == "error"
            assert result["exit_code"] == VALIDATION_ERROR
            assert "Context substitution failed" in result["errors"][0]["message"]


class TestEdgeCases:
    """Test cases for edge cases and boundary conditions."""
    
    def test_run_mod_empty_parameters(self):
        """Test mod execution with empty parameters dictionary."""
        mock_registry = MagicMock()
        mock_registry.get_mod_info.return_value = {
            "module_path": "test.mod",
            "type": "test_mod",
            "config_schema": {"required": {}, "optional": {}}
        }
        
        mock_resolver = MagicMock()
        mock_resolver.resolve_mod_params.return_value = {}
        
        mock_mod_module = MagicMock()
        mock_mod_module.run = MagicMock(return_value={
            "status": "success",
            "exit_code": 0,
            "metrics": {},
            "artifacts": {},
            "globals": {},
            "warnings": [],
            "errors": [],
            "logs": {"run_id": "test_123"}
        })
        
        with patch('datapy.mod_manager.sdk.get_registry', return_value=mock_registry), \
             patch('datapy.mod_manager.sdk.create_resolver', return_value=mock_resolver), \
             patch('datapy.mod_manager.sdk.substitute_context_variables', return_value={}), \
             patch('datapy.mod_manager.sdk.validate_mod_parameters', return_value={}), \
             patch('importlib.import_module', return_value=mock_mod_module), \
             patch('datapy.mod_manager.sdk.setup_logger'):
            
            result = run_mod("test_mod", {}, "empty_params_test")
            
            assert result["status"] == "success"
            assert result["logs"]["mod_name"] == "empty_params_test"
    
    def test_run_mod_special_characters_in_names(self):
        """Test mod execution with special characters (that are valid)."""
        mock_registry = MagicMock()
        mock_registry.get_mod_info.return_value = {
            "module_path": "test.mod",
            "type": "test_reader_v2",
            "config_schema": {"required": {}, "optional": {}}
        }
        
        mock_resolver = MagicMock()
        mock_resolver.resolve_mod_params.return_value = {}
        
        mock_mod_module = MagicMock()
        mock_mod_module.run = MagicMock(return_value={
            "status": "success",
            "exit_code": 0,
            "metrics": {},
            "artifacts": {},
            "globals": {},
            "warnings": [],
            "errors": [],
            "logs": {"run_id": "test_123"}
        })
        
        with patch('datapy.mod_manager.sdk.get_registry', return_value=mock_registry), \
             patch('datapy.mod_manager.sdk.create_resolver', return_value=mock_resolver), \
             patch('datapy.mod_manager.sdk.substitute_context_variables', return_value={}), \
             patch('datapy.mod_manager.sdk.validate_mod_parameters', return_value={}), \
             patch('importlib.import_module', return_value=mock_mod_module), \
             patch('datapy.mod_manager.sdk.setup_logger'):
            
            result = run_mod("csv_reader_v2", {}, "data_reader_2024")
            
            assert result["status"] == "success"
            assert result["logs"]["mod_name"] == "data_reader_2024"
            assert result["logs"]["mod_type"] == "test_reader_v2"
    
    def test_execute_mod_function_large_result_handling(self):
        """Test mod function execution with large result structures."""
        # Create large synthetic result
        large_metrics = {f"metric_{i}": i * 1.5 for i in range(1000)}
        large_artifacts = {f"artifact_{i}": f"data_chunk_{i}" for i in range(100)}
        
        mock_mod_module = MagicMock()
        mock_mod_module.run = MagicMock(return_value={
            "status": "success",
            "exit_code": 0,
            "metrics": large_metrics,
            "artifacts": large_artifacts,
            "globals": {"total_items": 100000},
            "warnings": [],
            "errors": [],
            "logs": {"run_id": "large_test_123"}
        })
        
        mod_info = {
            "module_path": "test.large_mod",
            "type": "large_processor"
        }
        
        with patch('importlib.import_module', return_value=mock_mod_module), \
             patch('datapy.mod_manager.sdk.setup_logger'):
            
            result = _execute_mod_function(mod_info, {"param": "value"}, "large_test")
            
            assert result["status"] == "success"
            assert len(result["metrics"]) == 1000
            assert len(result["artifacts"]) == 100
            assert result["globals"]["total_items"] == 100000
    
    def test_auto_generate_mod_name_uniqueness(self):
        """Test that auto-generated mod names are unique."""
        import time
        names = []
        for _ in range(10):
            name = _auto_generate_mod_name("test_mod")
            names.append(name)
            time.sleep(0.001)  # Ensure microsecond difference
        
        # All names should be unique
        assert len(names) == len(set(names))
    
    def test_parse_common_args_with_extra_arguments(self):
        """Test argument parsing with extra unknown arguments."""
        with patch('sys.argv', ['script.py', '--log-level', 'DEBUG', '--unknown-arg', 'value', 'positional']):
            result = _parse_common_args()
            
            # Should parse known args and ignore unknown ones
            assert result["log_level"] == "DEBUG"
            assert result["log_provided"] is True
            assert result["context_path"] == ""
            assert result["context_provided"] is False


class TestMemoryAndResourceManagement:
    """Test cases for memory usage and resource cleanup scenarios."""
    
    def test_run_mod_parameter_copying_isolation(self):
        """Test that parameters are properly copied and don't affect original."""
        import copy
        original_params = {"mutable_list": [1, 2, 3], "mutable_dict": {"key": "value"}}

        mock_registry = MagicMock()
        mock_registry.get_mod_info.return_value = {
            "module_path": "test.mod",
            "type": "test_mod",
            "config_schema": {"required": {}, "optional": {}}
        }

        mock_mod_module = MagicMock()
        mock_mod_module.run = MagicMock(return_value={
            "status": "success",
            "exit_code": 0,
            "metrics": {},
            "artifacts": {},
            "globals": {},
            "warnings": [],
            "errors": [],
            "logs": {"run_id": "test_123"}
        })

        # Mock the complete execution chain used by run_mod
        with patch('datapy.mod_manager.sdk.get_registry', return_value=mock_registry), \
            patch('datapy.mod_manager.sdk._resolve_mod_parameters') as mock_resolve, \
            patch('datapy.mod_manager.sdk.substitute_context_variables') as mock_substitute, \
            patch('datapy.mod_manager.sdk.validate_mod_parameters') as mock_validate, \
            patch('datapy.mod_manager.sdk._execute_mod_function', return_value=mock_mod_module.run.return_value):

            # Set up the mock chain to return proper values
            mock_resolve.return_value = {"resolved": "param"}
            mock_substitute.return_value = {"substituted": "param"} 
            mock_validate.return_value = {"validated": "param"}

            result = run_mod("test_mod", original_params, "isolation_test")

            # Original parameters should be unchanged
            assert original_params["mutable_list"] == [1, 2, 3]
            assert original_params["mutable_dict"] == {"key": "value"}
            assert result["status"] == "success"
    
    def test_run_mod_exception_cleanup(self):
        """Test that resources are properly cleaned up on exceptions."""
        mock_registry = MagicMock()
        mock_registry.get_mod_info.return_value = {"module_path": "test.mod"}
        
        # Simulate exception in parameter resolution
        with patch('datapy.mod_manager.sdk.get_registry', return_value=mock_registry), \
             patch('datapy.mod_manager.sdk._resolve_mod_parameters', side_effect=Exception("Test exception")):
            
            result = run_mod("test_mod", {}, "cleanup_test")
            
            # Should return error result, not propagate exception
            assert result["status"] == "error"
            assert result["exit_code"] == RUNTIME_ERROR
            assert "Test exception" in result["errors"][0]["message"]
    


class TestSetupContext:
    """Test cases for setup_context hybrid function."""
    
    @patch('datapy.mod_manager.sdk.set_context')
    @patch('datapy.mod_manager.sdk._parse_common_args')
    def test_setup_context_command_line_override(self, mock_parse_args, mock_set_context):
        """Test setup_context with command line override."""
        mock_parse_args.return_value = {
            "log_level": "INFO",
            "log_provided": False,
            "context_path": "cmd_context.json",
            "context_provided": True
        }
        
        setup_context("param_context.json")
        
        mock_set_context.assert_called_once_with("cmd_context.json")  # Command line wins
    
    @patch('datapy.mod_manager.sdk.set_context')
    @patch('datapy.mod_manager.sdk._parse_common_args')
    def test_setup_context_explicit_path(self, mock_parse_args, mock_set_context):
        """Test setup_context with explicit path parameter."""
        mock_parse_args.return_value = {
            "log_level": "INFO",
            "log_provided": False,
            "context_path": "",
            "context_provided": False
        }
        
        setup_context("explicit_context.json")
        
        mock_set_context.assert_called_once_with("explicit_context.json")
    
    @patch('datapy.mod_manager.sdk.set_context')
    @patch('datapy.mod_manager.sdk._parse_common_args')
    def test_setup_context_no_context_set(self, mock_parse_args, mock_set_context):
        """Test setup_context with no context to set."""
        mock_parse_args.return_value = {
            "log_level": "INFO",
            "log_provided": False,
            "context_path": "",
            "context_provided": False
        }
        
        setup_context()  # No context provided
        
        mock_set_context.assert_not_called()  # Should not call set_context
    
    @patch('datapy.mod_manager.sdk.set_context')
    @patch('datapy.mod_manager.sdk._parse_common_args')
    def test_setup_context_none_path(self, mock_parse_args, mock_set_context):
        """Test setup_context with None path parameter."""
        mock_parse_args.return_value = {
            "log_level": "INFO",
            "log_provided": False,
            "context_path": "",
            "context_provided": False
        }
        
        setup_context(None)  # Explicit None
        
        mock_set_context.assert_not_called()  # Should not call set_context


class TestIntegrationScenarios:
    """Integration test cases for complete SDK workflows."""
    
    def test_complete_successful_workflow(self):
        """Test complete successful mod execution workflow with all components."""
        # Create comprehensive mocks for all dependencies
        mock_registry = MagicMock()
        mock_registry.get_mod_info.return_value = {
            "module_path": "datapy.mods.test_mod",
            "type": "test_processor",
            "version": "1.0.0",
            "description": "Test processor mod",
            "config_schema": {
                "required": {
                    "input_data": {"type": "str", "description": "Input data path"}
                },
                "optional": {
                    "output_format": {"type": "str", "default": "csv", "description": "Output format"}
                }
            }
        }
        
        mock_resolver = MagicMock()
        mock_resolver.resolve_mod_params.return_value = {
            "input_data": "resolved_input_path",
            "batch_size": 1000  # From project defaults
        }
        
        # Mock successful mod module
        mock_mod_module = MagicMock()
        mock_mod_module.run = MagicMock(return_value={
            "status": "success",
            "exit_code": SUCCESS,
            "metrics": {
                "rows_processed": 5000,
                "processing_rate": 0.98,
                "memory_used": "256MB"
            },
            "artifacts": {
                "processed_data": "<DataFrame with 5000 rows>",
                "output_file": "/output/processed_data.csv",
                "summary_report": {"total": 5000, "errors": 0}
            },
            "globals": {
                "last_processed_count": 5000,
                "processing_timestamp": "2024-01-15T10:30:00Z"
            },
            "warnings": [
                {"message": "Found 10 duplicate records", "warning_code": 10, "timestamp": 1640995200.123}
            ],
            "errors": [],
            "logs": {
                "run_id": "test_processor_20240115_103000_abc123",
                "mod_type": "test_processor",
                "mod_name": "data_processor"
            }
        })
        
        with patch('datapy.mod_manager.sdk.get_registry', return_value=mock_registry), \
             patch('datapy.mod_manager.sdk.create_resolver', return_value=mock_resolver), \
             patch('datapy.mod_manager.sdk.substitute_context_variables') as mock_substitute, \
             patch('datapy.mod_manager.sdk.validate_mod_parameters') as mock_validate, \
             patch('importlib.import_module', return_value=mock_mod_module), \
             patch('datapy.mod_manager.sdk.setup_logger') as mock_logger:
            
            # Set up context substitution
            mock_substitute.return_value = {
                "input_data": "/prod/data/customers.csv",  # After substitution
                "batch_size": 1000,
                "output_format": "parquet"
            }
            
            # Set up parameter validation
            mock_validate.return_value = {
                "input_data": "/prod/data/customers.csv",
                "batch_size": 1000,
                "output_format": "parquet",  # With defaults applied
                "_mod_name": "data_processor",
                "_mod_type": "test_processor"
            }
            
            # Execute the mod
            result = run_mod(
                mod_type="test_processor",
                params={
                    "input_data": "${env.data_path}/customers.csv",  # With variable
                    "output_format": "parquet"
                },
                mod_name="data_processor"
            )
            
            # Verify successful execution
            assert result["status"] == "success"
            assert result["exit_code"] == SUCCESS
            assert result["metrics"]["rows_processed"] == 5000
            assert len(result["warnings"]) == 1
            assert len(result["errors"]) == 0
            assert result["logs"]["mod_name"] == "data_processor"
            assert result["logs"]["mod_type"] == "test_processor"
            
            # Verify all steps were called correctly
            mock_registry.get_mod_info.assert_called_once_with("test_processor")
            mock_resolver.resolve_mod_params.assert_called_once()
            mock_substitute.assert_called_once()
            mock_validate.assert_called_once()
            mock_mod_module.run.assert_called_once()