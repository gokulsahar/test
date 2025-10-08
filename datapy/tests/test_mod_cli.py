"""
Test cases for datapy.mod_manager.mod_cli module.

Tests run-mod CLI command including parameter parsing, validation,
execution, and result handling.
"""

import sys
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add project root to Python path for testing
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pytest
from click.testing import CliRunner

from datapy.mod_manager.mod_cli import (
    run_mod_command,
    _parse_mod_config,
    _validate_and_prepare_mod_name,
    _setup_logging_and_context,
    _output_execution_info,
    _output_and_exit_with_result,
    _create_cli_result_summary,
    _handle_exit_code,
    mod_commands
)
from datapy.mod_manager.result import VALIDATION_ERROR, RUNTIME_ERROR, SUCCESS, SUCCESS_WITH_WARNINGS


class TestParseModConfig:
    """Test cases for _parse_mod_config function."""
    
    def test_parse_mod_config_success(self):
        """Test successful mod config parsing."""
        config = {
            "mods": {
                "extract_data": {
                    "_type": "csv_reader",
                    "file_path": "data.csv",
                    "encoding": "utf-8"
                }
            }
        }
        
        mod_type, mod_params = _parse_mod_config(config, "extract_data")
        
        assert mod_type == "csv_reader"
        assert mod_params == {"file_path": "data.csv", "encoding": "utf-8"}
        assert "_type" not in mod_params
    
    def test_parse_mod_config_missing_mods_section(self):
        """Test error when mods section is missing."""
        config = {"globals": {"log_level": "INFO"}}
        
        with pytest.raises(ValueError, match="missing or invalid 'mods' section"):
            _parse_mod_config(config, "test_mod")
    
    def test_parse_mod_config_invalid_mods_section(self):
        """Test error when mods section is not a dictionary."""
        config = {"mods": ["not", "a", "dict"]}
        
        with pytest.raises(ValueError, match="missing or invalid 'mods' section"):
            _parse_mod_config(config, "test_mod")
    
    def test_parse_mod_config_mod_not_found(self):
        """Test error when specified mod not found in config."""
        config = {
            "mods": {
                "mod1": {"_type": "type1"},
                "mod2": {"_type": "type2"}
            }
        }
        
        with pytest.raises(ValueError, match="Mod 'nonexistent' not found"):
            _parse_mod_config(config, "nonexistent")
    
    def test_parse_mod_config_not_dictionary(self):
        """Test error when mod config is not a dictionary."""
        config = {
            "mods": {
                "bad_mod": "not a dict"
            }
        }
        
        with pytest.raises(ValueError, match="configuration must be a dictionary"):
            _parse_mod_config(config, "bad_mod")
    
    def test_parse_mod_config_missing_type(self):
        """Test error when _type field is missing."""
        config = {
            "mods": {
                "no_type_mod": {
                    "param1": "value1"
                }
            }
        }
        
        with pytest.raises(ValueError, match="missing required '_type' field"):
            _parse_mod_config(config, "no_type_mod")
    
    def test_parse_mod_config_empty_type(self):
        """Test error when _type is empty."""
        config = {
            "mods": {
                "empty_type": {
                    "_type": ""
                }
            }
        }
        
        with pytest.raises(ValueError, match="_type must be a non-empty string"):
            _parse_mod_config(config, "empty_type")
    
    def test_parse_mod_config_type_not_string(self):
        """Test error when _type is not a string."""
        config = {
            "mods": {
                "bad_type": {
                    "_type": 123
                }
            }
        }
        
        with pytest.raises(ValueError, match="_type must be a non-empty string"):
            _parse_mod_config(config, "bad_type")


class TestValidateAndPrepareModName:
    """Test cases for _validate_and_prepare_mod_name function."""
    
    def test_validate_valid_mod_name(self):
        """Test validation of valid mod names."""
        valid_names = ["mod_name", "modName", "mod123", "_private_mod"]
        
        for name in valid_names:
            result = _validate_and_prepare_mod_name(name)
            assert result == name
    
    def test_validate_mod_name_with_spaces(self):
        """Test validation strips spaces."""
        result = _validate_and_prepare_mod_name("  mod_name  ")
        assert result == "mod_name"
    
    def test_validate_empty_mod_name(self):
        """Test error with empty mod name."""
        with pytest.raises(SystemExit) as exc_info:
            _validate_and_prepare_mod_name("")
        assert exc_info.value.code == VALIDATION_ERROR
    
    def test_validate_none_mod_name(self):
        """Test error with None mod name."""
        with pytest.raises(SystemExit) as exc_info:
            _validate_and_prepare_mod_name(None)
        assert exc_info.value.code == VALIDATION_ERROR
    
    def test_validate_whitespace_only(self):
        """Test error with whitespace-only mod name."""
        with pytest.raises(SystemExit) as exc_info:
            _validate_and_prepare_mod_name("   ")
        assert exc_info.value.code == VALIDATION_ERROR
    
    def test_validate_invalid_identifier(self):
        """Test error with invalid Python identifier."""
        invalid_names = ["mod-name", "mod name", "123mod", "mod@name"]
        
        for name in invalid_names:
            with pytest.raises(SystemExit) as exc_info:
                _validate_and_prepare_mod_name(name)
            assert exc_info.value.code == VALIDATION_ERROR


class TestSetupLoggingAndContext:
    """Test cases for _setup_logging_and_context function."""
    
    def test_setup_with_log_level(self):
        """Test setup with log level specified."""
        with patch('datapy.mod_manager.mod_cli.set_log_level') as mock_set_log:
            _setup_logging_and_context("DEBUG", None)
            mock_set_log.assert_called_once_with("DEBUG")
    
    def test_setup_without_log_level(self):
        """Test setup without log level (uses default)."""
        with patch('datapy.mod_manager.mod_cli.setup_console_logging') as mock_console:
            _setup_logging_and_context(None, None)
            mock_console.assert_called_once()
    
    def test_setup_with_context_file(self):
        """Test setup with context file."""
        with patch('datapy.mod_manager.mod_cli.set_context') as mock_context:
            _setup_logging_and_context(None, "context.json")
            mock_context.assert_called_once_with("context.json")
    
    def test_setup_logging_error(self):
        """Test error handling when logging setup fails."""
        with patch('datapy.mod_manager.mod_cli.set_log_level', side_effect=Exception("Logging error")):
            with pytest.raises(SystemExit) as exc_info:
                _setup_logging_and_context("DEBUG", None)
            assert exc_info.value.code == RUNTIME_ERROR
    
    def test_setup_context_error(self):
        """Test error handling when context setup fails."""
        with patch('datapy.mod_manager.mod_cli.set_context', side_effect=Exception("Context error")):
            with pytest.raises(SystemExit) as exc_info:
                _setup_logging_and_context(None, "bad_context.json")
            assert exc_info.value.code == VALIDATION_ERROR


class TestCreateCliResultSummary:
    """Test cases for _create_cli_result_summary function."""
    
    def test_create_summary_with_all_fields(self):
        """Test creating summary with all result fields."""
        result = {
            "status": "success",
            "exit_code": 0,
            "metrics": {"rows": 100},
            "warnings": ["Warning 1"],
            "errors": [],
            "logs": {"run_id": "123"},
            "artifacts": {
                "file_path": "/path/to/file",
                "count": 42,
                "flag": True,
                "items": [1, 2, 3],
                "config": {"key": "value"}
            },
            "globals": {"total": 100}
        }
        
        summary = _create_cli_result_summary(result)
        
        assert summary["status"] == "success"
        assert summary["exit_code"] == 0
        assert summary["metrics"] == {"rows": 100}
        assert summary["warnings"] == ["Warning 1"]
        assert summary["errors"] == []
        assert summary["logs"] == {"run_id": "123"}
        assert summary["artifacts"]["file_path"] == "/path/to/file"
        assert summary["artifacts"]["count"] == 42
        assert summary["artifacts"]["flag"] is True
        assert summary["globals"] == {"total": 100}
    
    def test_create_summary_with_complex_artifacts(self):
        """Test creating summary with complex artifact objects."""
        import pandas as pd
        
        result = {
            "status": "success",
            "exit_code": 0,
            "metrics": {},
            "warnings": [],
            "errors": [],
            "logs": {},
            "artifacts": {
                "dataframe": pd.DataFrame({"col": [1, 2, 3]}),
                "simple_string": "text"
            },
            "globals": {}
        }
        
        summary = _create_cli_result_summary(result)
        
        assert summary["artifacts"]["dataframe"] == "<DataFrame>"
        assert summary["artifacts"]["simple_string"] == "text"
    
    def test_create_summary_empty_artifacts(self):
        """Test creating summary with no artifacts."""
        result = {
            "status": "success",
            "exit_code": 0,
            "metrics": {},
            "warnings": [],
            "errors": [],
            "logs": {},
            "artifacts": {},
            "globals": {}
        }
        
        summary = _create_cli_result_summary(result)
        
        # Empty artifacts dict is not added to summary
        assert summary["status"] == "success"
        assert summary["exit_code"] == 0


class TestHandleExitCode:
    """Test cases for _handle_exit_code function."""
    
    def test_handle_exit_code_success(self):
        """Test handling success exit code."""
        result = {"status": "success", "exit_code": SUCCESS}
        
        with pytest.raises(SystemExit) as exc_info:
            _handle_exit_code(result, True)
        assert exc_info.value.code == SUCCESS
    
    def test_handle_exit_code_warning(self):
        """Test handling warning exit code."""
        result = {"status": "warning", "exit_code": SUCCESS_WITH_WARNINGS}
        
        with pytest.raises(SystemExit) as exc_info:
            _handle_exit_code(result, True)
        assert exc_info.value.code == SUCCESS_WITH_WARNINGS
    
    def test_handle_exit_code_error_with_exit(self):
        """Test handling error with exit_on_error=True."""
        result = {"status": "error", "exit_code": RUNTIME_ERROR}
        
        with pytest.raises(SystemExit) as exc_info:
            _handle_exit_code(result, True)
        assert exc_info.value.code == RUNTIME_ERROR
    
    def test_handle_exit_code_error_without_exit(self):
        """Test handling error with exit_on_error=False."""
        result = {"status": "error", "exit_code": RUNTIME_ERROR}
        
        with pytest.raises(SystemExit) as exc_info:
            _handle_exit_code(result, False)
        # Will still exit with success code when exit_on_error is False
        assert exc_info.value.code == SUCCESS


class TestRunModCommand:
    """Test cases for run-mod CLI command."""
    
    def test_run_mod_success(self, tmp_path):
        """Test successful mod execution."""
        runner = CliRunner()
        
        # Create config file
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
mods:
  test_mod:
    _type: csv_reader
    file_path: data.csv
""")
        
        mock_result = {
            "status": "success",
            "exit_code": SUCCESS,
            "metrics": {"rows": 100},
            "warnings": [],
            "errors": [],
            "logs": {},
            "artifacts": {"data": "processed"},
            "globals": {}
        }
        
        with patch('datapy.mod_manager.mod_cli.run_mod', return_value=mock_result), \
             patch('datapy.mod_manager.mod_cli.load_job_config', return_value={
                 "mods": {"test_mod": {"_type": "csv_reader", "file_path": "data.csv"}}
             }):
            result = runner.invoke(run_mod_command, ['test_mod', '--params', str(config_file)], 
                                   obj={'log_level': None})
        
        assert result.exit_code == SUCCESS
        assert "Executing mod: test_mod" in result.output
        assert '"status": "success"' in result.output
    
    def test_run_mod_with_context(self, tmp_path):
        """Test mod execution with context file."""
        runner = CliRunner()
        
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
mods:
  test_mod:
    _type: csv_reader
    file_path: data.csv
""")
        
        context_file = tmp_path / "context.json"
        context_file.write_text('{"env": "prod"}')
        
        mock_result = {
            "status": "success",
            "exit_code": SUCCESS,
            "metrics": {},
            "warnings": [],
            "errors": [],
            "logs": {},
            "artifacts": {},
            "globals": {}
        }
        
        with patch('datapy.mod_manager.mod_cli.run_mod', return_value=mock_result), \
             patch('datapy.mod_manager.mod_cli.load_job_config', return_value={
                 "mods": {"test_mod": {"_type": "csv_reader", "file_path": "data.csv"}}
             }), \
             patch('datapy.mod_manager.mod_cli.set_context'):
            result = runner.invoke(run_mod_command, [
                'test_mod',
                '--params', str(config_file),
                '--context', str(context_file)
            ], obj={'log_level': None})
        
        assert result.exit_code == SUCCESS
        assert "Using context file:" in result.output
    
    def test_run_mod_with_log_level(self, tmp_path):
        """Test mod execution with custom log level."""
        runner = CliRunner()
        
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
mods:
  test_mod:
    _type: csv_reader
    file_path: data.csv
""")
        
        mock_result = {
            "status": "success",
            "exit_code": SUCCESS,
            "metrics": {},
            "warnings": [],
            "errors": [],
            "logs": {},
            "artifacts": {},
            "globals": {}
        }
        
        with patch('datapy.mod_manager.mod_cli.run_mod', return_value=mock_result), \
             patch('datapy.mod_manager.mod_cli.load_job_config', return_value={
                 "mods": {"test_mod": {"_type": "csv_reader", "file_path": "data.csv"}}
             }):
            result = runner.invoke(run_mod_command, [
                'test_mod',
                '--params', str(config_file)
            ], obj={'log_level': 'DEBUG'})
        
        assert result.exit_code == SUCCESS
    
    def test_run_mod_invalid_mod_name(self, tmp_path):
        """Test error with invalid mod name."""
        runner = CliRunner()
        
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
mods:
  valid_mod:
    _type: csv_reader
""")
        
        result = runner.invoke(run_mod_command, ['invalid-name', '--params', str(config_file)],
                               obj={'log_level': None})
        
        assert result.exit_code == VALIDATION_ERROR
        assert "must be a valid identifier" in result.output
    
    def test_run_mod_missing_params_file(self):
        """Test error when params file doesn't exist."""
        runner = CliRunner()
        
        result = runner.invoke(run_mod_command, ['test_mod', '--params', 'nonexistent.yaml'],
                               obj={'log_level': None})
        
        assert result.exit_code != 0
    
    def test_run_mod_invalid_yaml(self, tmp_path):
        """Test error with invalid YAML file."""
        runner = CliRunner()
        
        config_file = tmp_path / "invalid.yaml"
        config_file.write_text("invalid: yaml: content: [")
        
        result = runner.invoke(run_mod_command, ['test_mod', '--params', str(config_file)],
                               obj={'log_level': None})
        
        assert result.exit_code == RUNTIME_ERROR
        assert "CLI execution failed" in result.output
    
    def test_run_mod_mod_not_in_config(self, tmp_path):
        """Test error when mod not found in config."""
        runner = CliRunner()
        
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
mods:
  other_mod:
    _type: csv_reader
""")
        
        with patch('datapy.mod_manager.mod_cli.load_job_config', return_value={
            "mods": {"other_mod": {"_type": "csv_reader"}}
        }):
            result = runner.invoke(run_mod_command, ['test_mod', '--params', str(config_file)],
                                   obj={'log_level': None})
        
        assert result.exit_code == RUNTIME_ERROR
        assert "CLI execution failed" in result.output or "not found" in result.output
    
    def test_run_mod_execution_error(self, tmp_path):
        """Test handling of mod execution errors."""
        runner = CliRunner()
        
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
mods:
  test_mod:
    _type: csv_reader
    file_path: data.csv
""")
        
        mock_result = {
            "status": "error",
            "exit_code": RUNTIME_ERROR,
            "metrics": {},
            "warnings": [],
            "errors": [{"message": "File not found"}],
            "logs": {},
            "artifacts": {},
            "globals": {}
        }
        
        with patch('datapy.mod_manager.mod_cli.run_mod', return_value=mock_result), \
             patch('datapy.mod_manager.mod_cli.load_job_config', return_value={
                 "mods": {"test_mod": {"_type": "csv_reader", "file_path": "data.csv"}}
             }):
            result = runner.invoke(run_mod_command, ['test_mod', '--params', str(config_file)],
                                   obj={'log_level': None})
        
        assert result.exit_code == RUNTIME_ERROR
        assert '"status": "error"' in result.output
    
    def test_run_mod_with_warnings(self, tmp_path):
        """Test mod execution with warnings."""
        runner = CliRunner()
        
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
mods:
  test_mod:
    _type: csv_reader
    file_path: data.csv
""")
        
        mock_result = {
            "status": "warning",
            "exit_code": SUCCESS_WITH_WARNINGS,
            "metrics": {},
            "warnings": ["Warning: Empty rows found"],
            "errors": [],
            "logs": {},
            "artifacts": {},
            "globals": {}
        }
        
        with patch('datapy.mod_manager.mod_cli.run_mod', return_value=mock_result), \
             patch('datapy.mod_manager.mod_cli.load_job_config', return_value={
                 "mods": {"test_mod": {"_type": "csv_reader", "file_path": "data.csv"}}
             }):
            result = runner.invoke(run_mod_command, ['test_mod', '--params', str(config_file)],
                                   obj={'log_level': None})
        
        assert result.exit_code == SUCCESS_WITH_WARNINGS
        assert '"status": "warning"' in result.output


class TestModCommandsExport:
    """Test cases for mod_commands export."""
    
    def test_mod_commands_export(self):
        """Test that run-mod command is exported."""
        assert len(mod_commands) == 1
        assert run_mod_command in mod_commands


class TestCommandHelp:
    """Test cases for command help output."""
    
    def test_run_mod_help(self):
        """Test run-mod help output."""
        runner = CliRunner()
        result = runner.invoke(run_mod_command, ['--help'])
        
        assert result.exit_code == 0
        assert "run-mod" in result.output
        assert "MOD_NAME" in result.output
        assert "--params" in result.output
        assert "--context" in result.output
        assert "--exit-on-error" in result.output


class TestOutputExecutionInfo:
    """Test cases for _output_execution_info function."""
    
    def test_output_execution_info(self, capsys):
        """Test execution info output."""
        _output_execution_info("test_mod", "csv_reader", "config.yaml")
        
        captured = capsys.readouterr()
        assert "Executing mod: test_mod (type: csv_reader)" in captured.out
        assert "Using parameters from: config.yaml" in captured.out


class TestOutputAndExitWithResult:
    """Test cases for _output_and_exit_with_result function."""
    
    def test_output_and_exit_success(self):
        """Test output and exit with success result."""
        result = {
            "status": "success",
            "exit_code": SUCCESS,
            "metrics": {"rows": 100},
            "warnings": [],
            "errors": [],
            "logs": {},
            "artifacts": {},
            "globals": {}
        }
        
        with pytest.raises(SystemExit) as exc_info:
            _output_and_exit_with_result(result, True)
        
        assert exc_info.value.code == SUCCESS
    
    def test_output_and_exit_error(self):
        """Test output and exit with error result."""
        result = {
            "status": "error",
            "exit_code": RUNTIME_ERROR,
            "metrics": {},
            "warnings": [],
            "errors": [{"message": "Error occurred"}],
            "logs": {},
            "artifacts": {},
            "globals": {}
        }
        
        with pytest.raises(SystemExit) as exc_info:
            _output_and_exit_with_result(result, True)
        
        assert exc_info.value.code == RUNTIME_ERROR


class TestIntegrationScenarios:
    """Integration test cases for complete CLI workflows."""
    
    def test_complete_execution_workflow(self, tmp_path):
        """Test complete mod execution workflow from start to finish."""
        runner = CliRunner()
        
        # Setup files
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text("""
mods:
  extract_data:
    _type: csv_reader
    file_path: customers.csv
    encoding: utf-8
""")
        
        context_file = tmp_path / "context.json"
        context_file.write_text('{"data": {"input": "/data/input"}}')
        
        mock_result = {
            "status": "success",
            "exit_code": SUCCESS,
            "metrics": {"rows_read": 1000, "file_size": 52341},
            "warnings": [],
            "errors": [],
            "logs": {"run_id": "test_123"},
            "artifacts": {"data": "mock_dataframe", "file_path": "/data/input/customers.csv"},
            "globals": {"row_count": 1000}
        }
        
        with patch('datapy.mod_manager.mod_cli.run_mod', return_value=mock_result), \
             patch('datapy.mod_manager.mod_cli.load_job_config', return_value={
                 "mods": {"extract_data": {"_type": "csv_reader", "file_path": "customers.csv", "encoding": "utf-8"}}
             }), \
             patch('datapy.mod_manager.mod_cli.set_context'):
            result = runner.invoke(run_mod_command, [
                'extract_data',
                '--params', str(config_file),
                '--context', str(context_file)
            ], obj={'log_level': 'INFO'})
        
        assert result.exit_code == SUCCESS
        assert "extract_data" in result.output
        assert "csv_reader" in result.output
        assert "success" in result.output