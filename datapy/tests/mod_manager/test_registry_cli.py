"""
Test cases for datapy.mod_manager.registry_cli module.

Tests CLI commands for registry management including list-registry,
register-mod, validate-registry, mod-info, and delete-mod commands.
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

from datapy.mod_manager.registry_cli import (
    list_registry_command,
    register_mod_command,
    validate_registry_command,
    mod_info_command,
    delete_mod_command,
    registry_commands
)
from datapy.mod_manager.result import VALIDATION_ERROR, RUNTIME_ERROR
from datapy.mod_manager.base import ModMetadata, ConfigSchema


class TestListRegistryCommand:
    """Test cases for list-registry CLI command."""
    
    def test_list_registry_all_mods(self):
        """Test listing all mods without category filter."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        mock_registry.list_available_mods.return_value = ["csv_reader", "csv_filter", "csv_writer"]
        mock_registry.get_mod_info.side_effect = [
            {"version": "1.0.0", "description": "Read CSV files"},
            {"version": "1.1.0", "description": "Filter CSV data"},
            {"version": "1.0.0", "description": "Write CSV files"}
        ]
        
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(list_registry_command)
        
        assert result.exit_code == 0
        assert "csv_reader" in result.output
        assert "csv_filter" in result.output
        assert "csv_writer" in result.output
        assert "v1.0.0" in result.output
        assert "Read CSV files" in result.output
    
    def test_list_registry_with_category_filter(self):
        """Test listing mods filtered by category."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        mock_registry.list_available_mods.return_value = ["csv_reader", "excel_reader"]
        mock_registry.get_mod_info.side_effect = [
            {"version": "1.0.0", "description": "Read CSV files"},
            {"version": "2.0.0", "description": "Read Excel files"}
        ]
        
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(list_registry_command, ['--category', 'sources'])
        
        assert result.exit_code == 0
        assert "(sources)" in result.output
        assert "csv_reader" in result.output
        assert "excel_reader" in result.output
        mock_registry.list_available_mods.assert_called_once_with("sources")
    
    def test_list_registry_no_mods_found(self):
        """Test listing when no mods are found."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        mock_registry.list_available_mods.return_value = []
        
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(list_registry_command)
        
        assert result.exit_code == 0
        assert "No mods found" in result.output
    
    def test_list_registry_no_mods_in_category(self):
        """Test listing when no mods found in specific category."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        mock_registry.list_available_mods.return_value = []
        
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(list_registry_command, ['--category', 'transformers'])
        
        assert result.exit_code == 0
        assert "No mods found in category 'transformers'" in result.output
    
    def test_list_registry_mod_info_error(self):
        """Test listing when mod info retrieval fails."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        mock_registry.list_available_mods.return_value = ["bad_mod", "good_mod"]
        mock_registry.get_mod_info.side_effect = [
            Exception("Info retrieval failed"),
            {"version": "1.0.0", "description": "Good mod"}
        ]
        
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(list_registry_command)
        
        assert result.exit_code == 0
        assert "bad_mod" in result.output
        assert "Error loading info" in result.output
        assert "good_mod" in result.output
    
    def test_list_registry_exception(self):
        """Test error handling when registry access fails."""
        runner = CliRunner()
        
        with patch('datapy.mod_manager.registry_cli.get_registry', side_effect=Exception("Registry error")):
            result = runner.invoke(list_registry_command)
        
        assert result.exit_code == RUNTIME_ERROR
        assert "Error listing registry" in result.output


class TestRegisterModCommand:
    """Test cases for register-mod CLI command."""
    
    def test_register_mod_success(self):
        """Test successful mod registration."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        mock_registry.register_mod.return_value = True
        
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(register_mod_command, ['datapy.mods.sources.csv_reader'])
        
        assert result.exit_code == 0
        assert "Registering mod: datapy.mods.sources.csv_reader" in result.output
        assert "Successfully registered mod: csv_reader" in result.output
        mock_registry.register_mod.assert_called_once_with('datapy.mods.sources.csv_reader')
    
    def test_register_mod_failure(self):
        """Test mod registration failure."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        mock_registry.register_mod.return_value = False
        
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(register_mod_command, ['test.mod'])
        
        assert result.exit_code == RUNTIME_ERROR
        assert "Registration failed" in result.output
    
    def test_register_mod_value_error(self):
        """Test registration with ValueError (invalid mod)."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        mock_registry.register_mod.side_effect = ValueError("Invalid mod structure")
        
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(register_mod_command, ['invalid.mod'])
        
        assert result.exit_code == VALIDATION_ERROR
        assert "Registration failed: Invalid mod structure" in result.output
    
    def test_register_mod_general_exception(self):
        """Test registration with general exception."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        mock_registry.register_mod.side_effect = Exception("Unexpected error")
        
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(register_mod_command, ['test.mod'])
        
        assert result.exit_code == RUNTIME_ERROR
        assert "Registration failed: Unexpected error" in result.output


class TestValidateRegistryCommand:
    """Test cases for validate-registry CLI command."""
    
    def test_validate_registry_all_valid(self):
        """Test validation with all mods valid."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        mock_registry.validate_registry.return_value = []
        mock_registry.list_available_mods.return_value = ["mod1", "mod2", "mod3"]
        
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(validate_registry_command)
        
        assert result.exit_code == 0
        assert "Validating registry..." in result.output
        assert "Registry validation successful!" in result.output
        assert "All 3 mods are valid" in result.output
    
    def test_validate_registry_with_errors(self):
        """Test validation with errors found."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        mock_registry.validate_registry.return_value = [
            "Mod 'bad_mod' missing run() function",
            "Mod 'another_bad' import failed"
        ]
        
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(validate_registry_command)
        
        assert result.exit_code == VALIDATION_ERROR
        assert "Registry validation failed:" in result.output
        assert "Mod 'bad_mod' missing run() function" in result.output
        assert "Mod 'another_bad' import failed" in result.output
    
    def test_validate_registry_exception(self):
        """Test validation with exception."""
        runner = CliRunner()
        
        with patch('datapy.mod_manager.registry_cli.get_registry', side_effect=Exception("Registry error")):
            result = runner.invoke(validate_registry_command)
        
        assert result.exit_code == RUNTIME_ERROR
        assert "Registry validation failed: Registry error" in result.output


class TestModInfoCommand:
    """Test cases for mod-info CLI command."""
    
    def test_mod_info_success(self):
        """Test successful mod info retrieval."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        mock_registry.get_mod_info.return_value = {
            "module_path": "datapy.mods.sources.csv_reader",
            "version": "1.0.0",
            "category": "sources",
            "description": "Read CSV files",
            "input_ports": ["file_path"],
            "output_ports": ["data"],
            "globals": ["row_count"],
            "packages": ["pandas>=1.5.0"],
            "registered_at": "2024-01-01T12:00:00"
        }
        
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(mod_info_command, ['csv_reader'])
        
        assert result.exit_code == 0
        assert "Mod Information: csv_reader" in result.output
        assert "Module Path: datapy.mods.sources.csv_reader" in result.output
        assert "Version: 1.0.0" in result.output
        assert "Category: sources" in result.output
        assert "Description: Read CSV files" in result.output
        assert "Input Ports: ['file_path']" in result.output
        assert "Output Ports: ['data']" in result.output
        assert "Globals: ['row_count']" in result.output
        assert "Packages: ['pandas>=1.5.0']" in result.output
        assert "Registered: 2024-01-01T12:00:00" in result.output
    
    def test_mod_info_minimal_data(self):
        """Test mod info with minimal data."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        mock_registry.get_mod_info.return_value = {
            "type": "simple_mod"
        }
        
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(mod_info_command, ['simple_mod'])
        
        assert result.exit_code == 0
        assert "Module Path: unknown" in result.output
        assert "Version: unknown" in result.output
        assert "Category: unknown" in result.output
        assert "Description: No description" in result.output
    
    def test_mod_info_not_found(self):
        """Test mod info when mod not found."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        mock_registry.get_mod_info.side_effect = ValueError("Mod 'nonexistent' not found")
        
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(mod_info_command, ['nonexistent'])
        
        assert result.exit_code == VALIDATION_ERROR
        assert "Error: Mod 'nonexistent' not found" in result.output
    
    def test_mod_info_general_exception(self):
        """Test mod info with general exception."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        mock_registry.get_mod_info.side_effect = Exception("Unexpected error")
        
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(mod_info_command, ['test_mod'])
        
        assert result.exit_code == RUNTIME_ERROR
        assert "Error getting mod info: Unexpected error" in result.output


class TestDeleteModCommand:
    """Test cases for delete-mod CLI command."""
    
    def test_delete_mod_success_with_confirmation(self):
        """Test successful mod deletion with user confirmation."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        mock_registry.get_mod_info.return_value = {
            "description": "Test mod to delete",
            "module_path": "test.mod"
        }
        mock_registry.delete_mod.return_value = True
        
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            # Simulate user typing 'y' to confirm
            result = runner.invoke(delete_mod_command, ['test_mod'], input='y\n')
        
        assert result.exit_code == 0
        assert "Mod to delete: test_mod" in result.output
        assert "Test mod to delete" in result.output
        assert "Successfully deleted mod 'test_mod'" in result.output
        mock_registry.delete_mod.assert_called_once_with('test_mod')
    
    def test_delete_mod_cancelled_by_user(self):
        """Test mod deletion cancelled by user."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        mock_registry.get_mod_info.return_value = {
            "description": "Test mod",
            "module_path": "test.mod"
        }
        
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            # Simulate user typing 'n' to cancel
            result = runner.invoke(delete_mod_command, ['test_mod'], input='n\n')
        
        assert result.exit_code == 0
        assert "Deletion cancelled" in result.output
        mock_registry.delete_mod.assert_not_called()
    
    def test_delete_mod_with_force_flag(self):
        """Test mod deletion with --force flag (no confirmation)."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        mock_registry.get_mod_info.return_value = {"description": "Test mod"}
        mock_registry.delete_mod.return_value = True
        
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(delete_mod_command, ['test_mod', '--force'])
        
        assert result.exit_code == 0
        assert "Successfully deleted mod 'test_mod'" in result.output
        # Should not prompt for confirmation
        assert "Are you sure" not in result.output
    
    def test_delete_mod_not_found(self):
        """Test deleting non-existent mod."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        mock_registry.get_mod_info.side_effect = ValueError("Mod not found")
        
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(delete_mod_command, ['nonexistent'])
        
        assert result.exit_code == VALIDATION_ERROR
        assert "Error: Mod 'nonexistent' not found in registry" in result.output
    
    def test_delete_mod_deletion_failed(self):
        """Test when deletion operation fails."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        mock_registry.get_mod_info.return_value = {"description": "Test"}
        mock_registry.delete_mod.return_value = False
        
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(delete_mod_command, ['test_mod', '--force'])
        
        assert result.exit_code == RUNTIME_ERROR
        assert "Failed to delete mod 'test_mod'" in result.output
    
    def test_delete_mod_general_exception(self):
        """Test deletion with general exception."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        mock_registry.get_mod_info.return_value = {"description": "Test"}
        mock_registry.delete_mod.side_effect = Exception("Deletion error")
        
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(delete_mod_command, ['test_mod', '--force'])
        
        assert result.exit_code == RUNTIME_ERROR
        assert "Error deleting mod: Deletion error" in result.output


class TestRegistryCommandsExport:
    """Test cases for registry_commands export."""
    
    def test_registry_commands_export(self):
        """Test that all commands are exported."""
        assert len(registry_commands) == 5
        assert list_registry_command in registry_commands
        assert register_mod_command in registry_commands
        assert validate_registry_command in registry_commands
        assert mod_info_command in registry_commands
        assert delete_mod_command in registry_commands


class TestIntegrationScenarios:
    """Integration test cases for complete CLI workflows."""
    
    def test_complete_mod_management_workflow(self):
        """Test complete workflow: register, list, info, validate, delete."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        
        # Test register
        mock_registry.register_mod.return_value = True
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(register_mod_command, ['test.new_mod'])
        assert result.exit_code == 0
        
        # Test list
        mock_registry.list_available_mods.return_value = ["new_mod"]
        mock_registry.get_mod_info.return_value = {
            "version": "1.0.0",
            "description": "New test mod"
        }
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(list_registry_command)
        assert result.exit_code == 0
        assert "new_mod" in result.output
        
        # Test mod-info
        mock_registry.get_mod_info.return_value = {
            "module_path": "test.new_mod",
            "version": "1.0.0",
            "description": "New test mod"
        }
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(mod_info_command, ['new_mod'])
        assert result.exit_code == 0
        
        # Test validate
        mock_registry.validate_registry.return_value = []
        mock_registry.list_available_mods.return_value = ["new_mod"]
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(validate_registry_command)
        assert result.exit_code == 0
        
        # Test delete
        mock_registry.delete_mod.return_value = True
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(delete_mod_command, ['new_mod', '--force'])
        assert result.exit_code == 0
    
    def test_error_recovery_workflow(self):
        """Test error handling in various scenarios."""
        runner = CliRunner()
        
        mock_registry = MagicMock()
        
        # Test registering invalid mod
        mock_registry.register_mod.side_effect = ValueError("Invalid mod")
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(register_mod_command, ['invalid.mod'])
        assert result.exit_code == VALIDATION_ERROR
        
        # Test validation with errors
        mock_registry.validate_registry.return_value = ["Error 1", "Error 2"]
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(validate_registry_command)
        assert result.exit_code == VALIDATION_ERROR
        
        # Test getting info for non-existent mod
        mock_registry.get_mod_info.side_effect = ValueError("Not found")
        with patch('datapy.mod_manager.registry_cli.get_registry', return_value=mock_registry):
            result = runner.invoke(mod_info_command, ['nonexistent'])
        assert result.exit_code == VALIDATION_ERROR


class TestCommandHelp:
    """Test cases for command help output."""
    
    def test_list_registry_help(self):
        """Test list-registry help output."""
        runner = CliRunner()
        result = runner.invoke(list_registry_command, ['--help'])
        
        assert result.exit_code == 0
        assert "list-registry" in result.output
        assert "--category" in result.output
    
    def test_register_mod_help(self):
        """Test register-mod help output."""
        runner = CliRunner()
        result = runner.invoke(register_mod_command, ['--help'])
        
        assert result.exit_code == 0
        assert "register-mod" in result.output
        assert "MODULE_PATH" in result.output
    
    def test_validate_registry_help(self):
        """Test validate-registry help output."""
        runner = CliRunner()
        result = runner.invoke(validate_registry_command, ['--help'])
        
        assert result.exit_code == 0
        assert "validate-registry" in result.output
    
    def test_mod_info_help(self):
        """Test mod-info help output."""
        runner = CliRunner()
        result = runner.invoke(mod_info_command, ['--help'])
        
        assert result.exit_code == 0
        assert "mod-info" in result.output
        assert "MOD_TYPE" in result.output
    
    def test_delete_mod_help(self):
        """Test delete-mod help output."""
        runner = CliRunner()
        result = runner.invoke(delete_mod_command, ['--help'])
        
        assert result.exit_code == 0
        assert "delete-mod" in result.output
        assert "--force" in result.output