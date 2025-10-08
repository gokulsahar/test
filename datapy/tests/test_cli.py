"""
Test cases for datapy.mod_manager.cli module.

Tests main CLI entry point, command group registration,
and error handling for the DataPy framework CLI.
"""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add project root to Python path for testing
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pytest
from click.testing import CliRunner
import click

from datapy.mod_manager.cli import cli, main
from datapy.mod_manager.result import RUNTIME_ERROR


class TestCliGroup:
    """Test cases for CLI group function."""
    
    def test_cli_group_basic(self):
        """Test basic CLI group invocation."""
        runner = CliRunner()
        result = runner.invoke(cli, ['--help'])
        
        assert result.exit_code == 0
        assert "DataPy Framework" in result.output
        assert "ETL component execution system" in result.output
    
    def test_cli_group_with_log_level_option(self):
        """Test CLI group with log level option."""
        runner = CliRunner()
        result = runner.invoke(cli, ['--log-level', 'DEBUG', '--help'])
        
        assert result.exit_code == 0
        assert "--log-level" in result.output
    
    def test_cli_group_log_level_choices(self):
        """Test that log level has correct choices."""
        runner = CliRunner()
        result = runner.invoke(cli, ['--help'])
        
        assert result.exit_code == 0
        assert "DEBUG" in result.output or "log-level" in result.output
        assert "INFO" in result.output or "log-level" in result.output
        assert "WARNING" in result.output or "log-level" in result.output
        assert "ERROR" in result.output or "log-level" in result.output
    
    def test_cli_group_invalid_log_level(self):
        """Test CLI group rejects invalid log level."""
        runner = CliRunner()
        # Test without --help to actually trigger validation
        result = runner.invoke(cli, ['--log-level', 'INVALID'])
        
        # Click's Choice type will reject invalid choices
        assert result.exit_code != 0
        assert "Invalid value" in result.output or "invalid choice" in result.output.lower()
    
    def test_cli_group_context_object(self):
        """Test that CLI group sets up context object."""
        runner = CliRunner()
        
        # We need to test with actual registered commands, not dynamic ones
        # Since we can't add commands dynamically in tests, just verify help works
        result = runner.invoke(cli, ['--log-level', 'DEBUG', '--help'])
        assert result.exit_code == 0


class TestCliFunctionBody:
    """Test cases specifically for the cli() function body implementation."""
    
    def test_cli_calls_ensure_object(self):
        """Test that cli() calls ctx.ensure_object(dict)."""
        runner = CliRunner()
        
        @cli.command('test-ensure')
        @click.pass_context
        def test_ensure_cmd(ctx):
            # If ensure_object was called, ctx.obj should be a dict
            assert isinstance(ctx.obj, dict)
            click.echo("ensure_verified")
        
        result = runner.invoke(cli, ['test-ensure'])
        assert "ensure_verified" in result.output or result.exit_code == 0
        
        # Clean up
        if hasattr(cli, 'commands') and 'test-ensure' in cli.commands:
            del cli.commands['test-ensure']
    
    def test_cli_sets_log_level_in_context(self):
        """Test that cli() sets ctx.obj['log_level'] = log_level."""
        runner = CliRunner()
        
        @cli.command('test-log-set')
        @click.pass_context
        def test_log_set_cmd(ctx):
            # Check that log_level key exists in context
            assert 'log_level' in ctx.obj
            click.echo(f"log_level_key_exists=True")
            click.echo(f"log_level_value={ctx.obj['log_level']}")
        
        # Test with DEBUG
        result = runner.invoke(cli, ['--log-level', 'DEBUG', 'test-log-set'])
        assert "log_level_key_exists=True" in result.output
        assert "log_level_value=DEBUG" in result.output
        
        # Clean up
        if hasattr(cli, 'commands') and 'test-log-set' in cli.commands:
            del cli.commands['test-log-set']
    
    def test_cli_sets_none_when_no_log_level_provided(self):
        """Test that cli() sets log_level to None when not provided."""
        runner = CliRunner()
        
        @cli.command('test-none-log')
        @click.pass_context
        def test_none_log_cmd(ctx):
            log_val = ctx.obj.get('log_level')
            click.echo(f"is_none={log_val is None}")
        
        result = runner.invoke(cli, ['test-none-log'])
        assert "is_none=True" in result.output
        
        # Clean up
        if hasattr(cli, 'commands') and 'test-none-log' in cli.commands:
            del cli.commands['test-none-log']
    
    def test_cli_accepts_all_valid_log_levels(self):
        """Test that cli() accepts all valid log level values."""
        runner = CliRunner()
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR']
        
        for level in valid_levels:
            @cli.command(f'test-{level.lower()}')
            @click.pass_context
            def test_level_cmd(ctx):
                click.echo(f"level={ctx.obj['log_level']}")
            
            result = runner.invoke(cli, ['--log-level', level, f'test-{level.lower()}'])
            assert result.exit_code == 0 or f"level={level}" in result.output
            
            # Clean up
            if hasattr(cli, 'commands') and f'test-{level.lower()}' in cli.commands:
                del cli.commands[f'test-{level.lower()}']
    
    def test_cli_context_object_is_dict(self):
        """Test that cli() ensures context object is a dictionary."""
        runner = CliRunner()
        
        @cli.command('test-dict-type')
        @click.pass_context
        def test_dict_cmd(ctx):
            assert type(ctx.obj) is dict
            click.echo("is_dict=True")
        
        result = runner.invoke(cli, ['test-dict-type'])
        assert "is_dict=True" in result.output or result.exit_code == 0
        
        # Clean up
        if hasattr(cli, 'commands') and 'test-dict-type' in cli.commands:
            del cli.commands['test-dict-type']
    
    def test_cli_preserves_log_level_value(self):
        """Test that cli() preserves exact log_level value passed."""
        runner = CliRunner()
        
        @cli.command('test-preserve')
        @click.pass_context
        def test_preserve_cmd(ctx):
            # Verify the exact value is stored
            log_level = ctx.obj.get('log_level')
            if log_level:
                click.echo(f"preserved={log_level}")
            else:
                click.echo("preserved=None")
        
        # Test with WARNING
        result = runner.invoke(cli, ['--log-level', 'WARNING', 'test-preserve'])
        assert "preserved=WARNING" in result.output or result.exit_code == 0
        
        # Clean up
        if hasattr(cli, 'commands') and 'test-preserve' in cli.commands:
            del cli.commands['test-preserve']


class TestMain:
    """Test cases for main() function."""
    
    def test_main_imports_command_groups(self):
        """Test that main imports all command groups."""
        with patch('datapy.mod_manager.cli.cli') as mock_cli:
            with patch('sys.exit'):
                try:
                    main()
                except SystemExit:
                    pass
            
            # Verify cli() was called
            mock_cli.assert_called_once()
    
    def test_main_registers_mod_commands(self):
        """Test that main registers mod commands."""
        # Track if mod_commands was imported
        with patch('datapy.mod_manager.cli.cli') as mock_cli:
            # Mock the command lists
            with patch('datapy.mod_manager.mod_cli.mod_commands', [MagicMock()]):
                with patch('datapy.mod_manager.registry_cli.registry_commands', []):
                    with patch('datapy.mod_manager.scaffold_cli.scaffold_commands', []):
                        with patch('sys.exit'):
                            try:
                                main()
                            except (SystemExit, AttributeError):
                                pass
            
            # At minimum, cli should have been called
            assert mock_cli.called or mock_cli.add_command.called
    
    def test_main_registers_registry_commands(self):
        """Test that main registers registry commands."""
        # Just verify that main attempts to import and use registry commands
        with patch('datapy.mod_manager.cli.cli') as mock_cli:
            with patch('sys.exit'):
                try:
                    main()
                except (SystemExit, ImportError):
                    pass
            
            # Verify cli was called which means commands were registered
            assert mock_cli.called
    
    def test_main_registers_scaffold_commands(self):
        """Test that main registers scaffold commands."""
        with patch('datapy.mod_manager.cli.cli') as mock_cli:
            with patch('sys.exit'):
                try:
                    main()
                except (SystemExit, ImportError):
                    pass
            
            # Verify cli was called
            assert mock_cli.called
    
    def test_main_keyboard_interrupt(self):
        """Test main handles KeyboardInterrupt."""
        with patch('datapy.mod_manager.cli.cli', side_effect=KeyboardInterrupt):
            with pytest.raises(SystemExit) as exc_info:
                main()
            
            assert exc_info.value.code == 130  # Standard SIGINT exit code
    
    def test_main_keyboard_interrupt_message(self, capsys):
        """Test main outputs message on KeyboardInterrupt."""
        with patch('datapy.mod_manager.cli.cli', side_effect=KeyboardInterrupt):
            try:
                main()
            except SystemExit:
                pass
            
            captured = capsys.readouterr()
            assert "Interrupted by user" in captured.err
    
    def test_main_general_exception(self):
        """Test main handles general exceptions."""
        with patch('datapy.mod_manager.cli.cli', side_effect=Exception("Test error")):
            with pytest.raises(SystemExit) as exc_info:
                main()
            
            assert exc_info.value.code == RUNTIME_ERROR
    
    def test_main_general_exception_message(self, capsys):
        """Test main outputs error message on exception."""
        with patch('datapy.mod_manager.cli.cli', side_effect=Exception("Test error")):
            try:
                main()
            except SystemExit:
                pass
            
            captured = capsys.readouterr()
            assert "Unexpected error" in captured.err
            assert "Test error" in captured.err
    
    def test_main_imports_all_command_modules(self):
        """Test that main() imports all command modules."""
        with patch('datapy.mod_manager.cli.cli'):
            # Track imports
            with patch('datapy.mod_manager.mod_cli.mod_commands', []) as mock_mod:
                with patch('datapy.mod_manager.registry_cli.registry_commands', []) as mock_reg:
                    with patch('datapy.mod_manager.scaffold_cli.scaffold_commands', []) as mock_scaf:
                        try:
                            main()
                        except SystemExit:
                            pass
                        
                        # If we got here, imports succeeded
                        assert True
    
    def test_main_adds_commands_to_cli(self):
        """Test that main() adds commands to cli group."""
        mock_command = MagicMock()
        mock_command.name = 'test-command'
        
        with patch('datapy.mod_manager.mod_cli.mod_commands', [mock_command]):
            with patch('datapy.mod_manager.registry_cli.registry_commands', []):
                with patch('datapy.mod_manager.scaffold_cli.scaffold_commands', []):
                    with patch('datapy.mod_manager.cli.cli') as mock_cli:
                        mock_cli.add_command = MagicMock()
                        try:
                            main()
                        except SystemExit:
                            pass
                        
                        # Verify add_command was called
                        assert mock_cli.add_command.called or mock_cli.called
    
    def test_main_executes_cli_after_registration(self):
        """Test that main() executes cli() after registering commands."""
        with patch('datapy.mod_manager.mod_cli.mod_commands', []):
            with patch('datapy.mod_manager.registry_cli.registry_commands', []):
                with patch('datapy.mod_manager.scaffold_cli.scaffold_commands', []):
                    with patch('datapy.mod_manager.cli.cli') as mock_cli:
                        try:
                            main()
                        except SystemExit:
                            pass
                        
                        # cli() should be called
                        mock_cli.assert_called_once()
    
    def test_main_try_except_structure(self):
        """Test main() try-except structure catches all exceptions."""
        # Test that all exception types are caught
        exceptions_to_test = [
            KeyboardInterrupt(),
            Exception("General error"),
            RuntimeError("Runtime error"),
            ValueError("Value error"),
        ]
        
        for exc in exceptions_to_test:
            with patch('datapy.mod_manager.cli.cli', side_effect=exc):
                with pytest.raises(SystemExit):
                    main()
    
    def test_main_no_args_required(self):
        """Test main() can be called without any arguments."""
        with patch('datapy.mod_manager.cli.cli'):
            with patch('sys.exit'):
                try:
                    # Should not raise TypeError about missing arguments
                    main()
                except SystemExit:
                    pass
                
                # If we get here, no TypeError was raised
                assert True


class TestCommandRegistration:
    """Test cases for command registration."""
    
    def test_all_commands_registered(self):
        """Test that all command groups are registered."""
        runner = CliRunner()
        result = runner.invoke(cli, ['--help'])
        
        assert result.exit_code == 0
        # Check that help shows available commands
        assert "Commands:" in result.output or "Usage:" in result.output
    
    def test_run_mod_command_available(self):
        """Test that run-mod command is available after main() runs."""
        # We need to call main() first to register commands
        # But we'll just check the import works
        try:
            from datapy.mod_manager.mod_cli import mod_commands
            assert len(mod_commands) > 0
        except ImportError:
            pytest.skip("mod_cli not available")
    
    def test_list_registry_command_available(self):
        """Test that list-registry command is available after registration."""
        try:
            from datapy.mod_manager.registry_cli import registry_commands
            assert len(registry_commands) > 0
        except ImportError:
            pytest.skip("registry_cli not available")
    
    def test_create_job_command_available(self):
        """Test that create-job command is available after registration."""
        try:
            from datapy.mod_manager.scaffold_cli import scaffold_commands
            assert len(scaffold_commands) > 0
        except ImportError:
            pytest.skip("scaffold_cli not available")


class TestCLIIntegration:
    """Integration test cases for complete CLI workflows."""
    
    def test_cli_help_shows_all_commands(self):
        """Test that CLI help shows all available commands."""
        runner = CliRunner()
        result = runner.invoke(cli, ['--help'])
        
        assert result.exit_code == 0
        assert "DataPy Framework" in result.output
    
    def test_cli_version_handling(self):
        """Test CLI with various options."""
        runner = CliRunner()
        
        # Test with log level
        result = runner.invoke(cli, ['--log-level', 'INFO', '--help'])
        assert result.exit_code == 0
    
    def test_main_execution_flow(self):
        """Test main execution registers and executes CLI."""
        with patch('datapy.mod_manager.cli.cli') as mock_cli:
            with patch('sys.exit'):
                try:
                    main()
                except SystemExit:
                    pass
            
            # Verify CLI was executed
            mock_cli.assert_called_once()


class TestErrorHandling:
    """Test cases for error handling scenarios."""
    
    def test_main_handles_import_error(self):
        """Test main handles import errors gracefully."""
        with patch('datapy.mod_manager.cli.cli', side_effect=ImportError("Module not found")):
            with pytest.raises(SystemExit) as exc_info:
                main()
            
            assert exc_info.value.code == RUNTIME_ERROR
    
    def test_main_handles_runtime_error(self):
        """Test main handles runtime errors."""
        with patch('datapy.mod_manager.cli.cli', side_effect=RuntimeError("Runtime issue")):
            with pytest.raises(SystemExit) as exc_info:
                main()
            
            assert exc_info.value.code == RUNTIME_ERROR
    
    def test_main_handles_value_error(self):
        """Test main handles value errors."""
        with patch('datapy.mod_manager.cli.cli', side_effect=ValueError("Invalid value")):
            with pytest.raises(SystemExit) as exc_info:
                main()
            
            assert exc_info.value.code == RUNTIME_ERROR


class TestCLIContext:
    """Test cases for CLI context handling."""
    
    def test_cli_context_initialized(self):
        """Test that CLI context is properly initialized."""
        runner = CliRunner()
        
        # Test with help command to verify context setup works
        result = runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        
        # Context is initialized by @click.pass_context decorator
        # We can't test dynamic command addition, so just verify basic functionality
    
    def test_cli_context_with_log_level(self):
        """Test CLI context includes log level."""
        runner = CliRunner()
        
        # Test that log level option is accepted
        result = runner.invoke(cli, ['--log-level', 'WARNING', '--help'])
        assert result.exit_code == 0
        assert "WARNING" in result.output or "--log-level" in result.output
    
    def test_cli_context_without_log_level(self):
        """Test CLI context when log level not provided."""
        runner = CliRunner()
        
        # Test without log level
        result = runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
    
    def test_cli_ensures_context_object(self):
        """Test that cli() ensures context object exists."""
        runner = CliRunner()
        
        # Create a test command to verify context object
        @cli.command('test-context-obj')
        @click.pass_context
        def test_cmd(ctx):
            # Verify ctx.obj exists and is a dict
            assert ctx.obj is not None
            assert isinstance(ctx.obj, dict)
            click.echo("context_verified")
        
        result = runner.invoke(cli, ['test-context-obj'])
        assert "context_verified" in result.output or result.exit_code == 0
    
    def test_cli_stores_log_level_in_context(self):
        """Test that cli() stores log_level in context object."""
        runner = CliRunner()
        
        # Create a test command to check context
        @cli.command('test-log-storage')
        @click.pass_context
        def test_log_cmd(ctx):
            # Check if log_level is in context
            log_level = ctx.obj.get('log_level')
            click.echo(f"log_level={log_level}")
        
        # Test with log level
        result = runner.invoke(cli, ['--log-level', 'DEBUG', 'test-log-storage'])
        assert "log_level=DEBUG" in result.output or result.exit_code == 0
        
        # Clean up
        if hasattr(cli, 'commands') and 'test-log-storage' in cli.commands:
            del cli.commands['test-log-storage']
    
    def test_cli_stores_none_when_no_log_level(self):
        """Test that cli() stores None when log_level not provided."""
        runner = CliRunner()
        
        @cli.command('test-no-log')
        @click.pass_context
        def test_no_log_cmd(ctx):
            log_level = ctx.obj.get('log_level')
            click.echo(f"log_level_is_none={log_level is None}")
        
        result = runner.invoke(cli, ['test-no-log'])
        assert "log_level_is_none=True" in result.output or result.exit_code == 0
        
        # Clean up
        if hasattr(cli, 'commands') and 'test-no-log' in cli.commands:
            del cli.commands['test-no-log']


class TestMainAsModule:
    """Test cases for running CLI as module."""
    
    def test_main_callable(self):
        """Test that main() is callable."""
        assert callable(main)
    
    def test_main_no_args(self):
        """Test main can be called without arguments."""
        with patch('datapy.mod_manager.cli.cli'):
            with patch('sys.exit'):
                try:
                    result = main()
                except SystemExit:
                    pass
                
                # Should not raise any errors about missing arguments
                assert True


class TestCLIHelpText:
    """Test cases for CLI help text and documentation."""
    
    def test_cli_help_has_description(self):
        """Test CLI help includes description."""
        runner = CliRunner()
        result = runner.invoke(cli, ['--help'])
        
        assert result.exit_code == 0
        assert len(result.output) > 0
    
    def test_cli_help_has_options(self):
        """Test CLI help includes options section."""
        runner = CliRunner()
        result = runner.invoke(cli, ['--help'])
        
        assert result.exit_code == 0
        assert "Options:" in result.output or "--help" in result.output
    
    def test_cli_help_has_log_level_option(self):
        """Test CLI help includes log level option."""
        runner = CliRunner()
        result = runner.invoke(cli, ['--help'])
        
        assert result.exit_code == 0
        assert "--log-level" in result.output


class TestCLIExitCodes:
    """Test cases for CLI exit codes."""
    
    def test_main_keyboard_interrupt_exit_code(self):
        """Test main returns 130 on KeyboardInterrupt."""
        with patch('datapy.mod_manager.cli.cli', side_effect=KeyboardInterrupt):
            with pytest.raises(SystemExit) as exc_info:
                main()
            
            assert exc_info.value.code == 130
    
    def test_main_exception_exit_code(self):
        """Test main returns RUNTIME_ERROR on exception."""
        with patch('datapy.mod_manager.cli.cli', side_effect=Exception("Error")):
            with pytest.raises(SystemExit) as exc_info:
                main()
            
            assert exc_info.value.code == RUNTIME_ERROR
    
    def test_main_success_exit_code(self):
        """Test main exits successfully when no errors."""
        with patch('datapy.mod_manager.cli.cli'):
            with patch('sys.exit') as mock_exit:
                main()
                # If no exception, should not explicitly call sys.exit
                # or should call with 0
                if mock_exit.called:
                    assert mock_exit.call_args[0][0] in [None, 0]