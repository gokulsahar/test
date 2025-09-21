"""
Mod execution CLI commands for DataPy framework.

Handles run-mod command with registry-based execution.
"""

import json
import sys
from typing import Dict, Any, Optional

import click

from .context import set_context  
from .logger import set_log_level, setup_logger, setup_console_logging, DEFAULT_LOG_CONFIG
from .params import load_job_config
from .result import VALIDATION_ERROR, RUNTIME_ERROR, SUCCESS, SUCCESS_WITH_WARNINGS
from .sdk import  run_mod


def _parse_mod_config(config: Dict[str, Any], mod_name: str) -> tuple[str, Dict[str, Any]]:
    """
    Parse mod configuration from YAML config.
    
    Args:
        config: Full YAML configuration
        mod_name: Name of mod to extract
        
    Returns:
        Tuple of (mod_type, mod_params)
        
    Raises:
        ValueError: If mod configuration is invalid
    """
    if 'mods' not in config or not isinstance(config['mods'], dict):
        raise ValueError("YAML file missing or invalid 'mods' section")
    
    if mod_name not in config['mods']:
        available_mods = list(config['mods'].keys())
        raise ValueError(f"Mod '{mod_name}' not found. Available mods: {available_mods}")
    
    mod_config = config['mods'][mod_name]
    if not isinstance(mod_config, dict):
        raise ValueError(f"Mod '{mod_name}' configuration must be a dictionary")
    
    if '_type' not in mod_config:
        raise ValueError(f"Mod '{mod_name}' missing required '_type' field")
    
    mod_type = mod_config['_type']
    if not mod_type or not isinstance(mod_type, str):
        raise ValueError(f"Mod '{mod_name}' _type must be a non-empty string")
    
    # Extract parameters (excluding _type)
    mod_params = {k: v for k, v in mod_config.items() if k != '_type'}
    
    return mod_type.strip(), mod_params


@click.command('run-mod')
@click.argument('mod_name')
@click.option('--params', '-p', required=True,
              type=click.Path(exists=True, readable=True),
              help='YAML parameter file path')
@click.option('--context', '-c',
              type=click.Path(exists=True, readable=True),
              help='Context JSON file path for variable substitution')
@click.option('--exit-on-error', is_flag=True, default=True,
              help='Exit with error code on mod failure (default: True)')
@click.pass_context
def run_mod_command(ctx: click.Context, mod_name: str, params: str, context: Optional[str], exit_on_error: bool) -> None:
    """
    Execute a DataPy mod with registry-based execution.
    
    MOD_NAME: Variable name for the mod instance (must match YAML config)
    
    Examples:
        datapy run-mod extract_customers --params daily_job.yaml
        datapy run-mod extract_customers --params daily_job.yaml --context prod_context.json
    """
    log_level = ctx.obj.get('log_level')
    
    try:
        # Validate and setup
        validated_mod_name = _validate_and_prepare_mod_name(mod_name)
        _setup_logging_and_context(log_level, context)
        
        # Load configuration and execute
        config = load_job_config(params)
        mod_type, mod_params = _parse_mod_config(config, validated_mod_name)
        
        _output_execution_info(validated_mod_name, mod_type, params)
        
        # Execute and handle results
        result = run_mod(mod_type, mod_params, validated_mod_name)
        _output_and_exit_with_result(result, exit_on_error)
        
    except SystemExit:
        raise  # Re-raise sys.exit calls
    except Exception as e:
        click.echo(f"CLI execution failed: {e}", err=True)
        import traceback
        click.echo(traceback.format_exc(), err=True)
        sys.exit(RUNTIME_ERROR)

def _validate_and_prepare_mod_name(mod_name: str) -> str:
    """Validate and prepare mod name."""
    if not mod_name or not isinstance(mod_name, str) or not mod_name.strip():
        click.echo("Error: mod_name must be a non-empty string", err=True)
        sys.exit(VALIDATION_ERROR)
    
    mod_name = mod_name.strip()
    if not mod_name.isidentifier():
        click.echo(f"Error: mod_name '{mod_name}' must be a valid identifier", err=True)
        sys.exit(VALIDATION_ERROR)
    
    return mod_name

def _setup_logging_and_context(log_level: Optional[str], context: Optional[str]) -> None:
    """Setup logging and context configuration."""
    # Setup logging from CLI flag only (no YAML globals)
    try:
        if log_level:
            set_log_level(log_level)
        else:
            # Setup default console logging
            setup_console_logging(DEFAULT_LOG_CONFIG)
    except Exception as e:
        click.echo(f"Error setting up logging: {e}", err=True)
        sys.exit(RUNTIME_ERROR)
    
    # Setup context if provided
    if context:
        try:
            set_context(context)
            click.echo(f"Using context file: {context}")
        except Exception as e:
            click.echo(f"Error setting context file {context}: {e}", err=True)
            sys.exit(VALIDATION_ERROR)

def _output_execution_info(mod_name: str, mod_type: str, params: str) -> None:
    """Output execution information."""
    click.echo(f"Executing mod: {mod_name} (type: {mod_type})")
    click.echo(f"Using parameters from: {params}")

def _output_and_exit_with_result(result: Dict[str, Any], exit_on_error: bool) -> None:
    """Output result and exit with appropriate code."""
    # Create CLI-friendly summary
    cli_result = _create_cli_result_summary(result)
    
    # Output results wrapped in result:{}
    wrapped_result = {"result": cli_result}
    click.echo(json.dumps(wrapped_result, indent=2))
    
    # Handle exit code
    _handle_exit_code(result, exit_on_error)

def _create_cli_result_summary(result: Dict[str, Any]) -> Dict[str, Any]:
    """Create CLI-friendly result summary."""
    cli_result = {
        "status": result['status'],
        "exit_code": result['exit_code'],
        "metrics": result['metrics'],
        "warnings": result['warnings'],
        "errors": result['errors'],
        "logs": result['logs']
    }
    
    # Add artifacts (show file paths, URIs, and simple values)
    if result['artifacts']:
        cli_result["artifacts"] = {}
        for key, value in result['artifacts'].items():
            if isinstance(value, str):
                # File paths, URIs, simple strings
                cli_result["artifacts"][key] = value
            elif isinstance(value, (int, float, bool, list, dict)):
                # Simple data types
                cli_result["artifacts"][key] = value
            else:
                # Complex objects (DataFrame, etc.) - show type placeholder
                cli_result["artifacts"][key] = f"<{type(value).__name__}>"
    
    # Add globals (usually just simple values)
    cli_result["globals"] = result['globals']
    
    return cli_result

def _handle_exit_code(result: Dict[str, Any], exit_on_error: bool) -> None:
    """Handle exit code based on result status."""
    exit_code = result.get('exit_code', RUNTIME_ERROR)
    if result['status'] == 'error' and exit_on_error:
        click.echo(f"Mod failed with exit code: {exit_code}", err=True)
        sys.exit(exit_code)
    elif result['status'] == 'warning':
        sys.exit(SUCCESS_WITH_WARNINGS)
    else:
        sys.exit(SUCCESS)


# Export commands for main CLI
mod_commands = [run_mod_command]