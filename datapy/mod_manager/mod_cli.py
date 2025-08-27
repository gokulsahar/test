"""
Mod execution CLI commands for DataPy framework.

Handles run-mod and run-script commands with registry-based execution.
"""

import json
import sys
import subprocess
import os
from pathlib import Path
from typing import Dict, Any, Optional

import click

from .logger import setup_console_logging, setup_logger, DEFAULT_LOG_CONFIG
from .params import load_job_config
from .result import VALIDATION_ERROR, RUNTIME_ERROR, SUCCESS, SUCCESS_WITH_WARNINGS
from .sdk import set_global_config, run_mod


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
@click.option('--exit-on-error', is_flag=True, default=True,
              help='Exit with error code on mod failure (default: True)')
@click.pass_context
def run_mod_command(ctx: click.Context, mod_name: str, params: str, exit_on_error: bool) -> None:
    """
    Execute a DataPy mod with registry-based execution.
    
    MOD_NAME: Variable name for the mod instance (must match YAML config)
    
    Examples:
        datapy run-mod extract_customers --params daily_job.yaml
        datapy run-mod clean_data --params daily_job.yaml
    """
    log_level = ctx.obj.get('log_level')
    
    try:
        # Validate mod_name
        if not mod_name or not isinstance(mod_name, str) or not mod_name.strip():
            click.echo("Error: mod_name must be a non-empty string", err=True)
            sys.exit(VALIDATION_ERROR)
        
        mod_name = mod_name.strip()
        if not mod_name.isidentifier():
            click.echo(f"Error: mod_name '{mod_name}' must be a valid identifier", err=True)
            sys.exit(VALIDATION_ERROR)
        
        # Load and validate YAML configuration
        try:
            config = load_job_config(params)
            mod_type, mod_params = _parse_mod_config(config, mod_name)
            
        except Exception as e:
            click.echo(f"Error parsing YAML file {params}: {e}", err=True)
            sys.exit(VALIDATION_ERROR)
        
        # Setup global config for SDK
        try:
            globals_config = config.get('globals', {})
            if log_level:
                globals_config['log_level'] = log_level
            
            # Set global config for SDK execution
            set_global_config(globals_config)
            
        except Exception as e:
            click.echo(f"Error setting up global config: {e}", err=True)
            sys.exit(RUNTIME_ERROR)
        
        # Output execution info
        click.echo(f"Executing mod: {mod_name} (type: {mod_type})")
        click.echo(f"Using parameters from: {params}")
        
        # Execute the mod using registry-based SDK
        try:
            result = run_mod(mod_type, mod_params, mod_name)
            
        except Exception as e:
            click.echo(f"Error executing mod: {e}", err=True)
            sys.exit(RUNTIME_ERROR)
        
        # Create CLI-friendly summary (exclude complex objects)
        cli_result = {
            "status": result['status'],
            "execution_time": result['execution_time'],
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
        
        # Output results wrapped in result:{}
        wrapped_result = {"result": cli_result}
        click.echo(json.dumps(wrapped_result, indent=2))
        
        # Handle exit code
        exit_code = result.get('exit_code', RUNTIME_ERROR)
        if result['status'] == 'error' and exit_on_error:
            click.echo(f"Mod failed with exit code: {exit_code}", err=True)
            sys.exit(exit_code)
        elif result['status'] == 'warning':
            sys.exit(SUCCESS_WITH_WARNINGS)
        else:
            sys.exit(SUCCESS)
            
    except SystemExit:
        raise  # Re-raise sys.exit calls
    except Exception as e:
        click.echo(f"CLI execution failed: {e}", err=True)
        import traceback
        click.echo(traceback.format_exc(), err=True)
        sys.exit(RUNTIME_ERROR)


@click.command('run-script')
@click.argument('script_path', type=click.Path(exists=True, readable=True))
@click.option('--params', '-p',
              type=click.Path(exists=True, readable=True),
              help='YAML parameter file path')
@click.option('--exit-on-error', is_flag=True, default=True,
              help='Exit with error code on script failure (default: True)')
@click.pass_context
def run_script_command(ctx: click.Context, script_path: str, params: Optional[str], exit_on_error: bool) -> None:
    """
    Execute a Python script with DataPy framework features.
    
    SCRIPT_PATH: Path to Python script file
    
    Examples:
        datapy run-script jobs/daily-etl/pipeline.py --params config.yaml
        datapy run-script my_pipeline.py
    """
    log_level = ctx.obj.get('log_level')
    
    try:
        # Validate script path
        script_file = Path(script_path)
        if not script_file.is_file():
            click.echo(f"Error: Script path is not a file: {script_path}", err=True)
            sys.exit(VALIDATION_ERROR)
        
        # Load parameters and setup globals if provided
        globals_config = {}
        if params:
            try:
                config = load_job_config(params)
                globals_config = config.get('globals', {})
            except Exception as e:
                click.echo(f"Error loading parameters from {params}: {e}", err=True)
                sys.exit(VALIDATION_ERROR)
        
        # Override log level if specified
        if log_level:
            globals_config['log_level'] = log_level
        
        # Setup console logging
        try:
            log_config = DEFAULT_LOG_CONFIG.copy()
            log_config.update(globals_config)
            setup_console_logging(log_config)
            logger = setup_logger(__name__)
            
        except Exception as e:
            click.echo(f"Error setting up logging: {e}", err=True)
            sys.exit(RUNTIME_ERROR)
        
        # Output execution info
        click.echo(f"Executing script: {script_path}")
        if params:
            click.echo(f"Using parameters from: {params}")
        
        logger.info(f"CLI executing script {script_path}", extra={
            "script_path": script_path,
            "params_file": params
        })
        
        # Prepare environment for the script
        env = os.environ.copy()
        
        # Add DataPy config as environment variables
        if globals_config:
            env['DATAPY_CONFIG'] = json.dumps(globals_config)
        
        # Execute the Python script
        try:
            cmd = [sys.executable, str(script_file)]
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                cwd=script_file.parent,
                env=env,
                timeout=3600  # 1 hour timeout
            )
            
            # Output script results
            if result.stdout:
                click.echo("--- Script Output ---")
                click.echo(result.stdout)
            
            if result.stderr:
                click.echo("--- Script Errors ---", err=True)
                click.echo(result.stderr, err=True)
            
            logger.info(f"Script execution completed", extra={
                "exit_code": result.returncode,
                "stdout_length": len(result.stdout) if result.stdout else 0,
                "stderr_length": len(result.stderr) if result.stderr else 0
            })
            
            # Handle script exit code
            if result.returncode != 0 and exit_on_error:
                click.echo(f"Script failed with exit code: {result.returncode}", err=True)
                sys.exit(result.returncode)
            else:
                sys.exit(result.returncode)
                
        except subprocess.TimeoutExpired:
            logger.error("Script execution timed out after 1 hour")
            click.echo("Error: Script execution timed out after 1 hour", err=True)
            sys.exit(RUNTIME_ERROR)
        except Exception as e:
            logger.error(f"Failed to execute script: {e}", exc_info=True)
            click.echo(f"Failed to execute script: {e}", err=True)
            sys.exit(RUNTIME_ERROR)
            
    except SystemExit:
        raise  # Re-raise sys.exit calls
    except Exception as e:
        click.echo(f"CLI execution failed: {e}", err=True)
        import traceback
        click.echo(traceback.format_exc(), err=True)
        sys.exit(RUNTIME_ERROR)


# Export commands for main CLI
mod_commands = [run_mod_command, run_script_command]