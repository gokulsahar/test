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
    # FIXED: Add None check first
    if config is None:
        raise ValueError("Configuration is None - YAML file may be empty or invalid")
    
    if not isinstance(config, dict):
        raise ValueError(f"Configuration must be a dictionary, got {type(config)}")
    
    if 'mods' not in config:
        raise ValueError("YAML file missing 'mods' section")
    
    if not isinstance(config['mods'], dict):
        raise ValueError("YAML file 'mods' section must be a dictionary")
    
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
        # Validate mod_name
        if not mod_name or not isinstance(mod_name, str) or not mod_name.strip():
            click.echo("Error: mod_name must be a non-empty string")  # FIXED: removed err=True
            sys.exit(VALIDATION_ERROR)
        
        mod_name = mod_name.strip()
        if not mod_name.isidentifier():
            click.echo(f"Error: mod_name '{mod_name}' must be a valid identifier")  # FIXED: removed err=True
            sys.exit(VALIDATION_ERROR)
        
        # Setup logging from CLI flag only (no YAML globals)
        try:
            if log_level:
                set_log_level(log_level)
            else:
                # Setup default console logging
                setup_console_logging(DEFAULT_LOG_CONFIG)
        except Exception as e:
            click.echo(f"Error setting up logging: {e}")  # FIXED: removed err=True
            sys.exit(RUNTIME_ERROR)
        
        # Setup context if provided
        if context:
            try:
                set_context(context)
                click.echo(f"Using context file: {context}")
            except Exception as e:
                click.echo(f"Error setting context file {context}: {e}")  # FIXED: removed err=True
                sys.exit(VALIDATION_ERROR)
        
        # FIXED: Load and validate YAML configuration with specific error handling
        config = None
        mod_type = None
        mod_params = None
        
        try:
            config = load_job_config(params)
            if config is None:
                raise ValueError("YAML file loaded as None - file may be empty")
                
        except FileNotFoundError as e:
            click.echo(f"Error: Parameter file not found: {params}")
            sys.exit(VALIDATION_ERROR)
        except RuntimeError as e:
            # This catches YAML parsing errors from load_job_config
            click.echo(f"Error: Invalid YAML file: {e}")
            sys.exit(VALIDATION_ERROR)
        except Exception as e:
            click.echo(f"Error loading parameter file {params}: {e}")
            sys.exit(VALIDATION_ERROR)
        
        try:
            mod_type, mod_params = _parse_mod_config(config, mod_name)
        except ValueError as e:
            # This catches mod configuration errors
            click.echo(f"Error: {e}")
            sys.exit(VALIDATION_ERROR)
        except Exception as e:
            click.echo(f"Error parsing mod configuration: {e}")
            sys.exit(VALIDATION_ERROR)
        
        # Output execution info
        click.echo(f"Executing mod: {mod_name} (type: {mod_type})")
        click.echo(f"Using parameters from: {params}")
        
        # Execute the mod using registry-based SDK
        try:
            result = run_mod(mod_type, mod_params, mod_name)
            
        except Exception as e:
            click.echo(f"Error executing mod: {e}")  # FIXED: removed err=True
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
            click.echo(f"Mod failed with exit code: {exit_code}")  # FIXED: removed err=True
            sys.exit(exit_code)
        elif result['status'] == 'warning':
            sys.exit(SUCCESS_WITH_WARNINGS)
        else:
            sys.exit(SUCCESS)
            
    except SystemExit:
        raise  # Re-raise sys.exit calls
    except Exception as e:
        click.echo(f"CLI execution failed: {e}")  # FIXED: removed err=True
        import traceback
        click.echo(traceback.format_exc())  # FIXED: removed err=True
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
        # FIXED: Better path validation
        script_file = Path(script_path)
        if not script_file.exists():
            click.echo(f"Error: Script file not found: {script_path}")
            sys.exit(VALIDATION_ERROR)
        
        if not script_file.is_file():
            click.echo(f"Error: Script path is not a file: {script_path}")
            sys.exit(VALIDATION_ERROR)
        
        # FIXED: Check if file is readable
        try:
            with open(script_file, 'r') as f:
                f.read(1)  # Try to read first character
        except PermissionError:
            click.echo(f"Error: Cannot read script file: {script_path}")
            sys.exit(VALIDATION_ERROR)
        except Exception as e:
            click.echo(f"Error: Script file issue: {e}")
            sys.exit(VALIDATION_ERROR)
        
        # Setup logging from CLI flag only (no YAML globals)
        try:
            if log_level:
                set_log_level(log_level)
            else:
                # Setup default console logging
                setup_console_logging(DEFAULT_LOG_CONFIG)
        except Exception as e:
            click.echo(f"Error setting up logging: {e}")
            sys.exit(RUNTIME_ERROR)
        
        # Load parameters if provided (but don't extract globals)
        script_env = {}
        if params:
            try:
                config = load_job_config(params)
                if config is not None:
                    # Pass entire config as environment variable for script to use
                    script_env['DATAPY_CONFIG'] = json.dumps(config)
            except Exception as e:
                click.echo(f"Error loading parameters from {params}: {e}")
                sys.exit(VALIDATION_ERROR)
        
        logger = setup_logger(__name__)
        
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
        env.update(script_env)
        
        # FIXED: Execute the Python script with better error handling and validation
        try:
            # Validate Python executable
            if not sys.executable:
                click.echo("Error: Cannot find Python executable")
                sys.exit(RUNTIME_ERROR)
            
            cmd = [sys.executable, str(script_file)]
            
            # FIXED: Add shell=False explicitly and better error handling
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                cwd=script_file.parent,
                env=env,
                timeout=3600,  # 1 hour timeout
                shell=False  # Explicit shell=False for security
            )
            
            # Output script results to stdout (not stderr)
            if result.stdout:
                click.echo("--- Script Output ---")
                click.echo(result.stdout)
            
            if result.stderr:
                click.echo("--- Script Errors ---")
                click.echo(result.stderr)  # FIXED: Show stderr to stdout
            
            logger.info(f"Script execution completed", extra={
                "exit_code": result.returncode,
                "stdout_length": len(result.stdout) if result.stdout else 0,
                "stderr_length": len(result.stderr) if result.stderr else 0
            })
            
            # Handle script exit code
            if result.returncode != 0 and exit_on_error:
                click.echo(f"Script failed with exit code: {result.returncode}")
                sys.exit(result.returncode)
            else:
                sys.exit(result.returncode)
                
        except subprocess.TimeoutExpired:
            logger.error("Script execution timed out after 1 hour")
            click.echo("Error: Script execution timed out after 1 hour")
            sys.exit(RUNTIME_ERROR)
        except FileNotFoundError as e:
            click.echo(f"Error: Python executable or script not found: {e}")
            sys.exit(RUNTIME_ERROR)
        except PermissionError as e:
            click.echo(f"Error: Permission denied executing script: {e}")
            sys.exit(RUNTIME_ERROR)
        except Exception as e:
            logger.error(f"Failed to execute script: {e}", exc_info=True)
            click.echo(f"Failed to execute script: {e}")
            sys.exit(RUNTIME_ERROR)
            
    except SystemExit:
        raise  # Re-raise sys.exit calls
    except Exception as e:
        click.echo(f"CLI execution failed: {e}")
        import traceback
        click.echo(traceback.format_exc())
        sys.exit(RUNTIME_ERROR)


# Export commands for main CLI
mod_commands = [run_mod_command, run_script_command]