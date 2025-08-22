"""
Click-based CLI runner for DataPy framework with state-based logging.

Provides command-line interface for executing mods and Python scripts with
automatic state management and lifecycle handling.
"""

import json
import sys
import subprocess
import os
import time
from pathlib import Path
from typing import Dict, Any, Optional

import click

from .logger import (
    setup_job_logging, setup_logger, is_job_complete_cli, 
    archive_completed_state, initialize_job_state, DEFAULT_LOG_CONFIG
)
from .params import load_job_config
from .result import VALIDATION_ERROR, RUNTIME_ERROR, SUCCESS, SUCCESS_WITH_WARNINGS
from .sdk import set_global_config, run_mod


def _create_cli_job_files(yaml_name: str) -> tuple[str, str]:
    """
    Create job files for CLI execution.
    
    Args:
        yaml_name: Base name from YAML file
        
    Returns:
        Tuple of (log_file_path, state_file_path)
        
    Raises:
        RuntimeError: If file creation fails
    """
    try:
        log_base = Path("logs")
        state_running = log_base / "state" / "running"
        
        # Ensure directories exist
        state_running.mkdir(parents=True, exist_ok=True)
        log_base.mkdir(exist_ok=True)
        
        # Create new execution files
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        execution_id = f"{yaml_name}_{timestamp}"
        log_file_path = str(log_base / f"{execution_id}.log")
        state_file_path = str(state_running / f"{execution_id}.state")
        
        # Create log file
        Path(log_file_path).touch()
        
        return log_file_path, state_file_path
        
    except Exception as e:
        raise RuntimeError(f"Failed to create CLI job files: {e}")


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


@click.group()
@click.option('--log-level', 
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False),
              help='Override log level')
@click.option('--quiet', '-q', is_flag=True, help='Minimal output (status only)')
@click.pass_context
def cli(ctx: click.Context, log_level: Optional[str], quiet: bool) -> None:
    """DataPy Framework - ETL component execution system."""
    ctx.ensure_object(dict)
    ctx.obj['quiet'] = quiet
    ctx.obj['log_level'] = log_level


@cli.command('run-mod')
@click.argument('mod_name')
@click.option('--params', '-p', required=True,
              type=click.Path(exists=True, readable=True),
              help='YAML parameter file path')
@click.option('--exit-on-error', is_flag=True, default=True,
              help='Exit with error code on mod failure (default: True)')
@click.pass_context
def run_mod_command(ctx: click.Context, mod_name: str, params: str, exit_on_error: bool) -> None:
    """
    Execute a DataPy mod with state-based logging.
    
    MOD_NAME: Variable name for the mod instance (must match YAML config)
    
    Examples:
        datapy run-mod extract_customers --params daily_job.yaml
        datapy run-mod clean_data --params daily_job.yaml
    """
    quiet = ctx.obj.get('quiet', False)
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
        
        # Extract yaml_name for file naming
        yaml_name = Path(params).stem
        
        # Setup job files and state management
        try:
            log_file_path, state_file_path = _create_cli_job_files(yaml_name)
            
            # Initialize CLI state
            expected_mods = list(config.get('mods', {}).keys())
            initialize_job_state(state_file_path, params, expected_mods)
            
        except Exception as e:
            click.echo(f"Error setting up execution environment: {e}", err=True)
            sys.exit(RUNTIME_ERROR)
        
        # Setup logging
        try:
            globals_config = config.get('globals', {})
            if log_level:
                globals_config['log_level'] = log_level
            
            setup_job_logging(log_file_path, globals_config)
            logger = setup_logger(__name__, log_file_path)
            
        except Exception as e:
            click.echo(f"Error setting up logging: {e}", err=True)
            sys.exit(RUNTIME_ERROR)
        
        # Output execution info
        if not quiet:
            click.echo(f"Executing mod: {mod_name} (type: {mod_type})")
            click.echo(f"Using parameters from: {params}")
            click.echo(f"Log file: {log_file_path}")
        
        logger.info(f"CLI executing mod {mod_name}", extra={
            "mod_type": mod_type,
            "yaml_file": params,
            "state_file": state_file_path
        })
        
        # Execute the mod using SDK
        try:
            # Set global config for SDK
            set_global_config(globals_config)
            
            # Execute the mod
            result = run_mod(mod_type, mod_params, mod_name)
            
        except Exception as e:
            click.echo(f"Error executing mod: {e}", err=True)
            sys.exit(RUNTIME_ERROR)
        
        # Check for job completion and archive if complete
        try:
            if is_job_complete_cli(state_file_path):
                archive_completed_state(state_file_path)
                logger.info("Job completed, archived state file")
        except Exception as e:
            logger.warning(f"Failed to archive completed state: {e}")
        
        # Output results
        if quiet:
            click.echo(result['status'])
        else:
            click.echo(json.dumps(result, indent=2))
        
        # Handle exit code
        exit_code = result.get('exit_code', RUNTIME_ERROR)
        if result['status'] == 'error' and exit_on_error:
            if not quiet:
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
        if not quiet:
            import traceback
            click.echo(traceback.format_exc(), err=True)
        sys.exit(RUNTIME_ERROR)


@cli.command('run-script')
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
    quiet = ctx.obj.get('quiet', False)
    log_level = ctx.obj.get('log_level')
    
    try:
        # Validate script path
        script_file = Path(script_path)
        if not script_file.is_file():
            click.echo(f"Error: Script path is not a file: {script_path}", err=True)
            sys.exit(VALIDATION_ERROR)
        
        # Setup execution context
        script_name = script_file.stem
        
        try:
            log_file_path, state_file_path = _create_cli_job_files(f"script_{script_name}")
            
            # Initialize SDK-style state for scripts
            from .logger import initialize_job_state
            initialize_job_state(state_file_path, str(script_path), [])
            
        except Exception as e:
            click.echo(f"Error setting up execution environment: {e}", err=True)
            sys.exit(RUNTIME_ERROR)
        
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
        
        # Setup logging
        try:
            setup_job_logging(log_file_path, globals_config)
            logger = setup_logger(__name__, log_file_path)
            
        except Exception as e:
            click.echo(f"Error setting up logging: {e}", err=True)
            sys.exit(RUNTIME_ERROR)
        
        # Output execution info
        if not quiet:
            click.echo(f"Executing script: {script_path}")
            if params:
                click.echo(f"Using parameters from: {params}")
            click.echo(f"Log file: {log_file_path}")
        
        logger.info(f"CLI executing script {script_path}", extra={
            "script_path": script_path,
            "params_file": params,
            "state_file": state_file_path
        })
        
        # Prepare environment for the script
        env = os.environ.copy()
        
        # Add DataPy config as environment variables
        if globals_config:
            env['DATAPY_CONFIG'] = json.dumps(globals_config)
        
        # Add log file path for script's potential framework usage
        env['DATAPY_LOG_FILE'] = log_file_path
        env['DATAPY_STATE_FILE'] = state_file_path
        
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
            if not quiet:
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
                if quiet:
                    click.echo("error")
                else:
                    click.echo(f"Script failed with exit code: {result.returncode}", err=True)
                sys.exit(result.returncode)
            else:
                if quiet:
                    click.echo("success" if result.returncode == 0 else "warning")
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
        if not quiet:
            import traceback
            click.echo(traceback.format_exc(), err=True)
        sys.exit(RUNTIME_ERROR)


@cli.command('list-mods')
@click.option('--category', type=click.Choice(['sources', 'transformers', 'sinks', 'solos']),
              help='Filter by mod category')
@click.pass_context
def list_mods_command(ctx: click.Context, category: Optional[str]) -> None:
    """
    List available DataPy mods.
    
    Examples:
        datapy list-mods
        datapy list-mods --category sources
    """
    quiet = ctx.obj.get('quiet', False)
    
    try:
        import pkgutil
        import datapy.mods
        
        categories = [category] if category else ['sources', 'transformers', 'sinks', 'solos']
        
        if not quiet:
            click.echo("Available DataPy Mods:")
            click.echo("=" * 50)
        
        for cat in categories:
            try:
                cat_module = __import__(f'datapy.mods.{cat}', fromlist=[''])
                mod_names = [name for _, name, ispkg in pkgutil.iter_modules(cat_module.__path__) if not ispkg]
                
                if mod_names:
                    if not quiet:
                        click.echo(f"\n{cat.upper()}:")
                        for mod in sorted(mod_names):
                            click.echo(f"  - {mod}")
                    else:
                        for mod in sorted(mod_names):
                            click.echo(f"{cat}.{mod}")
                            
            except ImportError:
                if not quiet:
                    click.echo(f"\n{cat.upper()}: No mods found")
        
        if not quiet:
            click.echo("\nUse 'datapy run-mod <mod_name> --params <config.yaml>' to execute a mod")
            
    except Exception as e:
        click.echo(f"Error listing mods: {e}", err=True)
        sys.exit(RUNTIME_ERROR)


def main() -> None:
    """Main entry point for CLI."""
    try:
        cli()
    except KeyboardInterrupt:
        click.echo("\nInterrupted by user", err=True)
        sys.exit(130)  # Standard exit code for SIGINT
    except Exception as e:
        click.echo(f"Unexpected error: {e}", err=True)
        sys.exit(RUNTIME_ERROR)


if __name__ == '__main__':
    main()