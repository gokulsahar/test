"""
Click-based CLI runner for DataPy framework with state-based logging.

Provides command-line interface for executing mods and Python scripts with
automatic state management and lifecycle handling.
"""

import json
import sys
import subprocess
import os
from pathlib import Path
from typing import Dict, Any, Optional

import click

from .sdk import find_or_create_job_files, initialize_job_state_cli, initialize_job_state_sdk
from .logger import setup_job_logging, setup_logger, is_job_complete_cli, archive_completed_state
from .params import load_job_config
from .result import VALIDATION_ERROR, RUNTIME_ERROR, SUCCESS, SUCCESS_WITH_WARNINGS


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
        # Extract yaml_name from params file path
        yaml_name = Path(params).stem
        
        # Load and validate YAML configuration
        try:
            config = load_job_config(params)
            
            if 'mods' not in config or not config['mods']:
                click.echo(f"Error: YAML file {params} missing or empty 'mods' section", err=True)
                sys.exit(VALIDATION_ERROR)
            
            if mod_name not in config['mods']:
                click.echo(f"Error: Mod '{mod_name}' not found in {params}", err=True)
                sys.exit(VALIDATION_ERROR)
            
            mod_config = config['mods'][mod_name]
            if '_type' not in mod_config:
                click.echo(f"Error: Mod '{mod_name}' missing required '_type' field", err=True)
                sys.exit(VALIDATION_ERROR)
            
            mod_type = mod_config.pop('_type')
            mod_params = mod_config
            
        except Exception as e:
            click.echo(f"Error parsing YAML file {params}: {e}", err=True)
            sys.exit(VALIDATION_ERROR)
        
        # Setup job files and state management
        log_file_path, state_file_path = find_or_create_job_files(yaml_name, is_cli=True)
        
        # Initialize CLI state if it doesn't exist
        if not os.path.exists(state_file_path):
            initialize_job_state_cli(state_file_path, params)
        
        # Setup logging
        globals_config = config.get('globals', {})
        if log_level:
            globals_config['log_level'] = log_level
        
        setup_job_logging(log_file_path, globals_config)
        
        # Setup mod-specific logger
        logger = setup_logger(__name__, log_file_path)
        
        if not quiet:
            click.echo(f"Executing mod: {mod_name} (type: {mod_type})")
            click.echo(f"Using parameters from: {params}")
            click.echo(f"Log file: {log_file_path}")
        
        logger.info(f"CLI executing mod {mod_name}", extra={
            "mod_type": mod_type,
            "yaml_file": params,
            "state_file": state_file_path
        })
        
        # Import and execute via SDK (which handles all mod execution logic)
        from .sdk import run_mod, set_global_config
        
        # Set global config for SDK
        set_global_config(globals_config)
        
        # Override execution context for CLI (pass yaml_name to SDK)
        # Note: This is a bit of a hack, but SDK needs to know it's CLI context
        original_get_execution_context = None
        try:
            import datapy.mod_manager.sdk as sdk_module
            original_get_execution_context = sdk_module._get_execution_context
            sdk_module._get_execution_context = lambda: yaml_name
            
            # Execute the mod (CLI gets full result dict)
            result = run_mod(mod_type, mod_params, mod_name)
            
        finally:
            # Restore original function
            if original_get_execution_context:
                sdk_module._get_execution_context = original_get_execution_context
        
        # Check for job completion and archive if complete
        if is_job_complete_cli(state_file_path):
            archive_completed_state(state_file_path)
            logger.info(f"Job completed, archived state file")
        
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
        script_file = Path(script_path)
        
        # Use SDK-style state management for scripts
        execution_context = "sdk_execution"
        log_file_path, state_file_path = find_or_create_job_files(execution_context, is_cli=False)
        
        # Initialize SDK state if it doesn't exist
        if not os.path.exists(state_file_path):
            initialize_job_state_sdk(state_file_path)
        
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
        setup_job_logging(log_file_path, globals_config)
        logger = setup_logger(__name__, log_file_path)
        
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
                env=env
            )
            
            if not quiet:
                # Print script output
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


def main() -> None:
    """Main entry point for CLI."""
    cli()


if __name__ == '__main__':
    main()