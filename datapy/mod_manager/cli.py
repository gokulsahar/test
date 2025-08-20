"""
Click-based CLI runner for DataPy framework.

Provides command-line interface for executing mods and Python scripts.
CLI is a simple wrapper around the SDK that handles argument parsing,
config loading, and result formatting.
"""

import json
import sys
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional

import click
import yaml

from .sdk import run_mod, set_global_config
from .logger import execution_logger, setup_logger
from .params import load_job_config

logger = setup_logger(__name__)


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
@click.argument('mod_path')
@click.option('--params', '-p', 
              type=click.Path(exists=True, readable=True),
              help='YAML parameter file path')
@click.option('--exit-on-error', is_flag=True, default=True,
              help='Exit with error code on mod failure (default: True)')
@click.pass_context
def run_mod_command(ctx: click.Context, mod_path: str, params: Optional[str], exit_on_error: bool) -> None:
    """
    Execute a DataPy mod with parameter resolution.
    
    MOD_PATH: Mod name (e.g., csv_reader) or full path (e.g., datapy.mods.sources.csv_reader)
    
    Examples:
        datapy run-mod csv_reader --params config.yaml
        datapy run-mod data_cleaner -p params.yaml
        datapy run-mod datapy.mods.sources.csv_reader --params config.yaml
    """
    quiet = ctx.obj.get('quiet', False)
    log_level = ctx.obj.get('log_level')
    
    try:
        # Load parameters from file
        job_params = {}
        globals_config = {}
        execution_name = "cli_execution"
        
        if params:
            try:
                config = load_job_config(params)
                globals_config = config.get('globals', {})
                mod_name = mod_path.split('.')[-1]
                job_params = config.get('mods', {}).get(mod_name, {})
                execution_name = Path(params).stem
            except Exception as e:
                click.echo(f"Error loading parameters from {params}: {e}", err=True)
                sys.exit(20)  # VALIDATION_ERROR
        
        # Override log level if specified
        if log_level:
            globals_config['log_level'] = log_level
        
        if not quiet:
            click.echo(f"Executing mod: {mod_path}")
            if params:
                click.echo(f"Using parameters from: {params}")
        
        # Use execution context manager for proper logging lifecycle
        with execution_logger(execution_name, globals_config) as exec_ctx:
            # Set global config for SDK
            set_global_config(globals_config)
            
            # Execute the mod (CLI automatically gets full result dict)
            result = run_mod(mod_path, job_params, mod_name=execution_name)
            
            # Output results
            if quiet:
                # Minimal output: just status
                click.echo(result['status'])
            else:
                # Full JSON result to stdout
                click.echo(json.dumps(result, indent=2))
            
            # Handle exit code
            exit_code = result.get('exit_code', 30)
            if result['status'] == 'error' and exit_on_error:
                if not quiet:
                    click.echo(f"Mod failed with exit code: {exit_code}", err=True)
                sys.exit(exit_code)
            elif result['status'] == 'warning':
                sys.exit(10)  # SUCCESS_WITH_WARNINGS
            else:
                sys.exit(0)   # SUCCESS
                
    except Exception as e:
        click.echo(f"CLI execution failed: {e}", err=True)
        if not quiet:
            import traceback
            click.echo(traceback.format_exc(), err=True)
        sys.exit(30)  # RUNTIME_ERROR


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
        datapy run-script my_pipeline.py -p params.yaml
    """
    quiet = ctx.obj.get('quiet', False)
    log_level = ctx.obj.get('log_level')
    
    try:
        script_file = Path(script_path)
        execution_name = script_file.name
        globals_config = {}
        
        # Load parameters and setup globals if provided
        if params:
            try:
                config = load_job_config(params)
                globals_config = config.get('globals', {})
                
                # Override log level if specified
                if log_level:
                    globals_config['log_level'] = log_level
                    
            except Exception as e:
                click.echo(f"Error loading parameters from {params}: {e}", err=True)
                sys.exit(20)  # VALIDATION_ERROR
        else:
            # Setup basic logging without params
            if log_level:
                globals_config['log_level'] = log_level
        
        if not quiet:
            click.echo(f"Executing script: {script_path}")
            if params:
                click.echo(f"Using parameters from: {params}")
        
        # Use execution context manager for proper logging lifecycle
        with execution_logger(execution_name, globals_config) as exec_ctx:
            # Set global config for any framework usage in the script
            set_global_config(globals_config)
            
            # Execute the Python script
            try:
                # Prepare environment variables for the script to access config
                import os
                env = os.environ.copy()
                
                # Add DataPy config as environment variables if needed
                if globals_config:
                    env['DATAPY_CONFIG'] = json.dumps(globals_config)
                
                # Use subprocess to execute the script
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
                click.echo(f"Failed to execute script: {e}", err=True)
                sys.exit(30)  # RUNTIME_ERROR
                
    except Exception as e:
        click.echo(f"CLI execution failed: {e}", err=True)
        if not quiet:
            import traceback
            click.echo(traceback.format_exc(), err=True)
        sys.exit(30)  # RUNTIME_ERROR


@cli.command('run-job')
@click.argument('job_config', type=click.Path(exists=True, readable=True))
@click.option('--exit-on-error', is_flag=True, default=True,
              help='Exit with error code on any mod failure (default: True)')
@click.pass_context
def run_job_command(ctx: click.Context, job_config: str, exit_on_error: bool) -> None:
    """
    Execute multiple mods from a job configuration file (YAML orchestration).
    
    JOB_CONFIG: Path to job configuration YAML file
    
    Example:
        datapy run-job jobs/daily-etl/daily_pipeline.yaml
        
    Job YAML format:
        globals:
          log_level: INFO
          base_path: "/data/2024-08-12"
        
        mods:
          customer_data:        # Unique mod name (variable identifier)
            _type: csv_reader   # Mod type to execute
            input_path: "customers.csv"
          clean_data:
            _type: data_cleaner
            input_data: "${customer_data.artifacts.output_file}"  # Reference previous mod
            strategy: "drop"
          final_output:
            _type: database_writer
            input_data: "${clean_data.artifacts.output_file}"
            table_name: "customers"
    """
    quiet = ctx.obj.get('quiet', False)
    log_level = ctx.obj.get('log_level')
    
    try:
        # Load job configuration
        config = load_job_config(job_config)
        globals_config = config.get('globals', {})
        mods_config = config.get('mods', {})
        
        # Validate new YAML format
        for mod_name, mod_config in mods_config.items():
            if not isinstance(mod_config, dict) or '_type' not in mod_config:
                click.echo(f"Error: Mod '{mod_name}' missing required '_type' field", err=True)
                click.echo("New format: mod_name: { _type: mod_type, param1: value1, ... }", err=True)
                sys.exit(20)  # VALIDATION_ERROR
        
        # Override log level if specified
        if log_level:
            globals_config['log_level'] = log_level
        
        execution_name = Path(job_config).stem
        
        if not quiet:
            click.echo(f"Executing job: {job_config}")
            click.echo(f"Mods to execute: {list(mods_config.keys())}")
        
        # Use execution context manager for proper logging lifecycle
        with execution_logger(execution_name, globals_config) as exec_ctx:
            # Set global config for SDK
            set_global_config(globals_config)
            
            results = []
            failed = False
            
            # Execute each mod in sequence
            for mod_name, mod_config in mods_config.items():
                mod_type = mod_config.pop('_type')  # Extract mod type
                mod_params = mod_config  # Remaining params
                
                if not quiet:
                    click.echo(f"\n--- Executing mod: {mod_name} (type: {mod_type}) ---")
                
                try:
                    # Call SDK (CLI automatically gets full result dict)
                    result = run_mod(mod_type, mod_params, mod_name=mod_name)
                    results.append({
                        "mod_name": mod_name,
                        "mod_type": mod_type,
                        "result": result
                    })
                    
                    if not quiet:
                        click.echo(f"Mod {mod_name} completed with status: {result['status']}")
                    
                    # Check for failure
                    if result['status'] == 'error':
                        failed = True
                        if exit_on_error:
                            click.echo(f"Job failed at mod: {mod_name}", err=True)
                            break
                            
                except Exception as e:
                    click.echo(f"Error executing mod {mod_name}: {e}", err=True)
                    failed = True
                    if exit_on_error:
                        break
            
            # Output final results
            if quiet:
                if failed:
                    click.echo("error")
                else:
                    click.echo("success")
            else:
                click.echo("\n--- Job Execution Summary ---")
                click.echo(json.dumps({
                    "job_config": job_config,
                    "execution_id": exec_ctx.execution_id,
                    "total_mods": len(mods_config),
                    "executed_mods": len(results),
                    "failed": failed,
                    "results": results
                }, indent=2))
            
            # Handle exit code
            if failed and exit_on_error:
                sys.exit(30)  # RUNTIME_ERROR
            elif failed:
                sys.exit(10)  # SUCCESS_WITH_WARNINGS
            else:
                sys.exit(0)   # SUCCESS
                
    except Exception as e:
        click.echo(f"Job execution failed: {e}", err=True)
        if not quiet:
            import traceback
            click.echo(traceback.format_exc(), err=True)
        sys.exit(30)  # RUNTIME_ERROR


def main() -> None:
    """Main entry point for CLI."""
    cli()


if __name__ == '__main__':
    main()