"""
Click-based CLI runner for DataPy framework.

Provides command-line interface for executing mods and Python scripts
with parameter resolution and structured logging.
"""

import json
import sys
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional

import click
import yaml

from .sdk import run_mod, set_global_config
from .logger import setup_logging, setup_logger
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
        
        if params:
            try:
                config = load_job_config(params)
                # TODO: Add support for JSON parameter files
                globals_config = config.get('globals', {})
                mod_name = mod_path.split('.')[-1]
                job_params = config.get('mods', {}).get(mod_name, {})
            except Exception as e:
                click.echo(f"Error loading parameters from {params}: {e}", err=True)
                sys.exit(20)  # VALIDATION_ERROR
        
        # Override log level if specified
        if log_level:
            globals_config['log_level'] = log_level
        
        # Setup logging and global config
        execution_name = Path(params).stem if params else 'cli_execution'
        setup_logging(globals_config, execution_name)
        set_global_config(globals_config)
        
        if not quiet:
            click.echo(f"Executing mod: {mod_path}")
            if params:
                click.echo(f"Using parameters from: {params}")
        
        # Execute the mod
        result = run_mod(mod_path, job_params)
        
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
        
        # Load parameters and setup globals if provided
        if params:
            try:
                config = load_job_config(params)
                # TODO: Add support for JSON parameter files
                globals_config = config.get('globals', {})
                
                # Override log level if specified
                if log_level:
                    globals_config['log_level'] = log_level
                
                # Setup logging and global config
                execution_name = script_file.stem
                setup_logging(globals_config, execution_name)
                set_global_config(globals_config)
                
            except Exception as e:
                click.echo(f"Error loading parameters from {params}: {e}", err=True)
                sys.exit(20)  # VALIDATION_ERROR
        else:
            # Setup basic logging without params
            globals_config = {}
            if log_level:
                globals_config['log_level'] = log_level
            
            execution_name = script_file.stem
            setup_logging(globals_config, execution_name)
            set_global_config(globals_config)
        
        if not quiet:
            click.echo(f"Executing script: {script_path}")
            if params:
                click.echo(f"Using parameters from: {params}")
        
        # Execute the Python script
        try:
            # Use subprocess to execute the script in the same Python environment
            cmd = [sys.executable, str(script_file)]
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=script_file.parent)
            
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


def main() -> None:
    """Main entry point for CLI."""
    cli()


if __name__ == '__main__':
    main()