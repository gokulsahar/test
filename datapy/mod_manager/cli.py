"""
Click-based CLI runner for DataPy framework with console-only output.

Provides command-line interface for executing mods and Python scripts with
simple console logging - no file management.
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
    Execute a DataPy mod with registry-based execution.
    
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
        if not quiet:
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
        
        # Output results
        if quiet:
            click.echo(result['status'])
        else:
            click.echo(json.dumps(cli_result, indent=2))
        
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
        if not quiet:
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


# =============================================================================
# REGISTRY MANAGEMENT COMMANDS
# =============================================================================

from .registry import get_registry

@cli.command('list-registry')
@click.option('--category', type=click.Choice(['sources', 'transformers', 'sinks', 'solos']),
              help='Filter by mod category')
@click.pass_context
def list_registry_command(ctx: click.Context, category: Optional[str]) -> None:
    """
    List available mods in the registry.
    
    Examples:
        datapy list-registry
        datapy list-registry --category sources
    """
    quiet = ctx.obj.get('quiet', False)
    
    try:
        registry = get_registry()
        mods = registry.list_available_mods(category)
        
        if not mods:
            filter_msg = f" in category '{category}'" if category else ""
            if not quiet:
                click.echo(f"No mods found{filter_msg}")
            return
        
        if not quiet:
            title = f"Registered Mods{f' ({category})' if category else ''}"
            click.echo(title)
            click.echo("=" * len(title))
            
            for mod_type in sorted(mods):
                try:
                    mod_info = registry.get_mod_info(mod_type)
                    description = mod_info.get('description', 'No description')
                    version = mod_info.get('version', 'unknown')
                    click.echo(f"  {mod_type} (v{version}) - {description}")
                except Exception as e:
                    click.echo(f"  {mod_type} - Error loading info: {e}")
        else:
            # Quiet mode: just list mod names
            for mod_type in sorted(mods):
                click.echo(mod_type)
                
    except Exception as e:
        click.echo(f"Error listing registry: {e}", err=True)
        sys.exit(RUNTIME_ERROR)


@cli.command('register-mod')
@click.argument('module_path')
@click.pass_context
def register_mod_command(ctx: click.Context, module_path: str) -> None:
    """
    Register a new mod in the registry.
    
    MODULE_PATH: Full module path to the mod (e.g., datapy.mods.sources.csv_reader)
    
    Examples:
        datapy register-mod datapy.mods.sources.csv_reader
        datapy register-mod my_project.custom_mods.data_processor
    """
    quiet = ctx.obj.get('quiet', False)
    
    try:
        registry = get_registry()
        
        if not quiet:
            click.echo(f"Registering mod: {module_path}")
        
        success = registry.register_mod(module_path)
        
        if success:
            mod_type = module_path.split('.')[-1]
            if quiet:
                click.echo("success")
            else:
                click.echo(f"Successfully registered mod: {mod_type}")
        else:
            if quiet:
                click.echo("error")
            else:
                click.echo("Registration failed", err=True)
            sys.exit(RUNTIME_ERROR)
            
    except ValueError as e:
        if quiet:
            click.echo("error")
        else:
            click.echo(f"Registration failed: {e}", err=True)
        sys.exit(VALIDATION_ERROR)
    except Exception as e:
        if quiet:
            click.echo("error")
        else:
            click.echo(f"Registration failed: {e}", err=True)
        sys.exit(RUNTIME_ERROR)


@cli.command('validate-registry')
@click.pass_context
def validate_registry_command(ctx: click.Context) -> None:
    """
    Validate all mods in the registry.
    
    Examples:
        datapy validate-registry
    """
    quiet = ctx.obj.get('quiet', False)
    
    try:
        registry = get_registry()
        
        if not quiet:
            click.echo("Validating registry...")
        
        errors = registry.validate_registry()
        
        if not errors:
            if quiet:
                click.echo("valid")
            else:
                total_mods = len(registry.list_available_mods())
                click.echo(f"Registry validation successful! All {total_mods} mods are valid.")
        else:
            if quiet:
                click.echo("invalid")
            else:
                click.echo("Registry validation failed:", err=True)
                for error in errors:
                    click.echo(f"  - {error}", err=True)
            sys.exit(VALIDATION_ERROR)
            
    except Exception as e:
        if quiet:
            click.echo("error")
        else:
            click.echo(f"Registry validation failed: {e}", err=True)
        sys.exit(RUNTIME_ERROR)


@cli.command('mod-info')
@click.argument('mod_type')
@click.pass_context
def mod_info_command(ctx: click.Context, mod_type: str) -> None:
    """
    Show detailed information about a registered mod.
    
    MOD_TYPE: Type of mod to show info for
    
    Examples:
        datapy mod-info csv_reader
        datapy mod-info data_cleaner
    """
    quiet = ctx.obj.get('quiet', False)
    
    try:
        registry = get_registry()
        mod_info = registry.get_mod_info(mod_type)
        
        if quiet:
            click.echo(json.dumps(mod_info, indent=2))
        else:
            click.echo(f"Mod Information: {mod_type}")
            click.echo("=" * (17 + len(mod_type)))
            click.echo(f"Module Path: {mod_info.get('module_path', 'unknown')}")
            click.echo(f"Version: {mod_info.get('version', 'unknown')}")
            click.echo(f"Category: {mod_info.get('category', 'unknown')}")
            click.echo(f"Description: {mod_info.get('description', 'No description')}")
            
            # Display data flow information
            input_ports = mod_info.get('input_ports', [])
            output_ports = mod_info.get('output_ports', [])
            globals_list = mod_info.get('globals', [])
            
            click.echo(f"Input Ports: {input_ports}")
            click.echo(f"Output Ports: {output_ports}")
            click.echo(f"Globals: {globals_list}")
            
            # Display dependencies
            packages = mod_info.get('packages', [])
            python_version = mod_info.get('python_version', 'unknown')
            click.echo(f"Packages: {packages}")
            click.echo(f"Python Version: {python_version}")
            
            if 'registered_at' in mod_info:
                click.echo(f"Registered: {mod_info['registered_at']}")
            
    except ValueError as e:
        if quiet:
            click.echo("not_found")
        else:
            click.echo(f"Error: {e}", err=True)
        sys.exit(VALIDATION_ERROR)
    except Exception as e:
        if quiet:
            click.echo("error")
        else:
            click.echo(f"Error getting mod info: {e}", err=True)
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