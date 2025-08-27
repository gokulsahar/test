"""
Registry management CLI commands for DataPy framework.

Handles registry-related commands: list-registry, register-mod, validate-registry, mod-info.
"""

import json
import sys
from typing import Optional

import click

from .result import VALIDATION_ERROR, RUNTIME_ERROR
from .registry import get_registry


@click.command('list-registry')
@click.option('--category', type=click.Choice(['sources', 'transformers', 'sinks', 'solos']),
              help='Filter by mod category')
def list_registry_command(category: Optional[str]) -> None:
    """
    List available mods in the registry.
    
    Examples:
        datapy list-registry
        datapy list-registry --category sources
    """
    try:
        registry = get_registry()
        mods = registry.list_available_mods(category)
        
        if not mods:
            filter_msg = f" in category '{category}'" if category else ""
            click.echo(f"No mods found{filter_msg}")
            return
        
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
                
    except Exception as e:
        click.echo(f"Error listing registry: {e}", err=True)
        sys.exit(RUNTIME_ERROR)


@click.command('register-mod')
@click.argument('module_path')
def register_mod_command(module_path: str) -> None:
    """
    Register a new mod in the registry.
    
    MODULE_PATH: Full module path to the mod (e.g., datapy.mods.sources.csv_reader)
    
    Examples:
        datapy register-mod datapy.mods.sources.csv_reader
        datapy register-mod my_project.custom_mods.data_processor
    """
    try:
        registry = get_registry()
        
        click.echo(f"Registering mod: {module_path}")
        
        success = registry.register_mod(module_path)
        
        if success:
            mod_type = module_path.split('.')[-1]
            click.echo(f"Successfully registered mod: {mod_type}")
        else:
            click.echo("Registration failed", err=True)
            sys.exit(RUNTIME_ERROR)
            
    except ValueError as e:
        click.echo(f"Registration failed: {e}", err=True)
        sys.exit(VALIDATION_ERROR)
    except Exception as e:
        click.echo(f"Registration failed: {e}", err=True)
        sys.exit(RUNTIME_ERROR)


@click.command('validate-registry')
def validate_registry_command() -> None:
    """
    Validate all mods in the registry.
    
    Examples:
        datapy validate-registry
    """
    try:
        registry = get_registry()
        
        click.echo("Validating registry...")
        
        errors = registry.validate_registry()
        
        if not errors:
            total_mods = len(registry.list_available_mods())
            click.echo(f"Registry validation successful! All {total_mods} mods are valid.")
        else:
            click.echo("Registry validation failed:", err=True)
            for error in errors:
                click.echo(f"  - {error}", err=True)
            sys.exit(VALIDATION_ERROR)
            
    except Exception as e:
        click.echo(f"Registry validation failed: {e}", err=True)
        sys.exit(RUNTIME_ERROR)


@click.command('mod-info')
@click.argument('mod_type')
def mod_info_command(mod_type: str) -> None:
    """
    Show detailed information about a registered mod.
    
    MOD_TYPE: Type of mod to show info for
    
    Examples:
        datapy mod-info csv_reader
        datapy mod-info data_cleaner
    """
    try:
        registry = get_registry()
        mod_info = registry.get_mod_info(mod_type)
        
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
        click.echo(f"Error: {e}", err=True)
        sys.exit(VALIDATION_ERROR)
    except Exception as e:
        click.echo(f"Error getting mod info: {e}", err=True)
        sys.exit(RUNTIME_ERROR)


@click.command('delete-mod')
@click.argument('mod_type')
@click.option('--force', is_flag=True, help='Skip confirmation prompt')
def delete_mod_command(mod_type: str, force: bool) -> None:
    """
    Delete a mod from the registry.
    
    MOD_TYPE: Type of mod to delete from registry
    
    Examples:
        datapy delete-mod csv_reader
        datapy delete-mod old_mod --force
    """
    try:
        registry = get_registry()
        
        # Check if mod exists
        try:
            mod_info = registry.get_mod_info(mod_type)
        except ValueError:
            click.echo(f"Error: Mod '{mod_type}' not found in registry", err=True)
            sys.exit(VALIDATION_ERROR)
        
        # Show mod info and confirm deletion
        if not force:
            click.echo(f"Mod to delete: {mod_type}")
            click.echo(f"Description: {mod_info.get('description', 'No description')}")
            click.echo(f"Module Path: {mod_info.get('module_path', 'unknown')}")
            
            if not click.confirm(f"Are you sure you want to delete '{mod_type}' from registry?"):
                click.echo("Deletion cancelled.")
                return
        
        # Delete from registry
        success = registry.delete_mod(mod_type)
        
        if success:
            click.echo(f"Successfully deleted mod '{mod_type}' from registry")
        else:
            click.echo(f"Failed to delete mod '{mod_type}'", err=True)
            sys.exit(RUNTIME_ERROR)
            
    except Exception as e:
        click.echo(f"Error deleting mod: {e}", err=True)
        sys.exit(RUNTIME_ERROR)


# Export commands for main CLI
registry_commands = [list_registry_command, register_mod_command, validate_registry_command, mod_info_command, delete_mod_command]