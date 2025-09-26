"""
Click-based CLI runner for DataPy framework with console-only output.

Provides command-line interface for executing mods and Python scripts with
simple console logging - no file management.
"""

import click
import sys
from typing import Optional

from .logger import DEFAULT_LOG_CONFIG
from .result import RUNTIME_ERROR


@click.group()
@click.option('--log-level', 
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False),
              help='Override log level')
@click.pass_context
def cli(ctx: click.Context, log_level: Optional[str]) -> None:
    """DataPy Framework - ETL component execution system."""
    ctx.ensure_object(dict)
    ctx.obj['log_level'] = log_level


def main() -> None:
    """Main entry point for CLI."""
    try:
        # Import and register command groups
        from .mod_cli import mod_commands
        from .registry_cli import registry_commands
        from .scaffold_cli import scaffold_commands
        
        # Add command groups to main CLI
        for command in mod_commands:
            cli.add_command(command)
        
        for command in registry_commands:
            cli.add_command(command)
        
        for command in scaffold_commands:
            cli.add_command(command)
        
        # Execute CLI
        cli()
        
    except KeyboardInterrupt:
        click.echo("\nInterrupted by user", err=True)
        sys.exit(130)  # Standard exit code for SIGINT
    except Exception as e:
        click.echo(f"Unexpected error: {e}", err=True)
        sys.exit(RUNTIME_ERROR)


if __name__ == '__main__':
    main()