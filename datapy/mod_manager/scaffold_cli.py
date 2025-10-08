"""
Scaffolding CLI commands for DataPy framework.

Handles create-job command for generating pipeline job structures.
"""

import json
import sys
from pathlib import Path
from typing import Optional

import click

from .result import VALIDATION_ERROR, RUNTIME_ERROR, SUCCESS


# Template for pipeline.py
PIPELINE_TEMPLATE = '''"""
Pipeline Name: {job_name}
Description: TODO - Add pipeline description here
"""

from datapy.mod_manager.sdk import run_mod, setup_logging, setup_context, get_context_value
from datapy.utils.script_monitor import monitor_execution


def pre_run():
    """Setup logging and context."""
    logger = setup_logging("INFO", "{job_name}_pipeline.py")  
    setup_context("{job_name}_context.json")
    logger.info("Starting {job_name} pipeline")
    return logger


def run_pipeline(logger):
    """
    Main pipeline execution.
    
    TODO: Implement your pipeline logic here.
    
    Example workflow:
    1. Read data: run_mod("csv_reader", {{"file_path": "${{data.input_path}}/input.csv"}})
    2. Transform: run_mod("csv_filter", {{"data": data["artifacts"]["data"]}})
    3. Write output: run_mod("csv_writer", {{"data": processed["artifacts"]["filtered_data"]}})
    """
    logger.info("Pipeline execution not yet implemented")
    
    # Add your mod executions here
    
    result = {{"status": "success", "metrics": {{}}, "artifacts": {{}}, "errors": []}}
    return result


def post_run(logger, result):
    """Final reporting."""
    if result["status"] in ["success", "warning"]:
        logger.info("Pipeline completed successfully!")
    else:
        logger.error(f"Pipeline failed: {{result.get('errors', [])}}")


@monitor_execution("{job_name}")
def main():
    """Execute complete pipeline."""
    logger = pre_run()
    result = run_pipeline(logger)
    post_run(logger, result)
    return result["status"] in ["success", "warning"]


if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)
'''


# Template for context.json
CONTEXT_TEMPLATE = {
    "_comment": "Context configuration for {job_name} pipeline",
    "_description": "TODO - Add pipeline description here",
    "pipeline": {
        "name": "{job_name}"
    },
    "data": {
        "input_path": "./input",
        "output_path": "./output"
    }
}


def _validate_job_name(job_name: str) -> str:
    """
    Validate job name follows Python naming conventions.
    
    Args:
        job_name: The job name to validate
        
    Returns:
        Validated job name
        
    Raises:
        SystemExit: If job name is invalid
    """
    if not job_name or not isinstance(job_name, str) or not job_name.strip():
        click.echo("Error: job_name must be a non-empty string", err=True)
        sys.exit(VALIDATION_ERROR)
    
    job_name = job_name.strip()
    
    # Check if valid Python identifier
    if not job_name.isidentifier():
        click.echo(f"Error: job_name '{job_name}' must be a valid Python identifier", err=True)
        sys.exit(VALIDATION_ERROR)
    
    return job_name


def _create_job_directory(job_name: str, force: bool, output_dir: Optional[str] = None) -> Path:
    """
    Create job directory.
    
    Args:
        job_name: Name of the job
        force: Whether to overwrite existing directory
        output_dir: Optional output directory (defaults to current directory)
        
    Returns:
        Path to created job directory
        
    Raises:
        SystemExit: If directory exists and force is False
    """
    # Determine base directory
    if output_dir:
        base_path = Path(output_dir).resolve()
        if not base_path.exists():
            click.echo(f"Error: Output directory '{output_dir}' does not exist", err=True)
            sys.exit(VALIDATION_ERROR)
    else:
        base_path = Path.cwd()
    
    job_path = base_path / job_name
    
    # Check if directory already exists
    if job_path.exists():
        if not force:
            click.echo(
                f"Error: Job directory '{job_name}' already exists. Use --force to overwrite.",
                err=True
            )
            sys.exit(VALIDATION_ERROR)
        else:
            click.echo(f"Warning: Overwriting existing job directory '{job_name}'")
    
    # Create directory
    try:
        job_path.mkdir(parents=True, exist_ok=True)
        return job_path
    except Exception as e:
        click.echo(f"Error: Failed to create job directory: {e}", err=True)
        sys.exit(RUNTIME_ERROR)


def _create_pipeline_file(job_path: Path, job_name: str) -> None:
    """
    Create pipeline.py file.
    
    Args:
        job_path: Path to job directory
        job_name: Name of the job
        
    Raises:
        SystemExit: If file creation fails
    """
    pipeline_file = job_path / f"{job_name}_pipeline.py"
    
    try:
        pipeline_content = PIPELINE_TEMPLATE.format(job_name=job_name)
        pipeline_file.write_text(pipeline_content, encoding='utf-8')
        click.echo(f"  ✓ Created {job_name}_pipeline.py")
    except Exception as e:
        click.echo(f"Error: Failed to create pipeline file: {e}", err=True)
        sys.exit(RUNTIME_ERROR)


def _create_context_file(job_path: Path, job_name: str) -> None:
    """
    Create context.json file.
    
    Args:
        job_path: Path to job directory
        job_name: Name of the job
        
    Raises:
        SystemExit: If file creation fails
    """
    context_file = job_path / f"{job_name}_context.json"
    
    try:
        # Format the template
        context_data = json.loads(json.dumps(CONTEXT_TEMPLATE))
        context_data["_comment"] = context_data["_comment"].format(job_name=job_name)
        context_data["pipeline"]["name"] = job_name
        
        # Write with nice formatting
        context_content = json.dumps(context_data, indent=2, ensure_ascii=False)
        context_file.write_text(context_content, encoding='utf-8')
        click.echo(f"  ✓ Created {job_name}_context.json")
    except Exception as e:
        click.echo(f"Error: Failed to create context file: {e}", err=True)
        sys.exit(RUNTIME_ERROR)


@click.command('create-job')
@click.argument('job_name')
@click.option('--force', '-f', is_flag=True, default=False,
              help='Overwrite existing job directory if it exists')
@click.option('--output-dir', '-o', type=str, default=None,
              help='Output directory for job (defaults to current directory)')
def create_job_command(job_name: str, force: bool, output_dir: Optional[str]) -> None:
    """
    Create a new pipeline job with scaffolded structure.
    
    Creates a directory with pipeline and context files following DataPy conventions.
    
    JOB_NAME: Name of the job to create (e.g., customer_etl, sales_report)
    
    Examples:
        datapy create-job customer_etl
        datapy create-job sales_report --force
        datapy create-job data_cleanup --output-dir ./jobs
    """
    try:
        # Validate job name
        validated_job_name = _validate_job_name(job_name)
        
        click.echo(f"Creating job: {validated_job_name}")
        
        # Create job directory
        job_path = _create_job_directory(validated_job_name, force, output_dir)
        
        # Create files
        _create_pipeline_file(job_path, validated_job_name)
        _create_context_file(job_path, validated_job_name)
        
        # Success message
        click.echo(f"\n✓ Successfully created job '{validated_job_name}'")
        click.echo(f"\nJob structure:")
        click.echo(f"  {validated_job_name}/")
        click.echo(f"  ├── {validated_job_name}_pipeline.py")
        click.echo(f"  └── {validated_job_name}_context.json")
        click.echo(f"\nNext steps:")
        click.echo(f"  1. Edit {validated_job_name}_context.json with your configuration")
        click.echo(f"  2. Add mod executions in {validated_job_name}_pipeline.py")
        click.echo(f"  3. Run: python {validated_job_name}/{validated_job_name}_pipeline.py")
        
        sys.exit(SUCCESS)
        
    except SystemExit:
        raise  # Re-raise sys.exit calls
    except Exception as e:
        click.echo(f"Error: Job creation failed: {e}", err=True)
        import traceback
        click.echo(traceback.format_exc(), err=True)
        sys.exit(RUNTIME_ERROR)


# Export commands for main CLI
scaffold_commands = [create_job_command]