"""
CSV Writer Mod for DataPy Framework.

Writes pandas DataFrame to CSV files with configurable options and validation.
"""

import pandas as pd
import os
from pathlib import Path
from typing import Dict, Any

from datapy.mod_manager.result import ModResult
from datapy.mod_manager.base import ModMetadata, ConfigSchema
from datapy.mod_manager.logger import setup_logger

# Required metadata
METADATA = ModMetadata(
    type="csv_writer",
    version="1.0.0",
    description="Writes pandas DataFrame to CSV files with configurable options",
    category="sink",
    input_ports=["data"],
    output_ports=[],
    globals=["output_path", "rows_written", "file_size"],
    packages=["pandas>=1.5.0"]
)

# Parameter schema
CONFIG_SCHEMA = ConfigSchema(
    required={
        "data": {
            "type": "object",
            "description": "Input DataFrame to write to CSV"
        },
        "output_path": {
            "type": "str",
            "description": "Path where CSV file will be written"
        }
    },
    optional={
        "encoding": {
            "type": "str",
            "default": "utf-8",
            "description": "File encoding for output CSV"
        },
        "delimiter": {
            "type": "str",
            "default": ",",
            "description": "CSV delimiter character"
        },
        "include_index": {
            "type": "bool",
            "default": False,
            "description": "Include DataFrame index in output"
        },
        "include_header": {
            "type": "bool",
            "default": True,
            "description": "Include column headers in output"
        },
        "create_directories": {
            "type": "bool",
            "default": True,
            "description": "Create parent directories if they don't exist"
        },
        "backup_existing": {
            "type": "bool",
            "default": False,
            "description": "Create backup of existing file before overwriting"
        }
    }
)


def run(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute CSV writer with given parameters.
    
    Args:
        params: Dictionary containing data and output configuration
        
    Returns:
        ModResult dictionary with write results, metrics, and status
    """
    # Extract mod context
    mod_name = params.get("_mod_name", "csv_writer")
    mod_type = params.get("_mod_type", "csv_writer")
    
    # Setup logging with mod context
    logger = setup_logger(__name__, mod_type, mod_name)
    
    # Initialize result
    result = ModResult(mod_type, mod_name)
    
    try:
        # Extract parameters
        data = params["data"]
        output_path = params["output_path"]
        encoding = params.get("encoding", "utf-8")
        delimiter = params.get("delimiter", ",")
        include_index = params.get("include_index", False)
        include_header = params.get("include_header", True)
        create_directories = params.get("create_directories", True)
        backup_existing = params.get("backup_existing", False)
        
        logger.info(f"Starting CSV write", extra={
            "output_path": output_path,
            "encoding": encoding,
            "delimiter": delimiter,
            "input_rows": len(data) if hasattr(data, '__len__') else 'unknown'
        })
        
        # Validate input data
        if not isinstance(data, pd.DataFrame):
            error_msg = f"Input data must be a pandas DataFrame, got {type(data)}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        # Convert output_path to Path object
        output_file = Path(output_path)
        
        # Create parent directories if requested and needed
        if create_directories and not output_file.parent.exists():
            try:
                output_file.parent.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created directories: {output_file.parent}")
            except Exception as e:
                error_msg = f"Failed to create directories {output_file.parent}: {e}"
                logger.error(error_msg)
                result.add_error(error_msg)
                return result.error()
        
        # Backup existing file if requested
        if backup_existing and output_file.exists():
            try:
                import datetime
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = output_file.with_suffix(f".backup_{timestamp}{output_file.suffix}")
                output_file.rename(backup_path)
                logger.info(f"Backed up existing file to: {backup_path}")
                result.add_artifact("backup_path", str(backup_path))
            except Exception as e:
                warning_msg = f"Failed to backup existing file: {e}"
                logger.warning(warning_msg)
                result.add_warning(warning_msg)
        
        # Validate data is not empty
        if data.empty:
            warning_msg = "Input DataFrame is empty, writing empty CSV"
            logger.warning(warning_msg)
            result.add_warning(warning_msg)
        
        # Write CSV file
        try:
            data.to_csv(
                output_file,
                encoding=encoding,
                sep=delimiter,
                index=include_index,
                header=include_header
            )
            
            logger.info(f"CSV file written successfully: {output_file}")
            
        except PermissionError as e:
            error_msg = f"Permission denied writing to {output_file}: {e}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
            
        except Exception as e:
            error_msg = f"Failed to write CSV file: {e}"
            logger.error(error_msg, exc_info=True)
            result.add_error(error_msg)
            return result.error()
        
        # Get file statistics
        try:
            file_stats = output_file.stat()
            file_size = file_stats.st_size
            logger.info(f"File size: {file_size} bytes")
        except Exception as e:
            logger.warning(f"Could not get file statistics: {e}")
            file_size = 0
        
        # Calculate metrics
        rows_written = len(data)
        columns_written = len(data.columns)
        
        logger.info(f"CSV write completed", extra={
            "output_path": str(output_file),
            "rows_written": rows_written,
            "columns_written": columns_written,
            "file_size": file_size
        })
        
        # Add metrics
        result.add_metric("rows_written", rows_written)
        result.add_metric("columns_written", columns_written)
        result.add_metric("file_size_bytes", file_size)
        result.add_metric("encoding_used", encoding)
        result.add_metric("delimiter_used", delimiter)
        result.add_metric("include_index", include_index)
        result.add_metric("include_header", include_header)
        
        # Add artifacts
        result.add_artifact("output_path", str(output_file.resolve()))
        result.add_artifact("output_filename", output_file.name)
        
        # Add globals for downstream mods or reporting
        result.add_global("output_path", str(output_file.resolve()))
        result.add_global("rows_written", rows_written)
        result.add_global("file_size", file_size)
        
        return result.success()
        
    except KeyError as e:
        error_msg = f"Missing required parameter: {e}"
        logger.error(error_msg)
        result.add_error(error_msg)
        return result.error()
        
    except Exception as e:
        error_msg = f"Unexpected error in CSV writer: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result.add_error(error_msg)
        return result.error()