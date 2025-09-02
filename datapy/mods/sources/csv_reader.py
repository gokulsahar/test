"""
CSV Reader Mod for DataPy Framework.

Reads CSV files and returns data as pandas DataFrame with comprehensive
error handling, encoding detection, and configurable options.
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
    type="csv_reader",
    version="1.0.0",
    description="Reads data from CSV files with configurable options",
    category="source",
    input_ports=[],
    output_ports=["data"],
    globals=["row_count", "column_count", "file_size"],
    packages=["pandas>=1.5.0"]
)

# Parameter schema
CONFIG_SCHEMA = ConfigSchema(
    required={
        "file_path": {
            "type": "str",
            "description": "Path to CSV file to read"
        }
    },
    optional={
        "encoding": {
            "type": "str",
            "default": "utf-8",
            "description": "File encoding (utf-8, latin-1, etc.)"
        },
        "delimiter": {
            "type": "str", 
            "default": ",",
            "description": "CSV delimiter character"
        },
        "header": {
            "type": "int",
            "default": 0,
            "description": "Row number for column headers (0=first row, None=no headers)"
        },
        "skip_rows": {
            "type": "int",
            "default": 0,
            "description": "Number of rows to skip at start of file"
        },
        "max_rows": {
            "type": "int",
            "default": None,
            "description": "Maximum number of rows to read (None=all)"
        }
    }
)


def run(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute CSV reader with given parameters.
    
    Args:
        params: Dictionary containing file_path and optional parameters
        
    Returns:
        ModResult dictionary with data, metrics, and status
    """
    # Extract mod context
    mod_name = params.get("_mod_name", "csv_reader")
    mod_type = params.get("_mod_type", "csv_reader")
    
    # Setup logging with mod context
    logger = setup_logger(__name__, mod_type, mod_name)
    
    # Initialize result
    result = ModResult(mod_type, mod_name)
    
    try:
        # Extract parameters
        file_path = params["file_path"]
        encoding = params.get("encoding", "utf-8")
        delimiter = params.get("delimiter", ",")
        header = params.get("header", 0)
        skip_rows = params.get("skip_rows", 0)
        max_rows = params.get("max_rows", None)
        
        logger.info(f"Starting CSV read", extra={
            "file_path": file_path,
            "encoding": encoding,
            "delimiter": delimiter
        })
        
        # Validate file exists
        file_obj = Path(file_path)
        if not file_obj.exists():
            error_msg = f"File not found: {file_path}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        if not file_obj.is_file():
            error_msg = f"Path is not a file: {file_path}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        # Get file size
        file_size = file_obj.stat().st_size
        logger.info(f"File size: {file_size} bytes")
        
        # Handle header parameter (pandas expects int or None)
        if header == "None" or header == -1:
            header = None
        
        # Handle max_rows parameter
        nrows = None if max_rows is None or max_rows <= 0 else max_rows
        
        # Read CSV file
        try:
            df = pd.read_csv(
                file_path,
                encoding=encoding,
                delimiter=delimiter,
                header=header,
                skiprows=skip_rows,
                nrows=nrows,
                dtype=str  # Read all as strings to avoid type inference issues
            )
            
        except pd.errors.EmptyDataError:
            warning_msg = "CSV file is empty or contains no parsable data"
            logger.warning(warning_msg)
            result.add_warning(warning_msg)
            return result.warning()
            
        except UnicodeDecodeError as e:
            logger.warning(f"Encoding error with {encoding}, trying utf-8-sig")
            try:
                df = pd.read_csv(
                    file_path,
                    encoding="utf-8-sig",
                    delimiter=delimiter,
                    header=header,
                    skiprows=skip_rows,
                    nrows=nrows,
                    dtype=str
                )
                result.add_warning(f"File encoding auto-corrected from {encoding} to utf-8-sig")
            except Exception as e2:
                error_msg = f"Failed to read CSV with multiple encodings: {e2}"
                logger.error(error_msg)
                result.add_error(error_msg)
                return result.error()
        
        except Exception as e:
            error_msg = f"Failed to read CSV file: {str(e)}"
            logger.error(error_msg, exc_info=True)
            result.add_error(error_msg)
            return result.error()
        
        # Validate we got data
        if df.empty:
            warning_msg = "CSV file is empty or contains no data rows"
            logger.warning(warning_msg)
            result.add_warning(warning_msg)
            return result.warning()
        
        # Calculate metrics
        row_count = len(df)
        column_count = len(df.columns)
        
        logger.info(f"CSV read successful", extra={
            "rows": row_count,
            "columns": column_count,
            "file_size": file_size
        })
        
        # Add metrics
        result.add_metric("rows_read", row_count)
        result.add_metric("columns_read", column_count)
        result.add_metric("file_size_bytes", file_size)
        result.add_metric("encoding_used", encoding)
        result.add_metric("delimiter_used", delimiter)
        
        # Add artifacts
        result.add_artifact("data", df)
        result.add_artifact("file_path", str(file_path))
        
        # Add globals for downstream mods
        result.add_global("row_count", row_count)
        result.add_global("column_count", column_count) 
        result.add_global("file_size", file_size)
        
        return result.success()
        
    except KeyError as e:
        error_msg = f"Missing required parameter: {e}"
        logger.error(error_msg)
        result.add_error(error_msg)
        return result.error()
        
    except Exception as e:
        error_msg = f"Unexpected error in CSV reader: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result.add_error(error_msg)
        return result.error()