"""
ETL-Focused File Writer Mod for DataPy Framework.

Writes CSV, Parquet, and Arrow files with ETL-optimized features including
append mode, directory creation, CSV options, and performance optimizations
for production data pipelines.
"""

import pandas as pd
import os
import zipfile
import gzip
from pathlib import Path
from typing import Dict, Any, Union, Optional

from datapy.mod_manager.result import ModResult
from datapy.mod_manager.base import ModMetadata, ConfigSchema
from datapy.mod_manager.logger import setup_logger

# Required metadata
METADATA = ModMetadata(
    type="file_writer",
    version="1.0.0", 
    description="ETL-focused writer for CSV, Parquet, and Arrow files with comprehensive data pipeline features",
    category="sink",
    input_ports=["data"],
    output_ports=[],
    globals=["rows_written", "files_created", "file_size", "engine_used"],
    packages=["pandas>=1.5.0", "polars>=0.20.0"]
)

# Parameter schema - ETL focused and consistent with file_reader
CONFIG_SCHEMA = ConfigSchema(
    required={
        "file_path": {
            "type": "str",
            "description": "Path to output file (CSV, Parquet, or Arrow format)"
        }
    },
    optional={
        "engine": {
            "type": "str",
            "default": "pandas",
            "description": "Processing engine: 'pandas' or 'polars'"
        },
        "encoding": {
            "type": "str", 
            "default": "utf-8",
            "description": "File encoding (utf-8, latin-1, etc.)"
        },
        "delimiter": {
            "type": "str",
            "default": ",", 
            "description": "Field separator character"
        },
        "row_separator": {
            "type": "str",
            "default": "\n",
            "description": "Line ending character (\\n, \\r\\n, \\r)"
        },
        "csv_options": {
            "type": "bool",
            "default": True,
            "description": "Enable CSV-specific handling (quotes, escaping)"
        },
        "text_enclosure": {
            "type": "str",
            "default": "\"",
            "description": "Quote character for text fields"
        },
        "escape_char": {
            "type": "str",
            "default": "\\",
            "description": "Escape character for special characters"
        },
        "append_mode": {
            "type": "bool",
            "default": False,
            "description": "Append to existing file instead of overwriting"
        },
        "include_header": {
            "type": "bool",
            "default": True,
            "description": "Include column headers in output file"
        },
        "create_directories": {
            "type": "bool",
            "default": True,
            "description": "Create parent directories if they don't exist"
        },
        "overwrite_existing": {
            "type": "bool",
            "default": True,
            "description": "Overwrite existing files (False will raise error if file exists)"
        }
    }
)


def _determine_file_format(file_path: str) -> str:
    """
    Determine file format based on extension.
    
    Args:
        file_path: Path to file
        
    Returns:
        File format: 'csv', 'parquet', or 'arrow'
    """
    suffix = Path(file_path).suffix.lower()
    
    format_map = {
        '.csv': 'csv',
        '.tsv': 'csv', 
        '.txt': 'csv',
        '.dat': 'csv',
        '.parquet': 'parquet',
        '.pq': 'parquet',
        '.arrow': 'arrow',
        '.feather': 'arrow'
    }
    
    return format_map.get(suffix, 'csv')


def _create_directories(file_path: str, logger) -> None:
    """
    Create parent directories if they don't exist.
    
    Args:
        file_path: Path to output file
        logger: Logger instance
    """
    parent_dir = Path(file_path).parent
    if not parent_dir.exists():
        parent_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory: {parent_dir}")


def _validate_file_operations(file_path: str, overwrite_existing: bool, append_mode: bool) -> None:
    """
    Validate file operations based on settings.
    
    Args:
        file_path: Path to output file
        overwrite_existing: Whether to allow overwriting
        append_mode: Whether appending to existing file
        
    Raises:
        FileExistsError: If file exists and overwrite is disabled
    """
    file_exists = Path(file_path).exists()
    
    if file_exists and not overwrite_existing and not append_mode:
        raise FileExistsError(f"File already exists and overwrite_existing=False: {file_path}")


def _determine_engine_from_data(data) -> str:
    """
    Determine which engine to use based on data type.
    
    Args:
        data: Input data (DataFrame or LazyFrame)
        
    Returns:
        Engine name: 'pandas' or 'polars'
    """
    # Check if it's a Polars DataFrame/LazyFrame
    if hasattr(data, 'collect') or (hasattr(data, 'schema') and hasattr(data, 'height')):
        return "polars"
    # Assume pandas DataFrame
    return "pandas"


def _write_with_pandas(data, file_path: str, file_format: str, params: Dict[str, Any], logger) -> Dict[str, Any]:
    """
    Write data using pandas engine.
    
    Args:
        data: pandas DataFrame to write
        file_path: Output file path
        file_format: File format (csv, parquet, arrow)
        params: Write parameters
        logger: Logger instance
        
    Returns:
        Dictionary with write metrics
    """
    encoding = params.get("encoding", "utf-8")
    delimiter = params.get("delimiter", ",")
    row_separator = params.get("row_separator", "\n")
    csv_options = params.get("csv_options", True)
    text_enclosure = params.get("text_enclosure", "\"")
    escape_char = params.get("escape_char", "\\")
    append_mode = params.get("append_mode", False)
    include_header = params.get("include_header", True)
    
    # Validate we have a pandas DataFrame
    if not isinstance(data, pd.DataFrame):
        raise ValueError(f"Expected pandas DataFrame, got {type(data)}")
    
    # Prepare write parameters
    write_params = {}
    
    try:
        if file_format == "csv":
            write_params = {
                "path_or_buf": file_path,
                "sep": delimiter,
                "encoding": encoding,
                "index": False,  # Never include index for cleaner output
                "header": include_header,
                "mode": "a" if append_mode else "w",
                "line_terminator": row_separator
            }
            
            # Add CSV-specific options
            if csv_options:
                write_params.update({
                    "quotechar": text_enclosure,
                    "escapechar": escape_char if escape_char != text_enclosure else None,
                    "quoting": 1,  # QUOTE_ALL for safety in ETL
                    "doublequote": True
                })
            
            # Handle header for append mode
            if append_mode and Path(file_path).exists():
                write_params["header"] = False  # Don't write header when appending
            
            data.to_csv(**write_params)
            
        elif file_format == "parquet":
            if append_mode:
                logger.warning("Append mode not supported for Parquet format, will overwrite")
            
            data.to_parquet(file_path, engine='pyarrow', index=False)
            
        elif file_format == "arrow":
            if append_mode:
                logger.warning("Append mode not supported for Arrow format, will overwrite")
            
            data.to_feather(file_path)
            
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        # Calculate metrics
        rows_written = len(data)
        file_size = Path(file_path).stat().st_size if Path(file_path).exists() else 0
        
        return {
            "rows_written": rows_written,
            "file_size": file_size,
            "files_created": 1
        }
        
    except Exception as e:
        logger.error(f"Write failed with pandas: {e}")
        raise


def _write_with_polars(data, file_path: str, file_format: str, params: Dict[str, Any], logger) -> Dict[str, Any]:
    """
    Write data using Polars engine.
    
    Args:
        data: Polars DataFrame or LazyFrame to write
        file_path: Output file path
        file_format: File format (csv, parquet, arrow)
        params: Write parameters
        logger: Logger instance
        
    Returns:
        Dictionary with write metrics
    """
    try:
        import polars as pl
    except ImportError:
        raise ImportError(
            "Polars engine requested but polars is not installed. "
            "Install with: pip install polars>=0.20.0"
        )
    
    encoding = params.get("encoding", "utf-8")
    delimiter = params.get("delimiter", ",")
    row_separator = params.get("row_separator", "\n")
    csv_options = params.get("csv_options", True)
    text_enclosure = params.get("text_enclosure", "\"")
    append_mode = params.get("append_mode", False)
    include_header = params.get("include_header", True)
    
    # Convert LazyFrame to DataFrame if needed
    if hasattr(data, 'collect'):
        df = data.collect()
    else:
        df = data
    
    # Validate we have a Polars DataFrame
    if not hasattr(df, 'schema') or not hasattr(df, 'height'):
        raise ValueError(f"Expected Polars DataFrame, got {type(df)}")
    
    try:
        if file_format == "csv":
            write_params = {
                "file": file_path,
                "separator": delimiter,
                "include_header": include_header,
                "line_terminator": row_separator
            }
            
            # Add CSV-specific options
            if csv_options:
                write_params.update({
                    "quote_char": text_enclosure,
                    "quote_style": "always"  # Quote all fields for ETL safety
                })
            
            # Handle append mode for CSV - streaming approach for production
            if append_mode and Path(file_path).exists():
                # Use streaming append - no memory overhead
                append_params = write_params.copy()
                append_params["include_header"] = False  # Don't repeat headers
                
                # Open file in append mode and write data
                with open(file_path, 'a', encoding=encoding, newline='') as f:
                    df.write_csv(
                        file=f,
                        separator=delimiter,
                        include_header=False,
                        line_terminator=row_separator,
                        quote_char=text_enclosure if csv_options else None,
                        quote_style="always" if csv_options else "never"
                    )
                logger.info(f"Appended {df.height} rows using streaming mode")
            else:
                df.write_csv(**write_params)
            
        elif file_format == "parquet":
            if append_mode:
                logger.warning("Append mode not supported for Parquet format, will overwrite")
            
            df.write_parquet(file_path)
            
        elif file_format == "arrow":
            if append_mode:
                logger.warning("Append mode not supported for Arrow format, will overwrite")
            
            df.write_ipc(file_path)
            
        else:
            raise ValueError(f"Unsupported file format for Polars: {file_format}")
        
        # Calculate metrics
        rows_written = df.height
        file_size = Path(file_path).stat().st_size if Path(file_path).exists() else 0
        
        return {
            "rows_written": rows_written,
            "file_size": file_size,
            "files_created": 1
        }
        
    except Exception as e:
        logger.error(f"Write failed with Polars: {e}")
        raise


def run(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute ETL-focused file writer with given parameters.
    
    Args:
        params: Dictionary containing file_path, data, and optional parameters
        
    Returns:
        ModResult dictionary with metrics and status
    """
    # Extract mod context
    mod_name = params.get("_mod_name", "file_writer")
    mod_type = params.get("_mod_type", "file_writer")
    
    # Setup logging with mod context
    logger = setup_logger(__name__, mod_type, mod_name)
    
    # Initialize result
    result = ModResult(mod_type, mod_name)
    
    try:
        # Extract parameters
        file_path = params["file_path"]
        data = params.get("data")
        engine = params.get("engine", "pandas").lower()
        create_directories = params.get("create_directories", True)
        overwrite_existing = params.get("overwrite_existing", True)
        append_mode = params.get("append_mode", False)
        
        # Validate we have data to write
        if data is None:
            error_msg = "No data provided to write"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        # Auto-detect engine from data if not specified or if mismatch
        data_engine = _determine_engine_from_data(data)
        if engine != data_engine:
            logger.info(f"Auto-switching engine from {engine} to {data_engine} based on data type")
            engine = data_engine
        
        logger.info(f"Starting ETL file write", extra={
            "file_path": file_path,
            "engine": engine,
            "append_mode": append_mode,
            "data_type": type(data).__name__
        })
        
        # Validate engine
        if engine not in ["pandas", "polars"]:
            error_msg = f"Invalid engine '{engine}'. Must be 'pandas' or 'polars'"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        # Get file format
        file_format = _determine_file_format(file_path)
        logger.info(f"Writing to format: {file_format}")
        
        # Create directories if needed
        if create_directories:
            _create_directories(file_path, logger)
        
        # Validate file operations
        _validate_file_operations(file_path, overwrite_existing, append_mode)
        
        # Check if data is empty
        if engine == "pandas":
            if isinstance(data, pd.DataFrame) and data.empty:
                logger.warning("Writing empty DataFrame")
        else:  # polars
            if hasattr(data, 'collect'):  # LazyFrame
                # For lazy frames, we'll let the write operation handle it
                pass
            elif hasattr(data, 'height') and data.height == 0:
                logger.warning("Writing empty DataFrame")
        
        # Write data based on engine
        if engine == "pandas":
            write_metrics = _write_with_pandas(data, file_path, file_format, params, logger)
        else:  # polars
            write_metrics = _write_with_polars(data, file_path, file_format, params, logger)
        
        logger.info(f"ETL file write successful", extra={
            "rows_written": write_metrics["rows_written"],
            "file_size": write_metrics["file_size"],
            "engine_used": engine,
            "file_format": file_format
        })
        
        # Add metrics
        result.add_metric("rows_written", write_metrics["rows_written"])
        result.add_metric("file_size_bytes", write_metrics["file_size"])
        result.add_metric("files_created", write_metrics["files_created"])
        result.add_metric("engine_used", engine)
        result.add_metric("file_format", file_format)
        result.add_metric("append_mode", append_mode)
        result.add_metric("write_options", {
            "include_header": params.get("include_header", True),
            "csv_options_used": params.get("csv_options", True) and file_format == "csv",
            "encoding": params.get("encoding", "utf-8"),
            "delimiter": params.get("delimiter", ",")
        })
        
        # Add artifacts
        result.add_artifact("file_path", str(file_path))
        result.add_artifact("write_info", {
            "engine": engine,
            "file_format": file_format,
            "file_exists": Path(file_path).exists(),
            "final_file_size": write_metrics["file_size"]
        })
        
        # Add globals for downstream mods
        result.add_global("rows_written", write_metrics["rows_written"])
        result.add_global("files_created", write_metrics["files_created"])
        result.add_global("file_size", write_metrics["file_size"])
        result.add_global("engine_used", engine)
        
        return result.success()
        
    except KeyError as e:
        error_msg = f"Missing required parameter: {e}"
        logger.error(error_msg)
        result.add_error(error_msg)
        return result.error()
        
    except ImportError as e:
        error_msg = str(e)
        logger.error(error_msg)
        result.add_error(error_msg)
        return result.error()
        
    except FileExistsError as e:
        error_msg = str(e)
        logger.error(error_msg)
        result.add_error(error_msg)
        return result.error()
        
    except Exception as e:
        error_msg = f"Unexpected error in ETL file writer: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result.add_error(error_msg)
        return result.error()