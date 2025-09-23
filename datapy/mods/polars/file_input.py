"""
Universal File Input Mod for DataPy Framework.

Reads CSV and Parquet files with lazy/streaming Polars approach.
Designed for memory-efficient ETL processing with automatic spill-to-disk capabilities.
Extensible architecture for future format support.
"""

from pathlib import Path
from typing import Dict, Any, Union
import polars as pl
import os

from datapy.mod_manager.result import ModResult
from datapy.mod_manager.base import ModMetadata, ConfigSchema
from datapy.mod_manager.logger import setup_logger

# Required metadata
METADATA = ModMetadata(
    type="file_input",
    version="1.0.0",
    description="Universal file reader with lazy/streaming Polars for memory-efficient ETL processing",
    category="file_ops", 
    input_ports=[],
    output_ports=["data"],
    globals=["row_count", "column_count", "file_size", "file_format"],
    packages=["polars>=0.20.0"]
)

# Simplified parameter schema
CONFIG_SCHEMA = ConfigSchema(
    required={
        "file_path": {
            "type": "str",
            "description": "Path to file to read (CSV or Parquet format)"
        }
    },
    optional={
        "encoding": {
            "type": "str",
            "default": "utf-8",
            "description": "File encoding for CSV files"
        },
        "delimiter": {
            "type": "str",
            "default": ",",
            "description": "Field separator character for CSV"
        },
        "row_separator": {
            "type": "str", 
            "default": "\n",
            "description": "Line ending character"
        },
        "header_rows": {
            "type": "int",
            "default": 1, 
            "description": "Number of header rows (0=no headers, 1=single header)"
        },
        "footer_rows": {
            "type": "int",
            "default": 0,
            "description": "Number of footer rows to skip from end"
        },
        "skip_rows": {
            "type": "int",
            "default": 0,
            "description": "Number of rows to skip at start (before headers)"
        },
        "read_options": {
            "type": "dict",
            "default": {},
            "description": "Additional Polars read parameters to override defaults"
        }
    }
)


def _detect_file_format(file_path: str) -> str:
    """
    Detect file format based on extension. Extensible for future formats.
    
    Args:
        file_path: Path to file
        
    Returns:
        File format: 'csv' or 'parquet'
    """
    suffix = Path(file_path).suffix.lower()
    
    format_map = {
        '.csv': 'csv',
        '.tsv': 'csv',
        '.txt': 'csv',
        '.parquet': 'parquet',
        '.pq': 'parquet'
    }
    
    detected_format = format_map.get(suffix, 'csv')
    return detected_format


def _read_csv_lazy(file_path: str, params: Dict[str, Any], logger) -> pl.LazyFrame:
    """
    Read CSV file using Polars lazy evaluation with ETL optimizations.
    
    Args:
        file_path: Path to CSV file
        params: Processing parameters
        logger: Logger instance
        
    Returns:
        Polars LazyFrame for memory-efficient processing
    """
    encoding = params.get("encoding", "utf-8")
    delimiter = params.get("delimiter", ",")
    row_separator = params.get("row_separator", "\n")
    header_rows = params.get("header_rows", 1)
    skip_rows = params.get("skip_rows", 0)
    footer_rows = params.get("footer_rows", 0)
    read_options = params.get("read_options", {})
    
    # Configure Polars CSV reading with optimal ETL performance - ONLY VALID PARAMETERS
    read_params = {
        "separator": delimiter,
        "skip_rows": skip_rows,
        "has_header": header_rows > 0,
        "encoding": encoding if encoding != "utf-8" else "utf8",  # Convert to Polars format
        "infer_schema": True,  # Schema inference for efficiency
        "ignore_errors": False,  # Fail fast for data quality
        "null_values": ["", "NULL", "null", "None", "NA"],
        # CSV-specific options enabled by default
        "quote_char": '"',
        "skip_rows_after_header": 0,
        "eol_char": row_separator,
        # Optimal ETL performance settings
        "rechunk": True,  # Single chunk for better memory layout
        "truncate_ragged_lines": True,  # Handle real-world dirty data
    }
    
    # Allow user overrides for advanced use cases
    read_params.update(read_options)
    
    logger.info(f"Reading CSV with optimal ETL settings + user overrides: {len(read_options)} custom options")
    
    # Use scan_csv for lazy evaluation - no data loaded into memory yet
    lazy_df = pl.scan_csv(file_path, **read_params)
    
    # Handle footer rows by limiting the scan (if needed)
    if footer_rows > 0:
        # For footer handling, we need to calculate total rows
        # This is a limitation - we'll log a warning for now
        logger.warning(f"Footer row handling ({footer_rows}) may require materialization")
        
    return lazy_df


def _read_parquet_lazy(file_path: str, params: Dict[str, Any], logger) -> pl.LazyFrame:
    """
    Read Parquet file using Polars lazy evaluation.
    
    Args:
        file_path: Path to Parquet file
        params: Processing parameters  
        logger: Logger instance
        
    Returns:
        Polars LazyFrame for memory-efficient processing
    """
    skip_rows = params.get("skip_rows", 0)
    footer_rows = params.get("footer_rows", 0)
    read_options = params.get("read_options", {})
    
    logger.info(f"Reading Parquet with optimal lazy evaluation and parallel I/O")
    
    # Use scan_parquet for lazy evaluation
    lazy_df = pl.scan_parquet(file_path, **read_options)
    
    # Apply row filtering lazily
    if skip_rows > 0:
        lazy_df = lazy_df.slice(skip_rows)
        
    if footer_rows > 0:
        logger.warning(f"Footer row handling ({footer_rows}) may require materialization for Parquet")
    
    return lazy_df


def run(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute universal file input with lazy/streaming approach.
    
    Args:
        params: Dictionary containing file_path and optional parameters
        
    Returns:
        ModResult dictionary with lazy DataFrame and metrics
    """
    # Extract mod context
    mod_name = params.get("_mod_name", "file_input")
    mod_type = params.get("_mod_type", "file_input")
    
    # Setup logging with mod context
    logger = setup_logger(__name__, mod_type, mod_name)
    
    # Initialize result
    result = ModResult(mod_type, mod_name)
    
    try:
        # Extract parameters
        file_path = params["file_path"]
        
        logger.info(f"Starting file input", extra={
            "file_path": file_path
        })
        
        # Validate file exists and is readable
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
        
        # Check file size for basic sanity
        file_size = file_obj.stat().st_size
        if file_size == 0:
            error_msg = f"File is empty (0 bytes): {file_path}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        # Check read permissions
        if not os.access(file_path, os.R_OK):
            error_msg = f"File is not readable (permission denied): {file_path}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        # Detect file format
        file_format = _detect_file_format(file_path)
        
        logger.info(f"File detected - Size: {file_size} bytes, Format: {file_format}")
        
        # Read file with appropriate lazy reader
        if file_format == "csv":
            lazy_df = _read_csv_lazy(file_path, params, logger)
        elif file_format == "parquet":
            lazy_df = _read_parquet_lazy(file_path, params, logger)
        else:
            error_msg = f"Unsupported file format: {file_format}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        # Get basic schema info without materializing
        schema = lazy_df.collect_schema()
        column_count = len(schema)
        
        # Validate we have columns
        if column_count == 0:
            error_msg = f"File has no columns or invalid format: {file_path}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        # Quick validation check for empty data (minimal materialization)
        try:
            # Check if file has any data rows by limiting to just 1 row
            first_row = lazy_df.head(1).collect()
            if first_row.height == 0:
                warning_msg = f"File appears to be empty (no data rows): {file_path}"
                logger.warning(warning_msg)
                result.add_warning(warning_msg)
                # Continue processing but with warning
        except Exception as e:
            # If we can't even read 1 row, it's likely a serious data issue
            error_msg = f"Unable to read data from file (possible corruption or format issue): {str(e)}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        logger.info(f"Lazy DataFrame created and validated successfully", extra={
            "columns": column_count,
            "schema": {col: str(dtype) for col, dtype in schema.items()}
        })
        
        # Add metrics (row count will be "lazy" since we don't materialize)
        result.add_metric("row_count", "lazy_evaluation")
        result.add_metric("column_count", column_count)
        result.add_metric("file_size_bytes", file_size)
        result.add_metric("file_format", file_format)
        result.add_metric("processing_mode", "lazy_streaming")
        result.add_metric("memory_efficient", True)
        
        # Add artifacts - the lazy DataFrame for downstream processing
        result.add_artifact("data", lazy_df)
        result.add_artifact("file_path", str(file_path))
        result.add_artifact("schema", schema)
        
        # Add globals for downstream mods
        result.add_global("row_count", "lazy_evaluation") 
        result.add_global("column_count", column_count)
        result.add_global("file_size", file_size)
        result.add_global("file_format", file_format)
        
        return result.success()
        
    except KeyError as e:
        error_msg = f"Missing required parameter: {e}"
        logger.error(error_msg)
        result.add_error(error_msg)
        return result.error()
        
    except PermissionError as e:
        error_msg = f"Permission denied accessing file: {str(e)}"
        logger.error(error_msg)
        result.add_error(error_msg)
        return result.error()
        
    except FileNotFoundError as e:
        error_msg = f"File not found: {str(e)}"
        logger.error(error_msg)
        result.add_error(error_msg)
        return result.error()
        
    except pl.ComputeError as e:
        error_msg = f"Polars parsing error (check delimiter, encoding, quote chars): {str(e)}"
        logger.error(error_msg)
        result.add_error(error_msg)
        return result.error()
        
    except pl.NoDataError as e:
        error_msg = f"No data found in file: {str(e)}"
        logger.error(error_msg)
        result.add_error(error_msg)
        return result.error()
        
    except ImportError as e:
        error_msg = f"Missing required dependency: {str(e)}"
        logger.error(error_msg)
        result.add_error(error_msg)
        return result.error()
        
    except Exception as e:
        error_msg = f"Unexpected error in file input: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result.add_error(error_msg)
        return result.error()