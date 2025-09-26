"""
File Input Mod for DataPy Framework - DuckDB Implementation.

Reads files (CSV, JSON, Parquet) into lazy DuckDB relations without execution.
Supports comprehensive CSV options and auto-detection of file formats.
"""

import duckdb
from typing import Dict, Any
from pathlib import Path

from datapy.mod_manager.result import ModResult
from datapy.mod_manager.base import ModMetadata, ConfigSchema
from datapy.mod_manager.logger import setup_logger

# Required metadata
METADATA = ModMetadata(
    type="file_input",
    version="1.0.0",
    description="Read files into lazy DuckDB relations with format auto-detection",
    category="duckdb",
    input_ports=[],
    output_ports=["output_data", "connection"],
    globals=["file_path", "file_type"],
    packages=["duckdb>=0.9.0"]
)

# Parameter schema
CONFIG_SCHEMA = ConfigSchema(
    required={
        "connection": {
            "type": "object",
            "description": "Shared DuckDB connection from duckdb_init"
        },
        "file_path": {
            "type": "str",
            "description": "Path to the input file (local or remote URL)"
        }
    },
    optional={
        "file_type": {
            "type": "str",
            "default": None,
            "description": "File format override (csv, json, parquet). Auto-detected if not provided",
            "enum": ["csv", "json", "parquet", None]
        },
        # CSV-specific options
        "delimiter": {
            "type": "str",
            "default": ",",
            "description": "CSV delimiter character"
        },
        "header": {
            "type": "bool",
            "default": True,
            "description": "CSV has header row"
        },
        "skip": {
            "type": "int",
            "default": 0,
            "description": "Number of lines to skip at start"
        },
        "columns": {
            "type": "dict",
            "default": None,
            "description": "Column names and types mapping (e.g., {'col1': 'INTEGER'})"
        },
        "quote": {
            "type": "str",
            "default": '"',
            "description": "Quote character for CSV"
        },
        "escape": {
            "type": "str",
            "default": '"',
            "description": "Escape character for CSV"
        },
        "nullstr": {
            "type": "str",
            "default": None,
            "description": "String representing NULL values"
        },
        "dateformat": {
            "type": "str",
            "default": None,
            "description": "Date parsing format string"
        },
        "timestampformat": {
            "type": "str",
            "default": None,
            "description": "Timestamp parsing format string"
        },
        "sample_size": {
            "type": "int",
            "default": 20480,
            "description": "Number of rows to sample for auto-detection"
        },
        "ignore_errors": {
            "type": "bool",
            "default": False,
            "description": "Skip rows with errors instead of failing"
        },
        # JSON-specific options
        "format": {
            "type": "str",
            "default": "auto",
            "description": "JSON format: 'auto', 'newline_delimited', 'array'",
            "enum": ["auto", "newline_delimited", "array"]
        },
        "compression": {
            "type": "str",
            "default": "auto",
            "description": "Compression type: 'auto', 'gzip', 'zstd'",
            "enum": ["auto", "gzip", "zstd", "none"]
        }
    }
)


def run(params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute the file input mod with given parameters."""
    # Standard initialization
    mod_name = params.get("_mod_name", "file_input")
    mod_type = params.get("_mod_type", "file_input")
    logger = setup_logger(__name__, mod_type, mod_name)
    result = ModResult(mod_type, mod_name)
    
    try:
        # Extract required parameters
        con = params.get("connection")
        file_path = params.get("file_path")
        
        # Validate inputs
        if con is None:
            error_msg = "Missing required parameter: 'connection'"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        if not isinstance(con, duckdb.DuckDBPyConnection):
            error_msg = f"Invalid connection type: {type(con)}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        if not file_path:
            error_msg = "Missing required parameter: 'file_path'"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        # Determine file type
        file_type = params.get("file_type")
        if not file_type:
            file_type = _detect_file_type(file_path)
            logger.info(f"Auto-detected file type: {file_type}")
        else:
            logger.info(f"Using specified file type: {file_type}")
        
        # Get file metadata (no execution)
        file_info = _get_file_info(file_path, file_type, logger)
        
        # Read file as lazy relation based on type
        logger.info(f"Reading file: {file_path}", extra={
            "file_type": file_type,
            "file_size_mb": file_info["file_size_mb"]
        })
        
        if file_type == "csv":
            relation = _read_csv(con, file_path, params, logger)
        elif file_type == "json":
            relation = _read_json(con, file_path, params, logger)
        elif file_type == "parquet":
            relation = _read_parquet(con, file_path, logger)
        else:
            error_msg = f"Unsupported file type: {file_type}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        # Add artifacts (lazy relation + connection + metadata)
        result.add_artifact("output_data", relation)
        result.add_artifact("connection", con)
        result.add_artifact("file_info", file_info)
        
        # Add globals for downstream mods
        result.add_global("file_path", file_path)
        result.add_global("file_type", file_type)
        
        # Add metrics
        result.add_metric("file_size_mb", file_info["file_size_mb"])
        result.add_metric("file_type", file_type)
        
        logger.info(f"File input completed successfully", extra={
            "file_path": file_path,
            "file_type": file_type
        })
        
        return result.success()
        
    except Exception as e:
        error_msg = f"Error in {mod_type}: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result.add_error(error_msg)
        return result.error()


def _detect_file_type(file_path: str) -> str:
    """Auto-detect file type from extension."""
    path = Path(file_path)
    ext = path.suffix.lower().lstrip('.')
    
    # Handle compressed extensions
    if ext in ['gz', 'gzip']:
        ext = path.stem.split('.')[-1].lower()
    
    if ext in ['csv', 'tsv', 'txt']:
        return "csv"
    elif ext in ['json', 'jsonl', 'ndjson']:
        return "json"
    elif ext in ['parquet', 'pq']:
        return "parquet"
    else:
        raise ValueError(f"Cannot auto-detect file type from extension: {ext}")


def _get_file_info(file_path: str, file_type: str, logger) -> Dict[str, Any]:
    """Get file metadata without executing queries."""
    info = {
        "file_path": file_path,
        "file_type": file_type,
        "file_size_mb": 0.0
    }
    
    # Try to get file size from local filesystem
    try:
        path = Path(file_path)
        if path.exists() and path.is_file():
            size_bytes = path.stat().st_size
            info["file_size_mb"] = round(size_bytes / (1024 * 1024), 2)
            logger.debug(f"File size: {info['file_size_mb']} MB")
    except Exception as e:
        logger.warning(f"Could not get file size: {e}")
    
    return info


def _read_csv(con, file_path: str, params: Dict[str, Any], logger) -> duckdb.DuckDBPyRelation:
    """Read CSV file as lazy DuckDB relation."""
    # Build CSV read options
    csv_options = {
        "delim": params.get("delimiter", ","),
        "header": params.get("header", True),
        "skip": params.get("skip", 0),
        "quote": params.get("quote", '"'),
        "escape": params.get("escape", '"'),
        "sample_size": params.get("sample_size", 20480),
        "ignore_errors": params.get("ignore_errors", False)
    }
    
    # Add optional parameters if provided
    if params.get("columns"):
        csv_options["columns"] = params["columns"]
    if params.get("nullstr"):
        csv_options["nullstr"] = params["nullstr"]
    if params.get("dateformat"):
        csv_options["dateformat"] = params["dateformat"]
    if params.get("timestampformat"):
        csv_options["timestampformat"] = params["timestampformat"]
    
    logger.debug(f"CSV options: {csv_options}")
    
    # Use DuckDB's read_csv (returns lazy relation)
    return con.read_csv(file_path, **csv_options)


def _read_json(con, file_path: str, params: Dict[str, Any], logger) -> duckdb.DuckDBPyRelation:
    """Read JSON file as lazy DuckDB relation."""
    json_format = params.get("format", "auto")
    compression = params.get("compression", "auto")
    
    # Build read_json options
    json_options = {}
    if json_format != "auto":
        json_options["format"] = json_format
    if compression != "auto":
        json_options["compression"] = compression
    
    logger.debug(f"JSON options: {json_options}")
    
    # Use DuckDB's read_json (returns lazy relation)
    return con.read_json(file_path, **json_options)


def _read_parquet(con, file_path: str, logger) -> duckdb.DuckDBPyRelation:
    """Read Parquet file as lazy DuckDB relation."""
    # DuckDB handles Parquet auto-detection efficiently
    logger.debug("Reading Parquet with auto-detection")
    return con.read_parquet(file_path)