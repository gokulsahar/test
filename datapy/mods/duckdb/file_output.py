"""
File Output Mod for DataPy Framework - DuckDB Implementation.

Writes DuckDB relations to files (CSV, JSON, Parquet). This is a SINK operation
that executes the entire lazy chain and materializes results to disk.
"""

import duckdb
from typing import Dict, Any
from pathlib import Path
import time

from datapy.mod_manager.result import ModResult
from datapy.mod_manager.base import ModMetadata, ConfigSchema
from datapy.mod_manager.logger import setup_logger

# Required metadata
METADATA = ModMetadata(
    type="file_output",
    version="1.0.0",
    description="Write DuckDB relations to files (CSV, JSON, Parquet) - SINK operation",
    category="duckdb",
    input_ports=["input_data", "connection"],
    output_ports=["output_data", "connection"],
    globals=["output_path", "file_type", "rows_written"],
    packages=["duckdb>=0.9.0"]
)

# Parameter schema
CONFIG_SCHEMA = ConfigSchema(
    required={
        "connection": {
            "type": "object",
            "description": "Shared DuckDB connection from upstream mod"
        },
        "input_data": {
            "type": "object",
            "description": "DuckDB relation to write to file"
        },
        "output_path": {
            "type": "str",
            "description": "Path to the output file"
        }
    },
    optional={
        "file_type": {
            "type": "str",
            "default": None,
            "description": "File format override (csv, json, parquet). Auto-detected if not provided",
            "enum": ["csv", "json", "parquet", None]
        },
        "overwrite": {
            "type": "bool",
            "default": True,
            "description": "Overwrite existing file if it exists"
        },
        "create_new_relation": {
            "type": "bool",
            "default": False,
            "description": "Read back the written file as new lazy relation for downstream mods"
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
            "description": "Include header row in CSV"
        },
        "compression": {
            "type": "str",
            "default": None,
            "description": "Compression type: 'gzip', 'zstd', etc.",
            "enum": [None, "gzip", "zstd", "none"]
        },
        "dateformat": {
            "type": "str",
            "default": None,
            "description": "Date format string for CSV output"
        },
        "timestampformat": {
            "type": "str",
            "default": None,
            "description": "Timestamp format string for CSV output"
        },
        # Parquet-specific options
        "parquet_compression": {
            "type": "str",
            "default": "snappy",
            "description": "Parquet compression codec",
            "enum": ["snappy", "gzip", "zstd", "uncompressed"]
        },
        "row_group_size": {
            "type": "int",
            "default": 122880,
            "description": "Parquet row group size"
        }
    }
)


def run(params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute the file output mod with given parameters."""
    # Standard initialization
    mod_name = params.get("_mod_name", "file_output")
    mod_type = params.get("_mod_type", "file_output")
    logger = setup_logger(__name__, mod_type, mod_name)
    result = ModResult(mod_type, mod_name)
    
    try:
        # Extract required parameters
        con = params.get("connection")
        input_relation = params.get("input_data")
        output_path = params.get("output_path")
        
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
        
        if input_relation is None:
            error_msg = "Missing required parameter: 'input_data'"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        if not isinstance(input_relation, duckdb.DuckDBPyRelation):
            error_msg = f"Invalid input_data type: {type(input_relation)}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        if not output_path:
            error_msg = "Missing required parameter: 'output_path'"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        # Determine file type
        file_type = params.get("file_type")
        if not file_type:
            file_type = _detect_file_type(output_path)
            logger.info(f"Auto-detected file type: {file_type}")
        else:
            logger.info(f"Using specified file type: {file_type}")
        
        # Check if file exists and overwrite settings
        overwrite = params.get("overwrite", True)
        output_file = Path(output_path)
        if output_file.exists() and not overwrite:
            error_msg = f"File already exists and overwrite=False: {output_path}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        # Create output directory if it doesn't exist
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Write file based on type (THIS EXECUTES THE LAZY CHAIN!)
        logger.info(f"Writing file: {output_path}", extra={
            "file_type": file_type,
            "overwrite": overwrite
        })
        
        start_time = time.time()
        
        if file_type == "csv":
            rows_written = _write_csv(input_relation, output_path, params, logger)
        elif file_type == "json":
            rows_written = _write_json(input_relation, output_path, params, logger)
        elif file_type == "parquet":
            rows_written = _write_parquet(input_relation, output_path, params, logger)
        else:
            error_msg = f"Unsupported file type: {file_type}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        write_time = time.time() - start_time
        
        # Get output file metadata
        file_info = _get_file_info(output_path, file_type, rows_written, logger)
        
        # Optionally create new relation from written file
        output_relation = None
        if params.get("create_new_relation", False):
            logger.info("Creating new lazy relation from written file")
            output_relation = _read_back(con, output_path, file_type, logger)
            result.add_artifact("output_data", output_relation)
        
        # Add artifacts
        result.add_artifact("connection", con)
        result.add_artifact("file_info", file_info)
        
        # Add globals for downstream mods
        result.add_global("output_path", output_path)
        result.add_global("file_type", file_type)
        result.add_global("rows_written", rows_written)
        
        # Add metrics
        result.add_metric("rows_written", rows_written)
        result.add_metric("file_size_mb", file_info["file_size_mb"])
        result.add_metric("write_time_seconds", round(write_time, 2))
        result.add_metric("file_type", file_type)
        
        logger.info(f"File output completed successfully", extra={
            "output_path": output_path,
            "rows_written": rows_written,
            "write_time": write_time,
            "file_size_mb": file_info["file_size_mb"]
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


def _get_file_info(file_path: str, file_type: str, rows_written: int, logger) -> Dict[str, Any]:
    """Get file metadata after writing."""
    info = {
        "output_path": file_path,
        "file_type": file_type,
        "rows_written": rows_written,
        "file_size_mb": 0.0
    }
    
    # Get file size from filesystem
    try:
        path = Path(file_path)
        if path.exists() and path.is_file():
            size_bytes = path.stat().st_size
            info["file_size_mb"] = round(size_bytes / (1024 * 1024), 2)
            logger.debug(f"Output file size: {info['file_size_mb']} MB")
    except Exception as e:
        logger.warning(f"Could not get output file size: {e}")
    
    return info


def _write_csv(relation: duckdb.DuckDBPyRelation, output_path: str, 
               params: Dict[str, Any], logger) -> int:
    """Write relation to CSV file - EXECUTES THE QUERY."""
    # Build CSV write options
    csv_options = {
        "sep": params.get("delimiter", ","),
        "header": params.get("header", True)
    }
    
    # Add optional parameters
    if params.get("compression"):
        csv_options["compression"] = params["compression"]
    if params.get("dateformat"):
        csv_options["dateformat"] = params["dateformat"]
    if params.get("timestampformat"):
        csv_options["timestampformat"] = params["timestampformat"]
    
    logger.debug(f"CSV write options: {csv_options}")
    
    # Execute and write (materializes the lazy chain)
    relation.write_csv(output_path, **csv_options)
    
    # Get row count from relation shape (already executed)
    rows_written = relation.shape[0] if hasattr(relation, 'shape') else 0
    
    return rows_written


def _write_json(relation: duckdb.DuckDBPyRelation, output_path: str,
                params: Dict[str, Any], logger) -> int:
    """Write relation to JSON file - EXECUTES THE QUERY."""
    json_options = {}
    
    # Add compression if specified
    if params.get("compression"):
        json_options["compression"] = params["compression"]
    
    logger.debug(f"JSON write options: {json_options}")
    
    # Execute and write (materializes the lazy chain)
    # Note: DuckDB doesn't have direct write_json, use COPY TO
    copy_sql = f"COPY ({relation}) TO '{output_path}' (FORMAT JSON"
    if json_options.get("compression"):
        copy_sql += f", COMPRESSION '{json_options['compression']}'"
    copy_sql += ")"
    
    relation.connection.execute(copy_sql)
    
    # Get row count
    rows_written = relation.shape[0] if hasattr(relation, 'shape') else 0
    
    return rows_written


def _write_parquet(relation: duckdb.DuckDBPyRelation, output_path: str,
                   params: Dict[str, Any], logger) -> int:
    """Write relation to Parquet file - EXECUTES THE QUERY."""
    # Build Parquet write options
    parquet_options = {
        "compression": params.get("parquet_compression", "snappy"),
        "row_group_size": params.get("row_group_size", 122880)
    }
    
    logger.debug(f"Parquet write options: {parquet_options}")
    
    # Execute and write (materializes the lazy chain)
    relation.write_parquet(output_path, **parquet_options)
    
    # Get row count from relation shape (already executed)
    rows_written = relation.shape[0] if hasattr(relation, 'shape') else 0
    
    return rows_written


def _read_back(con: duckdb.DuckDBPyConnection, file_path: str, 
               file_type: str, logger) -> duckdb.DuckDBPyRelation:
    """Read back the written file as a new lazy relation."""
    logger.debug(f"Reading back {file_type} file as new relation")
    
    if file_type == "csv":
        return con.read_csv(file_path)
    elif file_type == "json":
        return con.read_json(file_path)
    elif file_type == "parquet":
        return con.read_parquet(file_path)
    else:
        raise ValueError(f"Cannot read back file type: {file_type}")