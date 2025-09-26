"""
File Output Mod for DataPy Framework - DuckDB Implementation.

Writes DuckDB relations to files (CSV, Parquet). This is a SINK operation
that executes the entire lazy chain and materializes results to disk.
"""

import duckdb
from typing import Dict, Any
from pathlib import Path
import time
import os

from datapy.mod_manager.result import ModResult
from datapy.mod_manager.base import ModMetadata, ConfigSchema
from datapy.mod_manager.logger import setup_logger

METADATA = ModMetadata(
    type="file_output",
    version="1.0.0",
    description="Write DuckDB relations to files (CSV, Parquet) - SINK operation",
    category="duckdb",
    input_ports=["input_data", "connection"],
    output_ports=["output_data", "connection"],
    globals=["output_path", "file_type"],
    packages=["duckdb>=0.9.0"]
)

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
            "description": "File format (csv, parquet). Auto-detected if not provided",
            "enum": ["csv", "parquet", None]
        },
        "overwrite": {
            "type": "bool",
            "default": True,
            "description": "Overwrite existing file if it exists"
        },
        "create_new_relation": {
            "type": "bool",
            "default": False,
            "description": "Read back the written file as new lazy relation"
        },
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
            "description": "Compression: 'gzip', 'zstd'",
            "enum": [None, "gzip", "zstd"]
        },
        "dateformat": {
            "type": "str",
            "default": None,
            "description": "Date format string for CSV"
        },
        "timestampformat": {
            "type": "str",
            "default": None,
            "description": "Timestamp format string for CSV"
        },
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
    mod_name = params.get("_mod_name", "file_output")
    mod_type = params.get("_mod_type", "file_output")
    logger = setup_logger(__name__, mod_type, mod_name)
    result = ModResult(mod_type, mod_name)
    
    try:
        con = params.get("connection")
        input_relation = params.get("input_data")
        output_path = params.get("output_path")
        
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
        
        sanitized_path = _validate_path(output_path)
        
        file_type = params.get("file_type")
        if not file_type:
            file_type = _detect_file_type(sanitized_path)
            logger.info(f"Auto-detected file type: {file_type}")
        else:
            if file_type not in ["csv", "parquet"]:
                error_msg = f"Unsupported file type: {file_type}"
                logger.error(error_msg)
                result.add_error(error_msg)
                return result.error()
            logger.info(f"Using specified file type: {file_type}")
        
        overwrite = params.get("overwrite", True)
        output_file = Path(sanitized_path)
        
        if output_file.exists():
            if not overwrite:
                error_msg = f"File exists and overwrite=False: {sanitized_path}"
                logger.error(error_msg)
                result.add_error(error_msg)
                return result.error()
            else:
                output_file.unlink()
                logger.info(f"Deleted existing file: {sanitized_path}")
        
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Writing file: {sanitized_path}", extra={
            "file_type": file_type,
            "overwrite": overwrite
        })
        
        start_time = time.time()
        
        if file_type == "csv":
            _write_csv(con, input_relation, sanitized_path, params, logger)
        elif file_type == "parquet":
            _write_parquet(con, input_relation, sanitized_path, params, logger)
        else:
            error_msg = f"Unsupported file type: {file_type}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        write_time = time.time() - start_time
        
        file_info = _get_file_info(sanitized_path, file_type, logger)
        
        output_relation = None
        if params.get("create_new_relation", False):
            logger.info("Creating new lazy relation from written file")
            output_relation = _read_back(con, sanitized_path, file_type, logger)
            result.add_artifact("output_data", output_relation)
        
        result.add_artifact("connection", con)
        result.add_artifact("file_info", file_info)
        
        result.add_global("output_path", sanitized_path)
        result.add_global("file_type", file_type)
        
        result.add_metric("file_size_mb", file_info["file_size_mb"])
        result.add_metric("write_time_seconds", round(write_time, 2))
        result.add_metric("file_type", file_type)
        
        logger.info(f"File output completed successfully", extra={
            "output_path": sanitized_path,
            "write_time": write_time,
            "file_size_mb": file_info["file_size_mb"]
        })
        
        return result.success()
        
    except Exception as e:
        error_msg = f"Error in {mod_type}: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result.add_error(error_msg)
        return result.error()


def _validate_path(path: str) -> str:
    """Validate output path."""
    if not path or not isinstance(path, str):
        raise ValueError("Output path must be a non-empty string")
    
    if '\x00' in path:
        raise ValueError("Path contains null bytes")
    
    return os.path.normpath(path)


def _detect_file_type(file_path: str) -> str:
    """Auto-detect file type from extension."""
    path = Path(file_path)
    ext = path.suffix.lower().lstrip('.')
    
    if ext in ['gz', 'gzip']:
        ext = path.stem.split('.')[-1].lower()
    
    if ext in ['csv', 'tsv', 'txt']:
        return "csv"
    elif ext in ['parquet', 'pq']:
        return "parquet"
    else:
        raise ValueError(f"Cannot auto-detect file type from extension: {ext}")


def _get_file_info(file_path: str, file_type: str, logger) -> Dict[str, Any]:
    """Get file metadata after writing."""
    info = {
        "output_path": file_path,
        "file_type": file_type,
        "file_size_mb": 0.0
    }
    
    try:
        path = Path(file_path)
        if path.exists() and path.is_file():
            size_bytes = path.stat().st_size
            info["file_size_mb"] = round(size_bytes / (1024 * 1024), 2)
            logger.debug(f"Output file size: {info['file_size_mb']} MB")
    except Exception as e:
        logger.warning(f"Could not get output file size: {e}")
    
    return info


def _write_csv(con: duckdb.DuckDBPyConnection, relation: duckdb.DuckDBPyRelation, 
               output_path: str, params: Dict[str, Any], logger) -> None:
    """Write relation to CSV file using COPY TO."""
    delimiter = params.get("delimiter", ",")
    if not isinstance(delimiter, str) or len(delimiter) > 4:
        raise ValueError(f"Invalid delimiter: {delimiter}")
    
    compression = params.get("compression")
    if compression and compression not in ["gzip", "zstd"]:
        raise ValueError(f"Invalid compression: {compression}")
    
    copy_sql = f"COPY ({relation}) TO ? (FORMAT CSV"
    copy_params = [output_path]
    
    if params.get("header", True):
        copy_sql += ", HEADER TRUE"
    else:
        copy_sql += ", HEADER FALSE"
    
    copy_sql += f", DELIMITER '{delimiter}'"
    
    if compression:
        copy_sql += f", COMPRESSION '{compression}'"
    
    if params.get("dateformat"):
        dateformat = params["dateformat"]
        if len(dateformat) > 50:
            raise ValueError("dateformat too long")
        copy_sql += f", DATEFORMAT '{dateformat}'"
    
    if params.get("timestampformat"):
        timestampformat = params["timestampformat"]
        if len(timestampformat) > 50:
            raise ValueError("timestampformat too long")
        copy_sql += f", TIMESTAMPFORMAT '{timestampformat}'"
    
    copy_sql += ")"
    
    logger.debug(f"CSV COPY statement: {copy_sql}")
    
    con.execute(copy_sql, copy_params)


def _write_parquet(con: duckdb.DuckDBPyConnection, relation: duckdb.DuckDBPyRelation,
                   output_path: str, params: Dict[str, Any], logger) -> None:
    """Write relation to Parquet file using COPY TO."""
    compression = params.get("parquet_compression", "snappy")
    if compression not in ["snappy", "gzip", "zstd", "uncompressed"]:
        raise ValueError(f"Invalid Parquet compression: {compression}")
    
    row_group_size = params.get("row_group_size", 122880)
    if not isinstance(row_group_size, int) or row_group_size <= 0:
        raise ValueError(f"Invalid row_group_size: {row_group_size}")
    
    copy_sql = f"""
        COPY ({relation}) TO ? 
        (FORMAT PARQUET, COMPRESSION '{compression}', ROW_GROUP_SIZE {row_group_size})
    """
    
    logger.debug(f"Parquet COPY statement: {copy_sql}")
    
    con.execute(copy_sql, [output_path])


def _read_back(con: duckdb.DuckDBPyConnection, file_path: str, 
               file_type: str, logger) -> duckdb.DuckDBPyRelation:
    """Read back the written file as a new lazy relation."""
    logger.debug(f"Reading back {file_type} file as new relation")
    
    if file_type == "csv":
        return con.read_csv(file_path)
    elif file_type == "parquet":
        return con.read_parquet(file_path)
    else:
        raise ValueError(f"Cannot read back file type: {file_type}")