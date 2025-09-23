"""
Universal File Output Mod for DataPy Framework.

Writes CSV and Parquet files with lazy/streaming Polars approach.
Designed for memory-efficient ETL processing with automatic spill-to-disk capabilities.
Includes atomic writes, schema validation for append mode, and comprehensive error handling.
"""

from pathlib import Path
from typing import Dict, Any, Union
import polars as pl
import os
import tempfile
import shutil

from datapy.mod_manager.result import ModResult
from datapy.mod_manager.base import ModMetadata, ConfigSchema
from datapy.mod_manager.logger import setup_logger

# Required metadata
METADATA = ModMetadata(
    type="file_output",
    version="1.0.0",
    description="Universal file writer with lazy/streaming Polars for memory-efficient ETL processing",
    category="file_ops",
    input_ports=["data"],
    output_ports=[],
    globals=["rows_written", "file_size", "file_format", "write_mode"],
    packages=["polars>=0.20.0"]
)

# Streamlined parameter schema - 7 total parameters
CONFIG_SCHEMA = ConfigSchema(
    required={
        "data": {
            "type": "object",
            "description": "Lazy DataFrame to write to file"
        },
        "output_path": {
            "type": "str", 
            "description": "Path to output file (CSV or Parquet format)"
        }
    },
    optional={
        "append_mode": {
            "type": "bool",
            "default": False,
            "description": "Append to existing file instead of overwriting (with schema validation)"
        },
        "encoding": {
            "type": "str",
            "default": "utf-8",
            "description": "File encoding for text formats"
        },
        "delimiter": {
            "type": "str", 
            "default": ",",
            "description": "Field separator character for CSV"
        },
        "include_header": {
            "type": "bool",
            "default": True,
            "description": "Include column headers in output file"
        },
        "write_options": {
            "type": "dict",
            "default": {},
            "description": "Additional Polars write parameters (compression, text_enclosure, etc.)"
        }
    }
)


def _detect_file_format(file_path: str) -> str:
    """
    Detect file format based on extension. Auto-detect compression.
    
    Args:
        file_path: Path to output file
        
    Returns:
        File format: 'csv' or 'parquet'
    """
    path_obj = Path(file_path)
    
    # Handle compression extensions (.gz, .bz2, etc.)
    if path_obj.suffix.lower() in ['.gz', '.bz2', '.xz', '.lz4']:
        # Get the format from the second-to-last extension
        stem_suffix = Path(path_obj.stem).suffix.lower()
    else:
        stem_suffix = path_obj.suffix.lower()
    
    format_map = {
        '.csv': 'csv',
        '.tsv': 'csv', 
        '.txt': 'csv',
        '.parquet': 'parquet',
        '.pq': 'parquet'
    }
    
    return format_map.get(stem_suffix, 'csv')


def _validate_schema_for_append(existing_path: str, new_data: pl.LazyFrame, logger) -> bool:
    """
    Validate schema compatibility for append mode.
    
    Args:
        existing_path: Path to existing file
        new_data: New lazy DataFrame to append
        logger: Logger instance
        
    Returns:
        True if schemas are compatible, False otherwise
    """
    try:
        file_format = _detect_file_format(existing_path)
        
        # Read existing file schema
        if file_format == 'csv':
            existing_schema = pl.scan_csv(existing_path, n_rows=0).schema
        else:  # parquet
            existing_schema = pl.scan_parquet(existing_path).schema
            
        new_schema = new_data.schema
        
        # Check column names match exactly
        if set(existing_schema.keys()) != set(new_schema.keys()):
            logger.error(f"Schema mismatch - column names differ: existing={list(existing_schema.keys())}, new={list(new_schema.keys())}")
            return False
            
        # Check data types are compatible
        for col_name in existing_schema.keys():
            existing_dtype = existing_schema[col_name]
            new_dtype = new_schema[col_name]
            
            if existing_dtype != new_dtype:
                logger.error(f"Schema mismatch - column '{col_name}' type differs: existing={existing_dtype}, new={new_dtype}")
                return False
        
        logger.info("Schema validation passed for append mode")
        return True
        
    except Exception as e:
        logger.error(f"Schema validation failed: {str(e)}")
        return False


def _write_csv_streaming(data: pl.LazyFrame, file_path: str, params: Dict[str, Any], logger) -> Dict[str, Any]:
    """
    Write CSV file using streaming approach with memory efficiency.
    
    Args:
        data: Lazy DataFrame to write
        file_path: Output file path
        params: Write parameters
        logger: Logger instance
        
    Returns:
        Dictionary with write metrics
    """
    encoding = params.get("encoding", "utf-8")
    delimiter = params.get("delimiter", ",")
    include_header = params.get("include_header", True)
    write_options = params.get("write_options", {})
    
    # Merge write options with defaults
    csv_options = {
        "separator": delimiter,
        "include_header": include_header,
        **write_options  # User options override defaults
    }
    
    logger.info(f"Writing CSV with streaming approach", extra={
        "delimiter": delimiter,
        "include_header": include_header,
        "encoding": encoding
    })
    
    # Use streaming sink for memory efficiency - never materialize full dataset
    data.sink_csv(file_path, **csv_options)
    
    # Calculate metrics without loading data
    file_size = Path(file_path).stat().st_size
    
    return {
        "rows_written": "streaming_mode",  # Cannot count without materialization
        "file_size": file_size,
        "columns_written": len(data.columns)
    }


def _write_parquet_streaming(data: pl.LazyFrame, file_path: str, params: Dict[str, Any], logger) -> Dict[str, Any]:
    """
    Write Parquet file using streaming approach with optimal compression.
    
    Args:
        data: Lazy DataFrame to write  
        file_path: Output file path
        params: Write parameters
        logger: Logger instance
        
    Returns:
        Dictionary with write metrics
    """
    write_options = params.get("write_options", {})
    
    # Default Parquet options for performance
    parquet_options = {
        "compression": "snappy",  # Fast compression by default
        **write_options  # User options override defaults
    }
    
    logger.info(f"Writing Parquet with streaming approach and optimal compression")
    
    # Use streaming sink for memory efficiency
    data.sink_parquet(file_path, **parquet_options)
    
    # Calculate metrics
    file_size = Path(file_path).stat().st_size
    
    return {
        "rows_written": "streaming_mode", 
        "file_size": file_size,
        "columns_written": len(data.collect_schema().names())
    }


def run(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute universal file output with lazy/streaming approach.
    
    Args:
        params: Dictionary containing data, output_path and optional parameters
        
    Returns:
        ModResult dictionary with write metrics and status
    """
    # Extract mod context
    mod_name = params.get("_mod_name", "file_output")
    mod_type = params.get("_mod_type", "file_output") 
    
    # Setup logging with mod context
    logger = setup_logger(__name__, mod_type, mod_name)
    
    # Initialize result
    result = ModResult(mod_type, mod_name)
    
    try:
        # Extract required parameters
        data = params["data"]
        output_path = params["output_path"]
        
        logger.info(f"Starting file output", extra={
            "output_path": output_path
        })
        
        # Validate input data is lazy DataFrame
        if not isinstance(data, pl.LazyFrame):
            error_msg = f"Input data must be a Polars LazyFrame, got {type(data)}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        # Detect file format
        file_format = _detect_file_format(output_path)
        output_file = Path(output_path)
        
        # Create parent directories if needed
        if not output_file.parent.exists():
            output_file.parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created directories: {output_file.parent}")
        
        # Handle append mode with schema validation
        append_mode = params.get("append_mode", False)
        if append_mode and output_file.exists():
            if not _validate_schema_for_append(str(output_file), data, logger):
                error_msg = "Schema validation failed for append mode - cannot append data with incompatible schema"
                logger.error(error_msg)
                result.add_error(error_msg)
                return result.error()
        
        # Use atomic writes with temporary file
        temp_file = None
        try:
            if append_mode and output_file.exists():
                # For append mode, write directly (schema already validated)
                target_path = str(output_file)
                write_mode = "append"
            else:
                # For new files, use atomic write via temporary file
                temp_file = str(output_file) + ".tmp"
                target_path = temp_file
                write_mode = "overwrite"
            
            # Write file based on format using streaming approach
            if file_format == 'csv':
                write_metrics = _write_csv_streaming(data, target_path, params, logger)
            else:  # parquet
                if append_mode:
                    logger.warning("Append mode not supported for Parquet format - will overwrite")
                    write_mode = "overwrite"
                write_metrics = _write_parquet_streaming(data, target_path, params, logger)
            
            # Complete atomic write if using temporary file
            if temp_file and Path(temp_file).exists():
                shutil.move(temp_file, str(output_file))
                logger.info("Atomic write completed successfully")
            
        except Exception as write_error:
            # Cleanup temporary file on error
            if temp_file and Path(temp_file).exists():
                try:
                    Path(temp_file).unlink()
                    logger.info("Cleaned up temporary file after write failure")
                except:
                    pass
            raise write_error
        
        logger.info(f"File write successful", extra={
            "file_format": file_format,
            "file_size": write_metrics["file_size"],
            "write_mode": write_mode
        })
        
        # Add metrics
        result.add_metric("rows_written", write_metrics["rows_written"])
        result.add_metric("file_size_bytes", write_metrics["file_size"]) 
        result.add_metric("columns_written", write_metrics["columns_written"])
        result.add_metric("file_format", file_format)
        result.add_metric("write_mode", write_mode)
        result.add_metric("processing_mode", "lazy_streaming")
        result.add_metric("memory_efficient", True)
        
        # Add artifacts
        result.add_artifact("output_path", str(output_file))
        result.add_artifact("file_format", file_format)
        result.add_artifact("write_metrics", write_metrics)
        
        # Add globals for downstream mods
        result.add_global("rows_written", write_metrics["rows_written"])
        result.add_global("file_size", write_metrics["file_size"])
        result.add_global("file_format", file_format)
        result.add_global("write_mode", write_mode)
        
        return result.success()
        
    except KeyError as e:
        error_msg = f"Missing required parameter: {e}"
        logger.error(error_msg)
        result.add_error(error_msg)
        return result.error()
        
    except PermissionError as e:
        error_msg = f"Permission denied writing to file: {str(e)}"
        logger.error(error_msg)
        result.add_error(error_msg)
        return result.error()
        
    except Exception as e:
        error_msg = f"Unexpected error in file output: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result.add_error(error_msg)
        return result.error()