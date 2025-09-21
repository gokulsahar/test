"""
Optimized CSV Reader Mod for DataPy Framework.

Reads CSV, Parquet, and Arrow files with support for both pandas and Polars engines.
Includes performance optimizations, streaming capabilities, automatic encoding detection,
and comprehensive error handling.
"""

import pandas as pd
import os
import chardet
from pathlib import Path
from typing import Dict, Any, Union, Optional

from datapy.mod_manager.result import ModResult
from datapy.mod_manager.base import ModMetadata, ConfigSchema
from datapy.mod_manager.logger import setup_logger

# Required metadata
METADATA = ModMetadata(
    type="csv_reader",
    version="2.0.0", 
    description="Optimized reader for CSV, Parquet, and Arrow files with pandas/Polars engine support",
    category="source",
    input_ports=[],
    output_ports=["data"],
    globals=["row_count", "column_count", "file_size", "engine_used"],
    packages=["pandas>=1.5.0", "chardet>=5.0.0", "polars>=0.20.0"]
)

# Parameter schema
CONFIG_SCHEMA = ConfigSchema(
    required={
        "file_path": {
            "type": "str",
            "description": "Path to file to read (CSV, Parquet, or Arrow format)"
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
            "default": "auto",
            "description": "File encoding (auto, utf-8, latin-1, etc.) - auto uses detection"
        },
        "delimiter": {
            "type": "str",
            "default": ",", 
            "description": "CSV delimiter character"
        },
        "header": {
            "type": "str",
            "default": "infer",
            "description": "Row for column headers: 'infer', 'first_row', 'none', or row number"
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
        },
        "streaming": {
            "type": "bool",
            "default": False,
            "description": "Enable streaming/lazy evaluation for large files (Polars only)"
        },
        "chunk_size": {
            "type": "int",
            "default": None,
            "description": "Process file in chunks of specified size (pandas only)"
        },
        "dtype_inference": {
            "type": "bool",
            "default": True,
            "description": "Enable automatic data type inference (False reads all as strings)"
        },
        "validate_data": {
            "type": "bool",
            "default": False,
            "description": "Perform basic data validation and profiling"
        },
        "memory_map": {
            "type": "bool",
            "default": False,
            "description": "Use memory mapping for large files (when supported)"
        }
    }
)


def _detect_encoding(file_path: str, sample_size: int = 10000) -> str:
    """
    Detect file encoding using chardet library.
    
    Args:
        file_path: Path to file
        sample_size: Number of bytes to sample for detection
        
    Returns:
        Detected encoding string
    """
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(sample_size)
            result = chardet.detect(raw_data)
            encoding = result.get('encoding', 'utf-8')
            confidence = result.get('confidence', 0)
            
            # Fallback to utf-8 if confidence is too low
            if confidence < 0.7:
                encoding = 'utf-8'
                
            return encoding
            
    except Exception:
        return 'utf-8'


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
        '.parquet': 'parquet',
        '.pq': 'parquet',
        '.arrow': 'arrow',
        '.feather': 'arrow'
    }
    
    return format_map.get(suffix, 'csv')


def _process_header_param(header: str) -> Union[int, None, str]:
    """
    Process header parameter into appropriate format for engines.
    
    Args:
        header: Header parameter string
        
    Returns:
        Processed header value
    """
    if header == "infer":
        return 0  # Default to first row
    elif header == "first_row":
        return 0
    elif header == "none":
        return None
    else:
        try:
            return int(header)
        except (ValueError, TypeError):
            return 0


def _read_with_pandas(file_path: str, file_format: str, params: Dict[str, Any], logger) -> pd.DataFrame:
    """
    Read file using pandas engine.
    
    Args:
        file_path: Path to file
        file_format: File format (csv, parquet, arrow)
        params: Processing parameters
        logger: Logger instance
        
    Returns:
        pandas DataFrame
    """
    encoding = params.get("encoding", "auto")
    delimiter = params.get("delimiter", ",")
    header = _process_header_param(params.get("header", "infer"))
    skip_rows = params.get("skip_rows", 0)
    max_rows = params.get("max_rows", None)
    chunk_size = params.get("chunk_size", None)
    dtype_inference = params.get("dtype_inference", True)
    memory_map = params.get("memory_map", False)
    
    # Handle encoding detection
    if encoding == "auto":
        encoding = _detect_encoding(file_path)
        logger.info(f"Auto-detected encoding: {encoding}")
    
    # Prepare common parameters
    read_params = {
        "skiprows": skip_rows,
        "nrows": max_rows if max_rows and max_rows > 0 else None,
        "header": header
    }
    
    if not dtype_inference:
        read_params["dtype"] = str
    
    try:
        if file_format == "csv":
            read_params.update({
                "encoding": encoding,
                "delimiter": delimiter,
                "chunksize": chunk_size,
                "memory_map": memory_map
            })
            
            if chunk_size:
                # Handle chunked reading
                chunks = []
                chunk_reader = pd.read_csv(file_path, **read_params)
                for chunk in chunk_reader:
                    chunks.append(chunk)
                df = pd.concat(chunks, ignore_index=True)
            else:
                df = pd.read_csv(file_path, **read_params)
                
        elif file_format == "parquet":
            df = pd.read_parquet(file_path)
            # Apply post-read filtering for parquet
            if skip_rows > 0:
                df = df.iloc[skip_rows:]
            if max_rows and max_rows > 0:
                df = df.head(max_rows)
                
        elif file_format == "arrow":
            df = pd.read_feather(file_path)
            # Apply post-read filtering for arrow
            if skip_rows > 0:
                df = df.iloc[skip_rows:]
            if max_rows and max_rows > 0:
                df = df.head(max_rows)
                
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
            
        return df
        
    except UnicodeDecodeError as e:
        logger.warning(f"Encoding error with {encoding}, trying utf-8-sig")
        if file_format == "csv":
            read_params["encoding"] = "utf-8-sig"
            return pd.read_csv(file_path, **read_params)
        else:
            raise e


def _read_with_polars(file_path: str, file_format: str, params: Dict[str, Any], logger):
    """
    Read file using Polars engine.
    
    Args:
        file_path: Path to file
        file_format: File format (csv, parquet, arrow)
        params: Processing parameters
        logger: Logger instance
        
    Returns:
        Polars DataFrame or LazyFrame
    """
    try:
        import polars as pl
    except ImportError:
        raise ImportError(
            "Polars engine requested but polars is not installed. "
            "Install with: pip install polars>=0.20.0"
        )
    
    encoding = params.get("encoding", "auto")
    delimiter = params.get("delimiter", ",")
    header = _process_header_param(params.get("header", "infer"))
    skip_rows = params.get("skip_rows", 0)
    max_rows = params.get("max_rows", None)
    streaming = params.get("streaming", False)
    dtype_inference = params.get("dtype_inference", True)
    
    # Handle encoding detection for CSV
    if file_format == "csv" and encoding == "auto":
        encoding = _detect_encoding(file_path)
        logger.info(f"Auto-detected encoding: {encoding}")
    
    try:
        if file_format == "csv":
            read_params = {
                "separator": delimiter,
                "skip_rows": skip_rows,
                "n_rows": max_rows if max_rows and max_rows > 0 else None,
                "has_header": header is not None,
                "encoding": encoding if encoding != "auto" else "utf8",
                "infer_schema": dtype_inference
            }
            
            if streaming:
                df = pl.scan_csv(file_path, **read_params)
            else:
                df = pl.read_csv(file_path, **read_params)
                
        elif file_format == "parquet":
            if streaming:
                df = pl.scan_parquet(file_path)
                if skip_rows > 0:
                    df = df.slice(skip_rows)
                if max_rows and max_rows > 0:
                    df = df.head(max_rows)
            else:
                df = pl.read_parquet(file_path)
                if skip_rows > 0:
                    df = df.slice(skip_rows)
                if max_rows and max_rows > 0:
                    df = df.head(max_rows)
                    
        elif file_format == "arrow":
            # Polars reads IPC/Arrow files with read_ipc
            df = pl.read_ipc(file_path)
            if skip_rows > 0:
                df = df.slice(skip_rows)
            if max_rows and max_rows > 0:
                df = df.head(max_rows)
                
        else:
            raise ValueError(f"Unsupported file format for Polars: {file_format}")
            
        return df
        
    except Exception as e:
        if "encoding" in str(e).lower() and file_format == "csv":
            logger.warning(f"Encoding error with {encoding}, trying utf-8")
            read_params["encoding"] = "utf8"
            if streaming:
                return pl.scan_csv(file_path, **read_params)
            else:
                return pl.read_csv(file_path, **read_params)
        else:
            raise e


def _validate_data(df, engine: str, logger) -> Dict[str, Any]:
    """
    Perform basic data validation and profiling.
    
    Args:
        df: DataFrame to validate
        engine: Engine used ('pandas' or 'polars')
        logger: Logger instance
        
    Returns:
        Dictionary with validation results
    """
    validation_results = {}
    
    try:
        if engine == "pandas":
            validation_results.update({
                "null_count": df.isnull().sum().sum(),
                "duplicate_rows": df.duplicated().sum(),
                "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024,
                "numeric_columns": df.select_dtypes(include=['number']).columns.tolist(),
                "categorical_columns": df.select_dtypes(include=['category']).columns.tolist(),
                "object_columns": df.select_dtypes(include=['object']).columns.tolist()
            })
            
        elif engine == "polars":
            # For polars LazyFrame, collect basic info without full computation
            if hasattr(df, 'collect'):  # LazyFrame
                schema_info = df.schema
                validation_results.update({
                    "columns": list(schema_info.keys()),
                    "dtypes": {col: str(dtype) for col, dtype in schema_info.items()},
                    "is_lazy": True
                })
            else:  # DataFrame
                validation_results.update({
                    "null_count": df.null_count().sum(axis=1)[0],
                    "shape": df.shape,
                    "memory_usage_mb": df.estimated_size() / 1024 / 1024 if hasattr(df, 'estimated_size') else None,
                    "columns": df.columns,
                    "dtypes": {col: str(dtype) for col, dtype in df.schema.items()},
                    "is_lazy": False
                })
                
        logger.info(f"Data validation completed: {validation_results}")
        
    except Exception as e:
        logger.warning(f"Data validation failed: {e}")
        validation_results["validation_error"] = str(e)
    
    return validation_results


def run(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute optimized file reader with given parameters.
    
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
        engine = params.get("engine", "pandas").lower()
        validate_data_flag = params.get("validate_data", False)
        
        logger.info(f"Starting optimized file read", extra={
            "file_path": file_path,
            "engine": engine,
            "validation": validate_data_flag
        })
        
        # Validate engine
        if engine not in ["pandas", "polars"]:
            error_msg = f"Invalid engine '{engine}'. Must be 'pandas' or 'polars'"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
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
        
        # Get file size and format
        file_size = file_obj.stat().st_size
        file_format = _determine_file_format(file_path)
        logger.info(f"File info - Size: {file_size} bytes, Format: {file_format}")
        
        # Read file based on engine
        if engine == "pandas":
            df = _read_with_pandas(file_path, file_format, params, logger)
        else:  # polars
            df = _read_with_polars(file_path, file_format, params, logger)
        
        # Validate we got data
        if engine == "pandas":
            if df.empty:
                warning_msg = "File is empty or contains no data rows"
                logger.warning(warning_msg)
                result.add_warning(warning_msg)
                return result.warning()
            row_count = len(df)
            column_count = len(df.columns)
        else:  # polars
            if hasattr(df, 'collect'):  # LazyFrame
                # For lazy frames, we can't easily check if empty without collecting
                row_count = "lazy_evaluation"
                column_count = len(df.schema)
            else:  # DataFrame
                if df.height == 0:
                    warning_msg = "File is empty or contains no data rows"
                    logger.warning(warning_msg)
                    result.add_warning(warning_msg)
                    return result.warning()
                row_count = df.height
                column_count = df.width
        
        # Perform data validation if requested
        validation_results = {}
        if validate_data_flag:
            validation_results = _validate_data(df, engine, logger)
        
        logger.info(f"File read successful", extra={
            "rows": row_count,
            "columns": column_count,
            "engine_used": engine,
            "file_format": file_format
        })
        
        # Add metrics
        result.add_metric("rows_read", row_count)
        result.add_metric("columns_read", column_count)
        result.add_metric("file_size_bytes", file_size)
        result.add_metric("engine_used", engine)
        result.add_metric("file_format", file_format)
        result.add_metric("streaming_enabled", params.get("streaming", False))
        
        if validation_results:
            result.add_metric("validation_results", validation_results)
        
        # Add artifacts
        result.add_artifact("data", df)
        result.add_artifact("file_path", str(file_path))
        result.add_artifact("engine_info", {
            "engine": engine,
            "file_format": file_format,
            "is_lazy": hasattr(df, 'collect') if engine == "polars" else False
        })
        
        # Add globals for downstream mods
        result.add_global("row_count", row_count)
        result.add_global("column_count", column_count)
        result.add_global("file_size", file_size)
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
        
    except Exception as e:
        error_msg = f"Unexpected error in optimized reader: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result.add_error(error_msg)
        return result.error()