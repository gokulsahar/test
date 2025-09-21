"""
ETL-Focused File Reader Mod for DataPy Framework.

Reads CSV, Parquet, and Arrow files with ETL-optimized features including
footer handling, CSV options, data transformations, and performance optimizations
for production data pipelines.
"""

import pandas as pd
import os
import chardet
import zipfile
from pathlib import Path
from typing import Dict, Any, Union, Optional, List

from datapy.mod_manager.result import ModResult
from datapy.mod_manager.base import ModMetadata, ConfigSchema
from datapy.mod_manager.logger import setup_logger

# Required metadata
METADATA = ModMetadata(
    type="file_reader",
    version="1.0.0", 
    description="ETL-focused reader for CSV, Parquet, and Arrow files with comprehensive data pipeline features",
    category="source",
    input_ports=[],
    output_ports=["data"],
    globals=["row_count", "column_count", "file_size", "engine_used"],
    packages=["pandas>=1.5.0", "chardet>=5.0.0", "polars>=0.20.0"]
)

# Parameter schema - ETL focused with Talend-inspired features
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
            "description": "Field separator character"
        },
        "row_separator": {
            "type": "str",
            "default": "\n",
            "description": "Line ending character (\\n, \\r\\n, \\r)"
        },
        "header_rows": {
            "type": "int",
            "default": 1,
            "description": "Number of header rows (0=no headers, 1=single header)"
        },
        "footer_rows": {
            "type": "int",
            "default": 0,
            "description": "Number of footer rows to skip from end of file"
        },
        "skip_rows": {
            "type": "int",
            "default": 0,
            "description": "Number of rows to skip at start of file (before headers)"
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
        "skip_empty_rows": {
            "type": "bool",
            "default": True,
            "description": "Skip blank/empty rows during processing"
        },
        "column_mapping": {
            "type": "dict",
            "default": {},
            "description": "Dictionary to rename columns: {'old_name': 'new_name'}"
        },
        "date_columns": {
            "type": "list",
            "default": [],
            "description": "List of columns to parse as dates"
        },
        "numeric_columns": {
            "type": "list",
            "default": [],
            "description": "List of columns to force as numeric types"
        },
        "trim_columns": {
            "type": "str",
            "default": "none",
            "description": "Trim whitespace: 'none', 'all', or 'auto' (trim if detected)"
        },
        "streaming": {
            "type": "bool",
            "default": False,
            "description": "Enable streaming/lazy evaluation for large files (Polars only)"
        }
    }
)


def _detect_encoding(file_path: str, sample_size: int = 50000) -> str:
    """
    Detect file encoding using chardet library with ETL-optimized sampling.
    
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
            
            # Higher confidence threshold for ETL reliability
            if confidence < 0.8:
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
        '.dat': 'csv',
        '.parquet': 'parquet',
        '.pq': 'parquet',
        '.arrow': 'arrow',
        '.feather': 'arrow'
    }
    
    return format_map.get(suffix, 'csv')


def _handle_footer_rows(df, footer_rows: int, engine: str):
    """
    Remove footer rows from the DataFrame.
    
    Args:
        df: DataFrame to process
        footer_rows: Number of rows to remove from end
        engine: Processing engine used
        
    Returns:
        DataFrame with footer rows removed
    """
    if footer_rows <= 0:
        return df
    
    if engine == "pandas":
        return df.iloc[:-footer_rows] if len(df) > footer_rows else df.iloc[0:0]
    else:  # polars
        if hasattr(df, 'collect'):  # LazyFrame
            return df.slice(0, -footer_rows) if footer_rows > 0 else df
        else:  # DataFrame
            return df.slice(0, df.height - footer_rows) if df.height > footer_rows else df.clear()


def _apply_column_transformations(df, params: Dict[str, Any], engine: str, logger):
    """
    Apply ETL column transformations: mapping, date parsing, numeric conversion, trimming.
    
    Args:
        df: DataFrame to transform
        params: Transformation parameters
        engine: Processing engine
        logger: Logger instance
        
    Returns:
        Transformed DataFrame
    """
    column_mapping = params.get("column_mapping", {})
    date_columns = params.get("date_columns", [])
    numeric_columns = params.get("numeric_columns", [])
    trim_columns = params.get("trim_columns", "none")
    
    # Column renaming
    if column_mapping:
        if engine == "pandas":
            df = df.rename(columns=column_mapping)
        else:  # polars
            rename_map = {old: new for old, new in column_mapping.items() if old in df.columns}
            if rename_map:
                df = df.rename(rename_map)
        logger.info(f"Renamed columns: {column_mapping}")
    
    # Trimming whitespace
    if trim_columns in ["all", "auto"]:
        if engine == "pandas":
            # Trim all string/object columns
            string_cols = df.select_dtypes(include=['object']).columns
            if len(string_cols) > 0:
                df[string_cols] = df[string_cols].apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
                logger.info(f"Trimmed whitespace from {len(string_cols)} columns")
        else:  # polars
            import polars as pl
            string_cols = [col for col, dtype in df.schema.items() if dtype == pl.Utf8]
            if string_cols:
                if hasattr(df, 'collect'):  # LazyFrame
                    df = df.with_columns([pl.col(col).str.strip_chars() for col in string_cols])
                else:  # DataFrame
                    df = df.with_columns([pl.col(col).str.strip_chars() for col in string_cols])
                logger.info(f"Trimmed whitespace from {len(string_cols)} columns")
    
    # Date parsing
    if date_columns:
        if engine == "pandas":
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
        else:  # polars
            import polars as pl
            for col in date_columns:
                if col in df.columns:
                    if hasattr(df, 'collect'):  # LazyFrame
                        df = df.with_columns(pl.col(col).str.strptime(pl.Date, format=None, strict=False))
                    else:  # DataFrame
                        df = df.with_columns(pl.col(col).str.strptime(pl.Date, format=None, strict=False))
        logger.info(f"Parsed date columns: {date_columns}")
    
    # Numeric conversion
    if numeric_columns:
        if engine == "pandas":
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        else:  # polars
            import polars as pl
            for col in numeric_columns:
                if col in df.columns:
                    if hasattr(df, 'collect'):  # LazyFrame
                        df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
                    else:  # DataFrame
                        df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
        logger.info(f"Converted numeric columns: {numeric_columns}")
    
    return df


def _read_with_pandas(file_path: str, file_format: str, params: Dict[str, Any], logger) -> pd.DataFrame:
    """
    Read file using pandas engine with ETL optimizations.
    
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
    row_separator = params.get("row_separator", "\n")
    header_rows = params.get("header_rows", 1)
    skip_rows = params.get("skip_rows", 0)
    footer_rows = params.get("footer_rows", 0)
    csv_options = params.get("csv_options", True)
    text_enclosure = params.get("text_enclosure", "\"")
    escape_char = params.get("escape_char", "\\")
    skip_empty_rows = params.get("skip_empty_rows", True)
    
    # Handle encoding detection
    if encoding == "auto":
        encoding = _detect_encoding(file_path)
        logger.info(f"Auto-detected encoding: {encoding}")
    
    # Convert header_rows to pandas format
    if header_rows == 0:
        header = None
    elif header_rows == 1:
        header = 0
    else:
        header = list(range(header_rows))
    
    # Prepare common parameters - always memory efficient
    read_params = {
        "skiprows": skip_rows,
        "header": header,
        "low_memory": True,  # Always memory efficient
        "skip_blank_lines": skip_empty_rows
    }
    
    try:
        if file_format == "csv":
            read_params.update({
                "encoding": encoding,
                "sep": delimiter,
                "lineterminator": row_separator,
                "na_filter": True
            })
            
            # Add CSV-specific options
            if csv_options:
                read_params.update({
                    "quotechar": text_enclosure,
                    "escapechar": escape_char if escape_char != text_enclosure else None,
                    "quoting": 0,  # QUOTE_MINIMAL
                    "doublequote": True
                })
            
            # Handle footer by calculating rows to read
            if footer_rows > 0:
                with open(file_path, 'r', encoding=encoding) as f:
                    total_lines = sum(1 for _ in f)
                nrows = total_lines - skip_rows - footer_rows - (header_rows if header_rows > 0 else 0)
                if nrows > 0:
                    read_params["nrows"] = nrows
            
            df = pd.read_csv(file_path, **read_params)
                
        elif file_format == "parquet":
            df = pd.read_parquet(file_path)
            # Apply post-read filtering for parquet
            if skip_rows > 0:
                df = df.iloc[skip_rows:]
            if footer_rows > 0:
                df = _handle_footer_rows(df, footer_rows, "pandas")
                
        elif file_format == "arrow":
            df = pd.read_feather(file_path)
            # Apply post-read filtering for arrow
            if skip_rows > 0:
                df = df.iloc[skip_rows:]
            if footer_rows > 0:
                df = _handle_footer_rows(df, footer_rows, "pandas")
                
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
    Read file using Polars engine with ETL optimizations.
    
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
    header_rows = params.get("header_rows", 1)
    skip_rows = params.get("skip_rows", 0)
    footer_rows = params.get("footer_rows", 0)
    streaming = params.get("streaming", False)
    csv_options = params.get("csv_options", True)
    text_enclosure = params.get("text_enclosure", "\"")
    skip_empty_rows = params.get("skip_empty_rows", True)
    
    # Handle encoding detection for CSV
    if file_format == "csv" and encoding == "auto":
        encoding = _detect_encoding(file_path)
        logger.info(f"Auto-detected encoding: {encoding}")
    
    try:
        if file_format == "csv":
            read_params = {
                "separator": delimiter,
                "skip_rows": skip_rows,
                "has_header": header_rows > 0,
                "encoding": encoding if encoding != "auto" else "utf8",
                "infer_schema": True,  # Always infer for efficiency
                "ignore_errors": False,
                "skip_rows_after_header": 0
            }
            
            # Add CSV-specific options
            if csv_options:
                read_params.update({
                    "quote_char": text_enclosure,
                    "null_values": ["", "NULL", "null", "None"],
                })
            
            # Handle empty rows
            if skip_empty_rows:
                read_params["skip_rows_after_header"] = 0  # Polars handles this differently
            
            if streaming:
                df = pl.scan_csv(file_path, **read_params)
            else:
                df = pl.read_csv(file_path, **read_params)
                
            # Handle footer rows post-read
            if footer_rows > 0:
                df = _handle_footer_rows(df, footer_rows, "polars")
                
        elif file_format == "parquet":
            if streaming:
                df = pl.scan_parquet(file_path)
            else:
                df = pl.read_parquet(file_path)
            
            # Apply filtering
            if skip_rows > 0:
                if hasattr(df, 'collect'):  # LazyFrame
                    df = df.slice(skip_rows)
                else:  # DataFrame
                    df = df.slice(skip_rows)
            
            if footer_rows > 0:
                df = _handle_footer_rows(df, footer_rows, "polars")
                    
        elif file_format == "arrow":
            # Polars reads IPC/Arrow files with read_ipc
            df = pl.read_ipc(file_path)
            
            if skip_rows > 0:
                df = df.slice(skip_rows)
            if footer_rows > 0:
                df = _handle_footer_rows(df, footer_rows, "polars")
                
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


def run(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute ETL-focused file reader with given parameters.
    
    Args:
        params: Dictionary containing file_path and optional parameters
        
    Returns:
        ModResult dictionary with data, metrics, and status
    """
    # Extract mod context
    mod_name = params.get("_mod_name", "file_reader")
    mod_type = params.get("_mod_type", "file_reader")
    
    # Setup logging with mod context
    logger = setup_logger(__name__, mod_type, mod_name)
    
    # Initialize result
    result = ModResult(mod_type, mod_name)
    
    try:
        # Extract parameters
        file_path = params["file_path"]
        engine = params.get("engine", "pandas").lower()
        
        logger.info(f"Starting ETL file read", extra={
            "file_path": file_path,
            "engine": engine,
            "footer_rows": params.get("footer_rows", 0),
            "header_rows": params.get("header_rows", 1),
            "streaming": params.get("streaming", False)
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
        
        # Apply column transformations
        df = _apply_column_transformations(df, params, engine, logger)
        
        logger.info(f"ETL file read successful", extra={
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
        result.add_metric("footer_rows_skipped", params.get("footer_rows", 0))
        result.add_metric("header_rows_used", params.get("header_rows", 1))
        result.add_metric("transformations_applied", {
            "column_mapping": bool(params.get("column_mapping")),
            "date_parsing": bool(params.get("date_columns")),
            "numeric_conversion": bool(params.get("numeric_columns")),
            "whitespace_trimming": params.get("trim_columns", "none") != "none"
        })
        
        # Add artifacts
        result.add_artifact("data", df)
        result.add_artifact("file_path", str(file_path))
        result.add_artifact("etl_info", {
            "engine": engine,
            "file_format": file_format,
            "is_lazy": hasattr(df, 'collect') if engine == "polars" else False,
            "csv_options_used": params.get("csv_options", True),
            "encoding_detected": params.get("encoding", "auto") == "auto"
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
        error_msg = f"Unexpected error in ETL file reader: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result.add_error(error_msg)
        return result.error()