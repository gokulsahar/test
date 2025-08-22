"""
CSV Reader - Source mod for reading CSV files with validation.

Realistic example of a DataPy source mod that reads CSV files,
performs basic validation, and outputs clean data artifacts.
"""

from datapy.mod_manager.result import ModResult
from datapy.mod_manager.base import ModMetadata, ConfigSchema

# Complete metadata declaration
METADATA = ModMetadata(
    type="csv_reader",
    version="1.0.0",
    description="Reads data from CSV files with validation and format conversion",
    category="source",
    input_ports=[],
    output_ports=["data"],
    globals=["row_count", "file_size"],
    packages=["pandas>=1.5.0", "chardet>=4.0.0"],
    python_version=">=3.8"
)

# Parameter configuration schema
CONFIG_SCHEMA = ConfigSchema(
    required={
        "file_path": {
            "type": "str", 
            "description": "Path to CSV file"
        }
    },
    optional={
        "encoding": {
            "type": "str",
            "default": "utf-8",
            "description": "File encoding",
            "enum": ["utf-8", "latin-1", "cp1252"]
        },
        "delimiter": {
            "type": "str",
            "default": ",",
            "description": "CSV delimiter character"
        },
        "header_row": {
            "type": "int",
            "default": 0,
            "description": "Row number to use as column headers (0-indexed)"
        },
        "skip_rows": {
            "type": "int", 
            "default": 0,
            "description": "Number of rows to skip at start of file"
        }
    }
)


def run(params):
    """
    Read CSV file with validation and format conversion.
    
    Args:
        params: Dictionary with validated parameters from registry schema
        
    Returns:
        ModResult dictionary with data artifacts
    """
    import pandas as pd
    import os
    from pathlib import Path
    
    # Extract mod information
    mod_name = params.get("_mod_name", "csv_reader")
    result = ModResult("csv_reader", mod_name)
    
    try:
        # Get parameters with defaults (registry handles validation)
        file_path = params["file_path"]
        encoding = params.get("encoding", "utf-8")
        delimiter = params.get("delimiter", ",")
        header_row = params.get("header_row", 0)
        skip_rows = params.get("skip_rows", 0)
        
        # Validate file exists
        if not os.path.exists(file_path):
            result.add_error(f"File not found: {file_path}")
            return result.error(20)  # VALIDATION_ERROR
        
        # Read CSV file with parameters
        df = pd.read_csv(
            file_path, 
            encoding=encoding, 
            delimiter=delimiter,
            header=header_row,
            skiprows=skip_rows
        )
        
        # Basic validation
        if df.empty:
            result.add_warning("CSV file is empty")
        
        # Check for missing data
        missing_count = df.isnull().sum().sum()
        if missing_count > 0:
            result.add_warning(f"Found {missing_count} missing values")
        
        # Check for duplicate rows
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            result.add_warning(f"Found {duplicate_count} duplicate rows")
        
        # Add comprehensive metrics
        result.add_metric("rows_read", len(df))
        result.add_metric("columns_read", len(df.columns))
        result.add_metric("file_size_bytes", Path(file_path).stat().st_size)
        result.add_metric("missing_values", int(missing_count))
        result.add_metric("duplicate_rows", int(duplicate_count))
        result.add_metric("data_types", df.dtypes.astype(str).to_dict())
        
        # Add artifacts
        result.add_artifact("data", df)
        result.add_artifact("source_file", file_path)
        result.add_artifact("column_names", list(df.columns))
        result.add_artifact("file_info", {
            "path": file_path,
            "encoding": encoding,
            "delimiter": delimiter,
            "shape": df.shape
        })
        
        # Add globals for downstream mods
        result.add_global("row_count", len(df))
        result.add_global("file_size", Path(file_path).stat().st_size)
        result.add_global("source_encoding", encoding)
        
        # Return success or warning based on data quality
        if missing_count > 0 or duplicate_count > 0:
            return result.warning()
        else:
            return result.success()
        
    except FileNotFoundError as e:
        result.add_error(f"File not found: {e}")
        return result.error(20)  # VALIDATION_ERROR
    except pd.errors.EmptyDataError:
        result.add_error("CSV file is empty or invalid")
        return result.error(20)  # VALIDATION_ERROR
    except pd.errors.ParserError as e:
        result.add_error(f"CSV parsing error: {e}. Check delimiter or file format.")
        return result.error(20)  # VALIDATION_ERROR
    except UnicodeDecodeError as e:
        result.add_error(f"Encoding error: {e}. Try a different encoding.")
        return result.error(20)  # VALIDATION_ERROR
    except Exception as e:
        result.add_error(f"CSV reading failed: {e}")
        return result.error(30)  # RUNTIME_ERROR