"""
CSV Reader - Source mod for reading CSV files with validation.

Realistic example of a DataPy source mod that reads CSV files,
performs basic validation, and outputs clean data artifacts.
"""

from datapy.mod_manager.result import ModResult


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
        # Get parameters with defaults
        file_path = params["file_path"]
        encoding = params.get("encoding", "utf-8")
        delimiter = params.get("delimiter", ",")
        
        # Validate file exists
        if not os.path.exists(file_path):
            result.add_error(f"File not found: {file_path}")
            return result.error(20)  # VALIDATION_ERROR
        
        # Read CSV file
        df = pd.read_csv(file_path, encoding=encoding, delimiter=delimiter)
        
        # Basic validation
        if df.empty:
            result.add_warning("CSV file is empty")
        
        # Check for missing data
        missing_count = df.isnull().sum().sum()
        if missing_count > 0:
            result.add_warning(f"Found {missing_count} missing values")
        
        # Add metrics
        result.add_metric("rows_read", len(df))
        result.add_metric("columns_read", len(df.columns))
        result.add_metric("file_size_bytes", Path(file_path).stat().st_size)
        result.add_metric("missing_values", int(missing_count))
        
        # Add artifacts
        result.add_artifact("data", df)
        result.add_artifact("source_file", file_path)
        result.add_artifact("column_names", list(df.columns))
        
        # Add globals for downstream mods
        result.add_global("row_count", len(df))
        result.add_global("source_encoding", encoding)
        
        # Return success or warning based on data quality
        if missing_count > 0:
            return result.warning()
        else:
            return result.success()
        
    except FileNotFoundError as e:
        result.add_error(f"File not found: {e}")
        return result.error(20)  # VALIDATION_ERROR
    except pd.errors.EmptyDataError:
        result.add_error("CSV file is empty or invalid")
        return result.error(20)  # VALIDATION_ERROR
    except UnicodeDecodeError as e:
        result.add_error(f"Encoding error: {e}. Try a different encoding.")
        return result.error(20)  # VALIDATION_ERROR
    except Exception as e:
        result.add_error(f"CSV reading failed: {e}")
        return result.error(30)  # RUNTIME_ERROR