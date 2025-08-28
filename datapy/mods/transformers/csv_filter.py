"""
CSV Filter Mod for DataPy Framework.

Filters CSV data based on column conditions with flexible filtering rules.
"""

import pandas as pd
from typing import Dict, Any

from datapy.mod_manager.result import ModResult
from datapy.mod_manager.base import ModMetadata, ConfigSchema
from datapy.mod_manager.logger import setup_logger

# Required metadata
METADATA = ModMetadata(
    type="csv_filter",
    version="1.0.0",
    description="Filters CSV data based on column conditions and criteria",
    category="transformer",
    input_ports=["data"],
    output_ports=["filtered_data"],
    globals=["filtered_rows", "original_rows", "filter_rate"],
    packages=["pandas>=1.5.0"],
    python_version=">=3.8"
)

# Parameter schema
CONFIG_SCHEMA = ConfigSchema(
    required={
        "data": {
            "type": "object",
            "description": "Input DataFrame to filter"
        },
        "filter_conditions": {
            "type": "dict",
            "description": "Dictionary of column filters {column: {operator: value}}"
        }
    },
    optional={
        "keep_columns": {
            "type": "list",
            "default": None,
            "description": "List of columns to keep (None = keep all)"
        },
        "drop_duplicates": {
            "type": "bool",
            "default": False,
            "description": "Remove duplicate rows after filtering"
        },
        "sort_by": {
            "type": "str",
            "default": None,
            "description": "Column name to sort results by"
        }
    }
)


def run(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute CSV filter with given parameters.
    
    Args:
        params: Dictionary containing data and filter conditions
        
    Returns:
        ModResult dictionary with filtered data, metrics, and status
    """
    # Extract mod context
    mod_name = params.get("_mod_name", "csv_filter")
    mod_type = params.get("_mod_type", "csv_filter")
    
    # Setup logging with mod context
    logger = setup_logger(__name__, mod_type, mod_name)
    
    # Initialize result
    result = ModResult(mod_type, mod_name)
    
    try:
        # Extract parameters
        data = params["data"]
        filter_conditions = params["filter_conditions"]
        keep_columns = params.get("keep_columns", None)
        drop_duplicates = params.get("drop_duplicates", False)
        sort_by = params.get("sort_by", None)
        
        logger.info(f"Starting CSV filter", extra={
            "input_rows": len(data),
            "input_columns": len(data.columns),
            "filter_conditions": filter_conditions
        })
        
        # Validate input data
        if not isinstance(data, pd.DataFrame):
            error_msg = f"Input data must be a pandas DataFrame, got {type(data)}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        if data.empty:
            warning_msg = "Input data is empty"
            logger.warning(warning_msg)
            result.add_warning(warning_msg)
            result.add_artifact("filtered_data", data.copy())
            result.add_global("filtered_rows", 0)
            result.add_global("original_rows", 0)
            result.add_global("filter_rate", 0.0)
            return result.warning()
        
        # Start with copy of original data
        filtered_data = data.copy()
        original_rows = len(filtered_data)
        
        # Apply filter conditions
        for column, condition in filter_conditions.items():
            if column not in filtered_data.columns:
                warning_msg = f"Filter column '{column}' not found in data, skipping"
                logger.warning(warning_msg)
                result.add_warning(warning_msg)
                continue
            
            # Apply different operators
            for operator, value in condition.items():
                if operator == "eq":  # equals
                    filtered_data = filtered_data[filtered_data[column] == value]
                elif operator == "ne":  # not equals
                    filtered_data = filtered_data[filtered_data[column] != value]
                elif operator == "gt":  # greater than
                    filtered_data = filtered_data[pd.to_numeric(filtered_data[column], errors='coerce') > value]
                elif operator == "lt":  # less than
                    filtered_data = filtered_data[pd.to_numeric(filtered_data[column], errors='coerce') < value]
                elif operator == "gte":  # greater than or equal
                    filtered_data = filtered_data[pd.to_numeric(filtered_data[column], errors='coerce') >= value]
                elif operator == "lte":  # less than or equal
                    filtered_data = filtered_data[pd.to_numeric(filtered_data[column], errors='coerce') <= value]
                elif operator == "contains":  # string contains
                    filtered_data = filtered_data[filtered_data[column].astype(str).str.contains(str(value), na=False)]
                elif operator == "in":  # value in list
                    if isinstance(value, list):
                        filtered_data = filtered_data[filtered_data[column].isin(value)]
                    else:
                        logger.warning(f"'in' operator requires list value, got {type(value)}")
                else:
                    warning_msg = f"Unknown operator '{operator}' for column '{column}', skipping"
                    logger.warning(warning_msg)
                    result.add_warning(warning_msg)
                
                logger.info(f"Applied filter: {column} {operator} {value}, remaining rows: {len(filtered_data)}")
        
        # Keep only specified columns if requested
        if keep_columns:
            available_columns = [col for col in keep_columns if col in filtered_data.columns]
            missing_columns = [col for col in keep_columns if col not in filtered_data.columns]
            
            if missing_columns:
                warning_msg = f"Requested columns not found: {missing_columns}"
                logger.warning(warning_msg)
                result.add_warning(warning_msg)
            
            if available_columns:
                filtered_data = filtered_data[available_columns]
                logger.info(f"Kept columns: {available_columns}")
        
        # Remove duplicates if requested
        if drop_duplicates:
            before_dedup = len(filtered_data)
            filtered_data = filtered_data.drop_duplicates()
            after_dedup = len(filtered_data)
            if before_dedup != after_dedup:
                logger.info(f"Removed {before_dedup - after_dedup} duplicate rows")
        
        # Sort if requested
        if sort_by and sort_by in filtered_data.columns:
            try:
                filtered_data = filtered_data.sort_values(by=sort_by)
                logger.info(f"Sorted by column: {sort_by}")
            except Exception as e:
                warning_msg = f"Failed to sort by '{sort_by}': {e}"
                logger.warning(warning_msg)
                result.add_warning(warning_msg)
        
        # Calculate metrics
        filtered_rows = len(filtered_data)
        filter_rate = (original_rows - filtered_rows) / original_rows if original_rows > 0 else 0.0
        
        logger.info(f"CSV filter completed", extra={
            "original_rows": original_rows,
            "filtered_rows": filtered_rows,
            "filter_rate": f"{filter_rate:.2%}",
            "final_columns": len(filtered_data.columns)
        })
        
        # Add metrics
        result.add_metric("original_rows", original_rows)
        result.add_metric("filtered_rows", filtered_rows)
        result.add_metric("rows_removed", original_rows - filtered_rows)
        result.add_metric("filter_rate", round(filter_rate, 4))
        result.add_metric("final_columns", len(filtered_data.columns))
        
        # Add artifacts
        result.add_artifact("filtered_data", filtered_data)
        result.add_artifact("filter_conditions_applied", filter_conditions)
        
        # Add globals for downstream mods
        result.add_global("filtered_rows", filtered_rows)
        result.add_global("original_rows", original_rows)
        result.add_global("filter_rate", round(filter_rate, 4))
        
        return result.success()
        
    except KeyError as e:
        error_msg = f"Missing required parameter: {e}"
        logger.error(error_msg)
        result.add_error(error_msg)
        return result.error()
        
    except Exception as e:
        error_msg = f"Unexpected error in CSV filter: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result.add_error(error_msg)
        return result.error()