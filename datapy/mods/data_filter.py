"""
Data Filter Mod for DataPy Framework.

Advanced data filtering with custom expressions, standard operators, and dual engine support.
Supports both pandas and polars with production-grade error handling and performance optimization.
"""

import pandas as pd
import polars as pl
from typing import Dict, Any, Union, List
from pathlib import Path

from datapy.mod_manager.result import ModResult
from datapy.mod_manager.base import ModMetadata, ConfigSchema
from datapy.mod_manager.logger import setup_logger
from datapy.utils.expression_evaluator import get_expression_evaluator

# Required metadata
METADATA = ModMetadata(
    type="data_filter",
    version="1.0.0",
    description="Advanced data filtering with custom expressions and standard operators supporting pandas/polars",
    category="transformer",
    input_ports=["data"],
    output_ports=["filtered_data"],
    globals=["filtered_rows", "original_rows", "filter_rate"],
    packages=["pandas>=1.5.0", "polars>=0.20.0"]
)

# Parameter schema
CONFIG_SCHEMA = ConfigSchema(
    required={
        "data": {
            "type": "object",
            "description": "Input DataFrame to filter (pandas or polars)"
        },
        "filter_conditions": {
            "type": "dict",
            "description": "Filter configuration with operators and/or custom expressions"
        }
    },
    optional={
        "engine": {
            "type": "str",
            "default": "pandas",
            "description": "Processing engine: 'pandas' or 'polars'",
            "enum": ["pandas", "polars"]
        },
        "custom_functions": {
            "type": "dict",
            "default": {},
            "description": "Custom functions for expressions {name: function_or_import_path}"
        },
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
        },
        "ascending": {
            "type": "bool",
            "default": True,
            "description": "Sort direction when sort_by is specified"
        }
    }
)


def run(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute data filter with given parameters.
    
    Supports multiple filter types:
    1. Standard operators: eq, ne, gt, lt, gte, lte, contains, in, between
    2. Custom expressions with registered functions
    3. Mixed filtering approaches
    
    Args:
        params: Dictionary containing data and filter conditions
        
    Returns:
        ModResult dictionary with filtered data, metrics, and status
    """
    # Extract mod context
    mod_name = params.get("_mod_name", "data_filter")
    mod_type = params.get("_mod_type", "data_filter")
    
    # Setup logging with mod context
    logger = setup_logger(__name__, mod_type, mod_name)
    
    # Initialize result
    result = ModResult(mod_type, mod_name)
    
    try:
        # Extract parameters
        data = params["data"]
        filter_conditions = params["filter_conditions"]
        engine = params.get("engine", "pandas")
        custom_functions = params.get("custom_functions", {})
        keep_columns = params.get("keep_columns", None)
        drop_duplicates = params.get("drop_duplicates", False)
        sort_by = params.get("sort_by", None)
        ascending = params.get("ascending", True)
        
        logger.info(f"Starting data filter", extra={
            "input_rows": _get_row_count(data),
            "input_columns": _get_column_count(data),
            "engine": engine,
            "custom_functions_count": len(custom_functions),
            "filter_types": list(filter_conditions.keys())
        })
        
        # Validate engine
        if engine not in ["pandas", "polars"]:
            error_msg = f"Unsupported engine: {engine}. Must be 'pandas' or 'polars'"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        # Validate input data
        if not _is_valid_dataframe(data, engine):
            error_msg = f"Invalid input data type for {engine} engine: {type(data)}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        # Handle empty data
        if _is_empty_dataframe(data, engine):
            warning_msg = "Input data is empty"
            logger.warning(warning_msg)
            result.add_warning(warning_msg)
            result.add_artifact("filtered_data", data)
            result.add_global("filtered_rows", 0)
            result.add_global("original_rows", 0)
            result.add_global("filter_rate", 0.0)
            return result.warning()
        
        # Store original metrics
        original_rows = _get_row_count(data)
        
        # Apply filters based on engine
        if engine == "pandas":
            filtered_data = _filter_with_pandas(data, filter_conditions, custom_functions, logger)
        else:  # polars
            filtered_data = _filter_with_polars(data, filter_conditions, custom_functions, logger)
        
        # Apply post-processing
        filtered_data = _apply_post_processing(
            filtered_data, keep_columns, drop_duplicates, sort_by, ascending, engine, logger, result
        )
        
        # Calculate metrics
        filtered_rows = _get_row_count(filtered_data)
        filter_rate = (original_rows - filtered_rows) / original_rows if original_rows > 0 else 0.0
        
        logger.info(f"Data filter completed", extra={
            "original_rows": original_rows,
            "filtered_rows": filtered_rows,
            "filter_rate": f"{filter_rate:.2%}",
            "final_columns": _get_column_count(filtered_data),
            "engine": engine
        })
        
        # Add metrics
        result.add_metric("original_rows", original_rows)
        result.add_metric("filtered_rows", filtered_rows)
        result.add_metric("rows_removed", original_rows - filtered_rows)
        result.add_metric("filter_rate", round(filter_rate, 4))
        result.add_metric("final_columns", _get_column_count(filtered_data))
        result.add_metric("engine_used", engine)
        
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
        error_msg = f"Unexpected error in data filter: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result.add_error(error_msg)
        return result.error()


def _filter_with_pandas(data: pd.DataFrame, filter_conditions: Dict[str, Any], 
                       custom_functions: Dict[str, Any], logger) -> pd.DataFrame:
    """Apply filters using pandas engine."""
    filtered_data = data.copy()
    evaluator = get_expression_evaluator(logger)
    
    # Register custom functions if provided
    if custom_functions:
        evaluator.register_functions(custom_functions)
        logger.debug(f"Registered {len(custom_functions)} custom functions for pandas filtering")
    
    # Apply standard operator filters
    operator_filters = filter_conditions.get("operators", {})
    for column, conditions in operator_filters.items():
        filtered_data = _apply_operator_filters_pandas(filtered_data, column, conditions, logger)
    
    # Apply custom expression filters
    custom_expressions = filter_conditions.get("custom_expressions", [])
    for expression in custom_expressions:
        try:
            filtered_data = evaluator.evaluate_filter(filtered_data, expression, "pandas")
            remaining_rows = len(filtered_data)
            logger.info(f"Applied custom expression filter", extra={
                "expression": expression[:50] + "..." if len(expression) > 50 else expression,
                "remaining_rows": remaining_rows
            })
        except Exception as e:
            error_msg = f"Failed to apply custom expression '{expression}': {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e
    
    return filtered_data


def _filter_with_polars(data: Union[pl.DataFrame, pl.LazyFrame], filter_conditions: Dict[str, Any], 
                       custom_functions: Dict[str, Any], logger) -> Union[pl.DataFrame, pl.LazyFrame]:
    """Apply filters using polars engine."""
    filtered_data = data.clone() if hasattr(data, 'clone') else data
    evaluator = get_expression_evaluator(logger)
    
    # Register custom functions if provided
    if custom_functions:
        evaluator.register_functions(custom_functions)
        logger.debug(f"Registered {len(custom_functions)} custom functions for polars filtering")
    
    # Apply standard operator filters
    operator_filters = filter_conditions.get("operators", {})
    for column, conditions in operator_filters.items():
        filtered_data = _apply_operator_filters_polars(filtered_data, column, conditions, logger)
    
    # Apply custom expression filters
    custom_expressions = filter_conditions.get("custom_expressions", [])
    for expression in custom_expressions:
        try:
            filtered_data = evaluator.evaluate_filter(filtered_data, expression, "polars")
            remaining_rows = _get_row_count(filtered_data)
            logger.info(f"Applied custom expression filter", extra={
                "expression": expression[:50] + "..." if len(expression) > 50 else expression,
                "remaining_rows": remaining_rows
            })
        except Exception as e:
            error_msg = f"Failed to apply custom expression '{expression}': {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e
    
    return filtered_data


def _apply_operator_filters_pandas(data: pd.DataFrame, column: str, 
                                  conditions: Dict[str, Any], logger) -> pd.DataFrame:
    """Apply standard operator filters for pandas."""
    if column not in data.columns:
        warning_msg = f"Filter column '{column}' not found in data, skipping"
        logger.warning(warning_msg)
        return data
    
    filtered_data = data.copy()
    
    for operator, value in conditions.items():
        try:
            if operator == "eq":  # equals
                mask = filtered_data[column] == value
            elif operator == "ne":  # not equals
                mask = filtered_data[column] != value
            elif operator == "gt":  # greater than
                mask = pd.to_numeric(filtered_data[column], errors='coerce') > value
            elif operator == "lt":  # less than
                mask = pd.to_numeric(filtered_data[column], errors='coerce') < value
            elif operator == "gte":  # greater than or equal
                mask = pd.to_numeric(filtered_data[column], errors='coerce') >= value
            elif operator == "lte":  # less than or equal
                mask = pd.to_numeric(filtered_data[column], errors='coerce') <= value
            elif operator == "contains":  # string contains
                mask = filtered_data[column].astype(str).str.contains(str(value), na=False, regex=False)
            elif operator == "startswith":  # string starts with
                mask = filtered_data[column].astype(str).str.startswith(str(value), na=False)
            elif operator == "endswith":  # string ends with
                mask = filtered_data[column].astype(str).str.endswith(str(value), na=False)
            elif operator == "in":  # value in list
                if isinstance(value, (list, tuple)):
                    mask = filtered_data[column].isin(value)
                else:
                    logger.warning(f"'in' operator requires list/tuple value, got {type(value)}")
                    continue
            elif operator == "not_in":  # value not in list
                if isinstance(value, (list, tuple)):
                    mask = ~filtered_data[column].isin(value)
                else:
                    logger.warning(f"'not_in' operator requires list/tuple value, got {type(value)}")
                    continue
            elif operator == "between":  # value between two values
                if isinstance(value, (list, tuple)) and len(value) == 2:
                    col_numeric = pd.to_numeric(filtered_data[column], errors='coerce')
                    mask = (col_numeric >= value[0]) & (col_numeric <= value[1])
                else:
                    logger.warning(f"'between' operator requires list/tuple of 2 values, got {value}")
                    continue
            elif operator == "is_null":  # is null/NaN
                mask = filtered_data[column].isna() if value else filtered_data[column].notna()
            else:
                logger.warning(f"Unknown operator '{operator}' for column '{column}', skipping")
                continue
            
            # Apply mask
            filtered_data = filtered_data[mask]
            logger.info(f"Applied filter: {column} {operator} {value}, remaining rows: {len(filtered_data)}")
            
        except Exception as e:
            logger.warning(f"Failed to apply filter {column} {operator} {value}: {str(e)}")
            continue
    
    return filtered_data


def _apply_operator_filters_polars(data: Union[pl.DataFrame, pl.LazyFrame], column: str, 
                                  conditions: Dict[str, Any], logger) -> Union[pl.DataFrame, pl.LazyFrame]:
    """Apply standard operator filters for polars."""
    try:
        import polars as pl
    except ImportError:
        raise ImportError("Polars engine requested but polars is not installed")
    
    # Get column list (works for both DataFrame and LazyFrame)
    columns = data.columns if hasattr(data, 'columns') else data.collect_schema().names()
    
    if column not in columns:
        warning_msg = f"Filter column '{column}' not found in data, skipping"
        logger.warning(warning_msg)
        return data
    
    filters = []
    
    for operator, value in conditions.items():
        try:
            if operator == "eq":
                filters.append(pl.col(column) == value)
            elif operator == "ne":
                filters.append(pl.col(column) != value)
            elif operator == "gt":
                filters.append(pl.col(column) > value)
            elif operator == "lt":
                filters.append(pl.col(column) < value)
            elif operator == "gte":
                filters.append(pl.col(column) >= value)
            elif operator == "lte":
                filters.append(pl.col(column) <= value)
            elif operator == "contains":
                filters.append(pl.col(column).str.contains(str(value), literal=True))
            elif operator == "startswith":
                filters.append(pl.col(column).str.starts_with(str(value)))
            elif operator == "endswith":
                filters.append(pl.col(column).str.ends_with(str(value)))
            elif operator == "in":
                if isinstance(value, (list, tuple)):
                    filters.append(pl.col(column).is_in(value))
                else:
                    logger.warning(f"'in' operator requires list/tuple value, got {type(value)}")
                    continue
            elif operator == "not_in":
                if isinstance(value, (list, tuple)):
                    filters.append(~pl.col(column).is_in(value))
                else:
                    logger.warning(f"'not_in' operator requires list/tuple value, got {type(value)}")
                    continue
            elif operator == "between":
                if isinstance(value, (list, tuple)) and len(value) == 2:
                    filters.append((pl.col(column) >= value[0]) & (pl.col(column) <= value[1]))
                else:
                    logger.warning(f"'between' operator requires list/tuple of 2 values, got {value}")
                    continue
            elif operator == "is_null":
                if value:
                    filters.append(pl.col(column).is_null())
                else:
                    filters.append(pl.col(column).is_not_null())
            else:
                logger.warning(f"Unknown operator '{operator}' for column '{column}', skipping")
                continue
            
            logger.info(f"Added polars filter: {column} {operator} {value}")
            
        except Exception as e:
            logger.warning(f"Failed to create polars filter {column} {operator} {value}: {str(e)}")
            continue
    
    # Apply all filters
    if filters:
        combined_filter = filters[0]
        for f in filters[1:]:
            combined_filter = combined_filter & f
        
        filtered_data = data.filter(combined_filter)
        remaining_rows = _get_row_count(filtered_data)
        logger.info(f"Applied {len(filters)} polars filters, remaining rows: {remaining_rows}")
        return filtered_data
    
    return data


def _apply_post_processing(data, keep_columns: List[str], drop_duplicates: bool, 
                          sort_by: str, ascending: bool, engine: str, logger, result) -> Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame]:
    """Apply post-processing steps."""
    # Keep only specified columns
    if keep_columns:
        if engine == "pandas":
            available_columns = [col for col in keep_columns if col in data.columns]
            missing_columns = [col for col in keep_columns if col not in data.columns]
        else:  # polars
            columns = data.columns if hasattr(data, 'columns') else data.collect_schema().names()
            available_columns = [col for col in keep_columns if col in columns]
            missing_columns = [col for col in keep_columns if col not in columns]
        
        if missing_columns:
            warning_msg = f"Requested columns not found: {missing_columns}"
            logger.warning(warning_msg)
            result.add_warning(warning_msg)
        
        if available_columns:
            if engine == "pandas":
                data = data[available_columns]
            else:  # polars
                data = data.select(available_columns)
            logger.info(f"Kept columns: {available_columns}")
    
    # Remove duplicates
    if drop_duplicates:
        before_dedup = _get_row_count(data)
        if engine == "pandas":
            data = data.drop_duplicates()
        else:  # polars
            data = data.unique()
        after_dedup = _get_row_count(data)
        if before_dedup != after_dedup:
            logger.info(f"Removed {before_dedup - after_dedup} duplicate rows")
    
    # Sort if requested
    if sort_by:
        columns = data.columns if hasattr(data, 'columns') else data.collect_schema().names()
        if sort_by in columns:
            try:
                if engine == "pandas":
                    data = data.sort_values(by=sort_by, ascending=ascending)
                else:  # polars
                    data = data.sort(sort_by, descending=not ascending)
                logger.info(f"Sorted by column: {sort_by} ({'ascending' if ascending else 'descending'})")
            except Exception as e:
                warning_msg = f"Failed to sort by '{sort_by}': {e}"
                logger.warning(warning_msg)
                result.add_warning(warning_msg)
        else:
            warning_msg = f"Sort column '{sort_by}' not found in data"
            logger.warning(warning_msg)
            result.add_warning(warning_msg)
    
    return data


def _is_valid_dataframe(data, engine: str) -> bool:
    """Check if data is valid for the specified engine."""
    if engine == "pandas":
        return isinstance(data, pd.DataFrame)
    else:  # polars
        try:
            import polars as pl
            return isinstance(data, (pl.DataFrame, pl.LazyFrame))
        except ImportError:
            return False


def _is_empty_dataframe(data, engine: str) -> bool:
    """Check if dataframe is empty."""
    if engine == "pandas":
        return data.empty
    else:  # polars
        if hasattr(data, 'height'):  # DataFrame
            return data.height == 0
        else:  # LazyFrame
            return data.collect().height == 0


def _get_row_count(data) -> int:
    """Get row count for pandas or polars dataframe."""
    if isinstance(data, pd.DataFrame):
        return len(data)
    elif hasattr(data, 'height'):  # polars DataFrame
        return data.height
    elif hasattr(data, 'collect'):  # polars LazyFrame
        return data.collect().height
    else:
        return 0


def _get_column_count(data) -> int:
    """Get column count for pandas or polars dataframe."""
    if isinstance(data, pd.DataFrame):
        return len(data.columns)
    elif hasattr(data, 'width'):  # polars DataFrame
        return data.width
    elif hasattr(data, 'collect'):  # polars LazyFrame
        return data.collect().width
    else:
        return 0