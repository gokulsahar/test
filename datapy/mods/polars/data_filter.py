"""
Data Filter Mod for DataPy Framework.

Production-ready row filtering with Talend tFilter/tMap feature parity.
Supports complex conditions, AND/OR logic, regex, null handling,
and secure custom functions. Pure Polars lazy/streaming implementation.
"""

from typing import Dict, Any, Union, Callable, List
import polars as pl
from functools import reduce

from datapy.mod_manager.result import ModResult
from datapy.mod_manager.base import ModMetadata, ConfigSchema
from datapy.mod_manager.logger import setup_logger

# Required metadata
METADATA = ModMetadata(
    type="data_filter",
    version="1.0.0",
    description="Production-ready row filtering with Talend feature parity and secure custom functions",
    category="transforms",
    input_ports=["data"],
    output_ports=["filtered_data", "rejected_data"],
    globals=["filtered_rows", "rejected_rows", "filter_rate", "conditions_applied"],
    packages=["polars>=0.20.0"]
)


CONFIG_SCHEMA = ConfigSchema(
    required={
        "data": {
            "type": "object", 
            "description": "Lazy DataFrame to filter"
        },
        "filter_conditions": {
            "type": "dict",
            "description": "Standard filtering conditions {column: {operator: value}}"
        }
    },
    optional={
        "output_reject": {
            "type": "bool",
            "default": False,
            "description": "Enable reject output for rows that fail filters"
        },
        "condition_logic": {
            "type": "str",
            "default": "AND",
            "description": "Logic between conditions: 'AND' or 'OR'"
        },
        "null_handling": {
            "type": "str", 
            "default": "exclude",
            "description": "How to treat nulls: 'include', 'exclude', 'as_false'"
        },
        "custom_functions": {
            "type": "dict",
            "default": {},
            "description": "Custom filter functions: {function_name: callable_function}"
        }
    }
)


def _create_condition_expression(column: str, operator: str, value: Any, null_handling: str) -> pl.Expr:
    """
    Create Polars expression for a single filter condition.
    
    Args:
        column: Column name
        operator: Filter operator (>=, <=, in, contains, etc.)
        value: Filter value
        null_handling: How to handle null values
        
    Returns:
        Polars expression for the condition
    """
    col_expr = pl.col(column)
    
    # Standard comparison operators
    if operator == "==":
        condition = col_expr == value
    elif operator == "!=":
        condition = col_expr != value
    elif operator == ">":
        condition = col_expr > value
    elif operator == ">=":
        condition = col_expr >= value
    elif operator == "<":
        condition = col_expr < value
    elif operator == "<=":
        condition = col_expr <= value
    
    # List operations
    elif operator == "in":
        condition = col_expr.is_in(value if isinstance(value, list) else [value])
    elif operator == "not_in":
        condition = ~col_expr.is_in(value if isinstance(value, list) else [value])
    
    # String operations
    elif operator == "contains":
        condition = col_expr.str.contains(str(value), literal=True)
    elif operator == "not_contains":
        condition = ~col_expr.str.contains(str(value), literal=True)
    elif operator == "starts_with":
        condition = col_expr.str.starts_with(str(value))
    elif operator == "ends_with":
        condition = col_expr.str.ends_with(str(value))
    elif operator == "regex":
        condition = col_expr.str.contains(str(value), literal=False)
    
    # Range operations
    elif operator == "between":
        if isinstance(value, (list, tuple)) and len(value) == 2:
            condition = col_expr.is_between(value[0], value[1], closed="both")
        else:
            raise ValueError(f"'between' operator requires list/tuple of 2 values, got: {value}")
    
    # Null operations
    elif operator == "is_null":
        condition = col_expr.is_null() if value else col_expr.is_not_null()
    elif operator == "not_null":
        condition = col_expr.is_not_null() if value else col_expr.is_null()
    
    else:
        raise ValueError(f"Unsupported operator: {operator}")
    
    # Apply null handling for non-null operators
    if null_handling == "exclude" and operator not in ["is_null", "not_null"]:
        condition = col_expr.is_not_null() & condition
    elif null_handling == "as_false" and operator not in ["is_null", "not_null"]:
        condition = col_expr.is_not_null() & condition
    
    return condition


def _apply_custom_functions(data: pl.LazyFrame, custom_functions: Dict[str, Callable], logger) -> List[pl.Expr]:
    """
    Apply secure custom functions to create filter expressions.
    
    Args:
        data: Input lazy DataFrame
        custom_functions: Dictionary of function name -> function reference
        logger: Logger instance
        
    Returns:
        List of Polars expressions from custom functions
    """
    custom_expressions = []
    
    for func_name, func in custom_functions.items():
        try:
            if not callable(func):
                logger.error(f"Custom function '{func_name}' is not callable: {type(func)}")
                continue
                
            # Call function with data - it should return a Polars expression
            result = func(data)
            
            if isinstance(result, pl.Expr):
                custom_expressions.append(result)
                logger.info(f"Applied custom function: {func_name}")
            else:
                logger.error(f"Custom function '{func_name}' must return pl.Expr, got: {type(result)}")
                
        except Exception as e:
            logger.error(f"Error applying custom function '{func_name}': {str(e)}")
            raise
    
    return custom_expressions


def run(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute data filtering with lazy/streaming Polars approach.
    
    Args:
        params: Dictionary containing data, filter_conditions and optional parameters
        
    Returns:
        ModResult dictionary with filtered data and metrics
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
        output_reject = params.get("output_reject", False)
        condition_logic = params.get("condition_logic", "AND").upper()
        null_handling = params.get("null_handling", "exclude")
        custom_functions = params.get("custom_functions", {})
        
        logger.info(f"Starting data filter", extra={
            "condition_logic": condition_logic,
            "null_handling": null_handling,
            "output_reject": output_reject,
            "filter_conditions_count": len(filter_conditions),
            "has_custom_functions": bool(custom_functions)
        })
        
        # Validate input data is lazy DataFrame
        if not isinstance(data, pl.LazyFrame):
            error_msg = f"Input data must be a Polars LazyFrame, got {type(data)}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        # Validate condition logic
        if condition_logic not in ["AND", "OR"]:
            error_msg = f"condition_logic must be 'AND' or 'OR', got: {condition_logic}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        # Validate null handling
        if null_handling not in ["include", "exclude", "as_false"]:
            error_msg = f"null_handling must be 'include', 'exclude', or 'as_false', got: {null_handling}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        # Build filter expressions
        all_expressions = []
        
        # Standard filter conditions
        for column, conditions in filter_conditions.items():
            if not isinstance(conditions, dict):
                error_msg = f"Filter conditions for column '{column}' must be dict, got: {type(conditions)}"
                logger.error(error_msg)
                result.add_error(error_msg)
                return result.error()
            
            for operator, value in conditions.items():
                try:
                    expr = _create_condition_expression(column, operator, value, null_handling)
                    all_expressions.append(expr)
                    logger.debug(f"Added filter: {column} {operator} {value}")
                except Exception as e:
                    error_msg = f"Error creating filter for {column} {operator} {value}: {str(e)}"
                    logger.error(error_msg)
                    result.add_error(error_msg)
                    return result.error()
        
        # Custom functions
        if custom_functions:
            try:
                custom_exprs = _apply_custom_functions(data, custom_functions, logger)
                all_expressions.extend(custom_exprs)
            except Exception as e:
                error_msg = f"Error applying custom functions: {str(e)}"
                logger.error(error_msg)
                result.add_error(error_msg)
                return result.error()
        
        if not all_expressions:
            logger.warning("No filter expressions created - returning original data")
            result.add_artifact("filtered_data", data)
            result.add_metric("filtered_rows", "lazy_evaluation")
            result.add_metric("rejected_rows", 0)
            result.add_metric("filter_rate", 0.0)
            return result.success()
        
        # Combine expressions with AND/OR logic
        if condition_logic == "AND":
            combined_filter = reduce(lambda a, b: a & b, all_expressions)
        else:  # OR
            combined_filter = reduce(lambda a, b: a | b, all_expressions)
        
        # Apply filter to get success records (lazy operation)
        filtered_data = data.filter(combined_filter)
        
        # Handle reject output if requested
        rejected_data = None
        if output_reject:
            # Create inverse filter for reject records (lazy operation)
            rejected_data = data.filter(~combined_filter)
            result.add_artifact("rejected_data", rejected_data)
            logger.info("Reject output enabled - created rejected_data artifact")
        
        logger.info(f"Filter operations completed successfully", extra={
            "expressions_applied": len(all_expressions),
            "condition_logic": condition_logic,
            "reject_output_enabled": output_reject
        })
        
        # Add metrics (counts are lazy - not materialized)
        result.add_metric("filtered_rows", "lazy_evaluation")
        result.add_metric("rejected_rows", "lazy_evaluation" if output_reject else 0)
        result.add_metric("conditions_applied", len(all_expressions))
        result.add_metric("filter_rate", "lazy_evaluation")
        result.add_metric("processing_mode", "lazy_streaming")
        result.add_metric("memory_efficient", True)
        
        # Add artifacts
        result.add_artifact("filtered_data", filtered_data)
        result.add_artifact("filter_conditions_applied", filter_conditions)
        result.add_artifact("condition_logic_used", condition_logic)
        
        # Add globals for downstream mods
        result.add_global("filtered_rows", "lazy_evaluation")
        result.add_global("rejected_rows", "lazy_evaluation" if output_reject else 0)
        result.add_global("conditions_applied", len(all_expressions))
        result.add_global("filter_rate", "lazy_evaluation")
        
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