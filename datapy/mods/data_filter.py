"""
Data Filter Mod for DataPy Framework.

Advanced data filtering with enhanced reject mode handling, custom expressions, 
standard operators, and dual engine support (pandas/polars).
"""

import pandas as pd
import polars as pl
from typing import Dict, Any, Union, Tuple, Optional

from datapy.mod_manager.result import ModResult
from datapy.mod_manager.base import ModMetadata, ConfigSchema
from datapy.mod_manager.logger import setup_logger
from datapy.utils.expression_evaluator import get_expression_evaluator

# Supported engines configuration
SUPPORTED_ENGINES = {
    "pandas": {
        "module": "pandas",
        "dataframe_types": ["pandas.DataFrame"],
        "packages": ["pandas>=1.5.0"]
    },
    "polars": {
        "module": "polars",
        "dataframe_types": ["polars.DataFrame", "polars.LazyFrame"],
        "packages": ["polars>=0.20.0"]
    }
}

# Required metadata
METADATA = ModMetadata(
    type="data_filter",
    version="2.0.0",
    description="Advanced data filtering with reject modes, custom expressions and standard operators",
    category="transformer",
    input_ports=["data"],
    output_ports=["filtered_data", "rejected_data"],
    globals=["filtered_rows", "original_rows", "filter_rate", "rejected_rows"],
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
            "description": "Enhanced filter configuration with operators and/or custom expressions"
        }
    },
    optional={
        "engine": {
            "type": "str",
            "default": "pandas",
            "description": "Processing engine: " + ", ".join([f"'{eng}'" for eng in SUPPORTED_ENGINES.keys()]),
            "enum": list(SUPPORTED_ENGINES.keys())
        },
        "reject_mode": {
            "type": "str",
            "default": "drop",
            "description": "How to handle rejected rows: 'drop', 'flag', 'separate'",
            "enum": ["drop", "flag", "separate"]
        },
        "reject_column": {
            "type": "str",
            "default": "_filter_rejected",
            "description": "Column name for flagging rejected rows (used with 'flag' mode)"
        },
        "custom_functions": {
            "type": "dict",
            "default": {},
            "description": "Custom functions for expressions {name: function_or_import_path}"
        }
    }
)


def run(params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute data filter with enhanced reject mode handling."""
    mod_name = params.get("_mod_name", "data_filter")
    mod_type = params.get("_mod_type", "data_filter")
    logger = setup_logger(__name__, mod_type, mod_name)
    result = ModResult(mod_type, mod_name)
    
    try:
        # Extract parameters
        data = params["data"]
        filter_conditions = params["filter_conditions"]
        engine = params.get("engine", "pandas")
        reject_mode = params.get("reject_mode", "drop")
        reject_column = params.get("reject_column", "_filter_rejected")
        custom_functions = params.get("custom_functions", {})
        
        logger.info(f"Starting data filter", extra={
            "input_rows": _get_rows(data, engine),
            "engine": engine,
            "reject_mode": reject_mode,
            "custom_functions_count": len(custom_functions)
        })
        
        # Validate parameters
        if not _validate_engine(engine):
            result.add_error(f"Unsupported or unavailable engine: {engine}")
            return result.error()
        
        if not _is_valid_dataframe(data, engine):
            result.add_error(f"Invalid data type for {engine}: {type(data)}")
            return result.error()
        
        if reject_mode not in ["drop", "flag", "separate"]:
            result.add_error(f"Invalid reject_mode: {reject_mode}")
            return result.error()
        
        # Handle empty data
        if _is_empty(data, engine):
            result.add_warning("Input data is empty")
            result.add_artifact("filtered_data", data)
            _add_metrics(result, 0, 0, 0, 0.0, reject_mode, engine)
            return result.warning()
        
        original_rows = _get_rows(data, engine)
        
        # Create filter mask
        mask = _create_filter_mask(data, filter_conditions, custom_functions, engine, logger)
        
        # Apply filtering based on reject mode
        if reject_mode == "drop":
            filtered_data = _apply_mask(data, mask, engine)
            rejected_data = None
            filtered_rows = _get_rows(filtered_data, engine)
            rejected_rows = original_rows - filtered_rows
        elif reject_mode == "flag":
            filtered_data = _add_flag_column(data, mask, reject_column, engine)
            rejected_data = None
            filtered_rows = _get_rows(filtered_data, engine)
            rejected_rows = _count_rejected_in_mask(mask, engine)
        elif reject_mode == "separate":
            filtered_data = _apply_mask(data, mask, engine)
            rejected_data = _apply_mask(data, _invert_mask(mask, engine), engine)
            filtered_rows = _get_rows(filtered_data, engine)
            rejected_rows = _get_rows(rejected_data, engine)
        
        # Calculate metrics
        filter_rate = (original_rows - filtered_rows) / original_rows if original_rows > 0 else 0.0
        
        # Add results
        result.add_artifact("filtered_data", filtered_data)
        result.add_artifact("filter_conditions_applied", filter_conditions)
        if reject_mode == "separate" and rejected_data is not None:
            result.add_artifact("rejected_data", rejected_data)
        
        _add_metrics(result, original_rows, filtered_rows, rejected_rows, filter_rate, reject_mode, engine)
        
        logger.info(f"Filter completed", extra={
            "filtered_rows": filtered_rows,
            "rejected_rows": rejected_rows,
            "filter_rate": f"{filter_rate:.2%}"
        })
        return result.success()
        
    except Exception as e:
        logger.error(f"Filter error: {e}", exc_info=True)
        result.add_error(str(e))
        return result.error()


def _validate_engine(engine: str) -> bool:
    """Validate engine is supported."""
    return engine in SUPPORTED_ENGINES


def _is_valid_dataframe(data: Any, engine: str) -> bool:
    """Check if data is valid for engine."""
    if engine == "pandas":
        return isinstance(data, pd.DataFrame)
    elif engine == "polars":
        try:
            import polars as pl
            return isinstance(data, (pl.DataFrame, pl.LazyFrame))
        except ImportError:
            return False
    # elif engine == "dask": return isinstance(data, dd.DataFrame)
    return False


def _is_empty(data: Any, engine: str) -> bool:
    """Check if dataframe is empty."""
    if data is None:
        return True
    
    if engine == "pandas":
        return data.empty
    elif engine == "polars":
        return data.height == 0 if hasattr(data, 'height') else data.collect().height == 0
    # elif engine == "dask": return len(data) == 0
    return True


def _get_rows(data: Any, engine: str) -> int:
    """Get row count."""
    if data is None:
        return 0
    
    if engine == "pandas":
        return len(data)
    elif engine == "polars":
        return data.height if hasattr(data, 'height') else data.collect().height
    # elif engine == "dask": return len(data)
    return 0


def _create_filter_mask(data: Any, conditions: Dict[str, Any], custom_funcs: Dict[str, Any], engine: str, logger):
    """Create boolean mask for filtering."""
    evaluator = get_expression_evaluator(logger)
    if custom_funcs:
        evaluator.register_functions(custom_funcs)
        logger.debug(f"Registered {len(custom_funcs)} custom functions")
    
    if engine == "pandas":
        mask = pd.Series([True] * len(data), index=data.index)
        
        # Apply operator filters
        for column, conds in conditions.get("operators", {}).items():
            if column in data.columns:
                for op, val in conds.items():
                    column_mask = _apply_pandas_operator(data, column, op, val, logger)
                    mask = mask & column_mask
        
        # Apply custom expressions
        for expr in conditions.get("custom_expressions", []):
            try:
                expr_result = evaluator.evaluate_filter(data, expr, "pandas")
                if isinstance(expr_result, pd.DataFrame):
                    expr_mask = data.index.isin(expr_result.index)
                else:
                    expr_mask = expr_result
                mask = mask & expr_mask
                logger.info(f"Applied custom expression: {expr[:30]}...")
            except Exception as e:
                logger.error(f"Failed to apply expression '{expr}': {e}")
                raise
        
        return mask
    
    elif engine == "polars":
        mask_conditions = []
        
        # Apply operator filters
        for column, conds in conditions.get("operators", {}).items():
            cols = data.columns if hasattr(data, 'columns') else data.collect_schema().names()
            if column in cols:
                for op, val in conds.items():
                    mask_conditions.append(_apply_polars_operator(column, op, val, logger))
        
        # Apply custom expressions
        for expr in conditions.get("custom_expressions", []):
            try:
                expr_result = evaluator.evaluate_filter(data, expr, "polars")
                mask_conditions.append(expr_result)
                logger.info(f"Applied custom expression: {expr[:30]}...")
            except Exception as e:
                logger.error(f"Failed to apply expression '{expr}': {e}")
                raise
        
        # Combine all conditions with AND
        if mask_conditions:
            combined_mask = mask_conditions[0]
            for condition in mask_conditions[1:]:
                combined_mask = combined_mask & condition
            return combined_mask
        else:
            return pl.lit(True)
    
    # elif engine == "dask": return _create_dask_mask(data, conditions, evaluator, logger)
    raise ValueError(f"Mask creation not implemented for engine: {engine}")


def _apply_pandas_operator(data: pd.DataFrame, column: str, operator: str, value: Any, logger) -> pd.Series:
    """Apply pandas operator."""
    try:
        if operator == "eq": 
            return data[column] == value
        elif operator == "ne": 
            return data[column] != value
        elif operator == "gt": 
            return pd.to_numeric(data[column], errors='coerce') > value
        elif operator == "lt": 
            return pd.to_numeric(data[column], errors='coerce') < value
        elif operator == "gte": 
            return pd.to_numeric(data[column], errors='coerce') >= value
        elif operator == "lte": 
            return pd.to_numeric(data[column], errors='coerce') <= value
        elif operator == "contains": 
            return data[column].astype(str).str.contains(str(value), na=False, regex=False)
        elif operator == "startswith": 
            return data[column].astype(str).str.startswith(str(value), na=False)
        elif operator == "endswith": 
            return data[column].astype(str).str.endswith(str(value), na=False)
        elif operator == "in": 
            return data[column].isin(value) if isinstance(value, (list, tuple)) else pd.Series([False] * len(data))
        elif operator == "not_in":
            return ~data[column].isin(value) if isinstance(value, (list, tuple)) else pd.Series([True] * len(data))
        elif operator == "between":
            if isinstance(value, (list, tuple)) and len(value) == 2:
                numeric = pd.to_numeric(data[column], errors='coerce')
                return (numeric >= value[0]) & (numeric <= value[1])
        elif operator == "is_null": 
            return data[column].isna() if value else data[column].notna()
        else:
            logger.warning(f"Unknown operator: {operator}")
            return pd.Series([True] * len(data))
    except Exception as e:
        logger.warning(f"Error applying {operator}: {e}")
        return pd.Series([True] * len(data))


def _apply_polars_operator(column: str, operator: str, value: Any, logger):
    """Apply polars operator."""
    try:
        if operator == "eq": 
            return pl.col(column) == value
        elif operator == "ne": 
            return pl.col(column) != value
        elif operator == "gt": 
            return pl.col(column) > value
        elif operator == "lt": 
            return pl.col(column) < value
        elif operator == "gte": 
            return pl.col(column) >= value
        elif operator == "lte": 
            return pl.col(column) <= value
        elif operator == "contains": 
            return pl.col(column).cast(pl.Utf8).str.contains(str(value))
        elif operator == "startswith": 
            return pl.col(column).cast(pl.Utf8).str.starts_with(str(value))
        elif operator == "endswith": 
            return pl.col(column).cast(pl.Utf8).str.ends_with(str(value))
        elif operator == "in": 
            return pl.col(column).is_in(value) if isinstance(value, (list, tuple)) else pl.lit(False)
        elif operator == "not_in": 
            return ~pl.col(column).is_in(value) if isinstance(value, (list, tuple)) else pl.lit(True)
        elif operator == "between":
            if isinstance(value, (list, tuple)) and len(value) == 2:
                return pl.col(column).is_between(value[0], value[1])
        elif operator == "is_null": 
            return pl.col(column).is_null() if value else pl.col(column).is_not_null()
        else:
            logger.warning(f"Unknown operator: {operator}")
            return pl.lit(True)
    except Exception as e:
        logger.warning(f"Error applying {operator}: {e}")
        return pl.lit(True)


def _apply_mask(data: Any, mask: Any, engine: str) -> Any:
    """Apply boolean mask to data."""
    if engine == "pandas":
        return data[mask].copy()
    elif engine == "polars":
        return data.filter(mask)
    # elif engine == "dask": return data[mask]
    raise ValueError(f"Mask application not implemented for engine: {engine}")


def _invert_mask(mask: Any, engine: str) -> Any:
    """Invert boolean mask."""
    if engine == "pandas":
        return ~mask
    elif engine == "polars":
        return ~mask
    # elif engine == "dask": return ~mask
    return mask


def _add_flag_column(data: Any, mask: Any, reject_column: str, engine: str) -> Any:
    """Add reject flag column."""
    if engine == "pandas":
        result = data.copy()
        result[reject_column] = ~mask
        return result
    elif engine == "polars":
        return data.with_columns((~mask).alias(reject_column))
    # elif engine == "dask": return data.assign(**{reject_column: ~mask})
    raise ValueError(f"Flag column not implemented for engine: {engine}")


def _count_rejected_in_mask(mask: Any, engine: str) -> int:
    """Count rejected rows from mask."""
    if engine == "pandas":
        return int((~mask).sum())
    elif engine == "polars":
        return int((~mask).sum()) if hasattr(mask, 'sum') else 0
    # elif engine == "dask": return int((~mask).sum())
    return 0


def _add_metrics(result: ModResult, original: int, filtered: int, rejected: int, rate: float, mode: str, engine: str):
    """Add metrics and globals to result."""
    result.add_metric("original_rows", original)
    result.add_metric("filtered_rows", filtered)
    result.add_metric("rejected_rows", rejected)
    result.add_metric("rows_removed", original - filtered)
    result.add_metric("filter_rate", round(rate, 4))
    result.add_metric("reject_mode_used", mode)
    result.add_metric("engine_used", engine)
    
    result.add_global("filtered_rows", filtered)
    result.add_global("original_rows", original)
    result.add_global("rejected_rows", rejected)
    result.add_global("filter_rate", round(rate, 4))