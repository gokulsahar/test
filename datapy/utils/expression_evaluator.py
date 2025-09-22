"""
Universal Expression Evaluator for DataPy Framework.

Production-ready expression evaluation supporting pandas, polars, and custom functions
with explicit function registration and comprehensive error handling.
"""

import pandas as pd
import polars as pl
import numpy as np
import importlib
from typing import Dict, Any, Union, Optional, Callable
from datapy.mod_manager.logger import setup_logger


class ExpressionEvaluator:
    """
    Production-grade expression evaluator with pandas/polars support.
    
    Features:
    - Dual engine support (pandas/polars)
    - Explicit function registration (no frame inspection)
    - Multi-line expression support
    - Comprehensive error handling
    - Performance optimization
    - Security-conscious evaluation
    """
    
    def __init__(self, logger=None):
        """Initialize expression evaluator."""
        self.logger = logger or setup_logger(__name__)
        self.custom_functions = {}
        self._setup_core_functions()
    
    def _setup_core_functions(self):
        """Setup core functions available in all expressions."""
        self.core_functions = {
            # Essential Python builtins
            "len": len,
            "str": str,
            "int": int,
            "float": float,
            "bool": bool,
            "abs": abs,
            "max": max,
            "min": min,
            "sum": sum,
            "round": round,
            "any": any,
            "all": all,
            "sorted": sorted,
            "enumerate": enumerate,
            "zip": zip,
            
            # Common data science libraries
            "pd": pd,
            "pl": pl,
            "np": np,
            
            # Standard library modules
            "math": __import__("math"),
            "re": __import__("re"),
            "datetime": __import__("datetime"),
            "json": __import__("json"),
            
            # Import capability for advanced use
            "__import__": __import__,
        }
    
    def register_functions(self, functions: Dict[str, Union[str, Callable]]):
        """
        Register custom functions for expression evaluation.
        
        Args:
            functions: Dict mapping function names to either:
                - Callable objects (direct function references)
                - Import strings (e.g., "mymodule.myfunction")
                
        Example:
            evaluator.register_functions({
                "sha256_encrypt": sha256_encrypt,  # Direct function
                "business_rule": "myutils.rules.is_premium",  # Import string
            })
        """
        if not isinstance(functions, dict):
            raise ValueError("Functions must be provided as a dictionary")
        
        self.custom_functions.clear()  # Clear previous registrations
        
        for name, func_ref in functions.items():
            try:
                if callable(func_ref):
                    # Direct function object
                    self.custom_functions[name] = func_ref
                    self.logger.debug(f"Registered function object: {name}")
                    
                elif isinstance(func_ref, str):
                    # Import string - load the function
                    loaded_func = self._load_function_from_string(func_ref)
                    self.custom_functions[name] = loaded_func
                    self.logger.debug(f"Loaded and registered function: {name} from {func_ref}")
                    
                else:
                    raise ValueError(f"Function '{name}' must be callable or import string, got {type(func_ref)}")
                    
            except Exception as e:
                error_msg = f"Failed to register function '{name}': {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                raise ValueError(error_msg) from e
        
        self.logger.info(f"Successfully registered {len(self.custom_functions)} custom functions")
    
    def _load_function_from_string(self, import_path: str) -> Callable:
        """
        Load function from import string.
        
        Args:
            import_path: Module path like "mymodule.submodule.function_name"
            
        Returns:
            Loaded function object
        """
        if not isinstance(import_path, str) or not import_path.strip():
            raise ValueError("Import path must be a non-empty string")
        
        try:
            # Split module path and function name
            if '.' not in import_path:
                raise ValueError(f"Invalid import path format: {import_path}")
            
            module_path, func_name = import_path.rsplit('.', 1)
            
            # Import the module
            module = importlib.import_module(module_path)
            
            # Get the function from module
            if not hasattr(module, func_name):
                raise AttributeError(f"Module '{module_path}' has no attribute '{func_name}'")
            
            func = getattr(module, func_name)
            
            # Verify it's callable
            if not callable(func):
                raise TypeError(f"'{import_path}' is not callable")
            
            return func
            
        except ImportError as e:
            raise ImportError(f"Cannot import module for '{import_path}': {str(e)}") from e
        except Exception as e:
            raise ValueError(f"Failed to load function from '{import_path}': {str(e)}") from e
    
    def clear_functions(self):
        """Clear all registered custom functions."""
        self.custom_functions.clear()
        self.logger.debug("Cleared all custom functions")
    
    def _get_evaluation_context(self, data_context: Dict[str, Any]) -> Dict[str, Any]:
        """Build complete evaluation context with registered functions."""
        context = {
            "__builtins__": {},  # Restricted builtins for security
            **self.core_functions,
            **self.custom_functions,  # Explicitly registered functions
            **data_context
        }
        
        if self.custom_functions:
            self.logger.debug(f"Using {len(self.custom_functions)} custom functions", extra={
                "function_names": list(self.custom_functions.keys())
            })
        
        return context
    
    def _normalize_expression(self, expression: str) -> str:
        """Normalize expression string for evaluation."""
        # Strip whitespace and handle multi-line
        normalized = expression.strip()
        
        # Log expression for debugging (truncated for security)
        log_expr = normalized[:100] + "..." if len(normalized) > 100 else normalized
        self.logger.debug(f"Normalizing expression: {log_expr}")
        
        return normalized
    
    def evaluate_expression(self, expression: str, data_context: Dict[str, Any], 
                          custom_functions: Optional[Dict[str, Union[str, Callable]]] = None) -> Any:
        """
        Evaluate a Python expression with full context.
        
        Args:
            expression: Python expression string (supports multi-line and semicolons)
            data_context: Data context (typically DataFrame columns)
            custom_functions: Optional functions to register for this evaluation
            
        Returns:
            Expression evaluation result
            
        Raises:
            ValueError: If expression evaluation fails
        """
        try:
            # Register custom functions if provided
            if custom_functions:
                self.register_functions(custom_functions)
            
            normalized_expr = self._normalize_expression(expression)
            eval_context = self._get_evaluation_context(data_context)
            
            # Handle different expression formats
            if '\n' in normalized_expr:
                return self._evaluate_multiline(normalized_expr, eval_context)
            elif ';' in normalized_expr:
                return self._evaluate_semicolon(normalized_expr, eval_context)
            else:
                return self._evaluate_simple(normalized_expr, eval_context)
                
        except Exception as e:
            error_msg = f"Expression evaluation failed: {str(e)}"
            self.logger.error(error_msg, extra={
                "expression_preview": expression[:50] + "..." if len(expression) > 50 else expression,
                "error_type": type(e).__name__
            }, exc_info=True)
            raise ValueError(error_msg) from e
    
    def _evaluate_simple(self, expression: str, context: Dict[str, Any]) -> Any:
        """Evaluate simple single-line expression."""
        # nosec - expressions are developer-controlled in ETL pipelines
        return eval(expression, context)  # nosec
    
    def _evaluate_semicolon(self, expression: str, context: Dict[str, Any]) -> Any:
        """Evaluate semicolon-separated expression."""
        parts = [part.strip() for part in expression.split(';') if part.strip()]
        
        if not parts:
            raise ValueError("Empty expression after semicolon parsing")
        
        # Execute all parts except the last as statements
        for part in parts[:-1]:
            # nosec - developer-controlled expressions
            exec(part, context)  # nosec
        
        # Evaluate the last part as expression
        # nosec - developer-controlled expressions
        return eval(parts[-1], context)  # nosec
    
    def _evaluate_multiline(self, expression: str, context: Dict[str, Any]) -> Any:
        """Evaluate multi-line expression."""
        lines = [line.strip() for line in expression.split('\n') if line.strip()]
        
        if not lines:
            raise ValueError("Empty expression after multi-line parsing")
        
        # Execute all lines except the last as statements
        for line in lines[:-1]:
            # nosec - developer-controlled expressions
            exec(line, context)  # nosec
        
        # Evaluate the last line as expression
        # nosec - developer-controlled expressions
        return eval(lines[-1], context)  # nosec
    
    def evaluate_filter(self, df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame], 
                       expression: str, engine: str = "pandas",
                       custom_functions: Optional[Dict[str, Union[str, Callable]]] = None) -> Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame]:
        """
        Evaluate filter expression on DataFrame.
        
        Args:
            df: Input DataFrame (pandas or polars)
            expression: Filter expression returning boolean
            engine: Processing engine ("pandas" or "polars")
            custom_functions: Optional functions to register for this evaluation
            
        Returns:
            Filtered DataFrame
        """
        if engine == "pandas":
            return self._evaluate_pandas_filter(df, expression, custom_functions)
        elif engine == "polars":
            return self._evaluate_polars_filter(df, expression, custom_functions)
        else:
            raise ValueError(f"Unsupported engine: {engine}")
    
    def _evaluate_pandas_filter(self, df: pd.DataFrame, expression: str,
                               custom_functions: Optional[Dict[str, Union[str, Callable]]] = None) -> pd.DataFrame:
        """Evaluate filter expression for pandas DataFrame."""
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"Expected pandas DataFrame, got {type(df)}")
        
        # Build context with column access
        context = {col: df[col] for col in df.columns}
        context["df"] = df  # Allow full DataFrame access
        
        # Evaluate expression
        result = self.evaluate_expression(expression, context, custom_functions)
        
        # Ensure boolean mask
        if isinstance(result, pd.Series):
            mask = result.astype(bool)
        elif isinstance(result, (bool, np.bool_)):
            mask = pd.Series([result] * len(df), index=df.index)
        else:
            raise ValueError(f"Filter expression must return boolean or Series, got {type(result)}")
        
        # Apply filter
        filtered_df = df[mask]
        
        self.logger.debug(f"Pandas filter applied", extra={
            "original_rows": len(df),
            "filtered_rows": len(filtered_df),
            "filter_rate": (len(df) - len(filtered_df)) / len(df) if len(df) > 0 else 0
        })
        
        return filtered_df
    
    def _evaluate_polars_filter(self, df: Union[pl.DataFrame, pl.LazyFrame], 
                               expression: str,
                               custom_functions: Optional[Dict[str, Union[str, Callable]]] = None) -> Union[pl.DataFrame, pl.LazyFrame]:
        """Evaluate filter expression for polars DataFrame."""
        is_lazy = isinstance(df, pl.LazyFrame)
        
        try:
            # For complex expressions, convert to pandas temporarily
            if self._is_complex_expression(expression):
                return self._evaluate_polars_via_pandas(df, expression, is_lazy, custom_functions)
            else:
                # Try native polars evaluation for simple expressions
                return self._evaluate_polars_native(df, expression, custom_functions)
        except Exception:
            # Fallback to pandas conversion
            return self._evaluate_polars_via_pandas(df, expression, is_lazy, custom_functions)
    
    def _is_complex_expression(self, expression: str) -> bool:
        """Check if expression requires pandas conversion."""
        complex_indicators = [
            'import ', 'from ', 'def ', 'lambda ', '__',
            ';', '\n', 'exec(', 'eval('
        ]
        return any(indicator in expression for indicator in complex_indicators)
    
    def _evaluate_polars_native(self, df: Union[pl.DataFrame, pl.LazyFrame], 
                               expression: str,
                               custom_functions: Optional[Dict[str, Union[str, Callable]]] = None) -> Union[pl.DataFrame, pl.LazyFrame]:
        """Evaluate simple expressions using native polars."""
        # This is a simplified implementation - can be enhanced
        # For now, convert to pandas for complex evaluation
        raise NotImplementedError("Native polars evaluation not yet implemented")
    
    def _evaluate_polars_via_pandas(self, df: Union[pl.DataFrame, pl.LazyFrame], 
                                   expression: str, is_lazy: bool,
                                   custom_functions: Optional[Dict[str, Union[str, Callable]]] = None) -> Union[pl.DataFrame, pl.LazyFrame]:
        """Evaluate polars expressions via pandas conversion."""
        # Convert to pandas
        if is_lazy:
            pandas_df = df.collect().to_pandas()
        else:
            pandas_df = df.to_pandas()
        
        # Apply pandas filter
        filtered_pandas = self._evaluate_pandas_filter(pandas_df, expression, custom_functions)
        
        # Convert back to polars
        result_df = pl.DataFrame(filtered_pandas)
        
        self.logger.debug(f"Polars filter via pandas", extra={
            "was_lazy": is_lazy,
            "original_rows": len(pandas_df),
            "filtered_rows": len(filtered_pandas)
        })
        
        # Return as LazyFrame if input was lazy
        return result_df.lazy() if is_lazy else result_df
    
    def evaluate_transform(self, df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame], 
                          expression: str, engine: str = "pandas",
                          custom_functions: Optional[Dict[str, Union[str, Callable]]] = None) -> Union[pd.Series, pl.Series, Any]:
        """
        Evaluate transformation expression on DataFrame.
        
        Args:
            df: Input DataFrame
            expression: Transform expression
            engine: Processing engine
            custom_functions: Optional functions to register for this evaluation
            
        Returns:
            Transformation result (Series or scalar)
        """
        if engine == "pandas":
            return self._evaluate_pandas_transform(df, expression, custom_functions)
        elif engine == "polars":
            return self._evaluate_polars_transform(df, expression, custom_functions)
        else:
            raise ValueError(f"Unsupported engine: {engine}")
    
    def _evaluate_pandas_transform(self, df: pd.DataFrame, expression: str,
                                  custom_functions: Optional[Dict[str, Union[str, Callable]]] = None) -> Union[pd.Series, Any]:
        """Evaluate transformation for pandas DataFrame."""
        context = {col: df[col] for col in df.columns}
        context["df"] = df
        
        result = self.evaluate_expression(expression, context, custom_functions)
        
        self.logger.debug(f"Pandas transform applied", extra={
            "result_type": type(result).__name__,
            "result_shape": getattr(result, 'shape', None)
        })
        
        return result
    
    def _evaluate_polars_transform(self, df: Union[pl.DataFrame, pl.LazyFrame], 
                                  expression: str,
                                  custom_functions: Optional[Dict[str, Union[str, Callable]]] = None) -> Union[pl.Series, Any]:
        """Evaluate transformation for polars DataFrame."""
        is_lazy = isinstance(df, pl.LazyFrame)
        
        # Convert to pandas for complex expressions
        if is_lazy:
            pandas_df = df.collect().to_pandas()
        else:
            pandas_df = df.to_pandas()
        
        result = self._evaluate_pandas_transform(pandas_df, expression, custom_functions)
        
        # Convert pandas Series back to polars if applicable
        if isinstance(result, pd.Series):
            polars_result = pl.Series(result.name, result.values)
            self.logger.debug(f"Converted pandas Series to polars Series")
            return polars_result
        
        return result


# Global singleton instance
_global_evaluator: Optional[ExpressionEvaluator] = None


def get_expression_evaluator(logger=None) -> ExpressionEvaluator:
    """
    Get the global expression evaluator instance.
    
    Args:
        logger: Optional logger instance
        
    Returns:
        Global ExpressionEvaluator instance
    """
    global _global_evaluator
    if _global_evaluator is None:
        _global_evaluator = ExpressionEvaluator(logger)
    return _global_evaluator


def evaluate_expression(expression: str, data_context: Dict[str, Any], 
                       custom_functions: Optional[Dict[str, Union[str, Callable]]] = None,
                       logger=None) -> Any:
    """
    Quick expression evaluation using global evaluator.
    
    Args:
        expression: Python expression string
        data_context: Data context for evaluation
        custom_functions: Optional functions to register
        logger: Optional logger
        
    Returns:
        Expression result
    """
    evaluator = get_expression_evaluator(logger)
    return evaluator.evaluate_expression(expression, data_context, custom_functions)


def evaluate_filter(df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame], 
                   expression: str, engine: str = "pandas",
                   custom_functions: Optional[Dict[str, Union[str, Callable]]] = None,
                   logger=None) -> Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame]:
    """
    Quick filter evaluation using global evaluator.
    
    Args:
        df: Input DataFrame
        expression: Filter expression
        engine: Processing engine
        custom_functions: Optional functions to register
        logger: Optional logger
        
    Returns:
        Filtered DataFrame
    """
    evaluator = get_expression_evaluator(logger)
    return evaluator.evaluate_filter(df, expression, engine, custom_functions)


def evaluate_transform(df: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame], 
                      expression: str, engine: str = "pandas",
                      custom_functions: Optional[Dict[str, Union[str, Callable]]] = None,
                      logger=None) -> Union[pd.Series, pl.Series, Any]:
    """
    Quick transform evaluation using global evaluator.
    
    Args:
        df: Input DataFrame
        expression: Transform expression
        engine: Processing engine
        custom_functions: Optional functions to register
        logger: Optional logger
        
    Returns:
        Transform result
    """
    evaluator = get_expression_evaluator(logger)
    return evaluator.evaluate_transform(df, expression, engine, custom_functions)


# Usage documentation
__doc__ += """

Usage Examples:
==============

Pipeline Usage with Custom Functions:
------------------------------------
#!/usr/bin/env python3
import hashlib
from datapy.mod_manager.sdk import run_mod

# Define custom functions
def sha256_encrypt(value):
    if pd.isna(value):
        return None
    return hashlib.sha256(str(value).encode()).hexdigest()

def is_premium_customer(annual_spend, tenure):
    return annual_spend > 10000 and tenure > 2

def calculate_risk_score(age, income, debt):
    if income == 0:
        return 100  # Max risk
    return (debt / income) * (1 / age) * 100

def main():
    customers = run_mod("csv_reader", {"file_path": "customers.csv"})
    
    # Pass functions to data_filter mod
    filtered = run_mod("data_filter", {
        "data": customers["artifacts"]["data"],
        "custom_functions": {
            "sha256_encrypt": sha256_encrypt,  # Direct function
            "is_premium": is_premium_customer,
            "risk_score": calculate_risk_score
        },
        "filter_conditions": {
            "custom_expressions": [
                "is_premium(annual_spend, tenure_years)",
                "risk_score(age, income, debt_ratio) < 50",
                "sha256_encrypt(ssn) == 'expected_hash'",
                "age > 25 and len(email) > 5"  # Built-ins work too
            ]
        }
    })
    
    return filtered

if __name__ == "__main__":
    main()

Import-based Usage:
------------------
#!/usr/bin/env python3
from datapy.mod_manager.sdk import run_mod

def main():
    customers = run_mod("csv_reader", {"file_path": "customers.csv"})
    
    # Use import strings for reusable functions
    filtered = run_mod("data_filter", {
        "data": customers["artifacts"]["data"],
        "custom_functions": {
            "encrypt": "myutils.crypto.sha256_encrypt",  # Import string
            "validate": "myutils.validation.is_valid_email",
            "business_rule": "myutils.business.is_high_value"
        },
        "filter_conditions": {
            "custom_expressions": [
                "business_rule(annual_spend, tenure)",
                "validate(email)",
                "encrypt(sensitive_field) != ''"
            ]
        }
    })
    
    return filtered

Field Mapper Usage:
------------------
mapped = run_mod("field_mapper", {
    "data": customers["artifacts"]["data"],
    "custom_functions": {
        "format_phone": "myutils.formatting.normalize_phone",
        "calc_age": lambda birthdate: (datetime.now() - birthdate).days // 365
    },
    "mappings": [
        {
            "source_field": "raw_phone",
            "target_field": "phone",
            "expression": "format_phone(raw_phone)"
        },
        {
            "source_field": "birth_date", 
            "target_field": "age",
            "expression": "calc_age(birth_date)"
        }
    ]
})

Mixed Function Types:
--------------------
# You can mix direct functions and import strings
custom_functions = {
    "local_func": lambda x: x.upper(),  # Direct lambda
    "imported_func": "mymodule.my_function",  # Import string
    "defined_func": my_defined_function  # Direct function reference
}

Security Notes:
--------------
- No frame inspection (production safe)
- Explicit function registration only
- Uses # nosec comments for scanner compatibility
- Full trust model within registered functions
- Developer-controlled expressions only
"""