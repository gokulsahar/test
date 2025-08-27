"""
Python SDK for DataPy framework with registry-based mod execution.

Provides clean API for mod execution with parameter validation and execution
orchestration. No file management - simple console output for shell script capture.
"""

import importlib
from typing import Dict, Any, Optional

from .context import set_context as _set_context, clear_context as _clear_context, substitute_context_variables
from .logger import setup_logger, set_log_level as _set_log_level, DEFAULT_LOG_CONFIG
from .params import create_resolver
from .result import ModResult, validation_error, runtime_error
from .registry import get_registry
from .parameter_validation import validate_mod_parameters

logger = setup_logger(__name__)

def set_context(file_path: str) -> None:
    """
    Set context file path for variable substitution.
    
    Args:
        file_path: Path to context JSON file
        
    Raises:
        ValueError: If file_path is empty
    """
    _set_context(file_path)


def clear_context() -> None:
    """Clear context file path and cached data."""
    _clear_context()


def set_log_level(level: str) -> None:
    """
    Set logging level for the framework.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        
    Raises:
        ValueError: If level is invalid
    """
    _set_log_level(level)

def _auto_generate_mod_name(mod_type: str) -> str:
    """
    Auto-generate mod name from mod type and timestamp.
    
    Args:
        mod_type: Type of mod being executed
        
    Returns:
        Auto-generated mod name
    """
    import datetime
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    return f"{mod_type}_{timestamp}"


def _resolve_mod_parameters(mod_type: str, params: Dict[str, Any], mod_name: str) -> Dict[str, Any]:
    """
    Resolve parameters using project defaults and job parameters.
    
    Registry mod defaults are applied later during validation step.
    
    Args:
        mod_type: Type of mod being executed
        params: Raw parameters from caller
        mod_name: Name of mod instance
        
    Returns:
        Resolved parameters dictionary
        
    Raises:
        RuntimeError: If parameter resolution fails
    """
    try:
        resolver = create_resolver()
        
        # Project defaults + job params (registry defaults applied during validation)
        resolved_params = resolver.resolve_mod_params(
            mod_name=mod_type,
            job_params=params
        )
        
        return resolved_params
        
    except Exception as e:
        raise RuntimeError(f"Parameter resolution failed: {e}")
    

def _execute_mod_function(mod_info: Dict[str, Any], validated_params: Dict[str, Any], mod_name: str) -> Dict[str, Any]:
    """
    Execute mod function with validated parameters.
    
    Args:
        mod_info: Mod information from registry
        validated_params: Validated parameters
        mod_name: Name of mod instance
        
    Returns:
        ModResult dictionary
        
    Raises:
        RuntimeError: If mod execution fails
    """
    mod_type = mod_info.get('type', 'unknown')
    mod_logger = setup_logger(f"sdk.{mod_type}.execution", mod_type, mod_name)
    
    try:
        # Import the mod module
        module_path = mod_info['module_path']
        mod_logger.info(f"Importing mod module: {module_path}")
        mod_module = importlib.import_module(module_path)
        
        # Validate mod structure
        if not hasattr(mod_module, 'run'):
            return validation_error(mod_name, f"Mod {module_path} missing required 'run' function")
        
        run_func = mod_module.run
        if not callable(run_func):
            return validation_error(mod_name, f"Mod {module_path} 'run' must be callable")
        
        # Add mod metadata to validated params
        params_with_meta = validated_params.copy()
        params_with_meta['_mod_name'] = mod_name
        params_with_meta['_mod_type'] = mod_type
        
        # Execute mod function
        mod_logger.info(f"Executing mod function", extra={"params_count": len(validated_params)})
        result = run_func(params_with_meta)
        
        # Validate result structure
        if not isinstance(result, dict):
            return runtime_error(mod_name, f"Mod must return a dictionary, got {type(result)}")
        
        # Validate required result fields
        required_fields = [
            'status', 'execution_time', 'exit_code', 'metrics', 
            'artifacts', 'globals', 'warnings', 'errors', 'logs'
        ]
        missing_fields = [field for field in required_fields if field not in result]
        if missing_fields:
            return runtime_error(mod_name, f"Result missing required fields: {missing_fields}")
        
        if result['status'] not in ('success', 'warning', 'error'):
            return runtime_error(mod_name, f"Invalid status: {result['status']}")
        
        # Update result with mod information
        result['logs']['mod_name'] = mod_name
        result['logs']['mod_type'] = mod_type
        
        mod_logger.info(f"Mod execution completed", extra={
            "status": result['status'],
            "execution_time": result['execution_time'],
            "exit_code": result['exit_code']
        })
        
        return result
        
    except ImportError as e:
        mod_logger.error(f"Failed to import mod: {e}", exc_info=True)
        return validation_error(mod_name, f"Cannot import mod {module_path}: {e}")
    except Exception as e:
        mod_logger.error(f"Mod execution failed: {e}", exc_info=True)
        return runtime_error(mod_name, f"Mod execution failed: {e}")


def _validate_mod_execution_inputs(mod_type: str, params: Dict[str, Any], mod_name: str) -> None:
    """
    Validate inputs for mod execution.
    
    Args:
        mod_type: Type of mod to execute
        params: Parameters for the mod
        mod_name: Unique name for this mod instance
        
    Raises:
        ValueError: If inputs are invalid
    """
    if not mod_type or not isinstance(mod_type, str):
        raise ValueError("mod_type must be a non-empty string")
    
    if not isinstance(params, dict):
        raise ValueError("params must be a dictionary")
    
    if not mod_name or not isinstance(mod_name, str):
        raise ValueError("mod_name must be a non-empty string")


def run_mod(mod_type: str, params: Dict[str, Any], mod_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Execute a DataPy mod with complete parameter validation and execution orchestration.
    
    Args:
        mod_type: Mod type identifier (looked up in registry)
        params: Parameters for the mod
        mod_name: Unique name for this mod instance (auto-generated if None)
        
    Returns:
        ModResult dictionary with execution results
    """
    try:
        # Clean and validate inputs
        mod_type = mod_type.strip()
        
        # Auto-generate mod_name if not provided
        if mod_name is None:
            mod_name = _auto_generate_mod_name(mod_type)
        else:
            mod_name = mod_name.strip()
        
        _validate_mod_execution_inputs(mod_type, params, mod_name)
        
        # Start execution logging
        logger.info(f"Starting mod execution: {mod_name} ({mod_type})")
        
        # 1. Get mod info from registry (just lookup)
        registry = get_registry()
        try:
            mod_info = registry.get_mod_info(mod_type)
            logger.info(f"Found mod in registry: {mod_type}")
        except ValueError as e:
            suggestion = f"python -m datapy register-mod <module_path>"
            return validation_error(mod_name, f"{e}. Register it with: {suggestion}")
        
        # 2. Resolve parameters (project defaults + job params only)
        try:
            resolved_params = _resolve_mod_parameters(mod_type, params, mod_name)
            logger.debug(f"Parameters resolved", extra={"param_count": len(resolved_params)})
        except RuntimeError as e:
            return validation_error(mod_name, str(e))
        
        # 3. Context variable substitution (NEW - replaces global config substitution)
        try:
            substituted_params = substitute_context_variables(resolved_params)
            logger.debug(f"Context substitution completed")
        except (ValueError, RuntimeError) as e:
            return validation_error(mod_name, f"Context substitution failed: {e}")
        
        # 4. Validate parameters using JSON Schema
        try:
            validated_params = validate_mod_parameters(mod_info, substituted_params)
            logger.info(f"Parameters validated successfully")
        except ValueError as e:
            return validation_error(mod_name, str(e))
        
        # 5. Execute mod function
        result = _execute_mod_function(mod_info, validated_params, mod_name)
        
        return result
        
    except ValueError as e:
        return validation_error(mod_name or "unknown", str(e))
    except RuntimeError as e:
        return runtime_error(mod_name or "unknown", str(e))
    except Exception as e:
        return runtime_error(mod_name or "unknown", f"Unexpected error: {e}")

