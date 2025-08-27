"""
Python SDK for DataPy framework with registry-based mod execution.

Provides clean API for mod execution with parameter validation and execution
orchestration. No file management - simple console output for shell script capture.
"""

import importlib
from typing import Dict, Any, Optional

from .logger import setup_console_logging, setup_logger, DEFAULT_LOG_CONFIG
from .params import create_resolver
from .result import ModResult, validation_error, runtime_error
from .registry import get_registry
from .parameter_validation import validate_mod_parameters

# Global configuration storage
_global_config: Dict[str, Any] = {}

logger = setup_logger(__name__)


def set_global_config(config: Dict[str, Any]) -> None:
    """
    Set global configuration for SDK execution.
    
    Args:
        config: Configuration dictionary to merge with global config
        
    Raises:
        ValueError: If config is not a dictionary
    """
    global _global_config
    
    if not isinstance(config, dict):
        raise ValueError("config must be a dictionary")
    
    _global_config.update(config)
    
    # Setup console logging with new config
    log_config = DEFAULT_LOG_CONFIG.copy()
    log_config.update(_global_config)
    setup_console_logging(log_config)
    
    logger.debug(f"Updated global config", extra={"config_keys": list(config.keys())})


def get_global_config() -> Dict[str, Any]:
    """
    Get copy of current global configuration.
    
    Returns:
        Copy of global configuration dictionary
    """
    return _global_config.copy()


def clear_global_config() -> None:
    """Clear global configuration (useful for testing)."""
    global _global_config
    _global_config.clear()
    logger.debug("Cleared global config")


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
    Resolve parameters using the inheritance chain.
    
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
        
        # Registry mods don't have built-in defaults
        mod_defaults = {}
        
        resolved_params = resolver.resolve_mod_params(
            mod_name=mod_type,
            job_params=params,
            mod_defaults=mod_defaults,
            globals_override=_global_config
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
        
        # Ensure console logging is setup
        if not _global_config:
            setup_console_logging(DEFAULT_LOG_CONFIG)
        
        # 1. Get mod info from registry (just lookup)
        registry = get_registry()
        try:
            mod_info = registry.get_mod_info(mod_type)
            logger.info(f"Found mod in registry: {mod_type}")
        except ValueError as e:
            suggestion = f"python -m datapy register-mod <module_path>"
            return validation_error(mod_name, f"{e}. Register it with: {suggestion}")
        
        # 2. Resolve parameters (project defaults, globals, etc.)
        try:
            resolved_params = _resolve_mod_parameters(mod_type, params, mod_name)
            logger.debug(f"Parameters resolved", extra={"param_count": len(resolved_params)})
        except RuntimeError as e:
            return validation_error(mod_name, str(e))
        
        # 3. Validate parameters using JSON Schema (common for CLI and SDK)
        try:
            validated_params = validate_mod_parameters(mod_info, resolved_params)
            logger.info(f"Parameters validated successfully")
        except ValueError as e:
            return validation_error(mod_name, str(e))
        
        # 4. Execute mod function (moved from registry to SDK)
        result = _execute_mod_function(mod_info, validated_params, mod_name)
        
        return result
        
    except ValueError as e:
        return validation_error(mod_name or "unknown", str(e))
    except RuntimeError as e:
        return runtime_error(mod_name or "unknown", str(e))
    except Exception as e:
        return runtime_error(mod_name or "unknown", f"Unexpected error: {e}")

