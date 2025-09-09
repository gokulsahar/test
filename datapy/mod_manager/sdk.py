"""
Python SDK for DataPy framework with registry-based mod execution.

Provides clean API for mod execution with parameter validation and execution
orchestration. No file management - simple console output for shell script capture.
"""

import argparse
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


def _resolve_mod_parameters(mod_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Resolve parameters using project defaults and job parameters.
    
    Registry mod defaults are applied later during validation step.
    
    Args:
        mod_type: Type of mod being executed
        params: Raw parameters from caller
        
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
            resolved_params = _resolve_mod_parameters(mod_type, params)
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


# Command line argument parsing and setup functions

def _parse_common_args() -> Dict[str, Any]:
    """
    Parse common command line arguments for SDK.
    
    Returns:
        Dictionary with parsed arguments and flags:
        {
            "log_level": str,        # Log level value
            "log_provided": bool,    # Was --log-level explicitly provided?
            "context_path": str,     # Context file path  
            "context_provided": bool # Was --context explicitly provided?
        }
    """
    try:
        parser = argparse.ArgumentParser(add_help=False)  # Don't interfere with main script help
        parser.add_argument('--log-level', 
                           choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
        parser.add_argument('--context', 
                           help='Path to context JSON file for variable substitution')
        
        # Parse known args only, ignore everything else
        args, unknown = parser.parse_known_args()
        
        return {
            "log_level": args.log_level.upper() if args.log_level else "INFO",
            "log_provided": args.log_level is not None,
            "context_path": args.context if args.context else "",
            "context_provided": args.context is not None
        }
        
    except Exception:
        # Any failure - return safe defaults
        return {
            "log_level": "INFO",
            "log_provided": False,
            "context_path": "",
            "context_provided": False
        }


def setup_logging(level: str = None, name: str = None) -> Any:
    """
    Hybrid logging setup - sets global level and returns a logger for immediate use.
    
    Convenience wrapper that combines set_log_level() and setup_logger() into one call.
    
    Priority order (command line always wins):
    1. Command line --log-level argument (highest priority)
    2. Explicit level parameter (medium priority)  
    3. Default "INFO" (fallback)
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL). 
               Command line --log-level will override this if present.
        name: Logger name (typically pass __name__ from calling script).
              If None, uses "datapy.user" as fallback.
        
    Returns:
        Configured logger instance for immediate use
    """
    # Parse command line args
    cmd_args = _parse_common_args()
    
    # Determine final level - command line always wins if provided
    if cmd_args["log_provided"]:  # User explicitly provided --log-level
        final_level = cmd_args["log_level"]
    elif level is not None:  # No command line override, use explicit level
        final_level = level
    else:  # No command line, no explicit level
        final_level = "INFO"
    
    # Set global log level (affects ALL logging in framework)
    set_log_level(final_level)
    
    # Determine logger name
    logger_name = name if name else "datapy.user"
    
    # Create and return logger for immediate use
    from .logger import setup_logger
    return setup_logger(logger_name)


def setup_context(context_path: str = None) -> None:
    """
    Hybrid context setup - sets context file with command line override support.
    
    Priority order (command line always wins):
    1. Command line --context argument (highest priority)
    2. Explicit context_path parameter (medium priority)
    3. No context set (fallback)
    
    Args:
        context_path: Path to context JSON file.
                     Command line --context will override this if present.
    """
    # Parse command line args
    cmd_args = _parse_common_args()
    
    # Determine final context path - command line always wins
    if cmd_args["context_provided"]:  # User explicitly provided --context
        final_context_path = cmd_args["context_path"]
    elif context_path is not None:  # No command line override, use explicit path
        final_context_path = context_path
    else:  # No command line, no explicit path - don't set context
        return
    
    # Set context using existing SDK function
    set_context(final_context_path)