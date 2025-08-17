"""
Python SDK for DataPy framework.

Provides programmatic interface for running mods with automatic logging setup,
parameter resolution, and global configuration management.
"""

import importlib
import inspect
import os
import atexit
from typing import Dict, Any, Optional
from .logger import start_execution, finalize_execution, get_current_context, setup_logger
from .params import create_resolver
from .result import ModResult, validation_error, runtime_error
from .base import ModMetadata, BaseModParams

# Global configuration storage
_global_config: Dict[str, Any] = {}
_auto_context_started: bool = False

logger = setup_logger(__name__)


def set_global_config(config: Dict[str, Any]) -> None:
    """
    Set global configuration for the current Python execution session.
    
    Args:
        config: Global configuration dictionary
        
    Example:
        set_global_config({
            "base_path": "/data/2024-08-12",
            "log_level": "DEBUG",
            "log_path": "/logs/etl/"
        })
    """
    global _global_config
    
    _global_config.update(config)
    logger.info("Global configuration set", extra={"config": config})


def get_global_config() -> Dict[str, Any]:
    """
    Get the current global configuration.
    
    Returns:
        Copy of global configuration dictionary
    """
    return _global_config.copy()


def _detect_script_name() -> str:
    """
    Detect the main script filename using call stack inspection.
    
    Returns:
        Script filename or fallback name for interactive/edge cases
    """
    frame = inspect.currentframe()
    try:
        while frame:
            filename = frame.f_code.co_filename
            # Skip framework files and interactive contexts
            if (filename != __file__ and 
                not filename.endswith(('sdk.py', 'cli.py', 'logger.py', 'params.py', 'result.py', 'base.py')) and
                not filename.startswith('<') and  # Skip <stdin>, <console>, etc.
                not 'site-packages' in filename):  # Skip library files
                return os.path.basename(filename)
            frame = frame.f_back
    finally:
        del frame
    
    # Fallback for interactive/edge cases
    return "python_execution.py"


def _ensure_execution_context() -> None:
    """
    Ensure execution context exists, auto-starting if needed.
    
    This handles the automatic logging setup for SDK usage where users
    just call run_mod() without explicit context management.
    """
    global _auto_context_started
    
    if get_current_context() is None:
        # Auto-detect script name and start execution context
        script_name = _detect_script_name()
        
        # Merge global config for logging setup
        log_config = {}
        if _global_config:
            # Extract logging configuration from globals
            if "log_level" in _global_config:
                log_config["log_level"] = _global_config["log_level"]
            if "log_path" in _global_config:
                log_config["log_path"] = _global_config["log_path"]
            if "log_format" in _global_config:
                log_config["log_format"] = _global_config["log_format"]
        
        # Start execution context
        start_execution(script_name, log_config if log_config else None)
        _auto_context_started = True
        
        # Register cleanup when Python process exits
        atexit.register(finalize_execution)
        
        logger.info(f"Auto-started execution context for script: {script_name}")


def _resolve_mod_path(mod_identifier: str) -> str:
    """
    Resolve mod identifier to full module path.
    
    Args:
        mod_identifier: Either full path (datapy.mods.sources.csv_reader) 
                       or just mod name (csv_reader)
    
    Returns:
        Full module path
        
    Raises:
        ImportError: If mod cannot be found
    """
    # If it contains dots, assume it's already a full path
    if '.' in mod_identifier:
        return mod_identifier
    
    # Search for mod by name in standard locations
    search_paths = [
        f"datapy.mods.sources.{mod_identifier}",
        f"datapy.mods.transformers.{mod_identifier}",
        f"datapy.mods.sinks.{mod_identifier}",
        f"datapy.mods.solos.{mod_identifier}"
    ]
    
    for path in search_paths:
        try:
            importlib.import_module(path)
            logger.debug(f"Resolved mod '{mod_identifier}' to '{path}'")
            return path
        except ImportError:
            continue
    
    # If not found in standard locations, raise error
    raise ImportError(f"Mod '{mod_identifier}' not found in standard locations: {search_paths}")


def run_mod(mod_path: str, params: Dict[str, Any], mod_name: str) -> int:
    """
    Execute a DataPy mod and inject result into caller's namespace as a variable.
    
    This function automatically handles logging setup, parameter resolution,
    execution context management, and creates a variable with the mod result.
    
    Args:
        mod_path: Module path (e.g., "datapy.mods.sources.csv_reader") 
                 or just mod name (e.g., "csv_reader")
        params: Parameters dictionary for the mod
        mod_name: Variable name for storing the result (required, must be valid Python identifier)
        
    Returns:
        Exit code (0=success, 10=warning, 20+=error)
        
    Example:
        # Variable assignment pattern
        exit_code = run_mod("csv_reader", {"input_path": "data.csv"}, "customer_data")
        if exit_code == 0:
            print(f"Processed {customer_data['metrics']['rows_processed']} rows")
            
        # Multiple mods with unique names
        run_mod("csv_reader", {"input_path": "data.csv"}, "raw_data")
        run_mod("data_cleaner", {"strategy": "drop"}, "clean_data")
        run_mod("csv_reader", {"input_path": "backup.csv"}, "backup_data")
    """
    # Validate mod_name
    if not _validate_mod_name(mod_name):
        logger.error(f"Invalid mod_name '{mod_name}': must be valid Python identifier")
        return 20  # VALIDATION_ERROR
    
    # Check for variable conflicts in caller's namespace
    if not _check_variable_safety(mod_name):
        logger.error(f"mod_name '{mod_name}' would overwrite existing variable")
        return 20  # VALIDATION_ERROR
    
    # Ensure execution context exists (auto-start if needed)
    _ensure_execution_context()
    
    # Check for uniqueness within current execution
    if not _check_execution_uniqueness(mod_name):
        logger.error(f"mod_name '{mod_name}' already used in this execution")
        return 20  # VALIDATION_ERROR
    
    try:
        # Resolve mod path if just name provided
        resolved_mod_path = _resolve_mod_path(mod_path)
        mod_type = resolved_mod_path.split('.')[-1]
    except ImportError as e:
        result = validation_error(mod_name, str(e))
        _inject_variable(mod_name, result)
        return result['exit_code']
    
    try:
        # Dynamic module loading
        mod_module = importlib.import_module(resolved_mod_path)
        
        # Validate required components exist
        if not hasattr(mod_module, 'run'):
            result = validation_error(mod_name, f"Mod {resolved_mod_path} missing required 'run' function")
            _inject_variable(mod_name, result)
            return result['exit_code']
        
        if not hasattr(mod_module, 'METADATA'):
            result = validation_error(mod_name, f"Mod {resolved_mod_path} missing required 'METADATA'")
            _inject_variable(mod_name, result)
            return result['exit_code']
        
        if not hasattr(mod_module, 'Params'):
            result = validation_error(mod_name, f"Mod {resolved_mod_path} missing required 'Params' class")
            _inject_variable(mod_name, result)
            return result['exit_code']
        
        # Validate metadata
        metadata = mod_module.METADATA
        if not isinstance(metadata, ModMetadata):
            result = validation_error(mod_name, f"Mod {resolved_mod_path} METADATA must be ModMetadata instance")
            _inject_variable(mod_name, result)
            return result['exit_code']
        
        # Validate Params class inheritance
        params_class = mod_module.Params
        if not issubclass(params_class, BaseModParams):
            result = validation_error(mod_name, f"Mod {resolved_mod_path} Params must inherit from BaseModParams")
            _inject_variable(mod_name, result)
            return result['exit_code']
        
        # Validate run function signature
        run_func = mod_module.run
        sig = inspect.signature(run_func)
        if len(sig.parameters) != 1:
            result = validation_error(mod_name, f"Mod {resolved_mod_path} run function must accept exactly one parameter")
            _inject_variable(mod_name, result)
            return result['exit_code']
        
        # Parameter resolution
        resolver = create_resolver()
        
        # Get mod defaults if available
        mod_defaults = {}
        if hasattr(params_class, '__fields__'):
            # Extract default values from Pydantic fields
            for field_name, field_info in params_class.__fields__.items():
                if field_name != '_metadata' and field_info.default is not ...:
                    mod_defaults[field_name] = field_info.default
        
        # Resolve parameters using inheritance chain
        resolved_params = resolver.resolve_mod_params(
            mod_name=mod_type,  # Use mod_type for parameter resolution
            job_params=params,
            mod_defaults=mod_defaults,
            globals_override=_global_config
        )
        
        # Create and validate parameter instance
        try:
            param_instance = params_class(_metadata=metadata, **resolved_params)
        except Exception as e:
            result = validation_error(mod_name, f"Parameter validation failed: {e}")
            _inject_variable(mod_name, result)
            return result['exit_code']
        
        # Setup mod-specific logger (this gets the next mod instance automatically)
        mod_logger = setup_logger(f"{resolved_mod_path}.execution", mod_type=mod_type)
        mod_logger.info(f"Starting mod execution", extra={"params": resolved_params})
        
        # Execute the mod
        try:
            result = run_func(param_instance)
            
            # Validate result format
            if not isinstance(result, dict):
                result = runtime_error(mod_name, f"Mod {resolved_mod_path} must return a dictionary")
                _inject_variable(mod_name, result)
                return result['exit_code']
            
            required_fields = ['status', 'execution_time', 'exit_code', 'metrics', 'artifacts', 'globals', 'warnings', 'errors', 'logs']
            missing_fields = [field for field in required_fields if field not in result]
            if missing_fields:
                result = runtime_error(mod_name, f"Mod {resolved_mod_path} result missing required fields: {missing_fields}")
                _inject_variable(mod_name, result)
                return result['exit_code']
            
            if result['status'] not in ('success', 'warning', 'error'):
                result = runtime_error(mod_name, f"Mod {resolved_mod_path} returned invalid status: {result['status']}")
                _inject_variable(mod_name, result)
                return result['exit_code']
            
            # Update result with mod identification
            result['logs']['mod_name'] = mod_name
            result['logs']['mod_type'] = mod_type
            
            mod_logger.info(f"Mod execution completed", extra={
                "status": result['status'],
                "execution_time": result['execution_time'],
                "exit_code": result['exit_code']
            })
            
            # Inject result into caller's namespace
            _inject_variable(mod_name, result)
            return result['exit_code']
            
        except Exception as e:
            mod_logger.error(f"Mod execution failed: {e}", exc_info=True)
            result = runtime_error(mod_name, f"Mod execution failed: {e}")
            _inject_variable(mod_name, result)
            return result['exit_code']
    
    except ImportError as e:
        result = validation_error(mod_name, f"Cannot import mod {resolved_mod_path}: {e}")
        _inject_variable(mod_name, result)
        return result['exit_code']
    except Exception as e:
        result = runtime_error(mod_name, f"Unexpected error loading mod {resolved_mod_path}: {e}")
        _inject_variable(mod_name, result)
        return result['exit_code']


def _validate_mod_name(mod_name: str) -> bool:
    """
    Validate mod_name is a valid Python identifier.
    
    Args:
        mod_name: Proposed variable name
        
    Returns:
        True if valid, False otherwise
    """
    if not isinstance(mod_name, str):
        return False
    
    # Check if it's a valid Python identifier
    if not mod_name.isidentifier():
        return False
    
    # Check against Python keywords
    import keyword
    if keyword.iskeyword(mod_name):
        return False
    
    # Check against common builtins to avoid conflicts
    dangerous_names = {
        'print', 'len', 'str', 'int', 'float', 'list', 'dict', 'set', 'tuple',
        'range', 'open', 'file', 'input', 'output', 'type', 'object', 'class',
        'import', 'from', 'as', 'with', 'try', 'except', 'finally', 'raise',
        'def', 'return', 'yield', 'lambda', 'global', 'nonlocal', 'locals', 'globals'
    }
    
    if mod_name in dangerous_names:
        return False
    
    return True


def _check_variable_safety(mod_name: str) -> bool:
    """
    Check if creating this variable would overwrite something important.
    
    Args:
        mod_name: Proposed variable name
        
    Returns:
        True if safe to create, False if would overwrite
    """
    # Get caller's frame (two levels up: _check_variable_safety <- run_mod <- user_code)
    frame = inspect.currentframe().f_back.f_back
    
    try:
        # Check if variable already exists in caller's namespace
        if mod_name in frame.f_globals:
            existing = frame.f_globals[mod_name]
            # Allow overwriting previous mod results (dict with 'status' key)
            if isinstance(existing, dict) and 'status' in existing:
                return True
            else:
                logger.warning(f"Variable '{mod_name}' already exists and is not a mod result")
                return False
        
        return True
    finally:
        del frame


def _check_execution_uniqueness(mod_name: str) -> bool:
    """
    Check if mod_name is unique within current execution.
    
    Args:
        mod_name: Proposed mod name
        
    Returns:
        True if unique, False if already used
    """
    context = get_current_context()
    if context is None:
        return True
    
    # Store used mod names in context
    if not hasattr(context, 'used_mod_names'):
        context.used_mod_names = set()
    
    if mod_name in context.used_mod_names:
        return False
    
    context.used_mod_names.add(mod_name)
    return True


def _inject_variable(mod_name: str, result: Dict[str, Any]) -> None:
    """
    Inject result into caller's namespace as a variable.
    
    Args:
        mod_name: Variable name to create
        result: Mod result dictionary to store
    """
    # Get caller's frame (two levels up: _inject_variable <- run_mod <- user_code)
    frame = inspect.currentframe().f_back.f_back
    
    try:
        frame.f_globals[mod_name] = result
        logger.debug(f"Injected mod result into variable '{mod_name}'")
    except Exception as e:
        logger.error(f"Failed to inject variable '{mod_name}': {e}")
    finally:
        del frame