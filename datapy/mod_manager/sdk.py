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


def run_mod(mod_path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute a DataPy mod with automatic context management and parameter resolution.
    
    This function automatically handles logging setup, parameter resolution,
    and execution context management. Users can simply call run_mod() without
    worrying about context setup.
    
    Args:
        mod_path: Module path (e.g., "datapy.mods.sources.csv_reader") 
                 or just mod name (e.g., "csv_reader")
        params: Parameters dictionary for the mod
        
    Returns:
        Standardized mod result dictionary
        
    Example:
        # Simple usage - no context management needed
        result = run_mod("csv_reader", {"input_path": "data.csv"})
        
        # Multiple mods - all go to same log file
        result1 = run_mod("csv_reader", {"input_path": "data.csv"})
        result2 = run_mod("data_cleaner", {"strategy": "drop"})
    """
    # Ensure execution context exists (auto-start if needed)
    _ensure_execution_context()
    
    try:
        # Resolve mod path if just name provided
        resolved_mod_path = _resolve_mod_path(mod_path)
        mod_name = resolved_mod_path.split('.')[-1]
    except ImportError as e:
        return validation_error(mod_path, str(e))
    
    try:
        # Dynamic module loading
        mod_module = importlib.import_module(resolved_mod_path)
        
        # Validate required components exist
        if not hasattr(mod_module, 'run'):
            return validation_error(mod_name, f"Mod {resolved_mod_path} missing required 'run' function")
        
        if not hasattr(mod_module, 'METADATA'):
            return validation_error(mod_name, f"Mod {resolved_mod_path} missing required 'METADATA'")
        
        if not hasattr(mod_module, 'Params'):
            return validation_error(mod_name, f"Mod {resolved_mod_path} missing required 'Params' class")
        
        # Validate metadata
        metadata = mod_module.METADATA
        if not isinstance(metadata, ModMetadata):
            return validation_error(mod_name, f"Mod {resolved_mod_path} METADATA must be ModMetadata instance")
        
        # Validate Params class inheritance
        params_class = mod_module.Params
        if not issubclass(params_class, BaseModParams):
            return validation_error(mod_name, f"Mod {resolved_mod_path} Params must inherit from BaseModParams")
        
        # Validate run function signature
        run_func = mod_module.run
        sig = inspect.signature(run_func)
        if len(sig.parameters) != 1:
            return validation_error(mod_name, f"Mod {resolved_mod_path} run function must accept exactly one parameter")
        
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
            mod_name=mod_name,
            job_params=params,
            mod_defaults=mod_defaults,
            globals_override=_global_config
        )
        
        # Create and validate parameter instance
        try:
            param_instance = params_class(_metadata=metadata, **resolved_params)
        except Exception as e:
            return validation_error(mod_name, f"Parameter validation failed: {e}")
        
        # Setup mod-specific logger (this gets the next mod instance automatically)
        mod_logger = setup_logger(f"{resolved_mod_path}.execution", mod_name=mod_name)
        mod_logger.info(f"Starting mod execution", extra={"params": resolved_params})
        
        # Execute the mod
        try:
            result = run_func(param_instance)
            
            # Validate result format
            if not isinstance(result, dict):
                return runtime_error(mod_name, f"Mod {resolved_mod_path} must return a dictionary")
            
            required_fields = ['status', 'execution_time', 'exit_code', 'metrics', 'artifacts', 'globals', 'warnings', 'errors', 'logs']
            missing_fields = [field for field in required_fields if field not in result]
            if missing_fields:
                return runtime_error(mod_name, f"Mod {resolved_mod_path} result missing required fields: {missing_fields}")
            
            if result['status'] not in ('success', 'warning', 'error'):
                return runtime_error(mod_name, f"Mod {resolved_mod_path} returned invalid status: {result['status']}")
            
            mod_logger.info(f"Mod execution completed", extra={
                "status": result['status'],
                "execution_time": result['execution_time'],
                "exit_code": result['exit_code']
            })
            
            return result
            
        except Exception as e:
            mod_logger.error(f"Mod execution failed: {e}", exc_info=True)
            return runtime_error(mod_name, f"Mod execution failed: {e}")
    
    except ImportError as e:
        return validation_error(mod_name, f"Cannot import mod {resolved_mod_path}: {e}")
    except Exception as e:
        return runtime_error(mod_name, f"Unexpected error loading mod {resolved_mod_path}: {e}")


def clear_global_config() -> None:
    """
    Clear global configuration (primarily for testing).
    
    Warning: This will reset all global settings.
    """
    global _global_config, _auto_context_started
    _global_config.clear()
    _auto_context_started = False


def is_auto_context_active() -> bool:
    """
    Check if auto-context management is active.
    
    Returns:
        True if SDK auto-started the execution context
    """
    return _auto_context_started