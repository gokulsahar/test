"""
Python SDK for DataPy framework with registry-based mod execution.

Provides clean API for mod execution with parameter validation and execution
orchestration. No file management - simple console output for shell script capture.
"""

import argparse
import importlib
from typing import Dict, Any, Optional, Union, Tuple
import uuid
from pathlib import Path
import os
import sys
import traceback         
import contextlib

from .context import (
    setup_context as _setup_context,
    clear_context as _clear_context,
    get_context,      
    update_context,
    clear_runtime_context,  
    substitute_context_variables,
    get_context_info as _get_context_info
)
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
    _setup_context(file_path)


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
    
    Automatically detects project configuration from script location (sys.argv[0]).
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
        # Auto-detect project config from script location
        resolver = create_resolver()  # No search_path needed - auto-detected!
        
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
        mod_logger.debug(f"Importing mod module: {module_path}")
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
        mod_logger.debug("Executing mod function", extra={"params_count": len(validated_params)})
        result = run_func(params_with_meta)
        
        # Validate result structure
        if not isinstance(result, dict):
            return runtime_error(mod_name, f"Mod must return a dictionary, got {type(result)}")
        
        # Validate required result fields
        required_fields = [
            'status', 'exit_code', 'metrics', 
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
        
        mod_logger.debug("Mod execution completed", extra={
            "status": result['status'],
            "metrics": result["metrics"],
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
    
    This is the main entry point for mod execution. It handles parameter resolution,
    validation, and orchestrates execution with automatic monitoring.
    
    Args:
        mod_type: Mod type identifier (looked up in registry)
        params: Parameters for the mod
        mod_name: Unique name for this mod instance (auto-generated if None)
        
    Returns:
        ModResult dictionary with execution results and monitoring metrics
        
    Examples:
        # Basic execution
        result = run_mod("csv_reader", {"file_path": "data.csv"})
        
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
        logger.debug(f"Starting mod execution: {mod_name} ({mod_type})")

        # 1. Get mod info from registry (just lookup)
        registry = get_registry()
        try:
            mod_info = registry.get_mod_info(mod_type)
            logger.debug(f"Found mod in registry: {mod_type}")
        except ValueError as e:
            suggestion = "python -m datapy register-mod <module_path>"
            return validation_error(mod_name, f"{e}. Register it with: {suggestion}")
        
        # 2. Resolve parameters (project defaults + job params only)
        try:
            resolved_params = _resolve_mod_parameters(mod_type, params)
            logger.debug("Parameters resolved", extra={"param_count": len(resolved_params)})
        except RuntimeError as e:
            return validation_error(mod_name, str(e))
        
        # 3. Context variable substitution
        try:
            substituted_params = substitute_context_variables(resolved_params)
            logger.debug("Context substitution completed")
        except (ValueError, RuntimeError) as e:
            return validation_error(mod_name, f"Context substitution failed: {e}")
        
        # 4. Validate parameters using JSON Schema (CORRECT - old working version)
        try:
            validated_params = validate_mod_parameters(mod_info, substituted_params)
            logger.debug("Parameters validated successfully")
        except ValueError as e:
            return validation_error(mod_name, str(e))
        
        # 5. Execute mod function with monitoring (NEW - added execution monitoring)
        return _execute_mod_function(mod_info, validated_params, mod_name)

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
            "log_level": str,          # Log level value
            "log_provided": bool,      # Was --log-level explicitly provided?
            "context_path": str,       # Context file path  
            "context_provided": bool,  # Was --context explicitly provided?
            "profile_level": str,      # Profile level value
            "profile_provided": bool   # Was --profile-level explicitly provided?
        }
    """
    try:
        parser = argparse.ArgumentParser(add_help=False)  # Don't interfere with main script help
        parser.add_argument('--log-level', 
                           choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
        parser.add_argument('--context', 
                           help='Path to context JSON file for variable substitution')
        parser.add_argument('--profile-level',
                           choices=['off', 'low', 'medium', 'high'],
                           type=str.lower,
                           help='Profiling detail level (default: off)')
        
        # Parse known args only, ignore everything else
        args, _ = parser.parse_known_args()
        
        return {
            "log_level": args.log_level.upper() if args.log_level else "INFO",
            "log_provided": args.log_level is not None,
            "context_path": args.context if args.context else "",
            "context_provided": args.context is not None,
            "profile_level": args.profile_level.lower() if args.profile_level else "off",
            "profile_provided": args.profile_level is not None
        } 
        
    except Exception:
        # Any failure - return safe defaults
        return {
            "log_level": "INFO",
            "log_provided": False,
            "context_path": "",
            "context_provided": False,
            "profile_level": "off",
            "profile_provided": False
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
    
    # Set context using existing SDK wrapper
    set_context(final_context_path)
    
    
def get_context_value(variable_path: str) -> Any:
    """
    DEPRECATED: Use get_context() instead.
    
    Get a context value by path, loading context if needed.
    
    This function is maintained for backward compatibility.
    New code should use get_context() which supports defaults and runtime context.
    
    Args:
        variable_path: Dot-separated path like 'feature.enabled' or 'db.host'
        
    Returns:
        Value from context (preserves original type: str, int, bool, list, dict, etc.)
        
    Raises:
        RuntimeError: If no context file is set
        ValueError: If variable_path is invalid or not found in context
        
    Examples:
        # Old style (deprecated)
        db_host = get_context_value("database.host")
        
        # New style (recommended)
        db_host = get_context("database.host")
    """
    import warnings
    warnings.warn(
        "get_context_value() is deprecated, use get_context() instead",
        DeprecationWarning,
        stacklevel=2
    )
    from .context import _context_file_path
    if not _context_file_path:
        raise RuntimeError("No context loaded - call setup_context() first")
    
    if not variable_path or not isinstance(variable_path, str):
        raise ValueError("variable_path must be a non-empty string")
    
    variable_path = variable_path.strip()
    if not variable_path:
        raise ValueError("variable_path cannot be empty or whitespace only")
    
    # Use new get_context() function
    value = get_context(variable_path)
    
    if value is None:
        raise ValueError(f"Context variable not found: ${{{variable_path}}}")
    
    return value

def _resolve_job_path(job_script: Union[str, Path]) -> Path:
    p = Path(job_script).resolve()
    if not p.exists():
        raise FileNotFoundError(f"Job script not found: {job_script}")
    if not p.is_file():
        raise IsADirectoryError(f"Job script path is not a file: {job_script}")
    if p.suffix != ".py":
        # Keep as warning, behavior remains permissive
        logger.warning("Job script does not end with .py: %s", p)
    return p


def _apply_thread_context(job_dir: str, context_vars: Dict[str, Any]) -> None:
    update_context("is_subjob", True)
    update_context("job_dir", job_dir)
    for k, v in context_vars.items():
        update_context(k, v)

# --------------------------- module loading ---------------------------

def _load_job_module(unique_name: str, job_path: Path):
    spec = importlib.util.spec_from_file_location(unique_name, str(job_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load job script: {job_path}")
    module = importlib.util.module_from_spec(spec)
    # register early so relative imports inside the module can see it by name
    sys.modules[unique_name] = module
    module.__file__ = str(job_path)
    module.__package__ = None  # treat as a top-level script
    spec.loader.exec_module(module)
    return module

# --------------------------- module cleanup ---------------------------

def _normalized_prefixes(unique_name: str, job_dir: str) -> Tuple[str, str]:
    """Precompute normalized patterns for membership checks."""
    # Prefix used to match children of the dynamic module
    dotted_prefix = unique_name + "."
    # Normalized directory prefix for __file__ checks
    job_dir_norm = os.path.normcase(os.path.normpath(job_dir)) + os.sep
    return dotted_prefix, job_dir_norm


def _collect_modules_for_removal(unique_name: str, job_dir: str) -> set[str]:
    """
    Return the set of module names to remove:
      - the dynamic module itself
      - any of its children (name startswith '<unique_name>.')
      - any module whose __file__ lives under job_dir (best-effort)
    """
    dotted_prefix, job_dir_norm = _normalized_prefixes(unique_name, job_dir)
    to_delete: set[str] = set()

    # First pass: unique module + children by prefix
    for name in tuple(sys.modules):
        if name == unique_name or name.startswith(dotted_prefix):
            to_delete.add(name)

    # Second pass: anything loaded from job_dir (best-effort)
    for name, mod in tuple(sys.modules.items()):
        if name in to_delete or mod is None:
            continue
        f = getattr(mod, "__file__", None)
        if not f:
            continue
        try:
            if os.path.normcase(os.path.normpath(f)).startswith(job_dir_norm):
                to_delete.add(name)
        except Exception:
            # Non-fatal: path normalization oddities
            continue

    return to_delete


def _invoke_optional_cleanup(unique_name: str) -> None:
    """If the loaded module exposes cleanup(), invoke it (best-effort)."""
    mod = sys.modules.get(unique_name)
    if not mod:
        return
    cleanup = getattr(mod, "cleanup", None)
    if callable(cleanup):
        try:
            cleanup()
        except Exception:
            logger.warning("sub-job cleanup() raised; continuing", exc_info=True)


def _remove_modules(module_names: set[str]) -> None:
    """Remove modules from sys.modules; log (but do not raise) on failures."""
    for name in module_names:
        try:
            del sys.modules[name]
        except Exception:
            logger.warning("Failed to remove module %s from sys.modules", name, exc_info=True)


def _cleanup_modules(unique_name: str, job_dir: str) -> None:
    """
    Remove the dynamically-loaded module, its children, and any module whose
    __file__ resides under job_dir (best-effort). Kept intentionally defensive.
    """
    try:
        to_delete = _collect_modules_for_removal(unique_name, job_dir)
        _invoke_optional_cleanup(unique_name)
        _remove_modules(to_delete)
    except Exception:
        # Never let cleanup failures bubble up and poison host threads
        logger.warning("Module cleanup encountered an error", exc_info=True)

# --------------------------- execution helpers ---------------------------

def _error_payload(message: str, etype: str, trace: bool = False) -> Dict[str, Any]:
    payload = {"status": "error", "error": {"message": message, "type": etype}}
    if trace:
        payload["error"]["trace"] = traceback.format_exc()
    return payload


def _execute_main(module) -> Dict[str, Any]:
    """
    Execute module.main(); ignore its return (state flows through context).
    Re-raise KeyboardInterrupt; contain SystemExit to avoid killing the host thread.  # NOSONAR
    """
    main = getattr(module, "main", None)
    if not callable(main):
        raise AttributeError("Job script must define a main() function")

    try:
        _ = main()
        return {"status": "success"}
    except KeyboardInterrupt:
        raise
    except SystemExit as se:  # NOSONAR
        code = getattr(se, "code", None)
        msg = f"Child job attempted to exit (code {code})"
        logger.error(msg, exc_info=True)
        return _error_payload(msg, "SystemExit", trace=False)

# --------------------------- guard utilities ---------------------------

def _guard_no_trace(fn, *args, **kwargs) -> Tuple[Optional[Any], Optional[Dict[str, Any]]]:
    """Call fn; on non-Keyboard exceptions, log and return an error payload without stack trace."""
    try:
        return fn(*args, **kwargs), None
    except KeyboardInterrupt:
        raise
    except Exception as e:
        logger.error("%s failed", getattr(fn, "__name__", "call"), exc_info=True)
        return None, _error_payload(str(e), type(e).__name__, trace=False)


def _guard_trace(fn, *args, **kwargs) -> Tuple[Optional[Any], Optional[Dict[str, Any]]]:
    """Call fn; on non-Keyboard exceptions, log and return an error payload with stack trace."""
    try:
        return fn(*args, **kwargs), None
    except KeyboardInterrupt:
        raise
    except Exception as e:
        logger.error("%s failed", getattr(fn, "__name__", "call"), exc_info=True)
        return None, _error_payload(str(e), type(e).__name__, trace=True)

# --------------------------- lifecycle wrapper ---------------------------

@contextlib.contextmanager
def _module_lifecycle(unique_name: str, job_dir: str):
    try:
        yield
    finally:
        _cleanup_modules(unique_name, job_dir)

# ------------------------------- API ------------------------------------

def run_job(job_script: Union[str, Path], /, *, clear_runtime: bool = True, **context_vars: Any) -> Dict[str, Any]:

    """
    Thread-safe sub-job runner (no cwd/sys.path mutations).
    Contract:
      - On success: {'status': 'success'}
      - On error:   {'status': 'error', 'error': {'message','type','trace?'}}
    All data exchange happens via thread-local context (update_context).
    """
    job_path, err = _guard_no_trace(_resolve_job_path, job_script)
    if err:
        return err

    job_dir = str(job_path.parent)
    try:
        _, err = _guard_no_trace(_apply_thread_context, job_dir, context_vars)
        if err:
            return err

        unique_name = f"subjob_{job_path.stem}_{uuid.uuid4().hex}"

        with _module_lifecycle(unique_name, job_dir):
            module, err = _guard_trace(_load_job_module, unique_name, job_path)
            if err:
                return err
            result, err = _guard_trace(_execute_main, module)
            if err:
                return err
            return result
    finally:
        if clear_runtime:         # By default when the child job finishes the runtime context is cleared, i needed (when child writes something to context that is needed by the parent dont clear it)
            clear_runtime_context()