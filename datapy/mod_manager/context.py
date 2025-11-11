"""
Context file management for variable substitution in DataPy framework.

Provides eager-loading context from JSON files for ${} variable substitution
with thread-safe runtime context overrides.
"""

import json
import re
import threading
import warnings
from pathlib import Path
from typing import Dict, Any, Optional
import sys

from .logger import setup_logger

logger = setup_logger(__name__)

# Global context storage
_context_file_path: Optional[str] = None
_context_data: Optional[Dict[str, Any]] = None
_substitution_pattern = re.compile(r'\$\{([^}]+)\}')

# Thread-local storage for runtime context
_thread_local = threading.local()


def _get_script_directory() -> Path:
    """
    Get the directory of the main script being executed.
    
    Uses sys.argv[0] to determine the script location, which works for
    direct script execution and when scripts import each other within
    the same project (since they share context.json).
    
    Returns:
        Path to the directory containing the main script
    """
    try:
        main_script = Path(sys.argv[0]).resolve()
        return main_script.parent
    except (IndexError, OSError):
        # Fallback to current working directory if sys.argv[0] fails
        logger.warning("Could not determine script directory from sys.argv[0], using CWD")
        return Path.cwd()


def setup_context(file_path: str) -> None:
    """
    Load context from file immediately (fail-fast, eager loading).
    
    Relative paths are resolved from the script's directory (sys.argv[0] location).
    Absolute paths are used as-is.
    
    Args:
        file_path: Path to context JSON file (relative or absolute)
        
    Raises:
        ValueError: If file_path is empty
        RuntimeError: If context file cannot be loaded
        
    Examples:
        # Relative path - resolved from script directory
        setup_context("context.json")
        setup_context("config/production.json")
        
        # Absolute path - used as-is
        setup_context("/full/path/to/context.json")
    """
    global _context_file_path, _context_data
    
    if not file_path or not isinstance(file_path, str):
        raise ValueError("file_path must be a non-empty string")
    
    file_path = file_path.strip()
    
    # Resolve relative paths from script directory
    context_path = Path(file_path)
    if not context_path.is_absolute():
        script_dir = _get_script_directory()
        context_path = script_dir / file_path
        logger.debug(f"Resolving relative context path from script directory: {script_dir}")
    
    # Validate path
    if not context_path.exists():
        raise RuntimeError(f"Context file not found: {context_path}")
    
    if not context_path.is_file():
        raise RuntimeError(f"Context path is not a file: {context_path}")
    
    # EAGER LOAD - fail fast
    try:
        with open(context_path, 'r', encoding='utf-8') as f:
            _context_data = json.load(f)
        
        if not isinstance(_context_data, dict):
            raise RuntimeError(f"Context file must contain a JSON dictionary: {context_path}")
        
        _context_file_path = str(context_path)
        logger.info(f"Context loaded: {len(_context_data)} keys from {context_path}")
        
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON in context file {context_path}: {e}")
    except PermissionError as e:
        raise RuntimeError(f"Cannot read context file {context_path}: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed to load context file {context_path}: {e}")


def set_context(file_path: str) -> None:
    """
    DEPRECATED: Use setup_context() instead.
    
    Set context file path for variable substitution.
    This function is maintained for backward compatibility.
    """
    warnings.warn(
        "set_context() is deprecated, use setup_context() instead",
        DeprecationWarning,
        stacklevel=2
    )
    setup_context(file_path)


def clear_context() -> None:
    """Clear context file path and cached data."""
    global _context_file_path, _context_data
    
    _context_file_path = None
    _context_data = None
    
    logger.debug("Context cleared")


def get_context(key_path: str, default: Any = None) -> Any:
    """
    Get value from context (runtime > file), thread-safe.
    
    Checks thread-local runtime context first, then falls back to file context.
    Returns default if key not found in either.
    
    Args:
        key_path: Dot-separated path like 'db.host' or 'app.name'
        default: Value to return if key not found (default: None)
        
    Returns:
        Value from context (preserves original type) or default if not found
        
    Examples:
        # Get values with type preservation
        db_host = get_context("database.host")
        db_port = get_context("database.port")  # Returns int
        debug_mode = get_context("app.debug")  # Returns bool
        
        # With default value
        timeout = get_context("api.timeout", default=30)
    """
    # Check thread-local runtime context first (highest priority)
    if hasattr(_thread_local, 'runtime_context'):
        try:
            value = _thread_local.runtime_context
            for key in key_path.split('.'):
                value = value[key]
            return value
        except (KeyError, TypeError):
            pass  # Fall through to file context
    
    # Check file context
    if _context_data:
        try:
            value = _context_data
            for key in key_path.split('.'):
                value = value[key]
            return value
        except (KeyError, TypeError):
            pass  # Fall through to default
    
    # Key not found in either context
    logger.debug(f"Context key not found: {key_path}, returning default: {default}")
    return default


def update_context(key_path: str, value: Any) -> None:
    """
    Update runtime context (thread-local, writable).
    
    Runtime context takes precedence over file context for the current thread.
    This allows temporary overrides without modifying the context file.
    
    Args:
        key_path: Dot-separated path like 'db.host' or 'app.name'
        value: Value to set (any JSON-serializable type)
        
    Examples:
        # Set runtime overrides
        update_context("database.host", "override-db.example.com")
        update_context("processing.batch_size", 1000)
        update_context("features.experimental", True)
        
        # Nested paths are automatically created
        update_context("new.nested.value", "created")
    """
    # Initialize thread-local runtime context if needed
    if not hasattr(_thread_local, 'runtime_context'):
        _thread_local.runtime_context = {}
    
    keys = key_path.split('.')
    current = _thread_local.runtime_context
    
    # Navigate to parent dict, creating nested dicts as needed
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]
    
    # Set the final value
    current[keys[-1]] = value
    logger.debug(f"Runtime context updated: {key_path} = {value}")

def clear_runtime_context() -> None:
    """Clear runtime (thread-local) overrides for the current thread."""
    if hasattr(_thread_local, 'runtime_context'):
        _thread_local.runtime_context.clear()
        logger.debug("Runtime context cleared for current thread")


def substitute_context_variables(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Substitute ${} variables in parameters using context data.
    
    Uses merged context: runtime context overrides file context.
    Performs recursive substitution on all string values in the params dictionary.
    
    Args:
        params: Parameters dictionary to process
        
    Returns:
        Dictionary with context variables substituted
        
    Raises:
        ValueError: If variable substitution fails
    """
    if not _context_data:
        # No context set - return params as-is
        return params.copy()
    
    # Check if any substitution is actually needed
    if not _needs_substitution(params):
        logger.debug("No ${} patterns found - skipping context substitution")
        return params.copy()
    
    # Merge contexts: file context + runtime overrides
    merged_context = _context_data.copy()
    
    if hasattr(_thread_local, 'runtime_context'):
        _deep_merge(merged_context, _thread_local.runtime_context)
    
    # Perform substitution
    try:
        return _substitute_recursive(params, merged_context)
    except Exception as e:
        raise ValueError(f"Context variable substitution failed: {e}")


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> None:
    """
    Deep merge override dict into base dict (in-place).
    
    Args:
        base: Base dictionary to merge into
        override: Override dictionary to merge from
    """
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value


def _needs_substitution(obj: Any) -> bool:
    """
    Check if any substitution is needed in the object.
    
    Args:
        obj: Object to check for ${} patterns
        
    Returns:
        True if substitution patterns found
    """
    if isinstance(obj, str):
        return bool(_substitution_pattern.search(obj))
    elif isinstance(obj, dict):
        return any(_needs_substitution(v) for v in obj.values())
    elif isinstance(obj, list):
        return any(_needs_substitution(item) for item in obj)
    else:
        return False


def _substitute_recursive(obj: Any, context: Dict[str, Any]) -> Any:
    """
    Recursively substitute variables in an object.
    
    Args:
        obj: Object to process
        context: Context data for substitution
        
    Returns:
        Object with variables substituted
    """
    if isinstance(obj, str):
        return _substitute_string(obj, context)
    elif isinstance(obj, dict):
        return {k: _substitute_recursive(v, context) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_substitute_recursive(item, context) for item in obj]
    else:
        return obj


def _substitute_string(text: str, context: Dict[str, Any]) -> Any:
    """
    Substitute variables in a string, preserving types for pure substitutions.
    
    Args:
        text: String to process
        context: Context data for substitution
        
    Returns:
        - Original type if text is exactly "${var.key}" 
        - String if text contains mixed content
        
    Raises:
        ValueError: If variable substitution fails
    """
    # Check if entire string is single variable
    if _is_pure_variable_substitution(text):
        # Extract variable and return original type
        var_path = re.match(r'^\$\{([^}]+)\}$', text).group(1)
        return _get_context_value(var_path, context)
    else:
        # Mixed content - substitute and return string
        def replace_var(match):
            var_path = match.group(1)
            value = _get_context_value(var_path, context)
            return str(value)
        
        return _substitution_pattern.sub(replace_var, text)


def _is_pure_variable_substitution(text: str) -> bool:
    """Check if string is exactly one variable like '${var.key}' with no other content."""
    if not isinstance(text, str):
        return False
    pattern = r'^\$\{([^}]+)\}$'
    return bool(re.match(pattern, text))


def _get_context_value(var_path: str, context: Dict[str, Any]) -> Any:
    """
    Get context value preserving original type.
    
    Args:
        var_path: Variable path like 'db.port'
        context: Context data
        
    Returns:
        Original typed value from context
        
    Raises:
        ValueError: If variable path not found
    """
    try:
        value = context
        for key in var_path.split('.'):
            if not isinstance(value, dict):
                raise KeyError(f"Cannot access key '{key}' on non-dict value")
            value = value[key]
        return value
    except (KeyError, TypeError) as e:
        raise ValueError(f"Context variable not found: ${{{var_path}}} - {e}")


def get_context_info() -> Dict[str, Any]:
    """
    Get information about current context configuration.
    
    Returns:
        Dictionary with context status information
    """
    return {
        "context_file": _context_file_path,
        "context_loaded": _context_data is not None,
        "context_keys": list(_context_data.keys()) if _context_data else None
    }