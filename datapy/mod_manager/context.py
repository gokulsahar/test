"""
Context file management for variable substitution in DataPy framework.

Provides lazy-loading context from JSON files for ${} variable substitution
without any integration with global config - context is the single source
for all variable substitution needs.
"""

import json
import re
from pathlib import Path
from typing import Dict, Any, Optional, Set
import sys
from pathlib import Path

from .logger import setup_logger

logger = setup_logger(__name__)

# Global context storage
_context_file_path: Optional[str] = None
_context_data: Optional[Dict[str, Any]] = None
_substitution_pattern = re.compile(r'\$\{([^}]+)\}')

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


def set_context(file_path: str) -> None:
    """
    Set context file path for variable substitution.
    
    Relative paths are resolved from the script's directory (sys.argv[0] location).
    Absolute paths are used as-is.
    
    File is loaded lazily when substitution is first needed.
    
    Args:
        file_path: Path to context JSON file (relative or absolute)
        
    Raises:
        ValueError: If file_path is empty
        
    Examples:
        # Relative path - resolved from script directory
        set_context("context.json")
        set_context("config/production.json")
        
        # Absolute path - used as-is
        set_context("/full/path/to/context.json")
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
    
    _context_file_path = str(context_path)
    _context_data = None  # Reset cached data
    
    logger.debug(f"Context file set: {_context_file_path}")


def clear_context() -> None:
    """Clear context file path and cached data."""
    global _context_file_path, _context_data
    
    _context_file_path = None
    _context_data = None
    
    logger.debug("Context cleared")


def _load_context_data() -> Dict[str, Any]:
    """
    Load context data from JSON file with validation.
    
    Returns:
        Context data dictionary
        
    Raises:
        RuntimeError: If context file cannot be loaded
    """
    global _context_data
    
    if _context_data is not None:
        return _context_data
    
    if not _context_file_path:
        raise RuntimeError("No context file set - call set_context() first")
    
    try:
        context_file = Path(_context_file_path)
        
        if not context_file.exists():
            raise RuntimeError(f"Context file not found: {_context_file_path}")
        
        if not context_file.is_file():
            raise RuntimeError(f"Context path is not a file: {_context_file_path}")
        
        with open(context_file, 'r', encoding='utf-8') as f:
            _context_data = json.load(f)
        
        if not isinstance(_context_data, dict):
            raise RuntimeError(f"Context file must contain a JSON dictionary: {_context_file_path}")
        
        logger.debug(f"Context loaded: {len(_context_data)} top-level keys")
        return _context_data
        
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON in context file {_context_file_path}: {e}")
    except PermissionError as e:
        raise RuntimeError(f"Cannot read context file {_context_file_path}: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed to load context file {_context_file_path}: {e}")


def substitute_context_variables(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Substitute ${} variables in parameters using context data.
    
    Performs recursive substitution on all string values in the params dictionary.
    Only loads context file when substitution is actually needed (lazy loading).
    
    Args:
        params: Parameters dictionary to process
        
    Returns:
        Dictionary with context variables substituted
        
    Raises:
        RuntimeError: If context file loading fails
        ValueError: If variable substitution fails
    """
    if not _context_file_path:
        # No context set - return params as-is
        return params.copy()
    
    # Check if any substitution is actually needed
    if not _needs_substitution(params):
        logger.debug("No ${} patterns found - skipping context substitution")
        return params.copy()
    
    # Load context data (lazy loading)
    try:
        context_data = _load_context_data()
    except RuntimeError as e:
        raise RuntimeError(f"Context substitution failed: {e}")
    
    # Perform substitution with circular dependency detection
    try:
        return _substitute_recursive(params, context_data)
    except Exception as e:
        raise ValueError(f"Context variable substitution failed: {e}")


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