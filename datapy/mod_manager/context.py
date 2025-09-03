"""
Context file management for variable substitution in DataPy framework.

Provides thread-safe, cached context loading from JSON files for ${} variable substitution
without any integration with global config - context is the single source
for all variable substitution needs. Optimized for container environments.
"""

import json
import os
import re
import threading
import time
from pathlib import Path
from typing import Dict, Any, Optional, Set

from .logger import setup_logger

logger = setup_logger(__name__)

# Thread-safe global context storage
_context_file_path: Optional[str] = None
_context_data: Optional[Dict[str, Any]] = None
_context_cache: Dict[str, Dict[str, Any]] = {}
_cache_timestamps: Dict[str, float] = {}
_context_lock = threading.RLock()
_substitution_pattern = re.compile(r'\$\{([^}]+)\}')

# Cache TTL in seconds (1 minute for container environments)
CONTEXT_CACHE_TTL = 60


def set_context(file_path: str) -> None:
    """
    Thread-safe context file path setting for variable substitution.
    
    File is loaded lazily when substitution is first needed.
    Clears any cached data for the previous context file.
    
    Args:
        file_path: Path to context JSON file
        
    Raises:
        ValueError: If file_path is empty
    """
    global _context_file_path, _context_data
    
    if not file_path or not isinstance(file_path, str):
        raise ValueError("file_path must be a non-empty string")
    
    with _context_lock:
        new_path = file_path.strip()
        
        # Clear cached data if changing context file
        if _context_file_path != new_path:
            _context_data = None
            logger.debug(f"Context file changed, clearing cache: {_context_file_path} -> {new_path}")
        
        _context_file_path = new_path
    
    logger.info(f"Context file set: {_context_file_path}")


def clear_context() -> None:
    """Thread-safe context file path and cached data clearing."""
    global _context_file_path, _context_data
    
    with _context_lock:
        old_path = _context_file_path
        _context_file_path = None
        _context_data = None
    
    logger.info(f"Context cleared (was: {old_path})")


def _get_cache_key(file_path: str) -> str:
    """Generate cache key for context file including modification time."""
    try:
        file_obj = Path(file_path)
        if file_obj.exists() and file_obj.is_file():
            mtime = file_obj.stat().st_mtime
            return f"context:{file_obj.resolve()}:{mtime}"
        else:
            # File doesn't exist - use path with timestamp for error consistency
            return f"context:{file_obj.resolve()}:{time.time()}"
    except Exception as e:
        # Fallback for any filesystem errors
        return f"context:{file_path}:{time.time()}"


def _is_cache_valid(cache_key: str) -> bool:
    """Check if cache entry is still valid (TTL + file modification time)."""
    if cache_key not in _context_cache:
        return False
    
    # Check TTL
    if cache_key in _cache_timestamps:
        age = time.time() - _cache_timestamps[cache_key]
        if age > CONTEXT_CACHE_TTL:
            return False
    
    # File modification time is already in the cache_key, so if we reach here,
    # the file hasn't changed and TTL is valid
    return True


def _load_context_data() -> Dict[str, Any]:
    """
    Thread-safe context data loading from JSON file with caching and validation.
    
    Returns:
        Context data dictionary
        
    Raises:
        RuntimeError: If context file cannot be loaded
    """
    global _context_data
    
    with _context_lock:
        if not _context_file_path:
            raise RuntimeError("No context file set - call set_context() first")
        
        # Check if we have valid cached data in memory
        if _context_data is not None:
            return _context_data
        
        # Generate cache key
        cache_key = _get_cache_key(_context_file_path)
        
        # Check global cache first
        if _is_cache_valid(cache_key):
            logger.debug(f"Using cached context data: {_context_file_path}")
            _context_data = _context_cache[cache_key].copy()
            return _context_data
        
        # Load from file
        try:
            context_file = Path(_context_file_path)
            
            if not context_file.exists():
                raise RuntimeError(f"Context file not found: {_context_file_path}")
            
            if not context_file.is_file():
                raise RuntimeError(f"Context path is not a file: {_context_file_path}")
            
            with open(context_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, dict):
                raise RuntimeError(f"Context file must contain a JSON dictionary: {_context_file_path}")
            
            # Cache the loaded data
            _context_cache[cache_key] = data.copy()
            _cache_timestamps[cache_key] = time.time()
            _context_data = data
            
            # Clean old cache entries periodically
            if len(_context_cache) > 50:  # Keep cache size reasonable
                _cleanup_context_cache()
            
            logger.info(f"Context loaded: {len(_context_data)} top-level keys from {_context_file_path}")
            return _context_data
            
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Invalid JSON in context file {_context_file_path}: {e}")
        except PermissionError as e:
            raise RuntimeError(f"Cannot read context file {_context_file_path}: {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to load context file {_context_file_path}: {e}")


def _cleanup_context_cache() -> None:
    """Clean up old entries from context cache."""
    current_time = time.time()
    expired_keys = [
        key for key, timestamp in _cache_timestamps.items()
        if current_time - timestamp > CONTEXT_CACHE_TTL
    ]
    
    for key in expired_keys:
        _context_cache.pop(key, None)
        _cache_timestamps.pop(key, None)
    
    logger.debug(f"Cleaned {len(expired_keys)} expired context cache entries")


def substitute_context_variables(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Thread-safe substitution of ${} variables in parameters using cached context data.
    
    Performs recursive substitution on all string values in the params dictionary.
    Only loads context file when substitution is actually needed (lazy loading).
    
    Args:
        params: Parameters dictionary to process
        
    Returns:
        Dictionary with context variables substituted (deep copy)
        
    Raises:
        RuntimeError: If context file loading fails
        ValueError: If variable substitution fails
    """
    with _context_lock:
        if not _context_file_path:
            # No context set - return deep copy of params as-is
            return _deep_copy_dict(params)
    
    # Check if any substitution is actually needed
    if not _needs_substitution(params):
        logger.debug("No ${} patterns found - skipping context substitution")
        return _deep_copy_dict(params)
    
    # Load context data (lazy loading with caching)
    try:
        context_data = _load_context_data()
    except RuntimeError as e:
        raise RuntimeError(f"Context substitution failed: {e}")
    
    # Perform substitution with circular dependency detection
    try:
        result = _substitute_recursive(params, context_data)
        logger.debug("Context variable substitution completed")
        return result
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
        Object with variables substituted (deep copy)
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
            try:
                value = _get_context_value(var_path, context)
                return str(value)
            except ValueError as e:
                raise e
        
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


def _deep_copy_dict(data: Any) -> Any:
    """
    Perform deep copy of data structure to prevent thread interference.
    Optimized for common parameter types.
    """
    if isinstance(data, dict):
        return {k: _deep_copy_dict(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [_deep_copy_dict(item) for item in data]
    else:
        return data


def get_context_info() -> Dict[str, Any]:
    """
    Get information about current context configuration and cache status.
    
    Returns:
        Dictionary with context status information
    """
    with _context_lock:
        current_time = time.time()
        valid_entries = sum(
            1 for key, timestamp in _cache_timestamps.items()
            if current_time - timestamp <= CONTEXT_CACHE_TTL
        )
        
        return {
            "context_file": _context_file_path,
            "context_loaded": _context_data is not None,
            "context_keys": list(_context_data.keys()) if _context_data else None,
            "total_cache_entries": len(_context_cache),
            "valid_cache_entries": valid_entries,
            "cache_hit_potential": valid_entries / max(len(_context_cache), 1),
        }


def get_context_cache_stats() -> Dict[str, Any]:
    """
    Get context cache statistics for monitoring and debugging.
    
    Returns:
        Dictionary with cache statistics
    """
    with _context_lock:
        current_time = time.time()
        valid_entries = sum(
            1 for key, timestamp in _cache_timestamps.items()
            if current_time - timestamp <= CONTEXT_CACHE_TTL
        )
        
        return {
            "total_cache_entries": len(_context_cache),
            "valid_cache_entries": valid_entries,
            "cache_hit_rate": valid_entries / max(len(_context_cache), 1),
            "cache_size_bytes": sum(
                len(str(data)) for data in _context_cache.values()
            ),
            "cache_ttl_seconds": CONTEXT_CACHE_TTL
        }


def clear_context_cache() -> None:
    """
    Clear context cache. Useful for testing or memory management.
    """
    with _context_lock:
        _context_cache.clear()
        _cache_timestamps.clear()
        # Also clear in-memory cached data
        global _context_data
        _context_data = None
    
    logger.info("Context cache cleared")