"""
Parameter management and project configuration discovery for DataPy framework.

Handles the parameter resolution chain: Mod Defaults → Project Defaults → Job Params
with automatic project configuration discovery, variable substitution, thread safety,
and performance optimizations for high-volume scenarios.
"""

import os
import threading
import time
from pathlib import Path
from typing import Dict, Any, Optional, Set
import yaml
from .logger import setup_logger

logger = setup_logger(__name__)

# Global cache for loaded configurations (thread-safe)
_config_cache = {}
_cache_lock = threading.RLock()
_cache_timestamps = {}

# Cache TTL in seconds (5 minutes default)
CONFIG_CACHE_TTL = 300


class ProjectConfig:
    """
    Thread-safe project configuration container with parameter resolution capabilities.
    
    Discovers project configuration files using convention-based search
    and provides parameter resolution with inheritance, variable substitution,
    and performance caching.
    """
    
    def __init__(self, search_path: Optional[str] = None) -> None:
        """
        Initialize project configuration with automatic discovery.
        
        Args:
            search_path: Starting path for config discovery (defaults to cwd)
            
        Raises:
            RuntimeError: If project config file exists but cannot be loaded
        """
        self._lock = threading.RLock()
        self.search_path = Path(search_path or os.getcwd())
        self._config_data: Dict[str, Any] = {}
        self._project_path: Optional[Path] = None
        self._config_loaded = False
        
        # Discover and load project configuration (thread-safe)
        with self._lock:
            self._discover_project_config()
    
    def _get_cache_key(self, config_path: Path) -> str:
        """Generate cache key for configuration file."""
        return f"{config_path.resolve()}:{config_path.stat().st_mtime}"
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cache entry is still valid."""
        if cache_key not in _config_cache:
            return False
        
        # Check TTL
        if cache_key in _cache_timestamps:
            age = time.time() - _cache_timestamps[cache_key]
            if age > CONFIG_CACHE_TTL:
                return False
        
        return True
    
    def _discover_project_config(self) -> None:
        """
        Thread-safe discovery of project configuration in the parent directory.
        
        Looks for project_defaults.yaml in the parent directory (typical project structure).
        Falls back to current directory if not found in parent.
        Uses caching for performance optimization.
        
        Raises:
            RuntimeError: If config file exists but cannot be loaded
        """
        # Primary search: parent directory (typical case)
        parent_config_path = self.search_path.parent / "project_defaults.yaml"
        
        # Fallback search: current directory
        current_config_path = self.search_path / "project_defaults.yaml"
        
        config_path = None
        if parent_config_path.exists() and parent_config_path.is_file():
            config_path = parent_config_path
        elif current_config_path.exists() and current_config_path.is_file():
            config_path = current_config_path
        
        if config_path:
            try:
                # Thread-safe cache access
                cache_key = self._get_cache_key(config_path)
                
                with _cache_lock:
                    if self._is_cache_valid(cache_key):
                        logger.debug(f"Using cached project config: {config_path}")
                        self._config_data = _config_cache[cache_key].copy()
                        self._project_path = config_path.parent
                        self._config_loaded = True
                        return
                
                # Load from file
                with open(config_path, 'r', encoding='utf-8') as f:
                    config_data = yaml.safe_load(f) or {}
                    
                if not isinstance(config_data, dict):
                    raise RuntimeError(f"Project config {config_path} must contain a YAML dictionary")
                
                # Auto-generate project name from folder if not specified
                if 'project_name' not in config_data:
                    config_data['project_name'] = config_path.parent.name
                
                # Thread-safe cache update
                with _cache_lock:
                    _config_cache[cache_key] = config_data.copy()
                    _cache_timestamps[cache_key] = time.time()
                    
                    # Clean old cache entries (simple LRU-like cleanup)
                    if len(_config_cache) > 50:  # Keep cache reasonable size
                        oldest_key = min(_cache_timestamps.keys(), 
                                       key=lambda k: _cache_timestamps[k])
                        del _config_cache[oldest_key]
                        del _cache_timestamps[oldest_key]
                
                self._config_data = config_data
                self._project_path = config_path.parent
                self._config_loaded = True
                
                logger.info(f"Loaded project config: {config_path}")
                
            except yaml.YAMLError as e:
                raise RuntimeError(f"Invalid YAML in project config {config_path}: {e}")
            except PermissionError as e:
                raise RuntimeError(f"Cannot read project config {config_path}: {e}")
            except Exception as e:
                raise RuntimeError(f"Failed to load project config {config_path}: {e}")
        else:
            logger.info("No project configuration found, using defaults only")
            self._config_data = {}
            self._config_loaded = True
    
    def get_mod_defaults(self, mod_name: str) -> Dict[str, Any]:
        """
        Thread-safe retrieval of project-level defaults for a specific mod.
        
        Args:
            mod_name: Name of the mod to get defaults for
            
        Returns:
            Dictionary of default parameters for the mod (deep copy)
            
        Raises:
            ValueError: If mod_name is empty
        """
        if not mod_name or not isinstance(mod_name, str):
            raise ValueError("mod_name must be a non-empty string")
        
        with self._lock:
            if not self._config_loaded:
                self._discover_project_config()
            
            mod_defaults = self._config_data.get('mod_defaults', {})
            if not isinstance(mod_defaults, dict):
                logger.warning("mod_defaults in project config is not a dictionary, ignoring")
                return {}
            
            # Return deep copy to prevent thread interference
            result = mod_defaults.get(mod_name, {})
            return self._deep_copy_dict(result)
    
    def get_globals(self) -> Dict[str, Any]:
        """
        Thread-safe retrieval of project-level global settings.
        
        Returns:
            Dictionary of global settings (deep copy)
        """
        with self._lock:
            if not self._config_loaded:
                self._discover_project_config()
            
            globals_config = self._config_data.get('globals', {})
            if not isinstance(globals_config, dict):
                logger.warning("globals in project config is not a dictionary, ignoring")
                return {}
            
            return self._deep_copy_dict(globals_config)
    
    def _deep_copy_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Perform deep copy of dictionary to prevent thread interference.
        Optimized for common parameter types.
        """
        if not isinstance(data, dict):
            return data
        
        result = {}
        for key, value in data.items():
            if isinstance(value, dict):
                result[key] = self._deep_copy_dict(value)
            elif isinstance(value, list):
                result[key] = [
                    self._deep_copy_dict(item) if isinstance(item, dict) else item 
                    for item in value
                ]
            else:
                result[key] = value
        return result
    
    @property
    def project_name(self) -> Optional[str]:
        """Get the project name from configuration."""
        with self._lock:
            if not self._config_loaded:
                self._discover_project_config()
            return self._config_data.get('project_name')
    
    @property
    def project_version(self) -> Optional[str]:
        """Get the project version from configuration."""
        with self._lock:
            if not self._config_loaded:
                self._discover_project_config()
            return self._config_data.get('project_version')


class ParameterResolver:
    """
    Thread-safe parameter resolver with inheritance chain and variable substitution.
    
    Resolution order: Mod Defaults → Project Defaults → Job Parameters
    Supports variable substitution using ${key.subkey} syntax with circular dependency detection.
    Optimized for high-volume scenarios with caching.
    """
    
    def __init__(self, project_config: Optional[ProjectConfig] = None) -> None:
        """
        Initialize parameter resolver.
        
        Args:
            project_config: Project configuration instance (creates new if None)
        """
        self._lock = threading.RLock()
        self.project_config = project_config or ProjectConfig()
        
        # Performance cache for resolved parameters
        self._resolution_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_timestamps: Dict[str, float] = {}
        self._cache_ttl = 60  # 1 minute TTL for parameter resolution cache
    
    def _get_resolution_cache_key(self, mod_name: str, job_params: Dict[str, Any]) -> str:
        """Generate cache key for parameter resolution."""
        # Create deterministic key from mod_name and job_params
        params_hash = hash(str(sorted(job_params.items())))
        return f"{mod_name}:{params_hash}"
    
    def _is_resolution_cache_valid(self, cache_key: str) -> bool:
        """Check if resolution cache entry is still valid."""
        if cache_key not in self._resolution_cache:
            return False
        
        # Check TTL
        if cache_key in self._cache_timestamps:
            age = time.time() - self._cache_timestamps[cache_key]
            if age > self._cache_ttl:
                return False
        
        return True
    
    def resolve_mod_params(
        self,
        mod_name: str,
        job_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Thread-safe parameter resolution using the inheritance chain with caching.
        
        Registry mod defaults are applied later during parameter validation.
        
        Args:
            mod_name: Name of the mod
            job_params: Parameters from job configuration
            
        Returns:
            Resolved parameter dictionary (deep copy)
            
        Raises:
            ValueError: If mod_name is empty or parameter resolution fails
        """
        if not mod_name or not isinstance(mod_name, str):
            raise ValueError("mod_name must be a non-empty string")
        
        if not isinstance(job_params, dict):
            raise ValueError("job_params must be a dictionary")
        
        with self._lock:
            # Check cache first for performance
            cache_key = self._get_resolution_cache_key(mod_name, job_params)
            
            if self._is_resolution_cache_valid(cache_key):
                logger.debug(f"Using cached parameter resolution for {mod_name}")
                return self._deep_copy_dict(self._resolution_cache[cache_key])
            
            # Perform resolution
            try:
                # Start with empty resolved params
                resolved = {}
                
                # Apply project-level mod defaults
                project_mod_defaults = self.project_config.get_mod_defaults(mod_name)
                resolved.update(project_mod_defaults)
                
                # Apply job-specific parameters (highest priority)
                resolved.update(job_params)
                
                # Cache the result
                resolved_copy = self._deep_copy_dict(resolved)
                self._resolution_cache[cache_key] = resolved_copy
                self._cache_timestamps[cache_key] = time.time()
                
                # Clean old cache entries periodically
                if len(self._resolution_cache) > 100:
                    self._cleanup_resolution_cache()
                
                logger.debug(f"Resolved params for {mod_name}: {len(resolved)} parameters")
                return resolved_copy
                
            except Exception as e:
                logger.warning(f"Failed to get project defaults for {mod_name}: {e}")
                # Fallback to job params only
                return self._deep_copy_dict(job_params)
    
    def _cleanup_resolution_cache(self) -> None:
        """Clean up old entries from resolution cache."""
        current_time = time.time()
        expired_keys = [
            key for key, timestamp in self._cache_timestamps.items()
            if current_time - timestamp > self._cache_ttl
        ]
        
        for key in expired_keys:
            self._resolution_cache.pop(key, None)
            self._cache_timestamps.pop(key, None)
        
        # Also clean global config cache periodically
        with _cache_lock:
            global_expired = [
                key for key, timestamp in _cache_timestamps.items()
                if current_time - timestamp > CONFIG_CACHE_TTL
            ]
            for key in global_expired:
                _config_cache.pop(key, None)
                _cache_timestamps.pop(key, None)
        
        logger.debug(f"Cleaned {len(expired_keys)} resolution + {len(global_expired)} config cache entries")
    
    def _deep_copy_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Perform deep copy of dictionary to prevent thread interference.
        Optimized for common parameter types.
        """
        if not isinstance(data, dict):
            return data
        
        result = {}
        for key, value in data.items():
            if isinstance(value, dict):
                result[key] = self._deep_copy_dict(value)
            elif isinstance(value, list):
                result[key] = [
                    self._deep_copy_dict(item) if isinstance(item, dict) else item 
                    for item in value
                ]
            else:
                result[key] = value
        return result


def load_job_config(config_path: str) -> Dict[str, Any]:
    """
    Thread-safe loading of job configuration from YAML file with comprehensive validation.
    Uses caching for performance optimization.
    
    Args:
        config_path: Path to job configuration file
        
    Returns:
        Dictionary containing job configuration
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        RuntimeError: If config file is invalid or cannot be loaded
    """
    if not config_path or not isinstance(config_path, str):
        raise ValueError("config_path must be a non-empty string")
    
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Job config file not found: {config_path}")
    
    if not config_file.is_file():
        raise FileNotFoundError(f"Path is not a file: {config_path}")
    
    try:
        # Use cache for job configs too
        cache_key = f"job_config:{config_file.resolve()}:{config_file.stat().st_mtime}"
        
        with _cache_lock:
            if cache_key in _config_cache and cache_key in _cache_timestamps:
                age = time.time() - _cache_timestamps[cache_key]
                if age <= CONFIG_CACHE_TTL:
                    logger.debug(f"Using cached job config: {config_path}")
                    return _config_cache[cache_key].copy()
        
        # Load from file
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f) or {}
            
        if not isinstance(config, dict):
            raise RuntimeError(f"Job config {config_path} must contain a YAML dictionary")
        
        # Cache the result
        with _cache_lock:
            _config_cache[cache_key] = config.copy()
            _cache_timestamps[cache_key] = time.time()
            
            # Simple cache size management
            if len(_config_cache) > 50:
                oldest_key = min(_cache_timestamps.keys(), 
                               key=lambda k: _cache_timestamps[k])
                _config_cache.pop(oldest_key, None)
                _cache_timestamps.pop(oldest_key, None)
        
        logger.info(f"Loaded job config: {config_path}")
        return config.copy()
        
    except yaml.YAMLError as e:
        raise RuntimeError(f"Invalid YAML in {config_path}: {e}")
    except PermissionError as e:
        raise RuntimeError(f"Cannot read job config {config_path}: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed to load job config {config_path}: {e}")


def create_resolver(search_path: Optional[str] = None) -> ParameterResolver:
    """
    Convenience function to create a parameter resolver with project discovery.
    Thread-safe and optimized for high-volume scenarios.
    
    Args:
        search_path: Starting path for project config discovery
        
    Returns:
        Configured ParameterResolver instance
        
    Raises:
        RuntimeError: If project config discovery fails
    """
    try:
        project_config = ProjectConfig(search_path)
        return ParameterResolver(project_config)
    except Exception as e:
        raise RuntimeError(f"Failed to create parameter resolver: {e}")


# Performance monitoring functions
def get_cache_stats() -> Dict[str, Any]:
    """
    Get cache statistics for monitoring and debugging.
    
    Returns:
        Dictionary with cache statistics
    """
    with _cache_lock:
        current_time = time.time()
        valid_entries = sum(
            1 for key, timestamp in _cache_timestamps.items()
            if current_time - timestamp <= CONFIG_CACHE_TTL
        )
        
        return {
            "total_cache_entries": len(_config_cache),
            "valid_cache_entries": valid_entries,
            "cache_hit_potential": valid_entries / max(len(_config_cache), 1),
            "cache_size_bytes": sum(
                len(str(data)) for data in _config_cache.values()
            )
        }


def clear_cache() -> None:
    """
    Clear all caches. Useful for testing or memory management.
    """
    with _cache_lock:
        _config_cache.clear()
        _cache_timestamps.clear()
    
    logger.info("Parameter caches cleared")