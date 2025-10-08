"""
Parameter management and project configuration discovery for DataPy framework.

Handles the parameter resolution chain: Mod Defaults → Project Defaults → Job Params
with automatic project configuration discovery and variable substitution.
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional
import yaml
from .logger import setup_logger

logger = setup_logger(__name__)

# Global project config singleton (matches context.py pattern)
_global_project_config: Optional['ProjectConfig'] = None


def _get_script_directory() -> Path:
    """
    Get the directory of the main script being executed.
    
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


class ProjectConfig:
    """
    Project configuration container with parameter resolution capabilities.
    
    Discovers project configuration files using convention-based search
    and provides parameter resolution with inheritance and variable substitution.
    """
    
    def __init__(self, search_path: Optional[str] = None, max_depth: int = 1) -> None:
        """
        Initialize project configuration with automatic discovery.
        
        Args:
            search_path: Starting path for config discovery (defaults to script directory from sys.argv[0])
            max_depth: Maximum levels to search upward (default=1, searches current + 1 parent)
            
        Raises:
            RuntimeError: If project config file exists but cannot be loaded
        """
        if search_path is None:
            self.search_path = _get_script_directory()
        else:
            self.search_path = Path(search_path)
            
        self.max_depth = max_depth
        self.config_data: Dict[str, Any] = {}
        self.project_path: Optional[Path] = None
        
        # Discover and load project configuration
        self._discover_project_config()
    
    def _discover_project_config(self) -> None:
        """
        Discover project configuration by searching upward from search_path.
        
        Searches current directory first, then parent directories up to max_depth.
        
        Raises:
            RuntimeError: If config file exists but cannot be loaded
        """
        config_path = self._find_config_file()
        
        if config_path:
            self._load_config_file(config_path)
        else:
            logger.debug("No project configuration found, using defaults only")
            self.config_data = {}

    def _find_config_file(self) -> Optional[Path]:
        """
        Find project configuration file by searching upward from search_path.
        
        Searches from current directory upward through parent directories
        up to max_depth levels, returning the first project_defaults.yaml found.
        
        Returns:
            Path to config file if found, None otherwise
        """
        current = self.search_path
        
        # Search current directory + max_depth parent levels
        for level in range(self.max_depth + 1):
            config_path = current / "project_defaults.yaml"
            
            if config_path.exists() and config_path.is_file():
                logger.debug(f"Found project config at level {level}: {config_path}")
                return config_path
            
            # Move to parent directory
            parent = current.parent
            
            # Check if we've reached filesystem root
            if parent == current:
                logger.debug("Reached filesystem root, no config found")
                break
                
            current = parent
        
        return None

    def _load_config_file(self, config_path: Path) -> None:
        """
        Load and validate project configuration file.
        
        Args:
            config_path: Path to configuration file
            
        Raises:
            RuntimeError: If config file cannot be loaded or is invalid
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config_data = yaml.safe_load(f) or {}
                
            if not isinstance(self.config_data, dict):
                raise RuntimeError(f"Project config {config_path} must contain a YAML dictionary")
            
            self.project_path = config_path.parent
            self._set_default_project_name()
            
            logger.debug(f"Loaded project config: {config_path}")
            
        except yaml.YAMLError as e:
            raise RuntimeError(f"Invalid YAML in project config {config_path}: {e}")
        except PermissionError as e:
            raise RuntimeError(f"Cannot read project config {config_path}: {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to load project config {config_path}: {e}")

    def _set_default_project_name(self) -> None:
        """Set default project name if not specified in config."""
        if 'project_name' not in self.config_data:
            self.config_data['project_name'] = self.project_path.name
    
    def get_mod_defaults(self, mod_name: str) -> Dict[str, Any]:
        """
        Get project-level defaults for a specific mod.
        
        Args:
            mod_name: Name of the mod to get defaults for
            
        Returns:
            Dictionary of default parameters for the mod
            
        Raises:
            ValueError: If mod_name is empty
        """
        if not mod_name or not isinstance(mod_name, str):
            raise ValueError("mod_name must be a non-empty string")
        
        mod_defaults = self.config_data.get('mod_defaults', {})
        if not isinstance(mod_defaults, dict):
            logger.warning("mod_defaults in project config is not a dictionary, ignoring")
            return {}
        
        return mod_defaults.get(mod_name, {}).copy()
    
    def get_globals(self) -> Dict[str, Any]:
        """
        Get project-level global settings.
        
        Returns:
            Dictionary of global settings
        """
        globals_config = self.config_data.get('globals', {})
        if not isinstance(globals_config, dict):
            logger.warning("globals in project config is not a dictionary, ignoring")
            return {}
        
        return globals_config.copy()
    
    @property
    def project_name(self) -> Optional[str]:
        """Get the project name from configuration."""
        return self.config_data.get('project_name')
    
    @property
    def project_version(self) -> Optional[str]:
        """Get the project version from configuration."""
        return self.config_data.get('project_version')


class ParameterResolver:
    """
    Parameter resolver with inheritance chain and variable substitution.
    
    Resolution order: Mod Defaults → Project Defaults → Job Parameters
    Supports variable substitution using ${key.subkey} syntax with circular dependency detection.
    """
    
    def __init__(self, project_config: Optional[ProjectConfig] = None) -> None:
        """
        Initialize parameter resolver.
        
        Args:
            project_config: Project configuration instance
        """
        self.project_config = project_config or get_project_config()
    
    def resolve_mod_params(
        self,
        mod_name: str,
        job_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Resolve parameters for a mod using the inheritance chain.
        
        Registry mod defaults are applied later during parameter validation.
        
        Args:
            mod_name: Name of the mod
            job_params: Parameters from job configuration
            
        Returns:
            Resolved parameter dictionary
            
        Raises:
            ValueError: If mod_name is empty or parameter resolution fails
        """
        if not mod_name or not isinstance(mod_name, str):
            raise ValueError("mod_name must be a non-empty string")
        
        if not isinstance(job_params, dict):
            raise ValueError("job_params must be a dictionary")
        
        # Start with empty resolved params
        resolved = {}
        
        # Apply project-level mod defaults
        try:
            project_mod_defaults = self.project_config.get_mod_defaults(mod_name)
            resolved.update(project_mod_defaults)
        except Exception as e:
            logger.warning(f"Failed to get project defaults for {mod_name}: {e}")
        
        # Apply job-specific parameters (highest priority)
        resolved.update(job_params)
        
        logger.debug(f"Resolved params for {mod_name}: {resolved}")
        return resolved


def get_project_config(search_path: Optional[str] = None, max_depth: int = 1) -> ProjectConfig:
    """
    Get global project config instance (singleton for performance).
    
    Auto-detects project configuration from script location (sys.argv[0])
    when search_path is not provided.
    
    Args:
        search_path: Starting path for project config discovery (auto-detected if None)
        max_depth: Maximum levels to search upward (default=1)
        
    Returns:
        Cached ProjectConfig instance
        
    Raises:
        RuntimeError: If project config discovery fails
    """
    global _global_project_config
    
    if _global_project_config is None:
        try:
            _global_project_config = ProjectConfig(search_path, max_depth)
        except Exception as e:
            raise RuntimeError(f"Failed to create project config: {e}")
    
    return _global_project_config


def clear_project_config() -> None:
    """Clear cached project config (for testing)."""
    global _global_project_config
    _global_project_config = None
    logger.debug("Project config cache cleared")


def load_job_config(config_path: str) -> Dict[str, Any]:
    """
    Load job configuration from YAML file with comprehensive validation.
    
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
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f) or {}
            
        if not isinstance(config, dict):
            raise RuntimeError(f"Job config {config_path} must contain a YAML dictionary")
            
        logger.debug(f"Loaded job config: {config_path}")
        return config
        
    except yaml.YAMLError as e:
        raise RuntimeError(f"Invalid YAML in {config_path}: {e}")
    except PermissionError as e:
        raise RuntimeError(f"Cannot read job config {config_path}: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed to load job config {config_path}: {e}")


def create_resolver(search_path: Optional[str] = None, max_depth: int = 1) -> ParameterResolver:
    """
    Convenience function to create a parameter resolver with cached project config.
    
    Auto-detects project configuration from script location (sys.argv[0])
    when search_path is not provided.
    
    Args:
        search_path: Starting path for project config discovery (auto-detected if None)
        max_depth: Maximum levels to search upward (default=1)
        
    Returns:
        Configured ParameterResolver instance with cached project config
        
    Raises:
        RuntimeError: If project config discovery fails
    """
    try:
        project_config = get_project_config(search_path, max_depth)
        return ParameterResolver(project_config)
    except Exception as e:
        raise RuntimeError(f"Failed to create parameter resolver: {e}")