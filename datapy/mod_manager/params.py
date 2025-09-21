"""
Parameter management and project configuration discovery for DataPy framework.

Handles the parameter resolution chain: Mod Defaults → Project Defaults → Job Params
with automatic project configuration discovery and variable substitution.
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional, Set
import yaml
from .logger import setup_logger

logger = setup_logger(__name__)

# Global project config singleton (matches context.py pattern)
_global_project_config: Optional['ProjectConfig'] = None


class ProjectConfig:
    """
    Project configuration container with parameter resolution capabilities.
    
    Discovers project configuration files using convention-based search
    and provides parameter resolution with inheritance and variable substitution.
    """
    
    def __init__(self, search_path: Optional[str] = None) -> None:
        """
        Initialize project configuration with automatic discovery.
        
        Args:
            search_path: Starting path for config discovery (defaults to cwd)
            
        Raises:
            RuntimeError: If project config file exists but cannot be loaded
        """
        self.search_path = Path(search_path or os.getcwd())
        self.config_data: Dict[str, Any] = {}
        self.project_path: Optional[Path] = None
        
        # Discover and load project configuration
        self._discover_project_config()
    
    def _discover_project_config(self) -> None:
        """
        Discover project configuration in the parent directory.
        
        Looks for project_defaults.yaml in the parent directory (typical project structure).
        Falls back to current directory if not found in parent.
        
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
        Find project configuration file in standard locations.
        
        Returns:
            Path to config file if found, None otherwise
        """
        # Primary search: parent directory (typical case)
        parent_config_path = self.search_path.parent / "project_defaults.yaml"
        
        # Fallback search: current directory
        current_config_path = self.search_path / "project_defaults.yaml"
        
        if parent_config_path.exists() and parent_config_path.is_file():
            return parent_config_path
        elif current_config_path.exists() and current_config_path.is_file():
            return current_config_path
        
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
            
            logger.debug(f"Found project config: {config_path}")
            
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


def get_project_config(search_path: Optional[str] = None) -> ProjectConfig:
    """
    Get global project config instance (singleton for performance).
    
    Args:
        search_path: Starting path for project config discovery
        
    Returns:
        Cached ProjectConfig instance
        
    Raises:
        RuntimeError: If project config discovery fails
    """
    global _global_project_config
    
    if _global_project_config is None:
        try:
            _global_project_config = ProjectConfig(search_path)
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


def create_resolver(search_path: Optional[str] = None) -> ParameterResolver:
    """
    Convenience function to create a parameter resolver with cached project config.
    
    Args:
        search_path: Starting path for project config discovery
        
    Returns:
        Configured ParameterResolver instance with cached project config
        
    Raises:
        RuntimeError: If project config discovery fails
    """
    try:
        project_config = get_project_config(search_path)
        return ParameterResolver(project_config)
    except Exception as e:
        raise RuntimeError(f"Failed to create parameter resolver: {e}")