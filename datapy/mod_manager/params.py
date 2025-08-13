"""
Parameter management and project configuration discovery for DataPy framework.

Handles the parameter resolution chain: Mod Defaults → Project Defaults → Job Params
with automatic project configuration discovery and variable substitution.
"""

import os
import re
from pathlib import Path
from typing import Dict, Any, Optional, Union
import yaml
from .logger import setup_logger

logger = setup_logger(__name__)


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
        """
        self.search_path = Path(search_path or os.getcwd())
        self.config_data: Dict[str, Any] = {}
        self.project_path: Optional[Path] = None
        
        # Discover and load project configuration
        self._discover_project_config()
    
    def _discover_project_config(self) -> None:
        """
        Discover project configuration in the same directory.
        
        Looks for project_defaults.yaml in the current directory only.
        """
        config_path = self.search_path / "project_defaults.yaml"
        
        if config_path.exists() and config_path.is_file():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    self.config_data = yaml.safe_load(f) or {}
                self.project_path = config_path.parent
                
                # Auto-generate project name from folder if not specified
                if 'project_name' not in self.config_data:
                    self.config_data['project_name'] = self.project_path.name
                
                logger.info(f"Found project config: {config_path}")
            except yaml.YAMLError as e:
                raise yaml.YAMLError(f"Invalid YAML in project config {config_path}: {e}")
            except PermissionError as e:
                raise PermissionError(f"Cannot read project config {config_path}: {e}")
            except Exception as e:
                raise RuntimeError(f"Failed to load project config {config_path}: {e}")
        else:
            logger.info("No project configuration found, using defaults only")
            self.config_data = {}
    
    def get_mod_defaults(self, mod_name: str) -> Dict[str, Any]:
        """
        Get project-level defaults for a specific mod.
        
        Args:
            mod_name: Name of the mod to get defaults for
            
        Returns:
            Dictionary of default parameters for the mod
        """
        mod_defaults = self.config_data.get('mod_defaults', {})
        return mod_defaults.get(mod_name, {}).copy()
    
    def get_globals(self) -> Dict[str, Any]:
        """
        Get project-level global settings.
        
        Returns:
            Dictionary of global settings
        """
        return self.config_data.get('globals', {}).copy()
    
    def get_logging_config(self) -> Dict[str, Any]:
        """
        Get project-level logging configuration.
        
        Returns:
            Dictionary of logging settings
        """
        return self.config_data.get('logging', {}).copy()
    
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
    Thread-safe parameter resolver with inheritance chain and variable substitution.
    
    Resolution order: Mod Defaults → Project Defaults → Job Parameters
    Supports variable substitution using ${key.subkey} syntax.
    """
    
    def __init__(self, project_config: Optional[ProjectConfig] = None) -> None:
        """
        Initialize parameter resolver.
        
        Args:
            project_config: Project configuration instance
        """
        self.project_config = project_config or ProjectConfig()
        self._substitution_pattern = re.compile(r'\$\{([^}]+)\}')
        self._lock = __import__('threading').Lock()  # For thread safety
    
    def resolve_mod_params(
        self,
        mod_name: str,
        job_params: Dict[str, Any],
        mod_defaults: Optional[Dict[str, Any]] = None,
        globals_override: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Resolve parameters for a mod using the inheritance chain (thread-safe).
        
        Args:
            mod_name: Name of the mod
            job_params: Parameters from job configuration
            mod_defaults: Mod's built-in defaults
            globals_override: Global parameter overrides
            
        Returns:
            Fully resolved parameter dictionary
            
        Raises:
            ValueError: If variable substitution fails for required variables
        """
        with self._lock:
            # Start with mod's built-in defaults
            resolved = {}
            if mod_defaults:
                resolved.update(mod_defaults)
            
            # Apply project-level mod defaults
            project_mod_defaults = self.project_config.get_mod_defaults(mod_name)
            resolved.update(project_mod_defaults)
            
            # Apply job-specific parameters (highest priority)
            resolved.update(job_params)
            
            # Build globals context for substitution
            globals_context = {}
            globals_context.update(self.project_config.get_globals())
            if globals_override:
                globals_context.update(globals_override)
            
            # Perform variable substitution
            resolved = self._substitute_variables(resolved, globals_context)
            
            logger.debug(f"Resolved params for {mod_name}: {resolved}")
            return resolved
    
    def _substitute_variables(
        self, 
        params: Dict[str, Any], 
        globals_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Perform variable substitution on parameter values.
        
        Args:
            params: Parameter dictionary to process
            globals_context: Global variables for substitution
            
        Returns:
            Dictionary with variables substituted
        """
        def substitute_value(value: Any) -> Any:
            if isinstance(value, str):
                return self._substitute_string(value, globals_context)
            elif isinstance(value, dict):
                return {k: substitute_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [substitute_value(item) for item in value]
            else:
                return value
        
        return {k: substitute_value(v) for k, v in params.items()}
    
    def _substitute_string(self, text: str, context: Dict[str, Any]) -> str:
        """
        Substitute variables in a string using ${key.subkey} syntax.
        
        Args:
            text: String to process
            context: Variable context for substitution
            
        Returns:
            String with variables substituted
            
        Raises:
            ValueError: If variable substitution fails
        """
        def replace_var(match):
            var_path = match.group(1)
            try:
                # Navigate nested dictionary using dot notation
                value = context
                for key in var_path.split('.'):
                    value = value[key]
                return str(value)
            except (KeyError, TypeError) as e:
                raise ValueError(f"Variable substitution failed for ${{{var_path}}}: {e}")
        
        return self._substitution_pattern.sub(replace_var, text)


def load_job_config(config_path: str) -> Dict[str, Any]:
    """
    Load job configuration from YAML file.
    
    Args:
        config_path: Path to job configuration file
        
    Returns:
        Dictionary containing job configuration
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        yaml.YAMLError: If config file is invalid YAML
    """
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Job config file not found: {config_path}")
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f) or {}
        logger.info(f"Loaded job config: {config_path}")
        return config
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Invalid YAML in {config_path}: {e}")


def create_resolver(search_path: Optional[str] = None) -> ParameterResolver:
    """
    Convenience function to create a parameter resolver with project discovery.
    
    Args:
        search_path: Starting path for project config discovery
        
    Returns:
        Configured ParameterResolver instance
    """
    project_config = ProjectConfig(search_path)
    return ParameterResolver(project_config)