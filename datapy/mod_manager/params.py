"""
Parameter management and project configuration discovery for DataPy framework.

Handles the parameter resolution chain: Mod Defaults → Project Defaults → Job Params
with automatic project configuration discovery and variable substitution.
"""

import os
import re
from pathlib import Path
from typing import Dict, Any, Optional, Set
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
                with open(config_path, 'r', encoding='utf-8') as f:
                    self.config_data = yaml.safe_load(f) or {}
                    
                if not isinstance(self.config_data, dict):
                    raise RuntimeError(f"Project config {config_path} must contain a YAML dictionary")
                
                self.project_path = config_path.parent
                
                # Auto-generate project name from folder if not specified
                if 'project_name' not in self.config_data:
                    self.config_data['project_name'] = self.project_path.name
                
                logger.info(f"Found project config: {config_path}")
                
            except yaml.YAMLError as e:
                raise RuntimeError(f"Invalid YAML in project config {config_path}: {e}")
            except PermissionError as e:
                raise RuntimeError(f"Cannot read project config {config_path}: {e}")
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
    
    def get_logging_config(self) -> Dict[str, Any]:
        """
        Get project-level logging configuration.
        
        Returns:
            Dictionary of logging settings
        """
        logging_config = self.config_data.get('logging', {})
        if not isinstance(logging_config, dict):
            logger.warning("logging in project config is not a dictionary, ignoring")
            return {}
        
        return logging_config.copy()
    
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
        self.project_config = project_config or ProjectConfig()
        self._substitution_pattern = re.compile(r'\$\{([^}]+)\}')
        # Note: Removed thread safety - ETL processes are single-threaded per execution
        # TODO: Add distributed parameter resolution for future orchestrator
    
    def resolve_mod_params(
        self,
        mod_name: str,
        job_params: Dict[str, Any],
        mod_defaults: Optional[Dict[str, Any]] = None,
        globals_override: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Resolve parameters for a mod using the inheritance chain.
        
        Args:
            mod_name: Name of the mod
            job_params: Parameters from job configuration
            mod_defaults: Mod's built-in defaults
            globals_override: Global parameter overrides
            
        Returns:
            Fully resolved parameter dictionary
            
        Raises:
            ValueError: If mod_name is empty or variable substitution fails
        """
        if not mod_name or not isinstance(mod_name, str):
            raise ValueError("mod_name must be a non-empty string")
        
        if not isinstance(job_params, dict):
            raise ValueError("job_params must be a dictionary")
        
        # Start with mod's built-in defaults
        resolved = {}
        if mod_defaults and isinstance(mod_defaults, dict):
            resolved.update(mod_defaults)
        
        # Apply project-level mod defaults
        try:
            project_mod_defaults = self.project_config.get_mod_defaults(mod_name)
            resolved.update(project_mod_defaults)
        except Exception as e:
            logger.warning(f"Failed to get project defaults for {mod_name}: {e}")
        
        # Apply job-specific parameters (highest priority)
        resolved.update(job_params)
        
        # Build globals context for substitution
        globals_context = {}
        try:
            globals_context.update(self.project_config.get_globals())
        except Exception as e:
            logger.warning(f"Failed to get project globals: {e}")
        
        if globals_override and isinstance(globals_override, dict):
            globals_context.update(globals_override)
        
        # Perform variable substitution with circular dependency detection
        try:
            resolved = self._substitute_variables(resolved, globals_context)
        except Exception as e:
            raise ValueError(f"Variable substitution failed for mod {mod_name}: {e}")
        
        logger.debug(f"Resolved params for {mod_name}: {resolved}")
        return resolved
    
    def _substitute_variables(
        self, 
        params: Dict[str, Any], 
        globals_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Perform variable substitution on parameter values with circular dependency detection.
        
        Args:
            params: Parameter dictionary to process
            globals_context: Global variables for substitution
            
        Returns:
            Dictionary with variables substituted
            
        Raises:
            ValueError: If circular dependency detected or substitution fails
        """
        # Track substitution depth to detect circular dependencies
        substitution_stack: Set[str] = set()
        max_depth = 10  # Prevent infinite recursion
        
        def substitute_value(value: Any, depth: int = 0) -> Any:
            if depth > max_depth:
                raise ValueError(f"Maximum substitution depth ({max_depth}) exceeded - possible circular dependency")
            
            if isinstance(value, str):
                return self._substitute_string(value, globals_context, substitution_stack, depth)
            elif isinstance(value, dict):
                return {k: substitute_value(v, depth + 1) for k, v in value.items()}
            elif isinstance(value, list):
                return [substitute_value(item, depth + 1) for item in value]
            else:
                return value
        
        return {k: substitute_value(v) for k, v in params.items()}
    
    def _substitute_string(
        self, 
        text: str, 
        context: Dict[str, Any], 
        substitution_stack: Set[str], 
        depth: int
    ) -> str:
        """
        Substitute variables in a string using ${key.subkey} syntax.
        
        Args:
            text: String to process
            context: Variable context for substitution
            substitution_stack: Set of variables currently being substituted
            depth: Current substitution depth
            
        Returns:
            String with variables substituted
            
        Raises:
            ValueError: If variable substitution fails or circular dependency detected
        """
        def replace_var(match):
            var_path = match.group(1)
            
            # Check for circular dependency
            if var_path in substitution_stack:
                raise ValueError(f"Circular dependency detected: {var_path}")
            
            try:
                # Navigate nested dictionary using dot notation
                value = context
                for key in var_path.split('.'):
                    if not isinstance(value, dict):
                        raise KeyError(f"Cannot access key '{key}' on non-dict value")
                    value = value[key]
                
                result = str(value)
                
                # Recursively substitute if the result contains more variables
                if self._substitution_pattern.search(result):
                    substitution_stack.add(var_path)
                    try:
                        result = self._substitute_string(result, context, substitution_stack, depth + 1)
                    finally:
                        substitution_stack.discard(var_path)
                
                return result
                
            except (KeyError, TypeError) as e:
                raise ValueError(f"Variable substitution failed for ${{{var_path}}}: {e}")
        
        return self._substitution_pattern.sub(replace_var, text)


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
            
        logger.info(f"Loaded job config: {config_path}")
        return config
        
    except yaml.YAMLError as e:
        raise RuntimeError(f"Invalid YAML in {config_path}: {e}")
    except PermissionError as e:
        raise RuntimeError(f"Cannot read job config {config_path}: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed to load job config {config_path}: {e}")


def create_resolver(search_path: Optional[str] = None) -> ParameterResolver:
    """
    Convenience function to create a parameter resolver with project discovery.
    
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


# TODO: Future orchestrator placeholders
def resolve_distributed_params(mod_name: str, distributed_context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Resolve parameters in distributed execution context (future orchestrator).
    
    TODO: Implement when orchestrator supports distributed execution.
    Will handle:
    - Cross-server parameter synchronization
    - Distributed variable substitution
    - Cluster-wide configuration management
    
    Args:
        mod_name: Name of the mod
        distributed_context: Distributed execution context
        
    Returns:
        Resolved parameters for distributed execution
        
    Raises:
        NotImplementedError: Feature not yet implemented
    """
    raise NotImplementedError("Distributed parameter resolution not yet implemented - needed for Phase 2 orchestrator")


def validate_parameter_dependencies(params: Dict[str, Any]) -> bool:
    """
    Validate parameter dependencies for orchestrator scheduling (future).
    
    TODO: Implement when orchestrator supports dependency analysis.
    Will validate:
    - Parameter dependency graphs
    - Circular dependency detection across mods
    - Resource requirement validation
    
    Args:
        params: Parameter dictionary to validate
        
    Returns:
        True if dependencies are valid
        
    Raises:
        NotImplementedError: Feature not yet implemented
    """
    raise NotImplementedError("Parameter dependency validation not yet implemented - needed for Phase 2 orchestrator")