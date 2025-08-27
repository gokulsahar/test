"""
Registry-based mod discovery and execution for DataPy framework.

Provides fast mod lookup, validation, and execution via centralized registry
with no fallback discovery - registry is the single source of truth.
"""

import json
import importlib
import inspect
import os
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

from .logger import setup_logger
from .result import validation_error, runtime_error

logger = setup_logger(__name__)


class ModRegistry:
    """
    Central mod registry for fast discovery and execution.
    
    Registry is the single source of truth - no fallback discovery.
    Provides fast mod lookup, parameter validation, and execution.
    """
    
    def __init__(self, registry_path: Optional[str] = None) -> None:
        """
        Initialize registry with automatic discovery of registry file.
        
        Args:
            registry_path: Path to registry file (auto-discovered if None)
            
        Raises:
            RuntimeError: If registry file cannot be loaded
        """
        self.registry_path = registry_path or self._find_registry_file()
        self.registry_data = self._load_registry()
    
    def _find_registry_file(self) -> str:
        """
        Find registry file in the DataPy framework structure.
        
        Returns:
            Path to mod_registry.json file
            
        Raises:
            RuntimeError: If registry file not found
        """
        # Look for registry at framework level (same as mod_manager)
        current_dir = Path(__file__).parent
        framework_dir = current_dir.parent
        registry_path = framework_dir / "mod_registry.json"
        
        if registry_path.exists() and registry_path.is_file():
            return str(registry_path)
        
        raise RuntimeError(f"Registry file not found at: {registry_path}")
    
    def _load_registry(self) -> Dict[str, Any]:
        """
        Load registry data from JSON file with validation.
        
        Returns:
            Registry data dictionary
            
        Raises:
            RuntimeError: If registry file is invalid or cannot be loaded
        """
        try:
            with open(self.registry_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, dict):
                raise RuntimeError("Registry file must contain a JSON dictionary")
            
            if 'mods' not in data:
                raise RuntimeError("Registry file missing 'mods' section")
            
            if not isinstance(data['mods'], dict):
                raise RuntimeError("Registry 'mods' section must be a dictionary")
            
            logger.info(f"Loaded registry with {len(data['mods'])} mods")
            return data
            
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Invalid JSON in registry file {self.registry_path}: {e}")
        except FileNotFoundError:
            raise RuntimeError(f"Registry file not found: {self.registry_path}")
        except Exception as e:
            raise RuntimeError(f"Failed to load registry: {e}")
    
    def _save_registry(self) -> None:
        """
        Save registry data to JSON file atomically (Windows-compatible).
        
        Raises:
            RuntimeError: If registry cannot be saved
        """
        try:
            # Update metadata
            self.registry_data.setdefault('_metadata', {})
            self.registry_data['_metadata']['last_updated'] = datetime.now().isoformat()
            
            # Atomic write using temporary file (Windows-compatible)
            temp_file = self.registry_path + '.tmp'
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(self.registry_data, f, indent=2, ensure_ascii=False)
            
            # Windows-compatible atomic replace
            if os.path.exists(self.registry_path):
                backup_file = self.registry_path + '.backup'
                # Remove old backup if exists
                if os.path.exists(backup_file):
                    os.unlink(backup_file)
                # Move current to backup
                os.rename(self.registry_path, backup_file)
                # Move temp to current
                os.rename(temp_file, self.registry_path)
                # Remove backup
                os.unlink(backup_file)
            else:
                # Simple rename if target doesn't exist
                os.rename(temp_file, self.registry_path)
            
            logger.info("Registry saved successfully")
            
        except Exception as e:
            # Clean up temp file if it exists
            temp_file = self.registry_path + '.tmp'
            if Path(temp_file).exists():
                try:
                    os.unlink(temp_file)
                except:
                    pass
            raise RuntimeError(f"Failed to save registry: {e}")
    
    def get_mod_info(self, mod_type: str) -> Dict[str, Any]:
        """
        Get mod information from registry.
        
        Args:
            mod_type: Type of mod to lookup
            
        Returns:
            Mod information dictionary
            
        Raises:
            ValueError: If mod_type not found in registry
        """
        if not mod_type or not isinstance(mod_type, str):
            raise ValueError("mod_type must be a non-empty string")
        
        mod_type = mod_type.strip()
        if mod_type not in self.registry_data['mods']:
            available_mods = list(self.registry_data['mods'].keys())
            raise ValueError(f"Mod '{mod_type}' not found in registry. Available mods: {available_mods}")
        
        return self.registry_data['mods'][mod_type].copy()
    
    def execute_mod(self, mod_type: str, params: Dict[str, Any], mod_name: str) -> Dict[str, Any]:
        """
        Execute a mod via registry lookup.
        
        Args:
            mod_type: Type of mod to execute
            params: Parameters for the mod
            mod_name: Unique name for this mod instance
            
        Returns:
            ModResult dictionary
            
        Raises:
            ValueError: If mod not found or parameters invalid
            RuntimeError: If mod execution fails
        """
        # Validate inputs
        if not mod_type or not isinstance(mod_type, str):
            return validation_error(mod_name or "unknown", "mod_type must be a non-empty string")
        
        if not isinstance(params, dict):
            return validation_error(mod_name or "unknown", "params must be a dictionary")
        
        if not mod_name or not isinstance(mod_name, str):
            return validation_error("unknown", "mod_name must be a non-empty string")
        
        # Get mod info from registry
        try:
            mod_info = self.get_mod_info(mod_type)
        except ValueError as e:
            return validation_error(mod_name, str(e))
        
        # Import and execute mod
        try:
            module_path = mod_info['module_path']
            mod_module = importlib.import_module(module_path)
            
            if not hasattr(mod_module, 'run'):
                return validation_error(mod_name, f"Mod {module_path} missing required 'run' function")
            
            run_func = mod_module.run
            if not callable(run_func):
                return validation_error(mod_name, f"Mod {module_path} 'run' must be callable")
            
            # Add mod metadata to params for mod use
            params_with_meta = params.copy()
            params_with_meta['_mod_name'] = mod_name
            params_with_meta['_mod_type'] = mod_type
            
            # Execute mod
            result = run_func(params_with_meta)
            
            if not isinstance(result, dict):
                return runtime_error(mod_name, f"Mod must return a dictionary, got {type(result)}")
            
            return result
            
        except ImportError as e:
            return validation_error(mod_name, f"Cannot import mod {module_path}: {e}")
        except Exception as e:
            return runtime_error(mod_name, f"Mod execution failed: {e}")
    
    def delete_mod(self, mod_type: str) -> bool:
        """
        Delete a mod from the registry.
        
        Args:
            mod_type: Type of mod to delete
            
        Returns:
            True if deletion successful
            
        Raises:
            ValueError: If mod_type not found in registry
            RuntimeError: If deletion fails
        """
        if not mod_type or not isinstance(mod_type, str):
            raise ValueError("mod_type must be a non-empty string")
        
        mod_type = mod_type.strip()
        
        # Check if mod exists
        if mod_type not in self.registry_data['mods']:
            available_mods = list(self.registry_data['mods'].keys())
            raise ValueError(f"Mod '{mod_type}' not found in registry. Available mods: {available_mods}")
        
        try:
            # Remove mod from registry
            del self.registry_data['mods'][mod_type]
            
            # Save updated registry
            self._save_registry()
            
            logger.info(f"Deleted mod from registry: {mod_type}")
            return True
            
        except Exception as e:
            raise RuntimeError(f"Failed to delete mod {mod_type}: {e}")
    
    def list_available_mods(self, category: Optional[str] = None) -> List[str]:
        """
        List available mods in registry.
        
        Args:
            category: Filter by category (optional)
            
        Returns:
            List of available mod types
        """
        mods = list(self.registry_data['mods'].keys())
        
        if category:
            # Filter by category if specified
            filtered_mods = []
            for mod_type in mods:
                mod_info = self.registry_data['mods'][mod_type]
                mod_category = mod_info.get('category', '')
                if mod_category == category:
                    filtered_mods.append(mod_type)
            return filtered_mods
        
        return mods
    
    def register_mod(self, module_path: str) -> bool:
        """
        Register a new mod in the registry with complete metadata extraction.
        
        Args:
            module_path: Full module path to the mod
            
        Returns:
            True if registration successful
            
        Raises:
            ValueError: If mod is invalid or already registered
            RuntimeError: If registration fails
        """
        if not module_path or not isinstance(module_path, str):
            raise ValueError("module_path must be a non-empty string")
        
        module_path = module_path.strip()
        
        # Import and validate mod structure
        try:
            mod_module = importlib.import_module(module_path)
        except ImportError as e:
            raise ValueError(f"Cannot import mod {module_path}: {e}")
        
        # Validate mod has required components
        if not hasattr(mod_module, 'run'):
            raise ValueError(f"Mod {module_path} missing required 'run' function")
        
        if not hasattr(mod_module, 'METADATA'):
            raise ValueError(f"Mod {module_path} missing required 'METADATA'")
        
        if not hasattr(mod_module, 'CONFIG_SCHEMA'):
            raise ValueError(f"Mod {module_path} missing required 'CONFIG_SCHEMA'")
        
        run_func = mod_module.run
        if not callable(run_func):
            raise ValueError(f"Mod {module_path} 'run' must be callable")
        
        # Extract and validate metadata
        try:
            metadata = mod_module.METADATA
            config_schema = mod_module.CONFIG_SCHEMA
            
            # Validate metadata is proper type
            from .base import ModMetadata, ConfigSchema
            if not isinstance(metadata, ModMetadata):
                raise ValueError(f"METADATA must be ModMetadata instance, got {type(metadata)}")
            
            if not isinstance(config_schema, ConfigSchema):
                raise ValueError(f"CONFIG_SCHEMA must be ConfigSchema instance, got {type(config_schema)}")
                
        except Exception as e:
            raise ValueError(f"Invalid metadata in {module_path}: {e}")
        
        # Extract mod type from metadata (not module path)
        mod_type = metadata.type
        
        # Check if already registered
        if mod_type in self.registry_data['mods']:
            raise ValueError(f"Mod type '{mod_type}' already registered")
        
        # Create registry entry with complete metadata
        registry_entry = {
            "module_path": module_path,
            "type": metadata.type,
            "version": metadata.version,
            "description": metadata.description,
            "category": metadata.category,
            "input_ports": metadata.input_ports,
            "output_ports": metadata.output_ports,
            "globals": metadata.globals,
            "packages": metadata.packages,
            "python_version": metadata.python_version,
            "config_schema": {
                "required": config_schema.required,
                "optional": config_schema.optional
            },
            "registered_at": datetime.now().isoformat()
        }
        
        # Add to registry
        self.registry_data['mods'][mod_type] = registry_entry
        
        # Save registry
        self._save_registry()
        
        logger.info(f"Registered mod: {mod_type} ({module_path})")
        return True
    
    def _guess_category(self, module_path: str) -> str:
        """Guess mod category from module path (deprecated - use metadata.category)."""
        if '.sources.' in module_path:
            return 'sources'
        elif '.transformers.' in module_path:
            return 'transformers'
        elif '.sinks.' in module_path:
            return 'sinks'
        elif '.solos.' in module_path:
            return 'solos'
        else:
            return 'unknown'
    
    def validate_params_schema(self, mod_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate parameters against mod's config schema.
        
        Args:
            mod_type: Type of mod to validate against
            params: Parameters to validate
            
        Returns:
            Validated parameters with defaults applied
            
        Raises:
            ValueError: If validation fails
        """
        mod_info = self.get_mod_info(mod_type)
        config_schema = mod_info.get('config_schema', {})
        
        validated_params = params.copy()
        
        # Check required parameters
        required_params = config_schema.get('required', {})
        for param_name, param_def in required_params.items():
            if param_name not in validated_params:
                raise ValueError(f"Missing required parameter: {param_name}")
        
        # Apply defaults for optional parameters
        optional_params = config_schema.get('optional', {})
        for param_name, param_def in optional_params.items():
            if param_name not in validated_params and 'default' in param_def:
                validated_params[param_name] = param_def['default']
        
        return validated_params
    
    def validate_registry(self) -> List[str]:
        """
        Validate all mods in registry can be imported and have proper structure.
        
        Returns:
            List of validation errors (empty if all valid)
        """
        errors = []
        
        for mod_type, mod_info in self.registry_data['mods'].items():
            try:
                module_path = mod_info.get('module_path')
                if not module_path:
                    errors.append(f"Mod '{mod_type}' missing module_path")
                    continue
                
                # Try to import mod
                mod_module = importlib.import_module(module_path)
                
                # Check for required components
                if not hasattr(mod_module, 'run'):
                    errors.append(f"Mod '{mod_type}' missing run() function")
                    continue
                
                if not callable(mod_module.run):
                    errors.append(f"Mod '{mod_type}' run is not callable")
                
                # Check for metadata components
                if not hasattr(mod_module, 'METADATA'):
                    errors.append(f"Mod '{mod_type}' missing METADATA")
                
                if not hasattr(mod_module, 'CONFIG_SCHEMA'):
                    errors.append(f"Mod '{mod_type}' missing CONFIG_SCHEMA")
                
                # Validate metadata types if present
                if hasattr(mod_module, 'METADATA') and hasattr(mod_module, 'CONFIG_SCHEMA'):
                    try:
                        from .base import ModMetadata, ConfigSchema
                        
                        if not isinstance(mod_module.METADATA, ModMetadata):
                            errors.append(f"Mod '{mod_type}' METADATA is not ModMetadata instance")
                        
                        if not isinstance(mod_module.CONFIG_SCHEMA, ConfigSchema):
                            errors.append(f"Mod '{mod_type}' CONFIG_SCHEMA is not ConfigSchema instance")
                            
                    except Exception as e:
                        errors.append(f"Mod '{mod_type}' metadata validation failed: {e}")
                
            except ImportError as e:
                errors.append(f"Mod '{mod_type}' import failed: {e}")
            except Exception as e:
                errors.append(f"Mod '{mod_type}' validation failed: {e}")
        
        return errors


# Global registry instance (singleton pattern for performance)
_global_registry: Optional[ModRegistry] = None


def get_registry() -> ModRegistry:
    """
    Get global registry instance (singleton for performance).
    
    Returns:
        ModRegistry instance
    """
    global _global_registry
    if _global_registry is None:
        _global_registry = ModRegistry()
    return _global_registry