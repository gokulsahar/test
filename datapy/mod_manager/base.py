"""
Base classes and metadata definitions for DataPy mods.

Provides the foundational classes that all mods must inherit from to ensure
consistent metadata tracking and parameter validation across the framework.
"""

from pydantic import BaseModel, Field, field_validator
from typing import List, Dict, Any
import re


class ModMetadata(BaseModel):
    """
    Complete metadata for all DataPy mods.
    
    Example:
        METADATA = ModMetadata(
            type="csv_reader",
            version="1.0.0", 
            description="Reads data from CSV files",
            category="source",
            input_ports=[],
            output_ports=["data"],
            globals=["row_count", "file_size"],
            packages=["pandas>=1.5.0", "chardet>=4.0.0"]
        )
    """
    # Basic metadata
    type: str = Field(..., description="Mod type identifier (e.g., 'csv_reader', 'data_cleaner')")
    version: str = Field(..., description="Mod version (semver format)")
    description: str = Field(..., description="Mod description")
    category: str = Field(..., description="Mod category (e.g., 'source', 'transformer')")
    
    # Data flow metadata
    input_ports: List[str] = Field(default_factory=list, description="Input port names")
    output_ports: List[str] = Field(default_factory=list, description="Output port names") 
    globals: List[str] = Field(default_factory=list, description="Global variables produced by this mod")
    
    # Dependency metadata
    packages: List[str] = Field(default_factory=list, description="Required Python packages")
    
    @field_validator('type')
    @classmethod
    def validate_type(cls, v: str) -> str:
        """Validate type is a meaningful string."""
        if not v or not isinstance(v, str):
            raise ValueError("type cannot be empty")
        
        if len(v.strip()) < 2:
            raise ValueError("type should be at least 2 characters")
            
        return v.strip()
    
    @field_validator('version')
    @classmethod
    def validate_version(cls, v: str) -> str:
        """Validate version follows semantic versioning pattern."""
        if not v or not isinstance(v, str):
            raise ValueError("version cannot be empty")
            
        semver_pattern = r'^\d+\.\d+\.\d+$'
        if not re.match(semver_pattern, v):
            raise ValueError("version must follow format 'X.Y.Z' (e.g., '1.0.0')")
        return v
    
    @field_validator('description')
    @classmethod
    def validate_description(cls, v: str) -> str:
        """Validate description is meaningful."""
        if not v or not isinstance(v, str):
            raise ValueError("description cannot be empty")
        if len(v.strip()) < 10:
            raise ValueError("description should be at least 10 characters")
        return v.strip()
    
    @field_validator('category')
    @classmethod
    def validate_category(cls, v: str) -> str:
        """Validate category is a non-empty string."""
        if not v or not isinstance(v, str):
            raise ValueError("category cannot be empty")
        return v.strip()
    
    @field_validator('packages')
    @classmethod
    def validate_packages(cls, v: List[str]) -> List[str]:
        """Validate package requirements follow pip format."""
        if not isinstance(v, list):
            raise ValueError("packages must be a list")
        
        for pkg in v:
            if not isinstance(pkg, str) or not pkg.strip():
                raise ValueError("each package must be a non-empty string")
            
            # Basic validation for pip requirement format
            pkg_clean = pkg.strip()
            if not re.match(r'^[a-zA-Z0-9_-]+([><=!]+[0-9.]+.*)?$', pkg_clean):
                raise ValueError(f"invalid package requirement format: {pkg}")
        
        return [pkg.strip() for pkg in v]


class ConfigSchema(BaseModel):
    """
    Configuration schema for mod parameters.
    
    Example:
        CONFIG_SCHEMA = ConfigSchema(
            required={
                "file_path": {"type": "str", "description": "Path to CSV file"}
            },
            optional={
                "encoding": {"type": "str", "default": "utf-8", "description": "File encoding"},
                "delimiter": {"type": "str", "default": ",", "description": "CSV delimiter"}
            }
        )
    """
    required: Dict[str, Dict[str, Any]] = Field(default_factory=dict, description="Required parameters")
    optional: Dict[str, Dict[str, Any]] = Field(default_factory=dict, description="Optional parameters")
    
    @field_validator('required', 'optional')
    @classmethod
    def validate_param_schema(cls, v: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Validate parameter schema structure."""
        if not isinstance(v, dict):
            raise ValueError("Parameter schema must be a dictionary")
        
        valid_types = {'str', 'int', 'float', 'bool', 'list', 'dict', 'object'}
        
        for param_name, param_def in v.items():
            cls._validate_param_definition(param_name, param_def, valid_types)
        
        return v

    @staticmethod
    def _validate_param_definition(param_name: str, param_def: Dict[str, Any], valid_types: set) -> None:
        """Validate a single parameter definition."""
        if not isinstance(param_def, dict):
            raise ValueError(f"Parameter definition for '{param_name}' must be a dictionary")
        
        ConfigSchema._validate_required_fields(param_name, param_def)
        ConfigSchema._validate_param_type(param_name, param_def, valid_types)
        ConfigSchema._validate_description(param_name, param_def)
        ConfigSchema._validate_default_if_present(param_name, param_def)

    @staticmethod
    def _validate_required_fields(param_name: str, param_def: Dict[str, Any]) -> None:
        """Validate required fields are present."""
        if 'type' not in param_def:
            raise ValueError(f"Parameter '{param_name}' missing required 'type' field")
        
        if 'description' not in param_def:
            raise ValueError(f"Parameter '{param_name}' missing required 'description' field")

    @staticmethod
    def _validate_param_type(param_name: str, param_def: Dict[str, Any], valid_types: set) -> None:
        """Validate parameter type field."""
        param_type = param_def['type']
        if not isinstance(param_type, str) or param_type not in valid_types:
            raise ValueError(f"Parameter '{param_name}' has invalid type '{param_type}'. Valid types: {valid_types}")

    @staticmethod
    def _validate_description(param_name: str, param_def: Dict[str, Any]) -> None:
        """Validate parameter description field."""
        description = param_def['description']
        if not isinstance(description, str) or not description.strip():
            raise ValueError(f"Parameter '{param_name}' description must be a non-empty string")

    @staticmethod
    def _validate_default_if_present(param_name: str, param_def: Dict[str, Any]) -> None:
        """Validate default value type if present."""
        if 'default' in param_def:
            default_val = param_def['default']
            param_type = param_def['type']
            if not ConfigSchema._validate_default_type(default_val, param_type):
                raise ValueError(f"Parameter '{param_name}' default value type doesn't match declared type '{param_type}'")
    
    @staticmethod
    def _validate_default_type(value: Any, declared_type: str) -> bool:
        """Validate default value matches declared type."""
        if value is None:
            return True  # None is valid for any type
        
        type_checks = {
            'str': lambda x: isinstance(x, str),
            'int': lambda x: isinstance(x, int) and not isinstance(x, bool),  # bool is subclass of int
            'float': lambda x: isinstance(x, (int, float)) and not isinstance(x, bool),
            'bool': lambda x: isinstance(x, bool),
            'list': lambda x: isinstance(x, list),
            'dict': lambda x: isinstance(x, dict),
            'object': lambda x: True  # object accepts anything
        }
        
        return type_checks.get(declared_type, lambda x: False)(value)
    
    @field_validator('required')
    @classmethod
    def validate_required_no_defaults(cls, v: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Validate required parameters don't have default values."""
        for param_name, param_def in v.items():
            if 'default' in param_def:
                raise ValueError(f"Required parameter '{param_name}' cannot have a default value")
        
        return v