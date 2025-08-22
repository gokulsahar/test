"""
Base classes and metadata definitions for DataPy mods.

Provides the foundational classes that all mods must inherit from to ensure
consistent metadata tracking and parameter validation across the framework.
"""

from pydantic import BaseModel, Field, field_validator
from typing import List, Dict, Any, Optional
import re


class ModMetadata(BaseModel):
    """
    Complete metadata for all DataPy mods.
    
    Example:
        METADATA = ModMetadata(
            type="csv_reader",
            version="1.0.0", 
            description="Reads data from CSV files",
            category="sources",
            input_ports=[],
            output_ports=["data"],
            globals=["row_count", "file_size"],
            packages=["pandas>=1.5.0", "chardet>=4.0.0"],
            python_version=">=3.8"
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
    python_version: str = Field(default=">=3.8", description="Required Python version")
    
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
        """Validate category follows framework categories."""
        if not v or not isinstance(v, str):
            raise ValueError("category cannot be empty")
        
        # Allow framework categories
        valid_categories = ['source', 'transformer', 'sink', 'solo']
        if v not in valid_categories:
            raise ValueError(f"category must be one of: {valid_categories}")
            
        return v


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
        
        for param_name, param_def in v.items():
            if not isinstance(param_def, dict):
                raise ValueError(f"Parameter definition for '{param_name}' must be a dictionary")
            
            if 'type' not in param_def:
                raise ValueError(f"Parameter '{param_name}' missing required 'type' field")
            
            if 'description' not in param_def:
                raise ValueError(f"Parameter '{param_name}' missing required 'description' field")
        
        return v


class BaseModParams(BaseModel):
    """
    Base parameter class that all mod parameter classes must inherit from.
    
    Enforces required metadata and provides consistent parameter validation
    across all mods in the framework.
    
    Example:
        class Params(BaseModParams):
            metadata: ModMetadata = METADATA
            config_schema: ConfigSchema = CONFIG_SCHEMA
            input_path: str
            delimiter: str = ","
    """
    metadata: ModMetadata = Field(..., description="Required mod metadata")
    config_schema: ConfigSchema = Field(..., description="Required configuration schema")
    
    model_config = {"extra": "forbid"}  # Prevent typos in parameter names