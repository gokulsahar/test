"""
Base classes and metadata definitions for DataPy mods.

Provides the foundational classes that all mods must inherit from to ensure
consistent metadata tracking and parameter validation across the framework.
"""

from pydantic import BaseModel, Field, field_validator
import re


class ModMetadata(BaseModel):
    """
    Required metadata for all DataPy mods.
    
    Example:
        METADATA = ModMetadata(
            type="csv_reader",
            version="1.0.0", 
            description="Reads CSV files with validation and format conversion"
        )
    """
    type: str = Field(..., description="Mod type identifier (e.g., 'csv_reader')")
    version: str = Field(..., description="Mod version (semver format)")
    description: str = Field(..., description="Mod description")
    
    @field_validator('type')
    @classmethod
    def validate_type(cls, v: str) -> str:
        """Validate type follows naming conventions."""
        if not v or not isinstance(v, str):
            raise ValueError("type cannot be empty")
        
        # Must be valid Python identifier (for import paths)
        if not v.replace('_', 'a').isidentifier():
            raise ValueError("type must be a valid identifier (use underscores, not spaces)")
        
        # Recommend lowercase with underscores
        if v != v.lower():
            raise ValueError("type should be lowercase with underscores (e.g., 'csv_reader')")
            
        return v
    
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


class BaseModParams(BaseModel):
    """
    Base parameter class that all mod parameter classes must inherit from.
    
    Enforces required metadata and provides consistent parameter validation
    across all mods in the framework.
    
    Example:
        class Params(BaseModParams):
            _metadata: ModMetadata = METADATA
            input_path: str
            delimiter: str = ","
    """
    _metadata: ModMetadata = Field(..., description="Required mod metadata")
    
    model_config = {"extra": "forbid"}  # Prevent typos in parameter names