"""
Base classes and metadata definitions for DataPy mods.

Provides the foundational classes that all mods must inherit from to ensure
consistent metadata tracking and parameter validation across the framework.
"""

from pydantic import BaseModel, Field, field_validator
from typing import List
import re


class ModMetadata(BaseModel):
    """
    Required metadata for all DataPy mods.
    
    Ensures consistent tracking of mod information across the framework
    for version management, discovery, and documentation purposes.
    """
    name: str = Field(..., description="Mod name")
    version: str = Field(..., description="Mod version (semver)")
    description: str = Field(..., description="Mod description")
    tags: List[str] = Field(default_factory=list, description="Mod tags")
    
    @field_validator('version')
    @classmethod
    def validate_version(cls, v: str) -> str:
        """Validate version follows semantic versioning pattern."""
        semver_pattern = r'^\d+\.\d+\.\d+$'

        if not re.match(semver_pattern, v):
            raise ValueError("version must follow format 'X.Y.Z' (e.g., '1.0.0')")
        return v


class BaseModParams(BaseModel):
    """
    Base parameter class that all mod parameter classes must inherit from.
    
    Enforces required metadata for all mods while keeping it excluded
    from the actual parameter validation during execution.
    """
    _metadata: ModMetadata = Field(..., description="Required mod metadata")
    
    class Config:
        fields = {'_metadata': {'exclude': True}}