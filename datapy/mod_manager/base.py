"""
Base classes and metadata definitions for DataPy mods.

Provides the foundational classes that all mods must inherit from to ensure
consistent metadata tracking and parameter validation across the framework.
"""

from pydantic import BaseModel, Field


class ModMetadata(BaseModel):
    """
    Required metadata for all DataPy mods.
    
    Ensures consistent tracking of mod information across the framework
    for version management, discovery, and documentation purposes.
    """
    name: str = Field(..., description="Mod name")
    version: str = Field(..., description="Mod version (semver)")
    description: str = Field(..., description="Mod description")


class BaseModParams(BaseModel):
    """
    Base parameter class that all mod parameter classes must inherit from.
    
    Enforces required metadata for all mods while keeping it excluded
    from the actual parameter validation during execution.
    """
    _metadata: ModMetadata = Field(..., description="Required mod metadata")
    
    class Config:
        fields = {'_metadata': {'exclude': True}}