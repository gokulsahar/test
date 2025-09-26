"""
DuckDB Initialization Mod

Initializes a DuckDB connection with memory limits and custom configuration.
This connection is shared across the entire pipeline for zero-copy operations.
"""

import duckdb
from typing import Dict, Any
from datapy.mod_manager.base import ModMetadata, ConfigSchema
from datapy.mod_manager.result import ModResult
from datapy.mod_manager.logger import setup_logger

logger = setup_logger(__name__)


# Metadata
METADATA = ModMetadata(
    type="duckdb_init",
    version="1.0.0",
    description="Initialize DuckDB connection with memory and configuration settings",
    category="duckdb",
    input_ports=[],
    output_ports=["connection"],
    globals=[],
    packages=["duckdb>=1.0.0"]
)


# Configuration Schema
CONFIG_SCHEMA = ConfigSchema(
    required={},
    optional={
        "memory_limit": {
            "type": "str",
            "default": "4GB",
            "description": "Memory limit for DuckDB (e.g., '4GB', '2GB', '500MB')"
        },
        "config": {
            "type": "dict",
            "default": None,
            "description": "Power user dict for custom DuckDB settings (passed to duckdb.connect)"
        }
    }
)


def run(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Initialize DuckDB connection with specified settings.    
    Uses DuckDB's native config parameter in connect() for secure configuration.
    
    Args:
        params: Configuration parameters containing:
            - memory_limit (str, optional): Memory limit for DuckDB
            - config (dict, optional): Custom DuckDB settings
            
    Returns:
        ModResult dict with connection artifact and metrics
    """
    mod_name = params.get("_mod_name", "duckdb_init")
    result = ModResult("duckdb_init", mod_name)
    
    try:
        # Get parameters
        memory_limit = params.get("memory_limit", "4GB")
        custom_config = params.get("config", {})
        
        logger.debug(f"Initializing DuckDB connection with memory_limit={memory_limit}")
        
        duckdb_config = {
            "memory_limit": memory_limit
        }
        
        # Merge custom config if provided
        if custom_config and isinstance(custom_config, dict):
            logger.debug(f"Merging {len(custom_config)} custom config settings")
            duckdb_config.update(custom_config)
        
        con = duckdb.connect(":memory:", config=duckdb_config)
        
        logger.debug(f"DuckDB connection created successfully")
        logger.debug(f"Applied configuration: {duckdb_config}")
        
        # Add artifacts
        result.add_artifact("connection", con)
        
        # Add metrics
        result.add_metric("memory_limit", memory_limit)
        result.add_metric("config_applied", duckdb_config)
        
        return result.success()
        
    except Exception as e:
        result.add_error(f"Failed to initialize DuckDB connection: {str(e)}")
        return result.error()