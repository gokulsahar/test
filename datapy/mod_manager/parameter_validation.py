"""
Parameter validation module for DataPy framework.

Provides simple parameter validation: required params check and defaults application.
Mods handle domain-specific validation in their run() functions.
"""

from typing import Dict, Any

from .logger import setup_logger

logger = setup_logger(__name__)


def validate_mod_parameters(mod_info: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simple parameter validation: check required params and apply defaults.
    
    Args:
        mod_info: Mod information from registry containing config_schema
        params: Parameters to validate
        
    Returns:
        Parameters with defaults applied
        
    Raises:
        ValueError: If required parameters are missing or inputs are invalid
        TypeError: If mod_info or params are not dictionaries
    """
    # Validate input types
    if not isinstance(mod_info, dict):
        raise TypeError(f"mod_info must be a dictionary, got {type(mod_info).__name__}")
    
    if not isinstance(params, dict):
        raise TypeError(f"params must be a dictionary, got {type(params).__name__}")
    
    # Get config schema with safe access
    config_schema = mod_info.get('config_schema', {})
    
    if not isinstance(config_schema, dict):
        logger.warning(f"Invalid config_schema type: {type(config_schema).__name__}, using empty schema")
        config_schema = {}
    
    if not config_schema:
        logger.debug("No config schema found - returning params as-is")
        return params.copy()
    
    result = params.copy()
    
    # Check required parameters are present
    required = config_schema.get('required', {})
    if not isinstance(required, dict):
        logger.warning(f"Invalid required section type: {type(required).__name__}, skipping required validation")
        required = {}
    
    missing = [key for key in required if key not in params]
    if missing:
        missing_str = ', '.join(sorted(missing))
        raise ValueError(f"Missing required parameters: {missing_str}")
    
    # Apply defaults for optional parameters
    optional = config_schema.get('optional', {})
    if not isinstance(optional, dict):
        logger.warning(f"Invalid optional section type: {type(optional).__name__}, skipping defaults")
        optional = {}
    
    defaults_applied = 0
    for key, param_def in optional.items():
        if not isinstance(param_def, dict):
            logger.warning(f"Invalid parameter definition for '{key}': {type(param_def).__name__}, skipping")
            continue
            
        if key not in result and 'default' in param_def:
            result[key] = param_def['default']
            logger.debug(f"Applied default for {key}: {param_def['default']}")
            defaults_applied += 1
    
    logger.debug(f"Parameter validation completed - {len(result)} parameters, {defaults_applied} defaults applied")
    return result