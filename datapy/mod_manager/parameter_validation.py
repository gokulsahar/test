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
        ValueError: If required parameters are missing
    """
    config_schema = mod_info.get('config_schema', {})
    if not config_schema:
        logger.debug("No config schema found - returning params as-is")
        return params.copy()
    
    result = params.copy()
    
    # Check required parameters are present
    required = config_schema.get('required', {})
    missing = [key for key in required if key not in params]
    if missing:
        raise ValueError(f"Missing required parameters: {missing}")
    
    # Apply defaults for optional parameters
    optional = config_schema.get('optional', {})
    for key, param_def in optional.items():
        if key not in result and 'default' in param_def:
            result[key] = param_def['default']
            logger.debug(f"Applied default for {key}: {param_def['default']}")
    
    logger.debug(f"Parameter validation completed - {len(result)} parameters")
    return result