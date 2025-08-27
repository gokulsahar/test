"""
Parameter validation module for DataPy framework.

Provides JSON Schema-based parameter validation for mods, used by both
CLI and SDK execution paths for consistent validation logic.
"""

from typing import Dict, Any
import jsonschema

from .logger import setup_logger

logger = setup_logger(__name__)


def validate_mod_parameters(mod_info: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate parameters using JSON Schema - common for CLI and SDK.
    
    Args:
        mod_info: Mod information from registry containing config_schema
        params: Parameters to validate
        
    Returns:
        Validated parameters with defaults applied
        
    Raises:
        ValueError: If validation fails
    """
    config_schema = mod_info.get('config_schema', {})
    if not config_schema:
        logger.debug("No config schema found - skipping validation")
        return params.copy()
    
    try:
        # Apply defaults first
        validated_params = _apply_defaults(params, config_schema)
        
        # Convert to JSON Schema and validate
        json_schema = _convert_to_json_schema(config_schema)
        jsonschema.validate(validated_params, json_schema)
        
        logger.debug(f"Parameters validated successfully")
        return validated_params
        
    except jsonschema.ValidationError as e:
        # Create user-friendly error message
        error_path = '.'.join(str(p) for p in e.absolute_path) if e.absolute_path else 'root'
        raise ValueError(f"Parameter validation failed at '{error_path}': {e.message}")
    except Exception as e:
        raise ValueError(f"Parameter validation error: {e}")


def _apply_defaults(params: Dict[str, Any], config_schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply default values from schema to parameters.
    
    Args:
        params: Input parameters
        config_schema: Schema with default values
        
    Returns:
        Parameters with defaults applied
    """
    result = params.copy()
    
    # Apply defaults from optional parameters
    optional_params = config_schema.get('optional', {})
    for param_name, param_def in optional_params.items():
        if param_name not in result and 'default' in param_def:
            result[param_name] = param_def['default']
            logger.debug(f"Applied default for {param_name}: {param_def['default']}")
    
    return result


def _convert_to_json_schema(config_schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert custom schema format to standard JSON Schema.
    
    Args:
        config_schema: Custom schema from registry
        
    Returns:
        Standard JSON Schema dictionary
    """
    json_schema = {
        "type": "object",
        "properties": {},
        "required": [],
        "additionalProperties": True  # Allow extra fields like _mod_name
    }
    
    # Add required parameters
    required_params = config_schema.get('required', {})
    for param_name, param_def in required_params.items():
        json_schema["properties"][param_name] = _convert_param_def(param_def)
        json_schema["required"].append(param_name)
    
    # Add optional parameters
    optional_params = config_schema.get('optional', {})
    for param_name, param_def in optional_params.items():
        json_schema["properties"][param_name] = _convert_param_def(param_def)
    
    return json_schema


def _convert_param_def(param_def: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert parameter definition to JSON Schema property.
    
    Args:
        param_def: Parameter definition from custom schema
        
    Returns:
        JSON Schema property definition
    """
    # Map custom types to JSON Schema types
    type_mapping = {
        'str': 'string',
        'int': 'integer', 
        'float': 'number',
        'bool': 'boolean',
        'list': 'array',
        'dict': 'object'
    }
    
    json_prop = {
        "type": type_mapping.get(param_def.get('type', 'string'), 'string'),
        "description": param_def.get('description', 'No description')
    }
    
    # Add enum constraint if present
    if 'enum' in param_def:
        json_prop['enum'] = param_def['enum']
    
    # Add default value if present (for documentation)
    if 'default' in param_def:
        json_prop['default'] = param_def['default']
    
    return json_prop