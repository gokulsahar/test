"""
Python SDK for DataPy framework with registry-based mod execution.

Provides clean API for mod execution with registry lookup,
state-based logging and future orchestrator support.
"""

import os
import json
import time
import glob
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
from .logger import (
    setup_job_logging, setup_logger, archive_completed_state,
    DEFAULT_LOG_CONFIG
)
from .params import create_resolver, load_job_config
from .result import ModResult, validation_error, runtime_error
from .registry import get_registry

# Global configuration storage
_global_config: Dict[str, Any] = {}

logger = setup_logger(__name__)


def set_global_config(config: Dict[str, Any]) -> None:
    """
    Set global configuration for SDK execution.
    
    Args:
        config: Configuration dictionary to merge with global config
        
    Raises:
        ValueError: If config is not a dictionary
    """
    global _global_config
    
    if not isinstance(config, dict):
        raise ValueError("config must be a dictionary")
    
    _global_config.update(config)
    logger.debug(f"Updated global config", extra={"config_keys": list(config.keys())})


def get_global_config() -> Dict[str, Any]:
    """
    Get copy of current global configuration.
    
    Returns:
        Copy of global configuration dictionary
    """
    return _global_config.copy()


def clear_global_config() -> None:
    """Clear global configuration (useful for testing)."""
    global _global_config
    _global_config.clear()
    logger.debug("Cleared global config")


def _get_execution_context() -> str:
    """
    Get execution context for logging and state management.
    
    Returns:
        Execution context identifier
    """
    # Simplified context detection - no complex frame inspection
    return "sdk_execution"


def _find_or_create_job_files(execution_context: str, is_cli: bool = False) -> Tuple[str, str]:
    """
    Find existing or create new job files based on context.
    
    Args:
        execution_context: Context identifier for the execution
        is_cli: Whether this is CLI execution
        
    Returns:
        Tuple of (log_file_path, state_file_path)
        
    Raises:
        RuntimeError: If file creation fails
    """
    try:
        log_base = Path(_global_config.get('log_path', 'logs'))
        state_running = log_base / "state" / "running"
        
        # Ensure directories exist
        state_running.mkdir(parents=True, exist_ok=True)
        log_base.mkdir(exist_ok=True)
        
        # Look for existing state files for this context
        pattern = str(state_running / f"{execution_context}_*.state")
        existing_states = glob.glob(pattern)
        
        if existing_states:
            # Use existing execution
            state_file_path = existing_states[0]
            execution_id = Path(state_file_path).stem
            log_file_path = str(log_base / f"{execution_id}.log")
            
            # Ensure log file exists
            if not Path(log_file_path).exists():
                Path(log_file_path).touch()
            
            return log_file_path, state_file_path
        else:
            # Create new execution
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            execution_id = f"{execution_context}_{timestamp}"
            log_file_path = str(log_base / f"{execution_id}.log")
            state_file_path = str(state_running / f"{execution_id}.state")
            
            # Create log file
            Path(log_file_path).touch()
            
            return log_file_path, state_file_path
            
    except Exception as e:
        raise RuntimeError(f"Failed to create job files: {e}")


def _initialize_job_state_sdk(state_file_path: str) -> None:
    """
    Initialize job state for SDK execution.
    
    Args:
        state_file_path: Path to state file
        
    Raises:
        RuntimeError: If state initialization fails
    """
    try:
        execution_id = Path(state_file_path).stem
        
        initial_state = {
            "execution_id": execution_id,
            "yaml_file": None,
            "expected_mods": [],
            "completed_mods": [],
            "failed_mods": [],
            "is_complete": False,
            "mode": "sdk",
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        
        # Ensure directory exists
        Path(state_file_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Write state atomically
        temp_file = state_file_path + '.tmp'
        with open(temp_file, 'w') as f:
            json.dump(initial_state, f, indent=2)
        os.rename(temp_file, state_file_path)
        
        logger.info(f"Initialized SDK job state", extra={"state_file": state_file_path})
        
    except Exception as e:
        # Clean up partial files
        for temp_path in [state_file_path + '.tmp', state_file_path]:
            try:
                if Path(temp_path).exists():
                    os.unlink(temp_path)
            except:
                pass
        raise RuntimeError(f"Failed to initialize SDK job state: {e}")


def _add_mod_to_expected_list(state_file_path: str, mod_name: str) -> None:
    """
    Add mod to expected list in state file for SDK tracking.
    
    Args:
        state_file_path: Path to state file
        mod_name: Name of mod to add
    """
    try:
        if not Path(state_file_path).exists():
            logger.warning(f"State file does not exist: {state_file_path}")
            return
        
        with open(state_file_path, 'r') as f:
            state = json.load(f)
        
        if not isinstance(state, dict):
            logger.warning(f"Invalid state file format: {state_file_path}")
            return
        
        if mod_name not in state.get('expected_mods', []):
            state.setdefault('expected_mods', []).append(mod_name)
            state['last_updated'] = time.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            # Write atomically
            temp_file = state_file_path + '.tmp'
            with open(temp_file, 'w') as f:
                json.dump(state, f, indent=2)
            os.rename(temp_file, state_file_path)
            
            logger.debug(f"Added mod to expected list", extra={"mod_name": mod_name})
    
    except Exception as e:
        logger.warning(f"Failed to add mod {mod_name} to expected list: {e}")


def _update_job_state(state_file_path: str, mod_name: str, status: str) -> None:
    """
    Update job state with mod completion.
    
    Args:
        state_file_path: Path to state file
        mod_name: Name of completed mod
        status: Completion status
    """
    try:
        from .logger import update_job_state
        update_job_state(state_file_path, mod_name, status)
    except Exception as e:
        logger.error(f"Failed to update job state: {e}")


def _validate_mod_execution_inputs(mod_type: str, params: Dict[str, Any], mod_name: str) -> None:
    """
    Validate inputs for mod execution.
    
    Args:
        mod_type: Mod type identifier
        params: Parameters for the mod
        mod_name: Unique name for this mod instance
        
    Raises:
        ValueError: If inputs are invalid
    """
    if not mod_type or not isinstance(mod_type, str):
        raise ValueError("mod_type must be a non-empty string")
    
    if not isinstance(params, dict):
        raise ValueError("params must be a dictionary")
    
    if not mod_name or not isinstance(mod_name, str) or not mod_name.strip():
        raise ValueError("mod_name must be a non-empty string")
    
    if not mod_name.strip().isidentifier():
        raise ValueError(f"mod_name '{mod_name}' must be a valid Python identifier")


def _setup_mod_execution_environment(mod_name: str) -> Dict[str, Any]:
    """
    Setup execution environment for mod execution.
    
    Args:
        mod_name: Name of mod being executed
        
    Returns:
        Dictionary containing execution context info
        
    Raises:
        RuntimeError: If environment setup fails
    """
    execution_context = _get_execution_context()
    
    # Create job files for logging and state
    log_file_path, state_file_path = _find_or_create_job_files(execution_context, is_cli=False)
    
    # Initialize state if needed
    if not Path(state_file_path).exists():
        _initialize_job_state_sdk(state_file_path)
    
    # Add to expected mods list
    _add_mod_to_expected_list(state_file_path, mod_name)
    
    # Setup logging
    log_config = DEFAULT_LOG_CONFIG.copy()
    log_config.update(_global_config)
    setup_job_logging(log_file_path, log_config)
    
    return {
        'log_file_path': log_file_path,
        'state_file_path': state_file_path,
        'execution_context': execution_context
    }


def _resolve_mod_parameters(mod_type: str, params: Dict[str, Any], mod_name: str) -> Dict[str, Any]:
    """
    Resolve parameters using the inheritance chain.
    
    Args:
        mod_type: Type of mod being executed
        params: Raw parameters from caller
        mod_name: Name of mod instance
        
    Returns:
        Resolved parameters dictionary
        
    Raises:
        RuntimeError: If parameter resolution fails
    """
    try:
        resolver = create_resolver()
        
        # Registry mods don't have built-in defaults
        mod_defaults = {}
        
        resolved_params = resolver.resolve_mod_params(
            mod_name=mod_type,
            job_params=params,
            mod_defaults=mod_defaults,
            globals_override=_global_config
        )
        
        return resolved_params
        
    except Exception as e:
        raise RuntimeError(f"Parameter resolution failed: {e}")


def _execute_registry_mod(
    mod_type: str, 
    resolved_params: Dict[str, Any], 
    mod_name: str, 
    execution_context: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Execute mod via registry with full validation.
    
    Args:
        mod_type: Type of mod to execute
        resolved_params: Resolved parameters
        mod_name: Name of mod instance
        execution_context: Execution environment context
        
    Returns:
        ModResult dictionary
        
    Raises:
        RuntimeError: If mod execution fails
    """
    # Setup mod-specific logger
    mod_logger = setup_logger(
        f"registry.{mod_type}.execution", 
        execution_context['log_file_path'], 
        mod_type, 
        mod_name
    )
    
    # Registry lookup and execution
    registry = get_registry()
    mod_logger.info(f"Starting registry mod execution", extra={"params": resolved_params})
    
    try:
        result = registry.execute_mod(mod_type, resolved_params, mod_name)
        
        # Validate result structure
        _validate_mod_result(result)
        
        # Update result with mod information
        result['logs']['mod_name'] = mod_name
        result['logs']['mod_type'] = mod_type
        
        # Update job state
        _update_job_state(execution_context['state_file_path'], mod_name, result['status'])
        
        mod_logger.info(f"Registry mod execution completed", extra={
            "status": result['status'],
            "execution_time": result['execution_time'],
            "exit_code": result['exit_code']
        })
        
        return result
        
    except Exception as e:
        mod_logger.error(f"Registry mod execution failed: {e}", exc_info=True)
        _update_job_state(execution_context['state_file_path'], mod_name, 'error')
        raise RuntimeError(f"Mod execution failed: {e}")


def _validate_mod_result(result: Dict[str, Any]) -> None:
    """
    Validate mod result has required structure.
    
    Args:
        result: Result dictionary from mod execution
        
    Raises:
        RuntimeError: If result structure is invalid
    """
    if not isinstance(result, dict):
        raise RuntimeError(f"Mod must return a dictionary, got {type(result)}")
    
    required_fields = [
        'status', 'execution_time', 'exit_code', 'metrics', 
        'artifacts', 'globals', 'warnings', 'errors', 'logs'
    ]
    missing_fields = [field for field in required_fields if field not in result]
    if missing_fields:
        raise RuntimeError(f"Result missing required fields: {missing_fields}")
    
    if result['status'] not in ('success', 'warning', 'error'):
        raise RuntimeError(f"Invalid status: {result['status']}")


def run_mod(mod_path: str, params: Dict[str, Any], mod_name: str) -> Dict[str, Any]:
    """
    Execute a DataPy mod via registry with optimized performance.
    
    Args:
        mod_path: Mod type identifier (looked up in registry)
        params: Parameters for the mod
        mod_name: Unique name for this mod instance
        
    Returns:
        ModResult dictionary with execution results
    """
    try:
        # Clean and validate inputs
        mod_type = mod_path.strip()
        mod_name = mod_name.strip()
        
        _validate_mod_execution_inputs(mod_type, params, mod_name)
        
        # Check registry for mod existence
        registry = get_registry()
        try:
            registry.get_mod_info(mod_type)
        except ValueError as e:
            suggestion = f"python -m datapy register-mod <module_path>"
            return validation_error(mod_name, f"{e}. Register it with: {suggestion}")
        
        # Setup execution environment
        execution_context = _setup_mod_execution_environment(mod_name)
        
        # Resolve parameters
        resolved_params = _resolve_mod_parameters(mod_type, params, mod_name)
        
        # Execute mod
        result = _execute_registry_mod(mod_type, resolved_params, mod_name, execution_context)
        
        return result
        
    except ValueError as e:
        return validation_error(mod_name or "unknown", str(e))
    except RuntimeError as e:
        return runtime_error(mod_name or "unknown", str(e))
    except Exception as e:
        return runtime_error(mod_name or "unknown", f"Unexpected error: {e}")


def get_mod_result(mod_name: str) -> Optional[Dict[str, Any]]:
    """
    Get the result of a previously executed mod (for backwards compatibility).
    
    Note: With the new explicit return API, this function is no longer needed
    since results are returned directly from run_mod(). Kept for compatibility.
    
    Args:
        mod_name: Name of the mod to get results for
        
    Returns:
        None (results are now returned directly from run_mod)
    """
    logger.warning(f"get_mod_result() is deprecated - results are now returned directly from run_mod()")
    return None