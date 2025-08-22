"""
Python SDK for DataPy framework with explicit return values.

Provides clean API for mod execution without frame manipulation,
with state-based logging and future orchestrator support.
"""

import importlib
import inspect
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
from .base import ModMetadata, BaseModParams

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


def _resolve_mod_path(mod_identifier: str) -> str:
    """
    Resolve mod identifier to full module path.
    
    Args:
        mod_identifier: Mod identifier (short name or full path)
        
    Returns:
        Full module path
        
    Raises:
        ImportError: If mod cannot be found
    """
    if not mod_identifier or not isinstance(mod_identifier, str):
        raise ImportError("mod_identifier must be a non-empty string")
    
    # If already a full path, return as-is
    if '.' in mod_identifier:
        try:
            importlib.import_module(mod_identifier)
            return mod_identifier
        except ImportError:
            pass  # Fall through to search
    
    # Search in standard locations
    search_paths = [
        f"datapy.mods.sources.{mod_identifier}",
        f"datapy.mods.transformers.{mod_identifier}", 
        f"datapy.mods.sinks.{mod_identifier}",
        f"datapy.mods.solos.{mod_identifier}"
    ]
    
    for path in search_paths:
        try:
            importlib.import_module(path)
            logger.debug(f"Resolved mod path", extra={"identifier": mod_identifier, "path": path})
            return path
        except ImportError:
            continue
    
    raise ImportError(f"Mod '{mod_identifier}' not found in standard locations: {search_paths}")


def _validate_mod_structure(mod_module, mod_path: str) -> Tuple[ModMetadata, type, callable]:
    """
    Validate mod has required structure and return components.
    
    Args:
        mod_module: Imported mod module
        mod_path: Full module path for error messages
        
    Returns:
        Tuple of (metadata, params_class, run_function)
        
    Raises:
        ValueError: If mod structure is invalid
    """
    # Check for required components
    if not hasattr(mod_module, 'run'):
        raise ValueError(f"Mod {mod_path} missing required 'run' function")
    
    if not hasattr(mod_module, 'METADATA'):
        raise ValueError(f"Mod {mod_path} missing required 'METADATA'")
    
    if not hasattr(mod_module, 'Params'):
        raise ValueError(f"Mod {mod_path} missing required 'Params' class")
    
    # Validate metadata
    metadata = mod_module.METADATA
    if not isinstance(metadata, ModMetadata):
        raise ValueError(f"Mod {mod_path} METADATA must be ModMetadata instance")
    
    # Validate params class
    params_class = mod_module.Params
    if not issubclass(params_class, BaseModParams):
        raise ValueError(f"Mod {mod_path} Params must inherit from BaseModParams")
    
    # Validate run function signature
    run_func = mod_module.run
    if not callable(run_func):
        raise ValueError(f"Mod {mod_path} run must be callable")
    
    sig = inspect.signature(run_func)
    if len(sig.parameters) != 1:
        raise ValueError(f"Mod {mod_path} run function must accept exactly one parameter")
    
    return metadata, params_class, run_func


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


def run_mod(mod_path: str, params: Dict[str, Any], mod_name: str) -> Dict[str, Any]:
    """
    Execute a DataPy mod and return the result.
    
    Args:
        mod_path: Mod identifier or full module path
        params: Parameters for the mod
        mod_name: Unique name for this mod instance
        
    Returns:
        ModResult dictionary with execution results
        
    Raises:
        ValueError: If parameters are invalid
        RuntimeError: If execution fails
    """
    # Validate inputs
    if not mod_path or not isinstance(mod_path, str):
        return validation_error(mod_name or "unknown", "mod_path must be a non-empty string")
    
    if not isinstance(params, dict):
        return validation_error(mod_name or "unknown", "params must be a dictionary")
    
    if not mod_name or not isinstance(mod_name, str) or not mod_name.strip():
        return validation_error("unknown", "mod_name must be a non-empty string")
    
    mod_name = mod_name.strip()
    
    # Validate mod_name is a valid identifier
    if not mod_name.isidentifier():
        return validation_error(mod_name, f"mod_name '{mod_name}' must be a valid Python identifier")
    
    # Resolve mod path
    try:
        resolved_mod_path = _resolve_mod_path(mod_path)
        mod_type = resolved_mod_path.split('.')[-1]
    except ImportError as e:
        return validation_error(mod_name, str(e))
    
    # Setup execution context
    execution_context = _get_execution_context()
    
    try:
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
        
        # Setup mod-specific logger
        mod_logger = setup_logger(f"{resolved_mod_path}.execution", log_file_path, mod_type, mod_name)
        
    except Exception as e:
        return runtime_error(mod_name, f"Failed to setup execution environment: {e}")
    
    # Load and validate mod
    try:
        mod_module = importlib.import_module(resolved_mod_path)
        metadata, params_class, run_func = _validate_mod_structure(mod_module, resolved_mod_path)
        
    except Exception as e:
        return validation_error(mod_name, f"Invalid mod structure: {e}")
    
    # Resolve parameters
    try:
        resolver = create_resolver()
        
        # Extract mod defaults from params class
        mod_defaults = {}
        if hasattr(params_class, 'model_fields'):  # Pydantic V2
            for field_name, field_info in params_class.model_fields.items():
                if field_name != '_metadata' and field_info.default is not ...:
                    mod_defaults[field_name] = field_info.default
        
        resolved_params = resolver.resolve_mod_params(
            mod_name=mod_type,
            job_params=params,
            mod_defaults=mod_defaults,
            globals_override=_global_config
        )
        
        # Create parameter instance
        param_instance = params_class(_metadata=metadata, **resolved_params)
        
    except Exception as e:
        _update_job_state(state_file_path, mod_name, 'error')
        return validation_error(mod_name, f"Parameter validation failed: {e}")
    
    # Execute mod
    mod_logger.info(f"Starting mod execution", extra={"params": resolved_params})
    
    try:
        result = run_func(param_instance)
        
        # Validate result structure
        if not isinstance(result, dict):
            raise RuntimeError(f"Mod must return a dictionary, got {type(result)}")
        
        required_fields = ['status', 'execution_time', 'exit_code', 'metrics', 'artifacts', 'globals', 'warnings', 'errors', 'logs']
        missing_fields = [field for field in required_fields if field not in result]
        if missing_fields:
            raise RuntimeError(f"Result missing required fields: {missing_fields}")
        
        if result['status'] not in ('success', 'warning', 'error'):
            raise RuntimeError(f"Invalid status: {result['status']}")
        
        # Update result with mod information
        result['logs']['mod_name'] = mod_name
        result['logs']['mod_type'] = mod_type
        
        # Update job state
        _update_job_state(state_file_path, mod_name, result['status'])
        
        mod_logger.info(f"Mod execution completed", extra={
            "status": result['status'],
            "execution_time": result['execution_time'],
            "exit_code": result['exit_code']
        })
        
        return result
        
    except Exception as e:
        mod_logger.error(f"Mod execution failed: {e}", exc_info=True)
        _update_job_state(state_file_path, mod_name, 'error')
        return runtime_error(mod_name, f"Mod execution failed: {e}")


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


# TODO: Future orchestrator placeholders
def run_mod_async(mod_path: str, params: Dict[str, Any], mod_name: str) -> str:
    """
    Execute a mod asynchronously and return execution ID.
    
    TODO: Implement when orchestrator supports async execution.
    Will handle:
    - Non-blocking mod execution
    - Execution tracking and monitoring
    - Result retrieval by execution ID
    
    Args:
        mod_path: Mod identifier or full module path
        params: Parameters for the mod
        mod_name: Unique name for this mod instance
        
    Returns:
        Execution ID for tracking
        
    Raises:
        NotImplementedError: Feature not yet implemented
    """
    raise NotImplementedError("Async mod execution not yet implemented - needed for Phase 2 orchestrator")


def run_mod_distributed(mod_path: str, params: Dict[str, Any], mod_name: str, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute a mod in distributed mode across cluster.
    
    TODO: Implement when orchestrator supports distributed execution.
    Will handle:
    - Cross-server mod execution
    - Distributed parameter resolution
    - Cluster-wide result aggregation
    
    Args:
        mod_path: Mod identifier or full module path
        params: Parameters for the mod
        mod_name: Unique name for this mod instance
        cluster_config: Distributed cluster configuration
        
    Returns:
        ModResult dictionary with distributed execution results
        
    Raises:
        NotImplementedError: Feature not yet implemented
    """
    raise NotImplementedError("Distributed mod execution not yet implemented - needed for Phase 2 orchestrator")


def get_execution_status(execution_id: str) -> Dict[str, Any]:
    """
    Get status of async or distributed execution.
    
    TODO: Implement when orchestrator supports async/distributed execution.
    Will provide:
    - Execution progress tracking
    - Real-time status updates
    - Error and warning monitoring
    
    Args:
        execution_id: ID of execution to check
        
    Returns:
        Status dictionary with execution information
        
    Raises:
        NotImplementedError: Feature not yet implemented
    """
    raise NotImplementedError("Execution status tracking not yet implemented - needed for Phase 2 orchestrator")


def cancel_execution(execution_id: str) -> bool:
    """
    Cancel a running async or distributed execution.
    
    TODO: Implement when orchestrator supports execution cancellation.
    Will handle:
    - Graceful execution termination
    - Resource cleanup
    - State consistency maintenance
    
    Args:
        execution_id: ID of execution to cancel
        
    Returns:
        True if cancellation successful
        
    Raises:
        NotImplementedError: Feature not yet implemented
    """
    raise NotImplementedError("Execution cancellation not yet implemented - needed for Phase 2 orchestrator")