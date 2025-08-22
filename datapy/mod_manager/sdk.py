"""
Python SDK for DataPy framework with state-based logging.
"""

import importlib
import inspect
import os
import json
import time
import glob
from pathlib import Path
from typing import Dict, Any, Optional, Union, Tuple, List
from .logger import (
    setup_job_logging, setup_logger,
    update_job_state, archive_completed_state,
    DEFAULT_LOG_CONFIG
)
from .params import create_resolver, load_job_config
from .result import ModResult, validation_error, runtime_error
from .base import ModMetadata, BaseModParams

_global_config: Dict[str, Any] = {}

logger = setup_logger(__name__, "")


def set_global_config(config: Dict[str, Any]) -> None:
    global _global_config
    _global_config.update(config)


def get_global_config() -> Dict[str, Any]:
    return _global_config.copy()


def clear_global_config() -> None:
    global _global_config
    _global_config.clear()


def _is_called_from_cli() -> bool:
    frame = inspect.currentframe()
    try:
        while frame:
            filename = frame.f_code.co_filename
            if 'cli.py' in filename:
                return True
            frame = frame.f_back
    finally:
        del frame
    return False


def _get_execution_context() -> str:
    if _is_called_from_cli():
        raise RuntimeError("CLI should not call _get_execution_context")
    else:
        return "sdk_execution"


def find_or_create_job_files(execution_context: str, is_cli: bool = False) -> Tuple[str, str]:
    log_base = _global_config.get('log_path', 'logs')
    
    state_running = Path(log_base) / "state" / "running"
    state_running.mkdir(parents=True, exist_ok=True)
    Path(log_base).mkdir(exist_ok=True)
    
    pattern = str(state_running / f"{execution_context}_*.state")
    existing_states = glob.glob(pattern)
    
    if existing_states:
        state_file_path = existing_states[0]
        execution_id = Path(state_file_path).stem
        log_file_path = str(Path(log_base) / f"{execution_id}.log")
        
        if not Path(log_file_path).exists():
            Path(log_file_path).touch()
        
        return log_file_path, state_file_path
    else:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        execution_id = f"{execution_context}_{timestamp}"
        log_file_path = str(Path(log_base) / f"{execution_id}.log")
        state_file_path = str(state_running / f"{execution_id}.state")
        
        Path(log_file_path).touch()
        
        return log_file_path, state_file_path


def initialize_job_state_cli(state_file_path: str, yaml_file: str) -> None:
    try:
        config = load_job_config(yaml_file)
        expected_mods = list(config.get('mods', {}).keys())
    except Exception as e:
        expected_mods = []
        logger.warning(f"Failed to parse YAML {yaml_file} for expected mods: {e}")
    
    execution_id = Path(state_file_path).stem
    
    initial_state = {
        "execution_id": execution_id,
        "yaml_file": yaml_file,
        "expected_mods": expected_mods,
        "completed_mods": [],
        "failed_mods": [],
        "is_complete": False,
        "mode": "cli",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    
    Path(state_file_path).parent.mkdir(parents=True, exist_ok=True)
    
    with open(state_file_path, 'w') as f:
        json.dump(initial_state, f, indent=2)


def initialize_job_state_sdk(state_file_path: str) -> None:
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
    
    Path(state_file_path).parent.mkdir(parents=True, exist_ok=True)
    
    with open(state_file_path, 'w') as f:
        json.dump(initial_state, f, indent=2)


def _add_mod_to_expected_list(state_file_path: str, mod_name: str) -> None:
    try:
        if not Path(state_file_path).exists():
            return
        
        with open(state_file_path, 'r') as f:
            state = json.load(f)
        
        if mod_name not in state['expected_mods']:
            state['expected_mods'].append(mod_name)
            state['last_updated'] = time.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            temp_file = state_file_path + '.tmp'
            with open(temp_file, 'w') as f:
                json.dump(state, f, indent=2)
            os.rename(temp_file, state_file_path)
    
    except Exception as e:
        logger.error(f"Failed to add mod {mod_name} to expected list: {e}")


def is_job_complete_cli(state_file_path: str) -> bool:
    try:
        if not Path(state_file_path).exists():
            return False
        
        with open(state_file_path, 'r') as f:
            state = json.load(f)
        
        if state.get('mode') != 'cli':
            return False
        
        expected = set(state.get('expected_mods', []))
        completed = set(state.get('completed_mods', []))
        
        return len(expected) > 0 and expected == completed
    
    except Exception:
        return False


def _resolve_mod_path(mod_identifier: str) -> str:
    if '.' in mod_identifier:
        return mod_identifier
    
    search_paths = [
        f"datapy.mods.sources.{mod_identifier}",
        f"datapy.mods.transformers.{mod_identifier}",
        f"datapy.mods.sinks.{mod_identifier}",
        f"datapy.mods.solos.{mod_identifier}"
    ]
    
    for path in search_paths:
        try:
            importlib.import_module(path)
            return path
        except ImportError:
            continue
    
    raise ImportError(f"Mod '{mod_identifier}' not found in standard locations")


def _validate_mod_name(mod_name: str) -> bool:
    if not isinstance(mod_name, str) or not mod_name:
        return False
    
    if not mod_name.isidentifier():
        return False
    
    import keyword
    if keyword.iskeyword(mod_name):
        return False
    
    return True


def _check_variable_safety(mod_name: str) -> bool:
    if _is_called_from_cli():
        return True
    
    frame = inspect.currentframe().f_back.f_back
    
    try:
        if mod_name in frame.f_globals:
            existing = frame.f_globals[mod_name]
            if isinstance(existing, dict) and 'status' in existing:
                return True
            else:
                logger.warning(f"Variable '{mod_name}' already exists and is not a mod result")
                return False
        
        return True
    finally:
        del frame


def _inject_variable(mod_name: str, result: Dict[str, Any]) -> None:
    if _is_called_from_cli():
        return
    
    frame = inspect.currentframe().f_back.f_back
    
    try:
        frame.f_globals[mod_name] = result
        logger.debug(f"Injected mod result into variable '{mod_name}'")
    except Exception as e:
        logger.error(f"Failed to inject variable '{mod_name}': {e}")
    finally:
        del frame


def run_mod(mod_path: str, params: Dict[str, Any], mod_name: str) -> Union[int, Dict[str, Any]]:
    is_cli = _is_called_from_cli()
    
    if not _validate_mod_name(mod_name):
        error_result = validation_error(mod_name, f"Invalid mod_name '{mod_name}': must be valid Python identifier")
        if is_cli:
            return error_result
        else:
            _inject_variable(mod_name, error_result)
            return error_result['exit_code']
    
    if not is_cli and not _check_variable_safety(mod_name):
        error_result = validation_error(mod_name, f"mod_name '{mod_name}' would overwrite existing variable")
        _inject_variable(mod_name, error_result)
        return error_result['exit_code']
    
    try:
        resolved_mod_path = _resolve_mod_path(mod_path)
        mod_type = resolved_mod_path.split('.')[-1]
    except ImportError as e:
        error_result = validation_error(mod_name, str(e))
        if is_cli:
            return error_result
        else:
            _inject_variable(mod_name, error_result)
            return error_result['exit_code']
    
    if is_cli:
        execution_context = "cli_context"
        log_file_path, state_file_path = find_or_create_job_files(execution_context, is_cli=True)
    else:
        execution_context = _get_execution_context()
        log_file_path, state_file_path = find_or_create_job_files(execution_context, is_cli=False)
        
        if not Path(state_file_path).exists():
            initialize_job_state_sdk(state_file_path)
        
        _add_mod_to_expected_list(state_file_path, mod_name)
    
    log_config = DEFAULT_LOG_CONFIG.copy()
    log_config.update(_global_config)
    setup_job_logging(log_file_path, log_config)
    
    mod_logger = setup_logger(f"{resolved_mod_path}.execution", log_file_path, mod_type, mod_name)
    
    try:
        mod_module = importlib.import_module(resolved_mod_path)
        
        if not hasattr(mod_module, 'run'):
            error_result = validation_error(mod_name, f"Mod {resolved_mod_path} missing required 'run' function")
            if is_cli:
                return error_result
            else:
                _inject_variable(mod_name, error_result)
                return error_result['exit_code']
        
        if not hasattr(mod_module, 'METADATA'):
            error_result = validation_error(mod_name, f"Mod {resolved_mod_path} missing required 'METADATA'")
            if is_cli:
                return error_result
            else:
                _inject_variable(mod_name, error_result)
                return error_result['exit_code']
        
        if not hasattr(mod_module, 'Params'):
            error_result = validation_error(mod_name, f"Mod {resolved_mod_path} missing required 'Params' class")
            if is_cli:
                return error_result
            else:
                _inject_variable(mod_name, error_result)
                return error_result['exit_code']
        
        metadata = mod_module.METADATA
        if not isinstance(metadata, ModMetadata):
            error_result = validation_error(mod_name, f"Mod {resolved_mod_path} METADATA must be ModMetadata instance")
            if is_cli:
                return error_result
            else:
                _inject_variable(mod_name, error_result)
                return error_result['exit_code']
        
        params_class = mod_module.Params
        if not issubclass(params_class, BaseModParams):
            error_result = validation_error(mod_name, f"Mod {resolved_mod_path} Params must inherit from BaseModParams")
            if is_cli:
                return error_result
            else:
                _inject_variable(mod_name, error_result)
                return error_result['exit_code']
        
        run_func = mod_module.run
        sig = inspect.signature(run_func)
        if len(sig.parameters) != 1:
            error_result = validation_error(mod_name, f"Mod {resolved_mod_path} run function must accept exactly one parameter")
            if is_cli:
                return error_result
            else:
                _inject_variable(mod_name, error_result)
                return error_result['exit_code']
        
        resolver = create_resolver()
        
        mod_defaults = {}
        if hasattr(params_class, '__fields__'):
            for field_name, field_info in params_class.__fields__.items():
                if field_name != '_metadata' and field_info.default is not ...:
                    mod_defaults[field_name] = field_info.default
        
        resolved_params = resolver.resolve_mod_params(
            mod_name=mod_type,
            job_params=params,
            mod_defaults=mod_defaults,
            globals_override=_global_config
        )
        
        try:
            param_instance = params_class(_metadata=metadata, **resolved_params)
        except Exception as e:
            error_result = validation_error(mod_name, f"Parameter validation failed: {e}")
            if is_cli:
                return error_result
            else:
                _inject_variable(mod_name, error_result)
                return error_result['exit_code']
        
        mod_logger.info(f"Starting mod execution", extra={"params": resolved_params})
        
        try:
            result = run_func(param_instance)
            
            if not isinstance(result, dict):
                error_result = runtime_error(mod_name, f"Mod {resolved_mod_path} must return a dictionary")
                if is_cli:
                    return error_result
                else:
                    _inject_variable(mod_name, error_result)
                    return error_result['exit_code']
            
            required_fields = ['status', 'execution_time', 'exit_code', 'metrics', 'artifacts', 'globals', 'warnings', 'errors', 'logs']
            missing_fields = [field for field in required_fields if field not in result]
            if missing_fields:
                error_result = runtime_error(mod_name, f"Mod {resolved_mod_path} result missing required fields: {missing_fields}")
                if is_cli:
                    return error_result
                else:
                    _inject_variable(mod_name, error_result)
                    return error_result['exit_code']
            
            if result['status'] not in ('success', 'warning', 'error'):
                error_result = runtime_error(mod_name, f"Mod {resolved_mod_path} returned invalid status: {result['status']}")
                if is_cli:
                    return error_result
                else:
                    _inject_variable(mod_name, error_result)
                    return error_result['exit_code']
            
            result['logs']['mod_name'] = mod_name
            result['logs']['mod_type'] = mod_type
            
            update_job_state(state_file_path, mod_name, result['status'])
            
            if is_cli and is_job_complete_cli(state_file_path):
                archive_completed_state(state_file_path)
            
            mod_logger.info(f"Mod execution completed", extra={
                "status": result['status'],
                "execution_time": result['execution_time'],
                "exit_code": result['exit_code']
            })
            
            if is_cli:
                return result
            else:
                _inject_variable(mod_name, result)
                return result['exit_code']
            
        except Exception as e:
            mod_logger.error(f"Mod execution failed: {e}", exc_info=True)
            
            update_job_state(state_file_path, mod_name, 'error')
            
            error_result = runtime_error(mod_name, f"Mod execution failed: {e}")
            if is_cli:
                return error_result
            else:
                _inject_variable(mod_name, error_result)
                return error_result['exit_code']
    
    except Exception as e:
        error_result = runtime_error(mod_name, f"Unexpected error loading mod {resolved_mod_path}: {e}")
        if is_cli:
            return error_result
        else:
            _inject_variable(mod_name, error_result)
            return error_result['exit_code']


def get_mod_result(mod_name: str) -> Optional[Dict[str, Any]]:
    if _is_called_from_cli():
        return None
    
    frame = inspect.currentframe().f_back
    
    try:
        if mod_name in frame.f_globals:
            result = frame.f_globals[mod_name]
            if isinstance(result, dict) and 'status' in result:
                return result
        return None
    finally:
        del frame


def update_job_state_concurrent(state_file_path: str, mod_name: str, status: str) -> None:
    raise NotImplementedError("Concurrent state updates not yet implemented - needed for Phase 2 orchestrator")