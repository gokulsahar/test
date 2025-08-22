"""
State-based logging with JSON-only output for DataPy framework.

Provides consolidated logging with state file lifecycle management,
automatic job discovery, and mod instance tracking with fail-fast error handling.
"""

import logging
import json
import sys
import os
import shutil
import time
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
import glob

# Default configuration
DEFAULT_LOG_CONFIG = {
    "log_level": "INFO",
    "log_path": "logs",
    "log_format": "json"
}


class DataPyFormatter(logging.Formatter):
    """JSON formatter for structured logging with simplified fields."""
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON structure.
        
        Args:
            record: Log record to format
            
        Returns:
            JSON formatted log entry
        """
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # Add mod-specific context if available
        if hasattr(record, 'mod_type'):
            log_entry["mod_type"] = record.mod_type
        if hasattr(record, 'mod_name'):
            log_entry["mod_name"] = record.mod_name
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        # Add any extra fields (filter out internal logging fields)
        excluded_fields = {
            'name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 
            'filename', 'module', 'lineno', 'funcName', 'created', 'msecs', 
            'relativeCreated', 'thread', 'threadName', 'processName', 
            'process', 'stack_info', 'exc_info', 'exc_text', 'mod_type', 
            'mod_name', 'message'
        }
        
        for key, value in record.__dict__.items():
            if key not in excluded_fields and not key.startswith('_'):
                try:
                    # Ensure value is JSON serializable
                    json.dumps(value, default=str)
                    log_entry[key] = value
                except (TypeError, ValueError):
                    log_entry[key] = str(value)
        
        return json.dumps(log_entry, default=str)


def setup_job_logging(log_file_path: str, log_config: Dict[str, Any]) -> None:
    """
    Setup JSON logging handlers for specific log file with fail-fast error handling.
    
    Args:
        log_file_path: Path to log file
        log_config: Logging configuration dictionary
        
    Raises:
        RuntimeError: If logging setup fails (fail-fast approach)
    """
    if not log_file_path or not isinstance(log_file_path, str):
        raise RuntimeError("log_file_path must be a non-empty string")
    
    if not isinstance(log_config, dict):
        raise RuntimeError("log_config must be a dictionary")
    
    try:
        # Ensure log directory exists
        log_path = Path(log_file_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Test write permissions
        if not os.access(log_path.parent, os.W_OK):
            raise RuntimeError(f"No write permission for log directory: {log_path.parent}")
        
        # Configure root logger
        log_level = log_config.get("log_level", "INFO").upper()
        if log_level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            raise RuntimeError(f"Invalid log level: {log_level}")
        
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, log_level))
        
        # Clear any existing handlers to avoid duplicates
        root_logger.handlers.clear()
        
        # Setup file handler with JSON formatting
        try:
            file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
            file_handler.setFormatter(DataPyFormatter())
            root_logger.addHandler(file_handler)
        except (OSError, IOError) as e:
            raise RuntimeError(f"Failed to create log file handler: {e}")
        
        # REMOVED: stderr handler for clean console output during testing
        # stderr_handler = logging.StreamHandler(sys.stderr)
        # stderr_handler.setFormatter(DataPyFormatter())
        # root_logger.addHandler(stderr_handler)
            
        # Test logging works
        test_logger = logging.getLogger("datapy.logging.test")
        test_logger.info("Logging initialized successfully", extra={"log_file": log_file_path})
        
    except Exception as e:
        raise RuntimeError(f"Failed to setup job logging: {e}")


def setup_logger(
    name: str, 
    log_file_path: str = "", 
    mod_type: Optional[str] = None, 
    mod_name: Optional[str] = None
) -> logging.Logger:
    """
    Setup logger for specific component with mod context.
    
    Args:
        name: Logger name (typically __name__)
        log_file_path: Path to log file (optional for basic setup)
        mod_type: Type of the mod using this logger (e.g., "csv_reader")
        mod_name: Name of the mod instance (e.g., "extract_customers")
        
    Returns:
        Configured logger instance
        
    Raises:
        RuntimeError: If logger setup fails
    """
    if not name or not isinstance(name, str):
        raise RuntimeError("Logger name must be a non-empty string")
    
    try:
        logger = logging.getLogger(name)
        
        # Add mod context to all log records from this logger
        if mod_type or mod_name:
            def add_context(record):
                if mod_type:
                    record.mod_type = mod_type
                if mod_name:
                    record.mod_name = mod_name
                return True
            
            logger.addFilter(add_context)
        
        return logger
        
    except Exception as e:
        raise RuntimeError(f"Failed to setup logger {name}: {e}")


def initialize_job_state(state_file_path: str, yaml_file: str, expected_mods: List[str]) -> None:
    """
    Create initial state file for new job with validation.
    
    Args:
        state_file_path: Path where state file should be created
        yaml_file: Name of YAML configuration file
        expected_mods: List of mod names expected to run
        
    Raises:
        RuntimeError: If state file creation fails
    """
    if not state_file_path or not isinstance(state_file_path, str):
        raise RuntimeError("state_file_path must be a non-empty string")
    
    if not yaml_file or not isinstance(yaml_file, str):
        raise RuntimeError("yaml_file must be a non-empty string")
    
    if not isinstance(expected_mods, list):
        raise RuntimeError("expected_mods must be a list")
    
    try:
        execution_id = Path(state_file_path).stem
        
        initial_state = {
            "execution_id": execution_id,
            "yaml_file": yaml_file,
            "expected_mods": expected_mods,
            "completed_mods": [],
            "failed_mods": [],
            "is_complete": False,
            "mode": "cli",
            "started_at": datetime.now().isoformat() + "Z",
            "last_updated": datetime.now().isoformat() + "Z"
        }
        
        # Ensure directory exists
        state_dir = Path(state_file_path).parent
        state_dir.mkdir(parents=True, exist_ok=True)
        
        # Test write permissions
        if not os.access(state_dir, os.W_OK):
            raise RuntimeError(f"No write permission for state directory: {state_dir}")
        
        # Write initial state atomically
        temp_file = state_file_path + '.tmp'
        with open(temp_file, 'w') as f:
            json.dump(initial_state, f, indent=2)
        os.rename(temp_file, state_file_path)
        
        logger = logging.getLogger(__name__)
        logger.info(f"Initialized job state", extra={
            "state_file": state_file_path,
            "yaml_file": yaml_file,
            "expected_mods": expected_mods
        })
        
    except Exception as e:
        # Clean up partial files
        for temp_path in [state_file_path + '.tmp', state_file_path]:
            try:
                if Path(temp_path).exists():
                    os.unlink(temp_path)
            except:
                pass
        raise RuntimeError(f"Failed to initialize job state: {e}")


def update_job_state(state_file_path: str, mod_name: str, status: str) -> None:
    """
    Update state file with mod completion (atomic operation).
    
    Args:
        state_file_path: Path to state file
        mod_name: Name of completed mod
        status: Mod completion status ('success', 'warning', or 'error')
        
    Raises:
        RuntimeError: If state update fails
    """
    if not state_file_path or not isinstance(state_file_path, str):
        raise RuntimeError("state_file_path must be a non-empty string")
    
    if not mod_name or not isinstance(mod_name, str):
        raise RuntimeError("mod_name must be a non-empty string")
    
    if status not in ('success', 'warning', 'error'):
        raise RuntimeError(f"Invalid status: {status}. Must be success, warning, or error")
    
    try:
        # Read current state
        if not Path(state_file_path).exists():
            raise RuntimeError(f"State file does not exist: {state_file_path}")
        
        with open(state_file_path, 'r') as f:
            state = json.load(f)
        
        if not isinstance(state, dict):
            raise RuntimeError(f"Invalid state file format: {state_file_path}")
        
        # Update state based on status
        if status in ('success', 'warning'):
            if mod_name not in state.get('completed_mods', []):
                state.setdefault('completed_mods', []).append(mod_name)
        elif status == 'error':
            if mod_name not in state.get('failed_mods', []):
                state.setdefault('failed_mods', []).append(mod_name)
        
        # Update metadata
        state['last_updated'] = datetime.now().isoformat() + "Z"
        expected_mods = set(state.get('expected_mods', []))
        completed_mods = set(state.get('completed_mods', []))
        state['is_complete'] = len(expected_mods) > 0 and expected_mods == completed_mods
        
        # Atomic write using temporary file
        temp_file = state_file_path + '.tmp'
        with open(temp_file, 'w') as f:
            json.dump(state, f, indent=2)
        os.rename(temp_file, state_file_path)
        
        logger = logging.getLogger(__name__)
        logger.info(f"Updated job state", extra={
            "mod_name": mod_name,
            "status": status,
            "is_complete": state['is_complete']
        })
        
    except json.JSONDecodeError as e:
        # Handle corrupted state file
        logger = logging.getLogger(__name__)
        backup_path = f"{state_file_path}.corrupted.{int(time.time())}"
        try:
            shutil.copy2(state_file_path, backup_path)
            logger.error(f"Corrupted state file backed up to: {backup_path}")
        except:
            pass
        raise RuntimeError(f"Corrupted state file {state_file_path}: {e}")
    
    except Exception as e:
        # Clean up partial files
        temp_file = state_file_path + '.tmp'
        try:
            if Path(temp_file).exists():
                os.unlink(temp_file)
        except:
            pass
        raise RuntimeError(f"Failed to update state file {state_file_path}: {e}")


def is_job_complete_cli(state_file_path: str) -> bool:
    """
    Check if all expected mods have completed for CLI execution.
    
    Args:
        state_file_path: Path to state file
        
    Returns:
        True if job is complete, False otherwise
    """
    try:
        if not Path(state_file_path).exists():
            return False
        
        with open(state_file_path, 'r') as f:
            state = json.load(f)
        
        if not isinstance(state, dict):
            return False
        
        # Only check CLI mode jobs
        if state.get('mode') != 'cli':
            return False
        
        expected = set(state.get('expected_mods', []))
        completed = set(state.get('completed_mods', []))
        
        return len(expected) > 0 and expected == completed
    
    except (json.JSONDecodeError, Exception):
        return False


def archive_completed_state(running_state_path: str) -> None:
    """
    Move state from running/ to completed/ with completion metadata.
    
    Args:
        running_state_path: Path to state file in running directory
        
    Raises:
        RuntimeError: If archival fails
    """
    if not running_state_path or not isinstance(running_state_path, str):
        raise RuntimeError("running_state_path must be a non-empty string")
    
    try:
        if not Path(running_state_path).exists():
            logger = logging.getLogger(__name__)
            logger.warning(f"State file to archive does not exist: {running_state_path}")
            return
        
        # Read final state
        with open(running_state_path, 'r') as f:
            state = json.load(f)
        
        if not isinstance(state, dict):
            raise RuntimeError(f"Invalid state file format: {running_state_path}")
        
        # Add completion metadata
        state['completed_at'] = datetime.now().isoformat() + "Z"
        
        # Create completed directory
        completed_dir = Path("logs/state/completed")
        completed_dir.mkdir(parents=True, exist_ok=True)
        
        # Test write permissions
        if not os.access(completed_dir, os.W_OK):
            raise RuntimeError(f"No write permission for completed directory: {completed_dir}")
        
        # Move to completed directory
        completed_path = completed_dir / Path(running_state_path).name
        
        # Write final state to completed location atomically
        temp_completed = str(completed_path) + '.tmp'
        with open(temp_completed, 'w') as f:
            json.dump(state, f, indent=2)
        os.rename(temp_completed, str(completed_path))
        
        # Remove from running directory
        os.unlink(running_state_path)
        
        logger = logging.getLogger(__name__)
        logger.info(f"Archived completed job state", extra={
            "completed_path": str(completed_path),
            "execution_id": state.get('execution_id')
        })
        
    except Exception as e:
        # Clean up partial files
        try:
            temp_completed = str(Path("logs/state/completed") / Path(running_state_path).name) + '.tmp'
            if Path(temp_completed).exists():
                os.unlink(temp_completed)
        except:
            pass
        raise RuntimeError(f"Failed to archive state file {running_state_path}: {e}")


def reset_logging() -> None:
    """
    Reset logging state for testing.
    
    Warning: This should only be used in testing environments.
    """
    try:
        # Clear all handlers from root logger
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            try:
                handler.close()
            except:
                pass
            root_logger.removeHandler(handler)
        
        # Reset log level
        root_logger.setLevel(logging.WARNING)
        
    except Exception as e:
        # Don't fail on cleanup
        print(f"Warning: Failed to reset logging: {e}")


# TODO: Future orchestrator placeholders for concurrent execution
def update_job_state_concurrent(state_file_path: str, mod_name: str, status: str) -> None:
    """
    Update state file with file locking for concurrent mod execution.
    
    TODO: Implement when orchestrator supports parallel mod execution.
    Will be needed for:
    - Parallel mods with same dependencies
    - Retry/recovery scenarios  
    - Distributed execution across servers
    
    Implementation will use fcntl.flock() or equivalent for safe concurrent access.
    
    Args:
        state_file_path: Path to state file
        mod_name: Name of completed mod
        status: Mod completion status
        
    Raises:
        NotImplementedError: Feature not yet implemented
    """
    raise NotImplementedError("Concurrent state updates not yet implemented - needed for Phase 2 orchestrator")


def setup_distributed_logging(cluster_config: Dict[str, Any]) -> None:
    """
    Setup distributed logging for multi-server orchestration.
    
    TODO: Implement when orchestrator supports distributed execution.
    Will handle:
    - Centralized log aggregation
    - Cross-server log synchronization
    - Distributed state management
    
    Args:
        cluster_config: Distributed cluster configuration
        
    Raises:
        NotImplementedError: Feature not yet implemented
    """
    raise NotImplementedError("Distributed logging not yet implemented - needed for Phase 2 orchestrator")


def monitor_execution_health(execution_id: str) -> Dict[str, Any]:
    """
    Monitor execution health for orchestrator dashboards.
    
    TODO: Implement when orchestrator needs health monitoring.
    Will provide:
    - Execution progress tracking
    - Performance metrics
    - Error rate monitoring
    - Resource utilization stats
    
    Args:
        execution_id: ID of execution to monitor
        
    Returns:
        Health metrics dictionary
        
    Raises:
        NotImplementedError: Feature not yet implemented
    """
    raise NotImplementedError("Execution health monitoring not yet implemented - needed for Phase 2 orchestrator")