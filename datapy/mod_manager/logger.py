"""
State-based logging with JSON-only output for DataPy framework.

Provides consolidated logging with state file lifecycle management,
automatic job discovery, and mod instance tracking without execution contexts.
"""

import logging
import json
import sys
import os
import shutil
import time
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
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
        """Format log record as JSON structure."""
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
        
        # Add any extra fields
        for key, value in record.__dict__.items():
            if key not in {
                'name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 
                'filename', 'module', 'lineno', 'funcName', 'created', 'msecs', 
                'relativeCreated', 'thread', 'threadName', 'processName', 
                'process', 'stack_info', 'exc_info', 'exc_text', 'mod_type', 
                'mod_name'
            }:
                log_entry[key] = value
        
        return json.dumps(log_entry, default=str)


def setup_job_logging(log_file_path: str, log_config: Dict[str, Any]) -> None:
    """
    Setup JSON logging handlers for specific log file.
    
    Args:
        log_file_path: Path to log file
        log_config: Logging configuration dictionary
    """
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_config.get("log_level", "INFO").upper()))
    
    # Clear any existing handlers
    root_logger.handlers.clear()
    
    # Setup file handler with JSON formatting
    file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
    file_handler.setFormatter(DataPyFormatter())
    root_logger.addHandler(file_handler)
    
    # Setup stderr handler for JSON output (for machine parsing)
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(DataPyFormatter())
    root_logger.addHandler(stderr_handler)


def get_or_create_job_log_file(yaml_name: str) -> str:
    """
    Find existing or create new log file based on running state.
    
    Args:
        yaml_name: Base name of YAML file (without extension)
        
    Returns:
        Path to log file to use
    """
    # Ensure directories exist
    log_base = Path("logs")
    state_running = log_base / "state" / "running"
    state_running.mkdir(parents=True, exist_ok=True)
    log_base.mkdir(exist_ok=True)
    
    # Look for existing running state files for this yaml
    pattern = str(state_running / f"{yaml_name}_*.state")
    existing_states = glob.glob(pattern)
    
    if existing_states:
        # Use existing state file (should be only one)
        state_file = Path(existing_states[0])
        # Extract execution_id from state filename
        execution_id = state_file.stem
        log_file_path = str(log_base / f"{execution_id}.log")
        
        # Verify log file exists, create if missing
        if not Path(log_file_path).exists():
            logger = logging.getLogger(__name__)
            logger.warning(f"Log file {log_file_path} missing, creating new one")
            Path(log_file_path).touch()
        
        return log_file_path
    else:
        # Create new execution
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        execution_id = f"{yaml_name}_{timestamp}"
        log_file_path = str(log_base / f"{execution_id}.log")
        
        # Create log file
        Path(log_file_path).touch()
        return log_file_path


def setup_logger(name: str, log_file_path: str, mod_type: Optional[str] = None, mod_name: Optional[str] = None) -> logging.Logger:
    """
    Setup logger for specific log file with mod context.
    
    Args:
        name: Logger name (typically __name__)
        log_file_path: Path to log file
        mod_type: Type of the mod using this logger (e.g., "csv_reader")
        mod_name: Name of the mod instance (e.g., "extract_customers")
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Add mod context to all log records from this logger
    if mod_type or mod_name:
        # Create a custom filter to add context
        def add_context(record):
            if mod_type:
                record.mod_type = mod_type
            if mod_name:
                record.mod_name = mod_name
            return True
        
        logger.addFilter(add_context)
    
    return logger


def initialize_job_state(state_file_path: str, yaml_file: str, expected_mods: List[str]) -> None:
    """
    Create initial state file for new job.
    
    Args:
        state_file_path: Path where state file should be created
        yaml_file: Name of YAML configuration file
        expected_mods: List of mod names expected to run
    """
    execution_id = Path(state_file_path).stem
    
    initial_state = {
        "execution_id": execution_id,
        "yaml_file": yaml_file,
        "expected_mods": expected_mods,
        "completed_mods": [],
        "failed_mods": [],
        "is_complete": False,
        "started_at": datetime.now().isoformat() + "Z",
        "last_updated": datetime.now().isoformat() + "Z"
    }
    
    # Ensure directory exists
    Path(state_file_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Write initial state
    with open(state_file_path, 'w') as f:
        json.dump(initial_state, f, indent=2)


def update_job_state(state_file_path: str, mod_name: str, status: str) -> None:
    """
    Update state file with mod completion (Phase 1: simple atomic operations).
    
    Args:
        state_file_path: Path to state file
        mod_name: Name of completed mod
        status: Mod completion status ('success' or 'error')
    """
    _update_state_simple(state_file_path, mod_name, status)


def _update_state_simple(state_file_path: str, mod_name: str, status: str) -> None:
    """
    Simple atomic update using temp file + rename.
    
    Args:
        state_file_path: Path to state file
        mod_name: Name of completed mod
        status: Mod completion status
    """
    try:
        # Read current state
        if not Path(state_file_path).exists():
            logger = logging.getLogger(__name__)
            logger.warning(f"State file {state_file_path} missing, cannot update")
            return
        
        with open(state_file_path, 'r') as f:
            state = json.load(f)
        
        # Update state based on status
        if status == 'success':
            if mod_name not in state['completed_mods']:
                state['completed_mods'].append(mod_name)
        elif status == 'error':
            if mod_name not in state['failed_mods']:
                state['failed_mods'].append(mod_name)
        
        # Update metadata
        state['last_updated'] = datetime.now().isoformat() + "Z"
        state['is_complete'] = set(state['completed_mods']) == set(state['expected_mods'])
        
        # Atomic write
        temp_file = state_file_path + '.tmp'
        with open(temp_file, 'w') as f:
            json.dump(state, f, indent=2)
        os.rename(temp_file, state_file_path)
        
    except json.JSONDecodeError as e:
        # Handle corrupted state file
        logger = logging.getLogger(__name__)
        logger.error(f"Corrupted state file {state_file_path}: {e}")
        
        # Backup corrupted file
        backup_path = f"{state_file_path}.corrupted.{int(time.time())}"
        shutil.copy2(state_file_path, backup_path)
        logger.error(f"Backed up corrupted state file to {backup_path}")
        
        # Cannot update without knowing expected_mods, log the issue
        logger.error(f"Cannot update state for mod {mod_name} - state file corrupted")
    
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Failed to update state file {state_file_path}: {e}")


def is_job_complete(state_file_path: str) -> bool:
    """
    Check if all expected mods have completed.
    
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
        
        return state.get('is_complete', False)
    
    except (json.JSONDecodeError, Exception):
        return False


def archive_completed_state(running_state_path: str) -> None:
    """
    Move state from running/ to completed/ with completion metadata.
    
    Args:
        running_state_path: Path to state file in running directory
    """
    try:
        if not Path(running_state_path).exists():
            return
        
        # Read final state
        with open(running_state_path, 'r') as f:
            state = json.load(f)
        
        # Add completion metadata
        state['completed_at'] = datetime.now().isoformat() + "Z"
        
        # Create completed directory
        completed_dir = Path("logs/state/completed")
        completed_dir.mkdir(parents=True, exist_ok=True)
        
        # Move to completed directory
        completed_path = completed_dir / Path(running_state_path).name
        
        # Write final state to completed location
        with open(completed_path, 'w') as f:
            json.dump(state, f, indent=2)
        
        # Remove from running directory
        os.unlink(running_state_path)
        
        logger = logging.getLogger(__name__)
        logger.info(f"Archived completed job state to {completed_path}")
        
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Failed to archive state file {running_state_path}: {e}")


def get_running_state_path(yaml_name: str) -> Optional[str]:
    """
    Get path to running state file for yaml name.
    
    Args:
        yaml_name: Base name of YAML file
        
    Returns:
        Path to running state file or None if not found
    """
    state_running = Path("logs/state/running")
    pattern = str(state_running / f"{yaml_name}_*.state")
    existing_states = glob.glob(pattern)
    
    return existing_states[0] if existing_states else None


def create_state_file_path(yaml_name: str) -> str:
    """
    Create path for new state file.
    
    Args:
        yaml_name: Base name of YAML file
        
    Returns:
        Path where new state file should be created
    """
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    execution_id = f"{yaml_name}_{timestamp}"
    state_running = Path("logs/state/running")
    return str(state_running / f"{execution_id}.state")


# TODO: Future orchestrator will need file locking for concurrent mod execution
# TODO: Add _update_state_with_lock() for Phase 2 when parallel execution is implemented
# TODO: Use fcntl.flock() or similar for cross-process synchronization
# TODO: Consider distributed locking for multi-server orchestration

def update_job_state_concurrent(state_file_path: str, mod_name: str, status: str) -> None:
    """
    Update state file with file locking for concurrent access.
    
    TODO: Implement this when orchestrator supports parallel mod execution.
    Will be needed for:
    - Parallel mods with same dependencies
    - Retry/recovery scenarios  
    - Distributed execution across servers
    
    Implementation will use fcntl.flock() or equivalent for safe concurrent access.
    """
    raise NotImplementedError("Concurrent state updates not yet implemented - needed for Phase 2 orchestrator")


def reset_logging() -> None:
    """
    Reset logging state for testing.
    
    Warning: This should only be used in testing.
    """
    # Clear all handlers from root logger
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        handler.close()
        root_logger.removeHandler(handler)