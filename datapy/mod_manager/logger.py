"""
Structured logging with session management for DataPy framework.

Provides consolidated logging with JSON output to stderr and file output
with automatic session management and smart log file naming.
"""

import logging
import logging.handlers
import json
import sys
import uuid
import time
from pathlib import Path
from typing import Dict, Any, Optional
import threading

# Global session state
_session_lock = threading.Lock()
_session_id: Optional[str] = None
_session_log_file: Optional[str] = None
_loggers_configured: bool = False

# Default configuration
DEFAULT_LOG_CONFIG = {
    "log_level": "INFO",
    "log_path": ".",
    "log_format": "json",
    "console_output": True
}


class DataPyFormatter(logging.Formatter):
    """Custom formatter for DataPy structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON structure."""
        # Build structured log entry
        log_entry = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "session_id": get_session_id(),
        }
        
        # Add mod-specific context if available
        if hasattr(record, 'mod_name'):
            log_entry["mod_name"] = record.mod_name
        if hasattr(record, 'run_id'):
            log_entry["run_id"] = record.run_id
        
        # Add any extra fields
        for key, value in record.__dict__.items():
            if key not in ('name', 'msg', 'args', 'levelname', 'levelno', 
                          'pathname', 'filename', 'module', 'lineno', 'funcName',
                          'created', 'msecs', 'relativeCreated', 'thread',
                          'threadName', 'processName', 'process', 'stack_info',
                          'exc_info', 'exc_text', 'mod_name', 'run_id'):
                log_entry[key] = value
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_entry, default=str)


class DataPyConsoleFormatter(logging.Formatter):
    """Human-readable formatter for console output."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record for console display."""
        timestamp = self.formatTime(record, '%H:%M:%S')
        level = record.levelname.ljust(8)
        
        # Add mod context if available
        context = ""
        if hasattr(record, 'mod_name'):
            context = f"[{record.mod_name}] "
        
        message = f"{timestamp} {level} {context}{record.getMessage()}"
        
        # Add exception if present
        if record.exc_info:
            message += "\n" + self.formatException(record.exc_info)
        
        return message


def get_session_id() -> str:
    """
    Get or create session ID for current execution.
    
    Returns:
        Session ID string
    """
    global _session_id
    
    with _session_lock:
        if _session_id is None:
            _session_id = f"session_{uuid.uuid4().hex[:12]}"
    
    return _session_id


def get_session_log_file() -> Optional[str]:
    """
    Get the current session's log file path.
    
    Returns:
        Path to session log file or None if not set
    """
    return _session_log_file


def setup_logging(
    log_config: Optional[Dict[str, Any]] = None,
    execution_name: Optional[str] = None
) -> str:
    """
    Setup logging for DataPy framework with session management.
    
    Args:
        log_config: Logging configuration override
        execution_name: Name for log file (script/config name)
        
    Returns:
        Path to the created log file
        
    Raises:
        OSError: If log directory cannot be created or accessed
    """
    global _session_log_file, _loggers_configured
    
    with _session_lock:
        if _loggers_configured:
            return _session_log_file
        
        # Merge configuration
        config = DEFAULT_LOG_CONFIG.copy()
        if log_config:
            config.update(log_config)
        
        # Determine log file name
        if execution_name:
            base_name = Path(execution_name).stem
        else:
            base_name = "datapy_execution"
        
        # Create unique log file name with timestamp
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        log_filename = f"{base_name}_{timestamp}.log"
        
        # Setup log directory
        log_path = Path(config["log_path"])
        try:
            log_path.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise OSError(f"Cannot create log directory {log_path}: {e}")
        
        _session_log_file = str(log_path / log_filename)
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, config["log_level"].upper()))
        
        # Clear any existing handlers
        root_logger.handlers.clear()
        
        # Setup file handler with JSON formatting
        try:
            file_handler = logging.FileHandler(_session_log_file, encoding='utf-8')
            file_handler.setFormatter(DataPyFormatter())
            root_logger.addHandler(file_handler)
        except OSError as e:
            raise OSError(f"Cannot create log file {_session_log_file}: {e}")
        
        # Setup console handler for stderr (JSON format)
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setFormatter(DataPyFormatter())
        root_logger.addHandler(stderr_handler)
        
        # Setup console handler for stdout (human readable) if enabled
        if config.get("console_output", False):
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(DataPyConsoleFormatter())
            # Only show INFO and above on console
            console_handler.setLevel(logging.INFO)
            root_logger.addHandler(console_handler)
        
        _loggers_configured = True
        
        # Log session start
        logger = logging.getLogger(__name__)
        logger.info("DataPy logging session started", extra={
            "session_id": get_session_id(),
            "log_file": _session_log_file,
            "config": config
        })
        
        return _session_log_file


def setup_logger(name: str, mod_name: Optional[str] = None, run_id: Optional[str] = None) -> logging.Logger:
    """
    Setup a logger for a specific component with DataPy context.
    
    Args:
        name: Logger name (typically __name__)
        mod_name: Name of the mod using this logger
        run_id: Unique run ID for this execution
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Add mod context to all log records from this logger
    if mod_name or run_id:
        old_factory = logging.getLogRecordFactory()
        
        def record_factory(*args, **kwargs):
            record = old_factory(*args, **kwargs)
            if mod_name:
                record.mod_name = mod_name
            if run_id:
                record.run_id = run_id
            return record
        
        # Note: This is a simplified approach. In production, you might want
        # a more sophisticated way to manage per-logger context
        logging.setLogRecordFactory(record_factory)
    
    return logger


def configure_from_globals(globals_config: Dict[str, Any], execution_name: Optional[str] = None) -> str:
    """
    Configure logging from global configuration dictionary.
    
    Args:
        globals_config: Global configuration containing logging settings
        execution_name: Name for log file identification
        
    Returns:
        Path to created log file
        
    Raises:
        OSError: If logging setup fails
    """
    log_config = {}
    
    # Extract logging configuration from globals
    if "log_level" in globals_config:
        log_config["log_level"] = globals_config["log_level"]
    if "log_path" in globals_config:
        log_config["log_path"] = globals_config["log_path"]
    if "log_format" in globals_config:
        log_config["log_format"] = globals_config["log_format"]
    
    return setup_logging(log_config, execution_name)


def reset_session():
    """
    Reset session state for testing or new execution contexts.
    
    Warning: This should only be used in testing or when starting
    a completely new execution context.
    """
    global _session_id, _session_log_file, _loggers_configured
    
    with _session_lock:
        _session_id = None
        _session_log_file = None
        _loggers_configured = False
        
        # Clear all handlers from root logger
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)