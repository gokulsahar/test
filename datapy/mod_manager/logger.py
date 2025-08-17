"""
Simplified logging with execution context management for DataPy framework.

Provides consolidated logging with temp folder lifecycle management,
automatic file moving, and mod instance tracking.
"""

import logging
import json
import sys
import uuid
import time
import shutil
from pathlib import Path
from typing import Dict, Any, Optional
import threading
from contextlib import contextmanager

# Global execution context
_current_context: Optional['ExecutionContext'] = None
_context_lock = threading.Lock()

# Default configuration
DEFAULT_LOG_CONFIG = {
    "log_level": "INFO",
    "log_path": "logs",
    "log_format": "json",
    "console_output": True
}


class ExecutionContext:
    """
    Tracks execution state and manages log file lifecycle.
    """
    
    def __init__(self, execution_name: str, log_config: Dict[str, Any]):
        """
        Initialize execution context.
        
        Args:
            execution_name: Name for log file (script/config name)
            log_config: Logging configuration
        """
        self.execution_name = Path(execution_name).stem
        self.timestamp = time.strftime("%Y%m%d_%H%M%S")
        self.execution_id = f"{self.execution_name}_{self.timestamp}"
        self.execution_type = "cli" if execution_name.endswith('.yaml') else "script"
        
        # Setup paths
        self.log_config = log_config
        self.log_base_path = Path(log_config.get("log_path", "logs"))
        self.running_path = self.log_base_path / "running"
        self.completed_path = self.log_base_path
        
        # Create directories
        self.running_path.mkdir(parents=True, exist_ok=True)
        self.completed_path.mkdir(parents=True, exist_ok=True)
        
        # Log file paths
        self.log_filename = f"{self.execution_id}.log"
        self.current_log_path = self.running_path / self.log_filename
        self.final_log_path = self.completed_path / self.log_filename
        
        # Execution tracking
        self.mod_counter = 0
        self.start_time = time.time()
        self.loggers_configured = False
    
    def get_next_mod_instance(self) -> str:
        """Get next mod instance number."""
        self.mod_counter += 1
        return f"{self.mod_counter:03d}"
    
    def finalize(self) -> None:
        """Move log file from running to completed directory."""
        if self.current_log_path.exists():
            # Close all file handlers first
            self._close_file_handlers()
            
            # Move file to final location
            if self.final_log_path.exists():
                self.final_log_path.unlink()  # Remove existing file
            
            shutil.move(str(self.current_log_path), str(self.final_log_path))
    
    def _close_file_handlers(self) -> None:
        """Close all file handlers to release file locks."""
        for handler in logging.root.handlers[:]:
            if isinstance(handler, logging.FileHandler):
                handler.close()
                logging.root.removeHandler(handler)


class DataPyFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON structure."""
        context = get_current_context()
        
        log_entry = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # Add execution context
        if context:
            log_entry.update({
                "execution_id": context.execution_id,
                "execution_type": context.execution_type,
            })
        
        # Add mod-specific context if available
        if hasattr(record, 'mod_type'):
            log_entry["mod_type"] = record.mod_type
        if hasattr(record, 'mod_instance'):
            log_entry["mod_instance"] = record.mod_instance
        if hasattr(record, 'run_id'):
            log_entry["run_id"] = record.run_id
        
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
                'mod_instance', 'run_id'
            }:
                log_entry[key] = value
        
        return json.dumps(log_entry, default=str)


class DataPyConsoleFormatter(logging.Formatter):
    """Human-readable formatter for console output."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record for console display."""
        timestamp = self.formatTime(record, '%H:%M:%S')
        level = record.levelname.ljust(8)
        
        # Add mod context if available
        context_parts = []
        if hasattr(record, 'mod_type'):
            mod_context = record.mod_type
            if hasattr(record, 'mod_instance'):
                mod_context += f"#{record.mod_instance}"
            context_parts.append(mod_context)
        
        context = f"[{' '.join(context_parts)}] " if context_parts else ""
        message = f"{timestamp} {level} {context}{record.getMessage()}"
        
        # Add exception if present
        if record.exc_info:
            message += "\n" + self.formatException(record.exc_info)
        
        return message


def get_current_context() -> Optional[ExecutionContext]:
    """Get current execution context."""
    return _current_context


@contextmanager
def execution_logger(execution_name: str, log_config: Optional[Dict[str, Any]] = None):
    """
    Context manager for execution logging lifecycle.
    
    Args:
        execution_name: Name for log file (script/config name)
        log_config: Optional logging configuration override
        
    Yields:
        ExecutionContext instance
        
    Example:
        with execution_logger("daily_etl.yaml") as ctx:
            run_mod("csv_reader", params)
            run_mod("data_cleaner", params)
        # Log file automatically moved to completed/
    """
    global _current_context
    
    # Merge configuration
    config = DEFAULT_LOG_CONFIG.copy()
    if log_config:
        config.update(log_config)
    
    with _context_lock:
        # Create execution context
        context = ExecutionContext(execution_name, config)
        _current_context = context
        
        try:
            # Setup logging
            _setup_handlers(context)
            
            # Log execution start
            logger = logging.getLogger(__name__)
            logger.info("DataPy execution started", extra={
                "execution_id": context.execution_id,
                "execution_type": context.execution_type,
                "log_file": str(context.current_log_path),
                "config": config
            })
            
            yield context
            
        finally:
            # Log execution end
            execution_time = time.time() - context.start_time
            logger.info("DataPy execution completed", extra={
                "execution_time": round(execution_time, 3),
                "total_mods": context.mod_counter
            })
            
            # Finalize logging (move file)
            context.finalize()
            _current_context = None


def start_execution(execution_name: str, log_config: Optional[Dict[str, Any]] = None) -> str:
    """
    Start execution logging (alternative to context manager).
    
    Args:
        execution_name: Name for log file
        log_config: Optional logging configuration
        
    Returns:
        Path to current log file
        
    Note: Must call finalize_execution() when done
    """
    global _current_context
    
    # Merge configuration
    config = DEFAULT_LOG_CONFIG.copy()
    if log_config:
        config.update(log_config)
    
    with _context_lock:
        # Create execution context
        context = ExecutionContext(execution_name, config)
        _current_context = context
        
        # Setup logging
        _setup_handlers(context)
        
        # Log execution start
        logger = logging.getLogger(__name__)
        logger.info("DataPy execution started", extra={
            "execution_id": context.execution_id,
            "execution_type": context.execution_type,
            "log_file": str(context.current_log_path),
            "config": config
        })
        
        return str(context.current_log_path)


def finalize_execution() -> Optional[str]:
    """
    Finalize execution logging and move log file.
    
    Returns:
        Path to final log file location, or None if no active execution
    """
    global _current_context
    
    with _context_lock:
        if _current_context is None:
            return None
        
        context = _current_context
        
        # Log execution end
        execution_time = time.time() - context.start_time
        logger = logging.getLogger(__name__)
        logger.info("DataPy execution completed", extra={
            "execution_time": round(execution_time, 3),
            "total_mods": context.mod_counter
        })
        
        # Finalize and move file
        context.finalize()
        final_path = str(context.final_log_path)
        _current_context = None
        
        return final_path


def setup_logger(name: str, mod_type: Optional[str] = None) -> logging.Logger:
    """
    Setup a logger for a specific component with DataPy context.
    
    Args:
        name: Logger name (typically __name__)
        mod_type: Type of the mod using this logger (e.g., "csv_reader")
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Add mod context to all log records from this logger
    if mod_type:
        # Get mod instance number from current context
        context = get_current_context()
        mod_instance = context.get_next_mod_instance() if context else "000"
        run_id = f"{mod_type}_{mod_instance}_{uuid.uuid4().hex[:8]}"
        
        # Create a custom filter to add context
        def add_context(record):
            record.mod_type = mod_type
            record.mod_instance = mod_instance
            record.run_id = run_id
            return True
        
        logger.addFilter(add_context)
    
    return logger


def _setup_handlers(context: ExecutionContext) -> None:
    """Setup logging handlers for the execution context."""
    if context.loggers_configured:
        return
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, context.log_config["log_level"].upper()))
    
    # Clear any existing handlers
    root_logger.handlers.clear()
    
    # Setup file handler with JSON formatting
    file_handler = logging.FileHandler(context.current_log_path, encoding='utf-8')
    file_handler.setFormatter(DataPyFormatter())
    root_logger.addHandler(file_handler)
    
    # Setup stderr handler for JSON output (for machine parsing)
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(DataPyFormatter())
    root_logger.addHandler(stderr_handler)
    
    # Setup console handler for human readable output (if enabled)
    if context.log_config.get("console_output", False):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(DataPyConsoleFormatter())
        console_handler.setLevel(logging.INFO)  # Only INFO and above on console
        root_logger.addHandler(console_handler)
    
    context.loggers_configured = True


def get_current_log_file() -> Optional[str]:
    """
    Get the current log file path.
    
    Returns:
        Path to current log file or None if no active execution
    """
    context = get_current_context()
    return str(context.current_log_path) if context else None


def configure_from_globals(globals_config: Dict[str, Any], execution_name: Optional[str] = None) -> str:
    """
    Configure logging from global configuration dictionary (legacy support).
    
    Args:
        globals_config: Global configuration containing logging settings
        execution_name: Name for log file identification
        
    Returns:
        Path to created log file
    """
    log_config = {}
    
    # Extract logging configuration from globals
    if "log_level" in globals_config:
        log_config["log_level"] = globals_config["log_level"]
    if "log_path" in globals_config:
        log_config["log_path"] = globals_config["log_path"]
    if "log_format" in globals_config:
        log_config["log_format"] = globals_config["log_format"]
    
    execution_name = execution_name or "datapy_execution"
    return start_execution(execution_name, log_config)


def reset_logging():
    """
    Reset logging state for testing.
    
    Warning: This should only be used in testing.
    """
    global _current_context
    
    with _context_lock:
        if _current_context:
            _current_context.finalize()
        _current_context = None
        
        # Clear all handlers from root logger
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            handler.close()
            root_logger.removeHandler(handler)