"""
Simplified console logging for DataPy framework.

Provides tab-delimited console output with mod context tracking
and configurable log levels. No file management - output to stdout/stderr only.
"""

import logging
import json
import sys
from typing import Dict, Any, Optional
from datetime import datetime

# Default configuration
DEFAULT_LOG_CONFIG = {
    "log_level": "INFO"
}


# COMMENTED OUT: Original JSON formatter - kept for potential future use
# class DataPyFormatter(logging.Formatter):
#     """JSON formatter for structured logging with simplified fields."""
#     
#     def format(self, record: logging.LogRecord) -> str:
#         """
#         Format log record as JSON structure.
#         
#         Args:
#             record: Log record to format
#             
#         Returns:
#             JSON formatted log entry
#         """
#         log_entry = {
#             "timestamp": datetime.fromtimestamp(record.created).isoformat() + "Z",
#             "level": record.levelname,
#             "logger": record.name,
#             "message": record.getMessage(),
#         }
#         
#         # Add mod-specific context if available
#         if hasattr(record, 'mod_type'):
#             log_entry["mod_type"] = record.mod_type
#         if hasattr(record, 'mod_name'):
#             log_entry["mod_name"] = record.mod_name
#         
#         # Add exception info if present
#         if record.exc_info:
#             log_entry["exception"] = self.formatException(record.exc_info)
#         
#         # Add any extra fields (filter out internal logging fields)
#         excluded_fields = {
#             'name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 
#             'filename', 'module', 'lineno', 'funcName', 'created', 'msecs', 
#             'relativeCreated', 'thread', 'threadName', 'processName', 
#             'process', 'stack_info', 'exc_info', 'exc_text', 'mod_type', 
#             'mod_name', 'message'
#         }
#         
#         for key, value in record.__dict__.items():
#             if key not in excluded_fields and not key.startswith('_'):
#                 try:
#                     # Ensure value is JSON serializable
#                     json.dumps(value, default=str)
#                     log_entry[key] = value
#                 except (TypeError, ValueError):
#                     log_entry[key] = str(value)
#         
#         return json.dumps(log_entry, default=str)


class TabDelimitedFormatter(logging.Formatter):
    """Tab-delimited formatter for human-friendly logs that can be loaded into databases."""
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as tab-delimited structure.
        
        Format: TIMESTAMP \t LEVEL \t LOGGER \t MOD_TYPE \t MOD_NAME \t MESSAGE \t EXTRA_FIELDS
        
        Args:
            record: Log record to format
            
        Returns:
            Tab-delimited log entry
        """
        # Core fields
        timestamp = datetime.fromtimestamp(record.created).isoformat() + "Z"
        level = record.levelname
        logger_name = record.name
        message = record.getMessage()
        
        # Mod context fields (use '-' for empty)
        mod_type = getattr(record, 'mod_type', '-')
        mod_name = getattr(record, 'mod_name', '-')
        
        # Collect extra fields (exclude standard logging fields and mod context)
        excluded_fields = {
            'name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 
            'filename', 'module', 'lineno', 'funcName', 'created', 'msecs', 
            'relativeCreated', 'thread', 'threadName', 'processName', 
            'process', 'stack_info', 'exc_info', 'exc_text', 'mod_type', 
            'mod_name', 'message'
        }
        
        extra_fields = {}
        for key, value in record.__dict__.items():
            if key not in excluded_fields and not key.startswith('_'):
                try:
                    # Ensure value is JSON serializable
                    json.dumps(value, default=str)
                    extra_fields[key] = value
                except (TypeError, ValueError):
                    extra_fields[key] = str(value)
        
        # Handle exceptions
        if record.exc_info:
            exception_text = self.formatException(record.exc_info)
            extra_fields["exception"] = exception_text
        
        # Convert extra fields to JSON string
        if extra_fields:
            extra_fields_str = json.dumps(extra_fields, default=str)
        else:
            extra_fields_str = '-'
        
        # Escape tabs and newlines in core fields
        message = message.replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
        logger_name = logger_name.replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
        mod_type = mod_type.replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
        mod_name = mod_name.replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
        
        # Build tab-delimited log entry
        log_parts = [
            timestamp,
            level,
            logger_name,
            mod_type,
            mod_name,
            message,
            extra_fields_str
        ]
        
        return '\t'.join(log_parts)


def setup_console_logging(log_config: Dict[str, Any]) -> None:
    """
    Setup tab-delimited logging to console (stdout/stderr).
    
    Args:
        log_config: Logging configuration dictionary
        
    Raises:
        RuntimeError: If logging setup fails
    """
    if not isinstance(log_config, dict):
        raise RuntimeError("log_config must be a dictionary")
    
    try:
        # Configure root logger
        log_level = log_config.get("log_level", "INFO").upper()
        if log_level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            raise RuntimeError(f"Invalid log level: {log_level}")
        
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, log_level))
        
        # Clear any existing handlers to avoid duplicates
        root_logger.handlers.clear()
        
        # Setup console handler with tab-delimited formatting
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setFormatter(TabDelimitedFormatter())
        root_logger.addHandler(console_handler)
            
        # Test logging works
        test_logger = logging.getLogger("datapy.logging.test")
        test_logger.info("Console logging initialized successfully")
        
    except Exception as e:
        raise RuntimeError(f"Failed to setup console logging: {e}")


def setup_logger(
    name: str, 
    mod_type: Optional[str] = None, 
    mod_name: Optional[str] = None
) -> logging.Logger:
    """
    Setup logger for specific component with mod context.
    
    Args:
        name: Logger name (typically __name__)
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
    

def set_log_level(level: str) -> None:
    """
    Set logging level globally for the framework.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        
    Raises:
        ValueError: If level is invalid
    """
    if not level or not isinstance(level, str):
        raise ValueError("level must be a non-empty string")
    
    level = level.upper()
    if level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        raise ValueError(f"Invalid log level: {level}")
    
    log_config = DEFAULT_LOG_CONFIG.copy()
    log_config['log_level'] = level
    setup_console_logging(log_config)


def reset_logging() -> None:
    """
    Reset logging state for testing.
    
    Warning: This should only be used in testing environments.
    
    Raises:
        RuntimeError: If logging reset fails.
    """
    try:
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            handler.close()
            root_logger.removeHandler(handler)
        root_logger.setLevel(logging.WARNING)
    except Exception as e:
        raise RuntimeError(f"Failed to reset logging: {e}") from e