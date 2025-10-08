"""
Simplified console logging for DataPy framework.

Provides tab-delimited console output with mod context tracking
and configurable log levels. No file management - output to stdout/stderr only.
"""

import logging
import json
import sys
import traceback
from typing import Dict, Any, Optional
from datetime import datetime

# Default configuration
DEFAULT_LOG_CONFIG = {
    "log_level": "INFO"
}


class TabDelimitedFormatter(logging.Formatter):
    """Tab-delimited formatter for human-friendly logs that can be loaded into databases."""
    
    # Fields to exclude when collecting extra fields from log records
    EXCLUDED_FIELDS = {
        'name', 'msg', 'args', 'levelname', 'levelno', 'pathname',
        'filename', 'module', 'lineno', 'funcName', 'created', 'msecs',
        'relativeCreated', 'thread', 'threadName', 'processName',
        'process', 'stack_info', 'exc_info', 'exc_text', 'mod_type',
        'mod_name', 'message', 'taskName'
    }
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as tab-delimited structure with automatic stack traces.
        
        Format: TIMESTAMP \t LEVEL \t LOGGER \t MOD_TYPE \t MOD_NAME \t MESSAGE \t EXTRA_FIELDS
        
        Args:
            record: Log record to format
            
        Returns:
            Tab-delimited log entry with full stack traces for warnings/errors
        """
        # Extract core fields
        timestamp = datetime.fromtimestamp(record.created).isoformat() + "Z"
        level = record.levelname
        logger_name = record.name
        message = record.getMessage()
        mod_type = getattr(record, 'mod_type', '-')
        mod_name = getattr(record, 'mod_name', '-')
        
        # Collect extra fields
        extra_fields = self._collect_extra_fields(record)
        
        # Add stack trace for warnings and errors
        if record.levelno >= logging.WARNING:
            self._add_stack_trace(record, extra_fields)
            return self._format_error_log(
                timestamp, logger_name, message, 
                mod_type, mod_name, extra_fields
            )
        
        # Standard tab-delimited format for other levels
        return self._format_standard_log(
            timestamp, level, logger_name, 
            mod_type, mod_name, message, extra_fields
        )
    
    def _collect_extra_fields(self, record: logging.LogRecord) -> Dict[str, Any]:
        """
        Collect extra fields from log record, excluding standard logging fields.
        
        Args:
            record: Log record to extract fields from
            
        Returns:
            Dictionary of extra fields with JSON-serializable values
        """
        extra_fields = {}
        
        for key, value in record.__dict__.items():
            if key in self.EXCLUDED_FIELDS or key.startswith('_'):
                continue
            
            try:
                json.dumps(value, default=str)
                extra_fields[key] = value
            except (TypeError, ValueError):
                extra_fields[key] = str(value)
        
        return extra_fields
    
    def _add_stack_trace(
        self, 
        record: logging.LogRecord, 
        extra_fields: Dict[str, Any]
    ) -> None:
        """
        Add stack trace to extra fields for warning/error logs.
        
        Args:
            record: Log record to extract stack trace from
            extra_fields: Dictionary to add stack trace to (modified in place)
        """
        if record.exc_info:
            stack_trace = self.formatException(record.exc_info)
        else:
            current_stack = traceback.format_stack()
            filtered_stack = current_stack[:-3]
            stack_trace = ''.join(filtered_stack).strip()
        
        extra_fields["stack_trace"] = stack_trace
    
    def _format_error_log(
        self,
        timestamp: str,
        logger_name: str,
        message: str,
        mod_type: str,
        mod_name: str,
        extra_fields: Dict[str, Any]
    ) -> str:
        """
        Format error/warning logs with enhanced multi-line structure.
        
        Args:
            timestamp: ISO format timestamp
            logger_name: Name of the logger
            message: Log message
            mod_type: Mod type context
            mod_name: Mod name context
            extra_fields: Extra fields including stack trace
            
        Returns:
            Multi-line formatted error log
        """
        lines = [
            "=" * 80,
            f"ERROR: {message}",
            f"Time: {timestamp} | Logger: {logger_name}"
        ]
        
        if mod_type != '-' or mod_name != '-':
            lines.append(f"Mod: {mod_type} | Name: {mod_name}")
        
        if "stack_trace" in extra_fields:
            lines.extend([
                "Stack Trace:",
                "-" * 40,
                extra_fields["stack_trace"],
                "-" * 40
            ])
        
        other_fields = {k: v for k, v in extra_fields.items() if k != "stack_trace"}
        if other_fields:
            lines.append(f"Additional Info: {json.dumps(other_fields, default=str)}")
        
        lines.append("=" * 80)
        return '\n'.join(lines)
    
    def _format_standard_log(
        self,
        timestamp: str,
        level: str,
        logger_name: str,
        mod_type: str,
        mod_name: str,
        message: str,
        extra_fields: Dict[str, Any]
    ) -> str:
        """
        Format standard tab-delimited log entry.
        
        Args:
            timestamp: ISO format timestamp
            level: Log level name
            logger_name: Name of the logger
            mod_type: Mod type context
            mod_name: Mod name context
            message: Log message
            extra_fields: Extra fields to include
            
        Returns:
            Tab-delimited log entry
        """
        extra_fields_str = json.dumps(extra_fields, default=str) if extra_fields else '-'
        
        # Escape special characters in fields
        message = self._escape_field(message)
        logger_name = self._escape_field(logger_name)
        mod_type = self._escape_field(mod_type)
        mod_name = self._escape_field(mod_name)
        
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
    
    @staticmethod
    def _escape_field(field: str) -> str:
        """
        Escape tabs, newlines, and carriage returns in field values.
        
        Args:
            field: Field value to escape
            
        Returns:
            Escaped field value
        """
        return (field
                .replace('\t', '\\t')
                .replace('\n', '\\n')
                .replace('\r', '\\r'))


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
        test_logger.debug("Console logging initialized successfully")
        
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
    Reset logging configuration (useful for testing).
    
    Clears all handlers and resets to default state.
    
    Raises:
        RuntimeError: If handler cleanup fails
    """
    try:
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            handler.close()
            root_logger.removeHandler(handler)
        root_logger.setLevel(logging.WARNING)
    except Exception as e:
        raise RuntimeError(str(e)) from e