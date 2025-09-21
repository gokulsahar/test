"""
ModResult class for standardized output format across all DataPy mods.

Provides consistent result handling, metrics collection, and error reporting
for both CLI and Python SDK usage patterns with mod name/type tracking.
"""

from typing import Dict, List, Optional, Any, Union
import time
import uuid

# Exit code constants for consistent error handling
SUCCESS = 0
SUCCESS_WITH_WARNINGS = 10
VALIDATION_ERROR = 20
RUNTIME_ERROR = 30
TIMEOUT = 40  # Reserved for future orchestrator
CANCELED = 50  # Reserved for future orchestrator


class ModResult:
    """
    Standardized result container for mod execution outputs.
    
    Provides consistent interface for collecting metrics, warnings, errors,
    artifacts, and shared state across all framework components with
    mod identification tracking.
    """
    
    def __init__(self, mod_type: str, mod_name: str) -> None:
        """
        Initialize a new mod result container.
        
        Args:
            mod_type: Type of mod being executed (e.g., "csv_reader")
            mod_name: Unique name for this mod instance (e.g., "customer_data")
            
        Raises:
            ValueError: If mod_type or mod_name is empty or invalid
        """
        if not mod_type or not isinstance(mod_type, str) or not mod_type.strip():
            raise ValueError("mod_type must be a non-empty string")
        if not mod_name or not isinstance(mod_name, str) or not mod_name.strip():
            raise ValueError("mod_name must be a non-empty string")
            
        self.mod_type = mod_type.strip()
        self.mod_name = mod_name.strip()
        self.start_time = time.time()
        self.run_id = f"{self.mod_type}_{uuid.uuid4().hex[:8]}"
        self.warnings: List[Dict[str, Union[str, int]]] = []
        self.errors: List[Dict[str, Union[str, int]]] = []
        self.metrics: Dict[str, Any] = {}
        self.artifacts: Dict[str, Any] = {}
        self.globals: Dict[str, Any] = {}
    
    def add_warning(self, message: str, warning_code: int = SUCCESS_WITH_WARNINGS) -> None:
        """
        Add a warning message to the result.
        
        Args:
            message: Warning description
            warning_code: Warning severity code (default: SUCCESS_WITH_WARNINGS)
            
        Raises:
            ValueError: If message is empty
        """
        if not message or not isinstance(message, str):
            raise ValueError("warning message cannot be empty")
            
        warning_entry = {
            "message": str(message).strip(),
            "warning_code": warning_code,
            "timestamp": time.time()
        }
        self.warnings.append(warning_entry)
    
    def add_error(self, message: str, error_code: int = RUNTIME_ERROR) -> None:
        """
        Add an error message to the result.
        
        Args:
            message: Error description
            error_code: Error code (use constants: VALIDATION_ERROR, RUNTIME_ERROR, etc.)
            
        Raises:
            ValueError: If message is empty
        """
        if not message or not isinstance(message, str):
            raise ValueError("error message cannot be empty")
            
        error_entry = {
            "message": str(message).strip(),
            "error_code": error_code,
            "timestamp": time.time()
        }
        self.errors.append(error_entry)
    
    def add_metric(self, key: str, value: Any) -> None:
        """
        Add a metric key-value pair to the result.
        
        Args:
            key: Metric name (must be non-empty string)
            value: Metric value (should be JSON serializable)
            
        Raises:
            ValueError: If key is empty
        """
        if not key or not isinstance(key, str) or not key.strip():
            raise ValueError("metric key cannot be empty")
            
        self.metrics[key.strip()] = value
    
    def add_artifact(self, key: str, value: Any) -> None:
        """
        Add an artifact to the result.
        
        Mods can store any type of artifact - the framework doesn't enforce types.
        Common patterns:
        - inmemory: DataFrames, lists, dicts, objects
        - file: File paths as strings
        - uri: Database connections, S3 URIs, API endpoints as strings
        
        Args:
            key: Artifact identifier (must be non-empty string)
            value: Artifact value (can be any type)
            
        Raises:
            ValueError: If key is empty
        """
        if not key or not isinstance(key, str) or not key.strip():
            raise ValueError("artifact key cannot be empty")
            
        self.artifacts[key.strip()] = value
    
    def add_global(self, key: str, value: Any) -> None:
        """
        Add a global state value for cross-mod communication.
        
        Args:
            key: Global variable name (must be non-empty string)
            value: Value to store (should be JSON serializable)
            
        Raises:
            ValueError: If key is empty
        """
        if not key or not isinstance(key, str) or not key.strip():
            raise ValueError("global key cannot be empty")
            
        self.globals[key.strip()] = value
    
    def success(self) -> Dict[str, Any]:
        """
        Return a success result dictionary.
        
        Returns:
            Standardized success result dictionary
        """
        return self._build_result("success", SUCCESS)
    
    def warning(self) -> Dict[str, Any]:
        """
        Return a warning result dictionary.
        
        Returns:
            Standardized warning result dictionary
        """
        return self._build_result("warning", SUCCESS_WITH_WARNINGS)
    
    def error(self, exit_code: int = RUNTIME_ERROR) -> Dict[str, Any]:
        """
        Return an error result dictionary with specified exit code.
        
        Args:
            exit_code: Exit code for the error
            
        Returns:
            Standardized error result dictionary
        """
        return self._build_result("error", exit_code)
    
    def _build_result(self, status: str, exit_code: int) -> Dict[str, Any]:
        """
        Build the standardized result dictionary.
        
        Args:
            status: Result status (success, warning, error)
            exit_code: Process exit code
            
        Returns:
            Complete result dictionary
            
        Raises:
            ValueError: If required fields are missing or invalid
        """
        if status not in ("success", "warning", "error"):
            raise ValueError(f"Invalid status: {status}. Must be success, warning, or error")
        
        if not isinstance(exit_code, int) or exit_code < 0:
            raise ValueError(f"Invalid exit_code: {exit_code}. Must be non-negative integer")
        
        
        result = {
            "status": status,
            "exit_code": exit_code,
            "metrics": self.metrics.copy(),
            "artifacts": self.artifacts.copy(),
            "globals": self.globals.copy(),
            "warnings": self.warnings.copy(),
            "errors": self.errors.copy(),
            "logs": {
                "run_id": self.run_id,
                "mod_type": self.mod_type,
                "mod_name": self.mod_name
            }
        }
        
        return result


# Convenience functions for common error scenarios
def validation_error(mod_name: str, message: str) -> Dict[str, Any]:
    """Create a validation error result for parameter/input validation failures."""
    if not mod_name or not message:
        raise ValueError("mod_name and message cannot be empty")
    
    result = ModResult("unknown", mod_name)
    result.add_error(message, VALIDATION_ERROR)
    return result.error(VALIDATION_ERROR)


def runtime_error(mod_name: str, message: str) -> Dict[str, Any]:
    """Create a runtime error result for execution failures."""
    if not mod_name or not message:
        raise ValueError("mod_name and message cannot be empty")
    
    result = ModResult("unknown", mod_name)
    result.add_error(message, RUNTIME_ERROR)
    return result.error(RUNTIME_ERROR)