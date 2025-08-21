"""
ModResult class for standardized output format across all DataPy mods.

Provides consistent result handling, metrics collection, and error reporting
for both CLI and Python SDK usage patterns with mod name/type tracking.
"""

from typing import Dict, List, Optional, Any, Union
import time
import uuid
import logging

logger = logging.getLogger(__name__)

# Exit code constants for consistent error handling
SUCCESS = 0
SUCCESS_WITH_WARNINGS = 10
VALIDATION_ERROR = 20
RUNTIME_ERROR = 30
TIMEOUT = 40
CANCELED = 50


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
            ValueError: If mod_type or mod_name is empty or None
        """
        if not mod_type or not isinstance(mod_type, str):
            raise ValueError("mod_type must be a non-empty string")
        if not mod_name or not isinstance(mod_name, str):
            raise ValueError("mod_name must be a non-empty string")
            
        self.mod_type = mod_type
        self.mod_name = mod_name
        self.start_time = time.time()
        self.run_id = f"{mod_type}_{uuid.uuid4().hex[:8]}"
        self.warnings: List[str] = []
        self.errors: List[Dict[str, Union[str, int]]] = []
        self.metrics: Dict[str, Any] = {}
        self.artifacts: Dict[str, Any] = {}
        self.globals: Dict[str, Any] = {}
    
    def add_warning(self, message: str) -> None:
        """
        Add a warning message to the result.
        
        Args:
            message: Warning description
        """
        if message:
            self.warnings.append(str(message))
            logger.warning(f"[{self.mod_name}] {message}")
    
    def add_error(self, message: str, error_code: int = RUNTIME_ERROR) -> None:
        """
        Add an error message to the result with error code.
        
        Args:
            message: Error description
            error_code: Error code (use constants: VALIDATION_ERROR, RUNTIME_ERROR, etc.)
        """
        if message:
            error_entry = {"message": str(message), "error_code": error_code}
            self.errors.append(error_entry)
            logger.error(f"[{self.mod_name}] {message} (code: {error_code})")
    
    def add_metric(self, key: str, value: Any) -> None:
        """
        Add a metric key-value pair to the result.
        
        Args:
            key: Metric name
            value: Metric value (should be JSON serializable)
        """
        if key:
            self.metrics[str(key)] = value
    
    def add_artifact(self, key: str, value: Any) -> None:
        """
        Add an artifact to the result.
        
        Mods can store any type of artifact - the framework doesn't enforce types.
        Common patterns:
        - inmemory: DataFrames, lists, dicts, objects
        - file: File paths as strings
        - uri: Database connections, S3 URIs, API endpoints as strings
        
        Args:
            key: Artifact identifier
            value: Artifact value (can be any type - inmemory objects, file paths, URIs, etc.)
        """
        if key:
            self.artifacts[str(key)] = value
    
    def add_global(self, key: str, value: Any) -> None:
        """
        Add a global state value for cross-mod communication.
        
        Args:
            key: Global variable name
            value: Value to store (should be JSON serializable)
        """
        if key:
            self.globals[str(key)] = value
    
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
        execution_time = time.time() - self.start_time
        
        # Validation: ensure we have required fields
        if not self.mod_type:
            raise ValueError("mod_type cannot be empty")
        if not self.mod_name:
            raise ValueError("mod_name cannot be empty")
        if not self.run_id:
            raise ValueError("run_id cannot be empty")
        if status not in ("success", "warning", "error"):
            raise ValueError(f"Invalid status: {status}")
        
        result = {
            "status": status,
            "execution_time": round(execution_time, 3),
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
        
        # Log the final result
        logger.info(f"[{self.mod_name}] Execution completed with status: {status} "
                   f"(time: {result['execution_time']}s, exit_code: {exit_code})")
            
        return result


# Convenience functions for common exit codes
def validation_error(mod_name: str, message: str) -> Dict[str, Any]:
    """Create a validation error result."""
    result = ModResult("unknown", mod_name)
    result.add_error(message, VALIDATION_ERROR)
    return result.error(VALIDATION_ERROR)


def runtime_error(mod_name: str, message: str) -> Dict[str, Any]:
    """Create a runtime error result."""
    result = ModResult("unknown", mod_name)
    result.add_error(message, RUNTIME_ERROR)
    return result.error(RUNTIME_ERROR)


def timeout_error(mod_name: str, message: str) -> Dict[str, Any]:
    """Create a timeout error result."""
    result = ModResult("unknown", mod_name)
    result.add_error(message, TIMEOUT)
    return result.error(TIMEOUT)