"""
ModResult class for standardized output format across all DataPy mods.

Provides consistent result handling, metrics collection, and error reporting
for both CLI and Python SDK usage patterns.
"""

from typing import Dict, List, Optional, Any
import time
import uuid


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
    artifacts, and shared state across all framework components.
    """
    
    def __init__(self, mod_name: str):
        """
        Initialize a new mod result container.
        
        Args:
            mod_name: Name of the mod being executed
        """
        self.mod_name = mod_name
        self.start_time = time.time()
        self.run_id = f"{mod_name}_{uuid.uuid4().hex[:8]}"
        self.warnings: List[str] = []
        self.errors: List[str] = []
        self.metrics: Dict[str, Any] = {}
        self.artifacts: Dict[str, str] = {}
        self.globals: Dict[str, Any] = {}
    
    def add_warning(self, message: str) -> None:
        """Add a warning message to the result."""
        self.warnings.append(message)
    
    def add_error(self, message: str, error_code: int) -> None:
        """
        Add an error message to the result with mandatory error code.
        
        Args:
            message: Error description
            error_code: Error code (use constants: VALIDATION_ERROR, RUNTIME_ERROR, etc.)
        """
        self.errors.append({"message": message, "error_code": error_code})
    
    def add_metric(self, key: str, value: Any) -> None:
        """Add a metric key-value pair to the result."""
        self.metrics[key] = value
    
    def add_artifact(self, key: str, path: str) -> None:
        """Add an artifact path to the result."""
        self.artifacts[key] = path
    
    def add_global(self, key: str, value: Any) -> None:
        """Add a global state value for cross-mod communication."""
        self.globals[key] = value
    
    def success(self) -> Dict[str, Any]:
        """Return a success result dictionary."""
        return self._build_result("success", SUCCESS)
    
    def warning(self) -> Dict[str, Any]:
        """Return a warning result dictionary."""
        return self._build_result("warning", SUCCESS_WITH_WARNINGS)
    
    def error(self, exit_code: int = RUNTIME_ERROR) -> Dict[str, Any]:
        """Return an error result dictionary with specified exit code."""
        return self._build_result("error", exit_code)
    
    def _build_result(self, status: str, exit_code: int) -> Dict[str, Any]:
        """Build the standardized result dictionary."""
        execution_time = time.time() - self.start_time
        
        result = {
            "status": status,
            "execution_time": round(execution_time, 3),
            "exit_code": exit_code,
            "metrics": self.metrics,
            "artifacts": self.artifacts,
            "globals": self.globals,
            "warnings": self.warnings,
            "errors": self.errors,
            "logs": {
                "run_id": self.run_id,
                "mod_name": self.mod_name
            }
        }
            
        return result