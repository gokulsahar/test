"""
Lightweight execution monitoring for DataPy framework.

Tracks memory usage and execution time for analysis purposes only.
No control mechanisms - just measurement and reporting.
"""

import time
import threading
from typing import Dict, Any, Callable, Optional

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    psutil = None

from .logger import setup_logger

logger = setup_logger(__name__)

# Thread-safe singleton for process monitoring
_process_monitor = None
_monitor_lock = threading.Lock()


class ExecutionMonitor:
    """Simple memory monitoring for analysis."""
    
    def __init__(self):
        """Initialize process monitor."""
        self.process = None
        
        if not PSUTIL_AVAILABLE:
            logger.debug("psutil not available - memory monitoring disabled")
            return
        
        try:
            self.process = psutil.Process()
            logger.debug("ExecutionMonitor initialized")
        except Exception as e:
            logger.debug(f"Failed to initialize psutil: {e}")
            self.process = None
    
    def get_memory_usage_mb(self) -> float:
        """Get current memory usage in MB."""
        if self.process is None:
            return 0.0
        
        try:
            memory_info = self.process.memory_info()
            return round(memory_info.rss / (1024 * 1024), 2)
        except Exception:
            return 0.0
    
    def is_available(self) -> bool:
        """Check if memory monitoring is available."""
        return self.process is not None


def _get_execution_monitor() -> ExecutionMonitor:
    """Get thread-safe singleton ExecutionMonitor instance."""
    global _process_monitor, _monitor_lock
    
    with _monitor_lock:
        if _process_monitor is None:
            _process_monitor = ExecutionMonitor()
        return _process_monitor


def execute_with_monitoring(
    mod_type: str,
    params: Dict[str, Any],
    mod_name: Optional[str],
    original_executor: Callable
) -> Dict[str, Any]:
    """
    Execute mod with memory and timing monitoring (always enabled).
    
    Args:
        mod_type: Type of mod being executed
        params: Parameters for mod execution
        mod_name: Name of mod instance
        original_executor: Original mod execution function
        
    Returns:
        ModResult dictionary with monitoring metrics added
    """
    monitor = _get_execution_monitor()
    
    # Capture initial state
    start_time = time.perf_counter()
    memory_start = monitor.get_memory_usage_mb()
    
    try:
        # Execute the mod
        result = original_executor(mod_type, params, mod_name)
        
        # Capture final state
        end_time = time.perf_counter()
        memory_end = monitor.get_memory_usage_mb()
        execution_time = round(end_time - start_time, 3)
        
        # Add monitoring metrics to result
        if isinstance(result, dict):
            if "metrics" not in result:
                result["metrics"] = {}
            
            result["metrics"]["execution_monitoring"] = {
                "execution_time": execution_time,
                "memory_start_mb": memory_start,
                "memory_end_mb": memory_end,
                "memory_delta_mb": round(memory_end - memory_start, 2),
                "monitoring_available": monitor.is_available()
            }
        
        logger.debug(f"Monitoring for {mod_type}: {execution_time}s, "
                    f"memory_delta={memory_end - memory_start:.1f}MB")
        
        return result
        
    except Exception as e:
        # If monitoring fails, still return original result
        logger.debug(f"Monitoring failed for {mod_type}, falling back: {e}")
        return original_executor(mod_type, params, mod_name)