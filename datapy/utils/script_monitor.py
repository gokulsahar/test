"""
Production-ready Script Monitor for DataPy Utils

Accurate memory and execution time monitoring with real-time peak tracking.
Continuously samples memory usage to capture true peak values.
"""

import time
import os
import sys
import threading
from typing import Callable, Any, Optional, Dict
from functools import wraps

# Import your logger
from datapy.mod_manager.logger import setup_logger


class RealTimeMemoryTracker:
    """Real-time memory tracker that samples during execution."""
    
    def __init__(self):
        self.psutil_available = False
        self.process = None
        self.peak_memory = 0.0
        self.initial_memory = 0.0
        self.monitoring_thread = None
        self.stop_event = threading.Event()
        self._setup_psutil()
    
    def _setup_psutil(self):
        """Initialize psutil for system memory monitoring."""
        try:
            import psutil
            self.process = psutil.Process()
            self.psutil_available = True
        except (ImportError, AttributeError, OSError):
            self.psutil_available = False
    
    def _get_memory_mb(self) -> float:
        """Get current memory usage in MB."""
        if not self.psutil_available:
            return 0.0
        
        try:
            memory_info = self.process.memory_info()
            return memory_info.rss / (1024 * 1024)
        except Exception:
            return 0.0
    
    def _monitor_memory_continuously(self):
        """Continuously monitor memory usage in background thread."""
        while not self.stop_event.is_set():
            try:
                current_memory = self._get_memory_mb()
                if current_memory > self.peak_memory:
                    self.peak_memory = current_memory
            except Exception:
                pass
            
            # Sample every 5ms for high accuracy
            self.stop_event.wait(0.005)
    
    def start_monitoring(self) -> float:
        """Start continuous memory monitoring."""
        self.initial_memory = self._get_memory_mb()
        self.peak_memory = self.initial_memory
        
        if self.psutil_available:
            self.stop_event.clear()
            self.monitoring_thread = threading.Thread(
                target=self._monitor_memory_continuously,
                daemon=True
            )
            self.monitoring_thread.start()
        
        return self.initial_memory
    
    def stop_monitoring(self) -> float:
        """Stop monitoring and return peak memory."""
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            self.stop_event.set()
            self.monitoring_thread.join(timeout=0.1)
        
        final_memory = self._get_memory_mb()
        return max(self.peak_memory, final_memory)


def monitor_execution(name: Optional[str] = None) -> Callable:
    """
    Production-ready decorator for accurate execution monitoring.
    
    Features:
    - Real-time peak memory tracking with continuous sampling
    - High-precision timing with time.perf_counter()
    - System-level memory monitoring via psutil
    - Zero-failure design with graceful degradation
    
    Args:
        name: Optional custom name for the monitored function
        
    Returns:
        Decorated function that tracks and reports execution metrics
        
    Examples:
        @monitor_execution()
        def main():
            # Your code here - memory will be sampled continuously
            pass
            
        @monitor_execution("Data Processing")
        def process_data():
            # Peak memory will be captured even for brief spikes
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Auto-generate name if not provided
            display_name = name
            if display_name is None:
                try:
                    frame = sys._getframe(1)
                    filename = os.path.basename(frame.f_code.co_filename)
                    file_base = os.path.splitext(filename)[0]
                    display_name = f"{file_base}.{func.__name__}"
                except Exception:
                    display_name = func.__name__
            
            # Initialize tracker
            tracker = RealTimeMemoryTracker()
            
            # Start monitoring
            start_time = time.perf_counter()
            initial_memory = tracker.start_monitoring()
            
            try:
                # Execute the function while monitoring runs in background
                result = func(*args, **kwargs)
                
                # Stop monitoring and get results
                end_time = time.perf_counter()
                execution_time = end_time - start_time
                peak_memory = tracker.stop_monitoring()
                
                # Add metrics to result if it's a dict
                if isinstance(result, dict):
                    try:
                        metrics = result.setdefault('metrics', {})
                        if isinstance(metrics, dict):
                            metrics['execution_monitoring'] = {
                                'execution_time_seconds': round(execution_time, 3),
                                'peak_memory_mb': round(peak_memory, 2),
                                'function_name': display_name,
                                'monitoring_available': tracker.psutil_available
                            }
                    except Exception:
                        pass
                
                # Log summary using your logger
                logger = setup_logger(__name__)
                if tracker.psutil_available:
                    logger.info(f"{display_name} - EXECUTION COMPLETE")
                    logger.info(f"Execution Time: {execution_time:.3f} seconds")
                    logger.info(f"Peak Memory: {peak_memory:.2f} MB")
                else:
                    logger.info(f"{display_name} completed in {execution_time:.3f}s")
                    logger.warning("Memory monitoring unavailable (install psutil)")
                
                return result
                
            except Exception as e:
                # Ensure monitoring stops even on exception
                try:
                    tracker.stop_monitoring()
                except Exception:
                    pass
                
                end_time = time.perf_counter()
                execution_time = end_time - start_time
                
                logger = setup_logger(__name__)
                logger.error(f"{display_name} failed after {execution_time:.3f}s")
                
                raise  # Re-raise the original exception
        
        return wrapper
    return decorator


def get_current_memory_mb() -> float:
    """
    Get current memory usage in MB.
    
    Returns:
        Memory usage in MB, or 0.0 if monitoring unavailable
    """
    try:
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        return memory_info.rss / (1024 * 1024)
    except Exception:
        return 0.0