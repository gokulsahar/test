"""
Simple Script Monitor for DataPy Utils

Measures memory usage and execution time for any Python script.
Just use the decorator on your main function.
"""

import time
import psutil
import os
from typing import Callable, Any


def monitor_execution(name: str = None):
    """
    Decorator to monitor function execution time and memory usage.
    
    Args:
        name: Optional name for the script. If None, auto-generates from filename and function name.
        
    Returns:
        Decorated function that prints execution summary
        
    Examples:
        @monitor_execution()  # Auto-detects: "my_pipeline.main"
        def main():
            pass
            
        @monitor_execution("my_custom_name")  # Custom name
        def main():
            pass
    """
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs) -> Any:
            # Auto-generate name if not provided
            if name is None:
                # Get filename without extension and function name
                import inspect
                frame = inspect.currentframe()
                try:
                    # Get the filename where the decorator is used
                    filename = frame.f_back.f_code.co_filename
                    file_base = os.path.splitext(os.path.basename(filename))[0]
                    display_name = f"{file_base}.{func.__name__}"
                finally:
                    del frame  # Prevent reference cycles
            else:
                display_name = name
            
            # Start monitoring
            start_time = time.perf_counter()
            start_memory = _get_memory_mb()
            
            print(f"Starting {display_name}...")
            
            try:
                # Execute function
                result = func(*args, **kwargs)
                
                # Calculate metrics
                end_time = time.perf_counter()
                end_memory = _get_memory_mb()
                
                execution_time = end_time - start_time
                memory_delta = end_memory - start_memory
                
                # Print summary
                print(f"\n{'-'*50}")
                print(f"{display_name.upper()} EXECUTION SUMMARY")
                print(f"{'-'*50}")
                print(f"Execution time: {execution_time:.1f}s")
                print(f"Memory usage:")
                print(f"   Start: {start_memory:.1f} MB")
                print(f"   Peak: {end_memory:.1f} MB") 
                print(f"   Delta: {memory_delta:+.1f} MB")
                print(f"{'-'*50}")
                
                return result
                
            except Exception as e:
                # Still show timing on exception
                end_time = time.perf_counter()
                execution_time = end_time - start_time
                print(f"\n{display_name} failed after {execution_time:.1f}s")
                raise
        
        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        return wrapper
    return decorator


def _get_memory_mb() -> float:
    """Get current memory usage in MB."""
    try:
        return psutil.Process().memory_info().rss / (1024**2)
    except:
        return 0.0