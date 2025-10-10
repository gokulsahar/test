"""
Production-ready Script Monitor for DataPy Utils (with RealTimeMemoryTracker)

Accurate memory and execution time monitoring with configurable profiling levels.
Uses memray for detailed memory profiling; falls back to real-time peak tracking
(via psutil sampling every ~5ms) for Windows-friendly 'low' profiling.

Levels:
- off: no profiling
- low: CPU + (memray OR RealTimeMemoryTracker fallback)
- medium: requires memray (Python allocators)
- high: requires memray (native traces)
"""

import argparse
import os
import sys
import threading
import time
from functools import wraps
from typing import Any, Callable, Dict, Optional
import logging as _logging
import contextvars

from datapy.mod_manager.logger import setup_logger

# Module-level constants
DEFAULT_SAMPLING_INTERVAL_S = 0.005  # 5ms sampling interval for memory tracking
MAX_THREAD_JOIN_ATTEMPTS = 3  # Maximum attempts to join monitoring thread
THREAD_JOIN_TIMEOUT_S = 0.1  # Timeout per join attempt in seconds

logger = setup_logger(__name__)

# Re-entrancy guard so nested monitors don't duplicate sampling
_active_monitor_depth = contextvars.ContextVar("datapy_monitor_depth", default=0)


def _warn_early(msg: str):
    """
    Ensure the warning is visible even if app logging isn't configured yet.
    Attaches a temporary stderr handler only for this message, then removes it.
    """
    try:
        if not logger.handlers and not logger.propagate:
            h = _logging.StreamHandler(sys.stderr)
            h.setLevel(_logging.WARNING)
            h.set_name("datapy_temp_warn")
            h.setFormatter(_logging.Formatter("%(levelname)s: %(message)s"))
            
            # Store original level to restore after warning
            original_level = logger.level
            
            logger.addHandler(h)
            logger.setLevel(_logging.WARNING)
            try:
                logger.warning(msg)
            finally:
                logger.removeHandler(h)
                logger.setLevel(original_level)
        else:
            logger.warning(msg)
    except Exception:
        # last-resort fallback
        print(msg, file=sys.stderr)


def _parse_profile_level() -> str:
    """Parse profile level from command-line arguments."""
    try:
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument("--profile-level", choices=["off", "low", "medium", "high"], default="off")
        args, _ = parser.parse_known_args()
        return (args.profile_level or "off").lower()
    except Exception:
        return "off"


class RealTimeMemoryTracker:
    """
    Real-time memory tracker that samples during execution (Windows-friendly).
    
    Uses a background thread to sample memory usage at configurable intervals,
    tracking peak memory consumption during execution.
    """

    def __init__(self, interval_s: Optional[float] = None):
        """
        Initialize the memory tracker.
        
        Args:
            interval_s: Sampling interval in seconds (default: 0.005s / 5ms)
        """
        if interval_s is None:
            interval_s = DEFAULT_SAMPLING_INTERVAL_S
        self.interval_s = max(0.001, float(interval_s))
        self.psutil_available = False
        self.process = None
        self.peak_memory = 0.0
        self.initial_memory = 0.0
        self.monitoring_thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()
        self._setup_psutil()

    def _setup_psutil(self):
        """Initialize psutil process monitoring."""
        try:
            import psutil
            self.process = psutil.Process()
            self.psutil_available = True
        except (ImportError, AttributeError, OSError):
            self.psutil_available = False

    def _get_memory_mb(self) -> float:
        """Get current process memory usage in MB."""
        if not self.psutil_available:
            return 0.0
        try:
            memory_info = self.process.memory_info()
            return memory_info.rss / (1024 * 1024)
        except Exception:
            return 0.0

    def _monitor_memory_continuously(self):
        """Background thread function to continuously sample memory usage."""
        while not self.stop_event.is_set():
            try:
                current = self._get_memory_mb()
                if current > self.peak_memory:
                    self.peak_memory = current
            except Exception:
                pass
            self.stop_event.wait(self.interval_s)

    def start_monitoring(self) -> float:
        """
        Start memory monitoring in background thread.
        
        Returns:
            Initial memory usage in MB
        """
        self.initial_memory = self._get_memory_mb()
        self.peak_memory = self.initial_memory
        if self.psutil_available:
            self.stop_event.clear()
            self.monitoring_thread = threading.Thread(
                target=self._monitor_memory_continuously, 
                daemon=True, 
                name="RTMemTracker"
            )
            self.monitoring_thread.start()
        return self.initial_memory

    def stop_monitoring(self) -> float:
        """
        Stop memory monitoring and return peak memory usage.
        
        Returns:
            Peak memory usage in MB during monitoring period
        """
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            self.stop_event.set()
            
            # Try to gracefully join the thread
            thread_stopped = False
            for _ in range(MAX_THREAD_JOIN_ATTEMPTS):
                self.monitoring_thread.join(timeout=THREAD_JOIN_TIMEOUT_S)
                if not self.monitoring_thread.is_alive():
                    thread_stopped = True
                    break
            
            # Log warning if thread didn't stop gracefully
            if not thread_stopped:
                logger.warning(
                    f"Memory monitoring thread did not stop gracefully after "
                    f"{MAX_THREAD_JOIN_ATTEMPTS} attempts. Thread will be abandoned as daemon."
                )
        
        # Return only peak memory (final_memory is always <= peak_memory)
        return self.peak_memory


class ProfilerContext:
    """
    Context manager for execution profiling with low cognitive complexity.
    
    Orchestrates profiling lifecycle:
    - __enter__: start timing, initialize psutil, start memory profiling
    - __exit__: stop timing, finalize profilers, collect metrics, log summary
    """

    def __init__(self, profile_level: str, name: str):
        """
        Initialize profiler context.
        
        Args:
            profile_level: Profiling level ('off', 'low', 'medium', 'high')
            name: Display name for this profiling context
        """
        self.profile_level = profile_level
        self.name = name
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None

        # memray profiling
        self.memray_tracker = None
        self._memray_output_path: Optional[str] = None

        # CPU / psutil monitoring
        self.psutil_process = None
        self.cpu_times_start = None

        # Real-time memory tracker (fallback for 'low')
        self.rt_mem_tracker: Optional[RealTimeMemoryTracker] = None

        # Nesting control
        self._depth_token = None

    def __enter__(self):
        """Start profiling context."""
        # Increase nesting depth
        current_depth = _active_monitor_depth.get()
        self._depth_token = _active_monitor_depth.set(current_depth + 1)

        self.start_time = time.perf_counter()
        if self._is_off():
            return self

        # For nested monitors, don't start new samplers/profilers
        if _active_monitor_depth.get() > 1:
            return self

        self._init_psutil()
        self._start_memory_profiling()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop profiling context and log results."""
        execution_time = self._elapsed()

        # Only outermost monitor stops profilers
        if not self._is_off() and _active_monitor_depth.get() == 1:
            self._stop_profilers(exc_type, exc_val, exc_tb)

        metrics = self._base_metrics(execution_time)
        self._collect_cpu_metrics(metrics)
        self._collect_memory_metrics(metrics)
        self._log_summary(metrics, execution_time)

        # Decrease nesting depth
        if self._depth_token is not None:
            _active_monitor_depth.reset(self._depth_token)

        return None  # don't suppress exceptions

    def _is_off(self) -> bool:
        """Check if profiling is disabled."""
        return self.profile_level == "off"

    def _elapsed(self) -> float:
        """Calculate elapsed execution time."""
        self.end_time = time.perf_counter()
        start = self.start_time or self.end_time
        return (self.end_time - start) if self.end_time and start else 0.0

    def _init_psutil(self) -> None:
        """Initialize psutil for CPU monitoring."""
        try:
            import psutil
            self.psutil_process = psutil.Process()
            _ = self.psutil_process.cpu_percent(interval=None)  # prime the pump
            self.cpu_times_start = self.psutil_process.cpu_times()
        except Exception as e:
            logger.debug(f"psutil init failed: {e}")

    def _start_memory_profiling(self) -> None:
        """Start memory profiling with memray or fallback to RealTimeMemoryTracker."""
        if self.profile_level not in {"low", "medium", "high"}:
            return
        if self._try_start_memray():
            return
        self._handle_memray_missing_or_failed()

    def _default_memray_path(self) -> str:
        """Generate default memray output file path."""
        ts = int(time.time())
        pid = os.getpid()
        safe_name = "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in self.name)
        base = f"memray_{safe_name}_{pid}_{ts}.bin"
        return os.path.join(os.getcwd(), base)

    def _try_start_memray(self) -> bool:
        """
        Attempt to start memray profiling.
        
        Returns:
            True if memray started successfully, False otherwise
        """
        try:
            import memray as _mem  # noqa: F401

            native = self.profile_level == "high"
            trace_py = self.profile_level in {"medium", "high"}
            out_path = self._default_memray_path()

            self.memray_tracker = _mem.Tracker(
                out_path,
                native_traces=native,
                trace_python_allocators=trace_py,
                follow_fork=False,
            )
            self._memray_output_path = out_path
            self.memray_tracker.__enter__()
            return True
        except ImportError:
            return False
        except Exception as e:
            logger.debug(f"memray init error: {e}")
            return False

    def _handle_memray_missing_or_failed(self) -> None:
        """Handle case when memray is unavailable or failed to start."""
        if self.profile_level == "low":
            self._start_rt_mem_tracker()
            _warn_early(
                "----- Warning: memray unavailable; using RealTimeMemoryTracker for 'low'. -----"
            )
            return
        _warn_early(
            f"----- Warning: memray required for '{self.profile_level}' profiling; disabling profiling -----"
        )
        self.profile_level = "off"

    def _start_rt_mem_tracker(self) -> None:
        """Start real-time memory tracker as fallback."""
        self.rt_mem_tracker = RealTimeMemoryTracker()
        self.rt_mem_tracker.start_monitoring()

    def _stop_profilers(self, exc_type, exc_val, exc_tb) -> None:
        """Stop all active profilers."""
        if self.memray_tracker:
            try:
                self.memray_tracker.__exit__(exc_type, exc_val, exc_tb)
            except Exception as e:
                logger.debug(f"memray finalize error: {e}")
        if self.rt_mem_tracker:
            try:
                self.rt_mem_tracker.stop_monitoring()
            except Exception as e:
                logger.debug(f"Real-time memory tracker stop error: {e}")

    def _base_metrics(self, execution_time: float) -> Dict[str, Any]:
        """Create base metrics dictionary."""
        return {
            "name": self.name,
            "execution_time_seconds": round(execution_time, 3),
            "profile_level": self.profile_level,
        }

    def _collect_cpu_metrics(self, metrics: Dict[str, Any]) -> None:
        """Collect CPU usage metrics."""
        if self._is_off() or not self.psutil_process or not self.cpu_times_start:
            return
        try:
            cpu_percent = self.psutil_process.cpu_percent(interval=None)
            cpu_times_end = self.psutil_process.cpu_times()
            user = max(0.0, cpu_times_end.user - self.cpu_times_start.user)
            system = max(0.0, cpu_times_end.system - self.cpu_times_start.system)
            metrics.update(
                {
                    "cpu_percent": round(cpu_percent, 2),
                    "cpu_time_user": round(user, 3),
                    "cpu_time_system": round(system, 3),
                    "cpu_time_total": round(user + system, 3),
                }
            )
        except Exception as e:
            logger.debug(f"CPU metrics error: {e}")

    def _collect_memory_metrics(self, metrics: Dict[str, Any]) -> None:
        """Collect memory usage metrics."""
        if self._is_off():
            return
        if self.memray_tracker:
            metrics["memory_profiling"] = "memray"
            if self._memray_output_path:
                metrics["memray_file"] = self._memray_output_path
            return
        if self.rt_mem_tracker and self.rt_mem_tracker.psutil_available:
            peak = self.rt_mem_tracker.peak_memory
            init = self.rt_mem_tracker.initial_memory
            metrics.update(
                {
                    "memory_profiling": "realtime_sampler",
                    "peak_memory_mb": round(float(peak), 2),
                    "initial_memory_mb": round(float(init), 2),
                }
            )

    def _log_summary(self, metrics: Dict[str, Any], execution_time: float) -> None:
        """Log profiling summary with appropriate detail level."""
        logger.debug(f"Profiling metrics: {metrics}")
        if self._is_off():
            logger.info(f"{self.name} - COMPLETE: time={execution_time:.3f}s, profile_level=off")
            return

        cpu = metrics.get("cpu_percent", 0.0)

        # Prefer a compact human-friendly INFO line
        if metrics.get("memory_profiling") == "realtime_sampler" and "peak_memory_mb" in metrics:
            logger.info(
                f"{self.name} - COMPLETE: time={execution_time:.3f}s, "
                f"cpu={cpu:.1f}%, peak_memory={metrics['peak_memory_mb']}MB, "
                f"profile_level={self.profile_level}"
            )
            return

        if metrics.get("memory_profiling") == "memray":
            memray_file = metrics.get("memray_file", "<unknown>")
            logger.info(
                f"{self.name} - COMPLETE: time={execution_time:.3f}s, "
                f"cpu={cpu:.1f}%, profile_level={self.profile_level}, memray_file={memray_file}"
            )
            return

        # Fallback
        logger.info(
            f"{self.name} - COMPLETE: time={execution_time:.3f}s, "
            f"cpu={cpu:.1f}%, profile_level={self.profile_level}"
        )


def monitor_execution(name: Optional[str] = None, profile_level: Optional[str] = None) -> Callable:
    """
    Decorator for execution monitoring with memray + real-time fallback.
    
    Priority: CLI --profile-level > decorator arg > 'off'
    
    Args:
        name: Display name for monitoring (default: derived from function)
        profile_level: Profiling level ('off', 'low', 'medium', 'high')
    
    Returns:
        Decorator function
    
    Example:
        @monitor_execution("my_task", profile_level="low")
        def process_data():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            display_name = name or _default_display_name(func)
            cmd_profile = _parse_profile_level()
            final_level = cmd_profile if cmd_profile != "off" else (profile_level or "off")
            with ProfilerContext(final_level, display_name):
                return func(*args, **kwargs)
        return wrapper
    return decorator


def _default_display_name(func: Callable) -> str:
    """
    Generate default display name for function monitoring.
    
    Format: filename.function_name
    """
    try:
        frame = sys._getframe(1)
        filename = os.path.basename(frame.f_code.co_filename)
        return f"{os.path.splitext(filename)[0]}.{func.__name__}"
    except Exception:
        return func.__name__


def get_current_memory_mb() -> float:
    """
    Get current process RSS (Resident Set Size) in MB.
    
    Returns:
        Current memory usage in MB, or 0.0 if unavailable
    """
    try:
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        return memory_info.rss / (1024 * 1024)
    except Exception:
        return 0.0