"""Signal handler utility for graceful shutdown.

Provides thread-safe signal handling for SIGTERM and SIGINT.
Can be reused across different Python services.
"""

import signal
import threading
import logging
from typing import Callable, Optional, Tuple, Dict, Any

logger = logging.getLogger(__name__)


class SignalHandler:
    """Thread-safe signal handler utility.
    
    Registers handlers for shutdown signals (SIGTERM, SIGINT by default)
    and calls a callback when signal is received.
    
    Usage:
        handler = SignalHandler(callback=my_shutdown_function)
        handler.register()
        handler.wait()  # Block until signal received
        handler.restore()  # Clean up
    """
    
    def __init__(self, callback: Optional[Callable[[], None]] = None):
        """Initialize signal handler.
        
        Args:
            callback: Function to call when signal received (takes no args)
        """
        self.callback = callback
        self.shutdown_event = threading.Event()
        self._original_handlers: Dict[signal.Signals, Any] = {}
    
    def _handle_signal(self, signum: int, frame: Any) -> None:
        """Internal signal handler (called by OS).
        
        Args:
            signum: Signal number
            frame: Current stack frame (unused)
        """
        sig_name = signal.Signals(signum).name
        logger.info("Signal received: %s", sig_name)
        
        # Call user callback
        if self.callback:
            try:
                self.callback()
            except Exception as e:
                logger.error("Error in signal callback: %s", e, exc_info=True)
        
        # Set shutdown event
        self.shutdown_event.set()
    
    def register(
        self, 
        signals: Tuple[signal.Signals, ...] = (signal.SIGTERM, signal.SIGINT)
    ) -> None:
        """Register signal handlers.
        
        Args:
            signals: Tuple of signals to handle (default: SIGTERM, SIGINT)
        """
        for sig in signals:
            self._original_handlers[sig] = signal.signal(sig, self._handle_signal)
            logger.debug("Registered handler for %s", sig.name)
    
    def wait(self) -> None:
        """Block until signal received.
        
        This method blocks the calling thread until a registered signal
        is received.
        """
        logger.debug("Waiting for shutdown signal...")
        self.shutdown_event.wait()
        logger.debug("Shutdown signal received")
    
    def restore(self) -> None:
        """Restore original signal handlers.
        
        Should be called in cleanup/finally block to restore
        original signal handlers.
        """
        for sig, handler in self._original_handlers.items():
            signal.signal(sig, handler)
            logger.debug("Restored original handler for %s", sig.name)
        
        self._original_handlers.clear()
