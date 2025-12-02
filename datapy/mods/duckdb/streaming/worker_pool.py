"""Worker pool for message processing.

Each worker thread:
1. Pulls messages from processing queue
2. Validates message size
3. Calls user-provided processor_callable
4. Handles timeouts (if configured)
5. Routes failures to DLQ
6. Marks messages as processed for offset tracking
"""

import time
import queue
import threading
import traceback
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .shared_state import SharedState

logger = logging.getLogger(__name__)


def worker_loop(shared_state: 'SharedState', worker_id: int) -> None:
    """Main worker loop - runs in dedicated thread.
    
    Continuously processes messages from the processing queue.
    Stops when shared_state.running is cleared AND queue is empty.
    
    Args:
        shared_state: Shared state object
        worker_id: Unique worker ID (0 to worker_count-1)
    """
    config = shared_state.config
    thread_logger = shared_state.logger
    
    thread_logger.debug("Worker %d started", worker_id)
    processed_count = 0
    failed_count = 0
    
    while shared_state.running.is_set():
        try:
            # Get message from queue (with timeout to allow checking running flag)
            msg = shared_state.processing_queue.get(
                timeout=config.worker_queue_get_timeout_seconds
            )
        except queue.Empty:
            # No messages available, loop continues
            continue
        
        start_time = time.time()
        
        # Log message received
        thread_logger.debug(
            "Worker %d processing: partition=%d, offset=%d",
            worker_id, msg.partition, msg.offset
        )
        
        # Validate message size
        msg_size = len(msg.value) if msg.value else 0
        if config.max_message_size and msg_size > config.max_message_size:
            thread_logger.warning(
                "Worker %d: Message size %d exceeds max %d, sending to DLQ: offset=%d",
                worker_id, msg_size, config.max_message_size, msg.offset
            )
            _send_to_dlq(
                shared_state,
                msg,
                Exception(f"Message size {msg_size} exceeds max {config.max_message_size}"),
                0
            )
            _mark_processed(shared_state, msg, "failed")
            failed_count += 1
            continue
        
        # Process message
        try:
            if config.processing_timeout:
                # Process with timeout
                _process_with_timeout(config, msg, thread_logger)
            else:
                # Process without timeout
                config.processor_callable(msg.value)
            
            # Success
            processing_time_ms = (time.time() - start_time) * 1000
            thread_logger.info(
                "Worker %d processed: partition=%d, offset=%d, time=%.0fms",
                worker_id, msg.partition, msg.offset, processing_time_ms
            )
            _mark_processed(shared_state, msg, "success")
            processed_count += 1
        
        except Exception as e:
            # Failure - send to DLQ
            processing_time_ms = (time.time() - start_time) * 1000
            thread_logger.error(
                "Worker %d failed: partition=%d, offset=%d, error=%s, time=%.0fms",
                worker_id, msg.partition, msg.offset, type(e).__name__, processing_time_ms
            )
            _send_to_dlq(shared_state, msg, e, processing_time_ms)
            _mark_processed(shared_state, msg, "failed")
            failed_count += 1
    
    thread_logger.info(
        "Worker %d stopping: processed=%d, failed=%d",
        worker_id, processed_count, failed_count
    )


def _process_with_timeout(config, msg, thread_logger) -> None:
    """Process message with timeout.
    
    Runs processor_callable in a separate thread with timeout.
    If timeout is exceeded, raises TimeoutError.
    
    Args:
        config: Configuration object
        msg: Kafka message
        thread_logger: Logger instance
        
    Raises:
        TimeoutError: If processing exceeds timeout
        Exception: Any exception raised by processor_callable
    """
    result_container = []
    exception_container = []
    
    def target():
        """Target function for timeout thread."""
        try:
            result = config.processor_callable(msg.value)
            result_container.append(result)
        except Exception as e:
            exception_container.append(e)
    
    # Start processing in separate thread
    process_thread = threading.Thread(target=target, daemon=True)
    process_thread.start()
    
    # Wait for completion or timeout
    process_thread.join(timeout=config.processing_timeout)
    
    if process_thread.is_alive():
        # Timeout exceeded
        thread_logger.warning(
            "Processing timeout exceeded: %ds for offset=%d",
            config.processing_timeout, msg.offset
        )
        raise TimeoutError(
            f"Processing exceeded timeout of {config.processing_timeout}s"
        )
    
    # Check if exception occurred
    if exception_container:
        raise exception_container[0]
    
    # Success (result_container may be empty or have result, we don't care)


def _send_to_dlq(shared_state, msg, error: Exception, processing_time_ms: float) -> None:
    """Send failed message to DLQ queue.
    
    Args:
        shared_state: Shared state object
        msg: Kafka message that failed
        error: Exception that caused failure
        processing_time_ms: Time spent processing (milliseconds)
    """
    try:
        dlq_entry = {
            "msg": msg,
            "error": type(error).__name__,
            "error_message": str(error),
            "stack_trace": traceback.format_exc(),
            "processing_time_ms": processing_time_ms
        }
        
        shared_state.dlq_queue.put_nowait(dlq_entry)
        
        shared_state.logger.debug(
            "DLQ entry queued: offset=%d, error=%s",
            msg.offset, type(error).__name__
        )
    
    except queue.Full:
        # DLQ queue is full - this is critical
        shared_state.logger.error(
            "DLQ queue full, cannot record failure for offset %d (error: %s)",
            msg.offset, type(error).__name__
        )


def _mark_processed(shared_state, msg, status: str) -> None:
    """Mark message as processed for offset tracking.
    
    Args:
        shared_state: Shared state object
        msg: Kafka message
        status: "success" or "failed"
    """
    try:
        shared_state.processed_queue.put_nowait(
            (msg.partition, msg.offset, status)
        )
        
        shared_state.logger.debug(
            "Marked processed: partition=%d, offset=%d, status=%s",
            msg.partition, msg.offset, status
        )
    
    except queue.Full:
        # Processed queue is full - this is critical for offset tracking
        shared_state.logger.error(
            "Processed queue full, cannot track offset %d (status: %s)",
            msg.offset, status
        )
