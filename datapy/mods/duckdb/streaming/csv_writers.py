"""CSV writers for backup and DLQ.

Features:
- Hourly rotation (creates new CSV file each hour)
- Batch writing for performance
- UTF-8 decode with error handling
- CRITICAL: CSV initialization/write failures trigger graceful shutdown

CSV Files:
- Backup CSV: All messages (raw Kafka data)
- DLQ CSV: Failed messages with error details
"""

import csv
import time
import queue
import logging
from datetime import datetime
from typing import Dict, List, Any, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from .shared_state import SharedState

logger = logging.getLogger(__name__)

# CSV field definitions
BACKUP_CSV_FIELDS = [
    "timestamp",
    "topic",
    "partition",
    "offset",
    "key",
    "value",
    "message_size"
]

DLQ_CSV_FIELDS = [
    "timestamp",
    "topic",
    "partition",
    "offset",
    "key",
    "value",
    "error_type",
    "error_message",
    "stack_trace",
    "processing_time_ms",
    "retry_count"
]


def backup_csv_loop(shared_state: 'SharedState') -> None:
    """Backup CSV writer loop - runs in dedicated thread.
    
    Writes all Kafka messages to CSV for backup/audit purposes.
    Rotates CSV file hourly.
    
    CRITICAL: If CSV cannot be initialized or rotated, triggers graceful shutdown.
    
    Args:
        shared_state: Shared state object
    """
    config = shared_state.config
    thread_logger = shared_state.logger
    
    thread_logger.info("Backup CSV thread started: path=%s", config.backup_path)
    
    # CRITICAL: Initialize CSV writer - if this fails, trigger shutdown
    try:
        csv_context = _init_csv_writer(config.backup_path, BACKUP_CSV_FIELDS, thread_logger)
        thread_logger.info(
            "Backup CSV initialized: file=%s",
            csv_context['path']
        )
    except Exception as e:
        thread_logger.critical(
            "Failed to initialize backup CSV: %s - TRIGGERING SHUTDOWN",
            e, exc_info=True
        )
        shared_state.stop()  # Trigger graceful shutdown
        return
    
    batch: List[Dict[str, Any]] = []
    last_flush = time.time()
    last_rotation_check = time.time()
    write_count = 0
    
    # Main loop
    try:
        while shared_state.running.is_set() or not shared_state.backup_csv_queue.empty():
            # Check for hourly rotation
            try:
                csv_context, should_clear = _check_rotation(
                    csv_context,
                    config.backup_path,
                    BACKUP_CSV_FIELDS,
                    last_rotation_check,
                    batch,
                    config.csv_rotation_check_interval_seconds,
                    thread_logger
                )
                
                # CRITICAL: If rotation fails, exception is raised above
                # and we go to except block which triggers shutdown
                
                if should_clear:
                    batch = []
                
                last_rotation_check = time.time()
            
            except Exception as e:
                thread_logger.critical(
                    "CSV rotation failed: %s - TRIGGERING SHUTDOWN",
                    e, exc_info=True
                )
                shared_state.stop()  # Trigger graceful shutdown
                break
            
            # Get message from queue
            try:
                msg_data = shared_state.backup_csv_queue.get(
                    timeout=config.queue_get_timeout_seconds
                )
                batch.append(msg_data)
                
                thread_logger.debug(
                    "Backup CSV batched: offset=%d, batch_size=%d",
                    msg_data.get("offset"), len(batch)
                )
            
            except queue.Empty:
                pass
            
            # Check if we should flush
            should_flush = (
                len(batch) >= config.csv_batch_size or
                (time.time() - last_flush) >= config.csv_flush_interval_seconds
            )
            
            if should_flush and batch:
                try:
                    _write_batch(csv_context, batch, thread_logger)
                    write_count += len(batch)
                    thread_logger.debug(
                        "Backup CSV flushed: %d messages (total: %d)",
                        len(batch), write_count
                    )
                    batch = []
                    last_flush = time.time()
                
                except Exception as e:
                    thread_logger.critical(
                        "CSV write failed: %s - TRIGGERING SHUTDOWN",
                        e, exc_info=True
                    )
                    shared_state.stop()  # Trigger graceful shutdown
                    break
    
    except Exception as e:
        thread_logger.critical(
            "Backup CSV loop crashed: %s - TRIGGERING SHUTDOWN",
            e, exc_info=True
        )
        shared_state.stop()  # Trigger graceful shutdown
    
    # Final flush
    if batch:
        try:
            _write_batch(csv_context, batch, thread_logger)
            write_count += len(batch)
            thread_logger.info("Final backup CSV flush: %d messages", len(batch))
        except Exception as e:
            thread_logger.error("Final flush failed: %s", e, exc_info=True)
    
    # Close CSV file
    try:
        csv_context['file'].close()
        thread_logger.info("Backup CSV closed: %d messages written", write_count)
    except Exception as e:
        thread_logger.error("Error closing backup CSV: %s", e)


def dlq_csv_loop(shared_state: 'SharedState') -> None:
    """DLQ CSV writer loop - runs in dedicated thread.
    
    Writes failed messages to DLQ CSV with error details.
    Rotates CSV file hourly.
    
    CRITICAL: If CSV cannot be initialized or rotated, triggers graceful shutdown.
    
    Args:
        shared_state: Shared state object
    """
    config = shared_state.config
    thread_logger = shared_state.logger
    
    thread_logger.info("DLQ CSV thread started: path=%s", config.dlq_path)
    
    # CRITICAL: Initialize CSV writer - if this fails, trigger shutdown
    try:
        csv_context = _init_csv_writer(config.dlq_path, DLQ_CSV_FIELDS, thread_logger)
        thread_logger.info(
            "DLQ CSV initialized: file=%s",
            csv_context['path']
        )
    except Exception as e:
        thread_logger.critical(
            "Failed to initialize DLQ CSV: %s - TRIGGERING SHUTDOWN",
            e, exc_info=True
        )
        shared_state.stop()  # Trigger graceful shutdown
        return
    
    batch: List[Dict[str, Any]] = []
    last_flush = time.time()
    last_rotation_check = time.time()
    write_count = 0
    
    # Main loop
    try:
        while shared_state.running.is_set() or not shared_state.dlq_queue.empty():
            # Check for hourly rotation
            try:
                csv_context, should_clear = _check_rotation(
                    csv_context,
                    config.dlq_path,
                    DLQ_CSV_FIELDS,
                    last_rotation_check,
                    batch,
                    config.csv_rotation_check_interval_seconds,
                    thread_logger
                )
                
                if should_clear:
                    batch = []
                
                last_rotation_check = time.time()
            
            except Exception as e:
                thread_logger.critical(
                    "DLQ CSV rotation failed: %s - TRIGGERING SHUTDOWN",
                    e, exc_info=True
                )
                shared_state.stop()  # Trigger graceful shutdown
                break
            
            # Get DLQ entry from queue
            try:
                dlq_entry = shared_state.dlq_queue.get(
                    timeout=config.queue_get_timeout_seconds
                )
                
                # Format for CSV
                row = _format_dlq_row(dlq_entry)
                batch.append(row)
                
                thread_logger.debug(
                    "DLQ CSV batched: offset=%d, error=%s, batch_size=%d",
                    row["offset"], row["error_type"], len(batch)
                )
            
            except queue.Empty:
                pass
            
            # Check if we should flush
            should_flush = (
                len(batch) >= config.dlq_csv_batch_size or
                (time.time() - last_flush) >= config.csv_flush_interval_seconds
            )
            
            if should_flush and batch:
                try:
                    _write_batch(csv_context, batch, thread_logger)
                    write_count += len(batch)
                    thread_logger.info(
                        "DLQ CSV flushed: %d failures (total: %d)",
                        len(batch), write_count
                    )
                    batch = []
                    last_flush = time.time()
                
                except Exception as e:
                    thread_logger.critical(
                        "DLQ CSV write failed: %s - TRIGGERING SHUTDOWN",
                        e, exc_info=True
                    )
                    shared_state.stop()  # Trigger graceful shutdown
                    break
    
    except Exception as e:
        thread_logger.critical(
            "DLQ CSV loop crashed: %s - TRIGGERING SHUTDOWN",
            e, exc_info=True
        )
        shared_state.stop()  # Trigger graceful shutdown
    
    # Final flush
    if batch:
        try:
            _write_batch(csv_context, batch, thread_logger)
            write_count += len(batch)
            thread_logger.info("Final DLQ CSV flush: %d failures", len(batch))
        except Exception as e:
            thread_logger.error("Final DLQ flush failed: %s", e)
    
    # Close CSV file
    try:
        csv_context['file'].close()
        thread_logger.info("DLQ CSV closed: %d failures written", write_count)
    except Exception as e:
        thread_logger.error("Error closing DLQ CSV: %s", e)


def _check_rotation(
    csv_context: Dict[str, Any],
    base_path: str,
    fieldnames: List[str],
    last_check: float,
    batch: List[Dict[str, Any]],
    check_interval: int,
    thread_logger
) -> Tuple[Dict[str, Any], bool]:
    """Check if hourly CSV rotation is needed.
    
    Returns (new_context, should_clear_batch).
    If rotation happens, flushes batch first, then rotates.
    Caller is responsible for clearing batch.
    
    CRITICAL: Raises exception if rotation fails (disk full, permissions, etc.)
    This will trigger graceful shutdown in the calling loop.
    
    Args:
        csv_context: Current CSV writer context
        base_path: Base path for CSV files
        fieldnames: CSV field names
        last_check: Last rotation check timestamp
        batch: Current batch (will be flushed if rotation happens)
        check_interval: How often to check for rotation (seconds)
        thread_logger: Logger instance
        
    Returns:
        Tuple of (new_csv_context, should_clear_batch)
        
    Raises:
        Exception: If CSV rotation fails
    """
    # Check if it's time to check
    if time.time() - last_check < check_interval:
        return csv_context, False
    
    # Calculate new path
    new_path = _get_hourly_csv_path(base_path)
    
    # Check if we're already using the correct file
    if new_path == csv_context['path']:
        return csv_context, False
    
    # Hour has changed - rotate
    thread_logger.info(
        "Rotating CSV: old=%s, new=%s",
        csv_context['path'], new_path
    )
    
    # Flush pending batch (but don't clear - caller does that)
    if batch:
        thread_logger.debug("Flushing %d entries before rotation", len(batch))
        _write_batch(csv_context, batch, thread_logger)
    
    # Close old file
    csv_context['file'].close()
    thread_logger.debug("Closed old CSV file: %s", csv_context['path'])
    
    # Open new file - CRITICAL: This can raise exception
    new_context = _init_csv_writer(base_path, fieldnames, thread_logger)
    thread_logger.info("Rotated to new CSV file: %s", new_context['path'])
    
    return new_context, True


def _init_csv_writer(base_path: str, fieldnames: List[str], thread_logger) -> Dict[str, Any]:
    """Initialize CSV writer for current hour.
    
    CRITICAL: This function can raise exceptions (disk full, permissions, etc.)
    Caller MUST handle exceptions to trigger shutdown.
    
    Args:
        base_path: Base path for CSV files
        fieldnames: CSV field names
        thread_logger: Logger instance
        
    Returns:
        Dict with 'path', 'file', and 'writer'
        
    Raises:
        IOError: If file cannot be opened
        OSError: If disk full or permissions issue
    """
    current_path = _get_hourly_csv_path(base_path)
    
    thread_logger.debug("Initializing CSV writer: %s", current_path)
    
    # Open file - CRITICAL: Can raise exception
    csv_file = open(current_path, 'a', newline='', buffering=1)
    csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    
    # Write header if new file
    if csv_file.tell() == 0:
        csv_writer.writeheader()
        thread_logger.debug("Wrote CSV header to new file")
    
    return {
        'path': current_path,
        'file': csv_file,
        'writer': csv_writer
    }


def _get_hourly_csv_path(base_path: str) -> str:
    """Generate CSV path with hourly timestamp.
    
    Format: base_name.YYYYMMDD_HH.csv
    Example: kafka_backup.20250102_14.csv
    
    Args:
        base_path: Base path (e.g., "kafka_backup.csv")
        
    Returns:
        Path with hourly timestamp
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H")
    base = base_path.replace('.csv', '')
    return f"{base}.{timestamp}.csv"


def _write_batch(csv_context: Dict[str, Any], batch: List[Dict[str, Any]], thread_logger) -> None:
    """Write batch of rows to CSV.
    
    CRITICAL: This function can raise exceptions (disk full, I/O errors)
    Caller MUST handle exceptions to trigger shutdown.
    
    Args:
        csv_context: CSV writer context
        batch: List of row dicts to write
        thread_logger: Logger instance
        
    Raises:
        IOError: If write fails
    """
    writer = csv_context['writer']
    csv_file = csv_context['file']
    
    # Write all rows
    for row in batch:
        writer.writerow(row)
    
    # Flush to disk
    csv_file.flush()
    
    thread_logger.debug("Wrote %d rows to %s", len(batch), csv_context['path'])


def _format_dlq_row(dlq_entry: Dict[str, Any]) -> Dict[str, Any]:
    """Format DLQ entry for CSV row.
    
    Extracts Kafka message details and error information.
    
    Args:
        dlq_entry: DLQ entry with 'msg' and error details
        
    Returns:
        Dict matching DLQ_CSV_FIELDS
    """
    msg = dlq_entry["msg"]
    
    return {
        'timestamp': datetime.utcnow().isoformat(),
        'topic': msg.topic,
        'partition': msg.partition,
        'offset': msg.offset,
        # CRITICAL: UTF-8 decode with errors='replace' to avoid crashes
        'key': msg.key.decode('utf-8', errors='replace') if msg.key else None,
        'value': msg.value.decode('utf-8', errors='replace') if msg.value else None,
        'error_type': dlq_entry["error"],
        'error_message': dlq_entry["error_message"],
        'stack_trace': dlq_entry.get("stack_trace", ""),
        'processing_time_ms': dlq_entry["processing_time_ms"],
        'retry_count': 0  # Future: Could track retries
    }
