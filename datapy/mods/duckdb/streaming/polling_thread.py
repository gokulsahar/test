"""Kafka polling thread.

Continuously polls messages from Kafka and distributes them to:
1. Processing queue (for workers)
2. Backup CSV queue (if enabled)

Handles:
- Backpressure (blocking on full processing queue)
- UTF-8 decode errors (errors='replace')
- Stop at offset feature
"""

import time
import queue
import logging
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .shared_state import SharedState

logger = logging.getLogger(__name__)


def polling_loop(shared_state: 'SharedState') -> None:
    """Main polling loop - runs in dedicated thread.
    
    Polls messages from Kafka and enqueues them for processing.
    Stops when shared_state.running is cleared.
    
    Args:
        shared_state: Shared state object
    """
    config = shared_state.config
    consumer = shared_state.kafka_consumer
    thread_logger = shared_state.logger
    
    thread_logger.info("Polling thread started")
    poll_count = 0
    message_count = 0
    
    while shared_state.running.is_set():
        try:
            # Poll Kafka for messages
            message_batch = consumer.poll(
                timeout_ms=config.poll_timeout_ms,
                max_records=config.max_poll_records
            )
            
            if not message_batch:
                # No messages - continue polling
                continue
            
            poll_count += 1
            
            # Flatten messages from all partitions
            messages = []
            for partition_messages in message_batch.values():
                messages.extend(partition_messages)
            
            message_count += len(messages)
            thread_logger.debug(
                "Poll #%d: Received %d messages (total: %d)",
                poll_count, len(messages), message_count
            )
            
            # Process each message
            for msg in messages:
                # Check stop_at_offset (if configured)
                if config.stop_at_offset:
                    partition_key = (msg.topic, msg.partition)
                    target_offset = config.stop_at_offset.get(partition_key)
                    
                    if target_offset is not None and msg.offset >= target_offset:
                        thread_logger.info(
                            "Reached stop_at_offset: partition=%s, offset=%d (target=%d)",
                            partition_key, msg.offset, target_offset
                        )
                        shared_state.stop()
                        return
                
                # Send to backup CSV (non-blocking)
                if config.backup_enabled and shared_state.backup_csv_queue:
                    try:
                        entry = {
                            "timestamp": datetime.utcnow().isoformat(),
                            "topic": msg.topic,
                            "partition": msg.partition,
                            "offset": msg.offset,
                            # CRITICAL: UTF-8 with errors='replace' to avoid crashes
                            "key": msg.key.decode('utf-8', errors='replace') if msg.key else None,
                            "value": msg.value.decode('utf-8', errors='replace') if msg.value else None,
                            "message_size": len(msg.value) if msg.value else 0
                        }
                        shared_state.backup_csv_queue.put_nowait(entry)
                        thread_logger.debug(
                            "Backup CSV queued: offset=%d, size=%d",
                            msg.offset, entry["message_size"]
                        )
                    except queue.Full:
                        thread_logger.warning(
                            "Backup CSV queue full, skipping backup for offset %d",
                            msg.offset
                        )
                
                # Send to processing queue (blocking - backpressure)
                try:
                    thread_logger.debug(
                        "Enqueuing message: partition=%d, offset=%d",
                        msg.partition, msg.offset
                    )
                    
                    shared_state.processing_queue.put(
                        msg,
                        timeout=config.processing_queue_put_timeout_seconds
                    )
                    
                    thread_logger.debug(
                        "Message enqueued: offset=%d, queue_size=%d",
                        msg.offset, shared_state.processing_queue.qsize()
                    )
                
                except queue.Full:
                    # This is critical - we couldn't enqueue after timeout
                    thread_logger.error(
                        "Processing queue full after %ds timeout, DROPPING message: "
                        "partition=%d, offset=%d (THIS IS DATA LOSS)",
                        config.processing_queue_put_timeout_seconds,
                        msg.partition, msg.offset
                    )
                    # Note: In production, you might want to trigger shutdown here
                    # rather than silently dropping messages
        
        except Exception as e:
            thread_logger.error(
                "Error in polling loop: %s",
                e, exc_info=True
            )
            # Sleep briefly to avoid tight error loop
            time.sleep(1)
    
    thread_logger.info(
        "Polling thread stopping: polled %d batches, %d messages total",
        poll_count, message_count
    )
