"""Offset manager for tracking and committing processed messages.

Implements max contiguous offset algorithm to ensure:
- Only commit offsets for messages that have been processed
- Don't skip offsets (maintain at-least-once guarantee)
- Handle out-of-order processing

Example:
    Processed: {1001, 1002, 1003, 1005, 1006}
    Last committed: 1000
    Max contiguous: 1003 (can't commit 1005 because 1004 is missing)
"""

import time
import logging
from typing import Set, Dict, TYPE_CHECKING
from kafka import TopicPartition, OffsetAndMetadata

if TYPE_CHECKING:
    from .shared_state import SharedState

logger = logging.getLogger(__name__)


def commit_loop(shared_state: 'SharedState') -> None:
    """Main commit loop - runs in dedicated thread.
    
    Periodically:
    1. Drains processed queue
    2. Calculates max contiguous offsets per partition
    3. Commits offsets to Kafka (unless dev_mode)
    
    Args:
        shared_state: Shared state object
    """
    config = shared_state.config
    consumer = shared_state.kafka_consumer
    thread_logger = shared_state.logger
    
    thread_logger.info("Commit thread started (interval=%ds)", config.commit_interval_seconds)
    
    if config.dev_mode:
        thread_logger.warning("Dev mode enabled - no commits will be made")
    
    # Tracking state
    processed_offsets: Dict[int, Set[int]] = {}  # {partition: set(offsets)}
    last_committed: Dict[int, int] = {}  # {partition: last_committed_offset}
    
    commit_count = 0
    
    while shared_state.running.is_set():
        # Drain processed queue
        drained_count = _drain_processed_queue(
            shared_state,
            processed_offsets,
            config.commit_interval_seconds
        )
        
        thread_logger.debug(
            "Drained %d entries from processed queue, tracking %d partitions",
            drained_count, len(processed_offsets)
        )
        
        if not processed_offsets:
            # Nothing to commit
            continue
        
        # Dev mode - just log what would be committed
        if config.dev_mode:
            max_offsets = {p: max(offsets) for p, offsets in processed_offsets.items() if offsets}
            thread_logger.info("Dev mode - would commit: %s", max_offsets)
            processed_offsets.clear()
            continue
        
        # Commit offsets
        _commit_offsets(config, consumer, processed_offsets, last_committed, thread_logger)
        commit_count += 1
    
    # Final commit before shutdown
    if not config.dev_mode and processed_offsets:
        thread_logger.info("Performing final commit before shutdown")
        _commit_offsets(config, consumer, processed_offsets, last_committed, thread_logger)
        commit_count += 1
    
    thread_logger.info("Commit thread stopping: %d commits made", commit_count)


def _drain_processed_queue(
    shared_state: 'SharedState',
    processed_offsets: Dict[int, Set[int]],
    commit_interval: int
) -> int:
    """Drain processed queue and update processed_offsets.
    
    Sleeps in small intervals to allow checking running flag.
    
    Args:
        shared_state: Shared state object
        processed_offsets: Dict to update with processed offsets
        commit_interval: Total time to drain (seconds)
        
    Returns:
        Number of entries drained
    """
    sleep_interval = 0.1  # 100ms
    iterations = int(commit_interval / sleep_interval)
    drained_count = 0
    
    for _ in range(iterations):
        if not shared_state.running.is_set():
            break
        
        # Drain all available entries (non-blocking)
        while True:
            try:
                partition, offset, status = shared_state.processed_queue.get_nowait()
                
                # Add to tracking
                if partition not in processed_offsets:
                    processed_offsets[partition] = set()
                processed_offsets[partition].add(offset)
                
                drained_count += 1
                
                shared_state.logger.debug(
                    "Drained: partition=%d, offset=%d, status=%s",
                    partition, offset, status
                )
            
            except Exception:  # queue.Empty
                break
        
        # Sleep briefly
        time.sleep(sleep_interval)
    
    return drained_count


def _find_max_contiguous(offsets: Set[int], last_committed: int) -> int:
    """Find maximum contiguous offset.
    
    Returns the highest offset that forms a contiguous sequence
    starting from last_committed + 1.
    
    Example:
        last_committed = 1000
        offsets = {1001, 1002, 1003, 1005, 1006}
        Result: 1003 (can't include 1005 because 1004 is missing)
    
    CRITICAL: last_committed starts at -1 for new partitions.
    This is correct because Kafka offsets start at 0, so -1 + 1 = 0.
    
    Args:
        offsets: Set of processed offsets
        last_committed: Last committed offset for this partition
        
    Returns:
        Maximum contiguous offset (or last_committed if no progress)
    """
    if not offsets:
        return last_committed
    
    sorted_offsets = sorted(offsets)
    max_contiguous = last_committed
    
    for offset in sorted_offsets:
        if offset == max_contiguous + 1:
            max_contiguous = offset
        else:
            # Gap found - stop here
            break
    
    return max_contiguous


def _commit_offsets(
    config,
    consumer,
    processed_offsets: Dict[int, Set[int]],
    last_committed: Dict[int, int],
    thread_logger
) -> None:
    """Calculate and commit offsets to Kafka.
    
    Uses max contiguous algorithm to determine safe commit points.
    
    Args:
        config: Configuration object
        consumer: Kafka consumer
        processed_offsets: Processed offsets per partition
        last_committed: Last committed offsets per partition
        thread_logger: Logger instance
    """
    offsets_to_commit: Dict[int, int] = {}
    
    # Calculate max contiguous for each partition
    for partition, offsets in processed_offsets.items():
        if not offsets:
            continue
        
        last_offset = last_committed.get(partition, -1)
        max_contiguous = _find_max_contiguous(offsets, last_offset)
        
        if max_contiguous > last_offset:
            # We have progress - prepare to commit
            # Kafka commits "next offset to read", so add 1
            offsets_to_commit[partition] = max_contiguous + 1
            
            thread_logger.debug(
                "Partition %d: last_committed=%d, max_contiguous=%d, will_commit=%d",
                partition, last_offset, max_contiguous, max_contiguous + 1
            )
    
    if not offsets_to_commit:
        thread_logger.debug("No new offsets to commit")
        return
    
    # Commit to Kafka
    try:
        # Build commit payload
        commit_payload = {}
        for partition, offset in offsets_to_commit.items():
            tp = TopicPartition(config.topic, partition)
            commit_payload[tp] = OffsetAndMetadata(offset, None)
        
        # Commit
        consumer.commit(offsets=commit_payload)
        
        thread_logger.info("Committed offsets: %s", offsets_to_commit)
        
        # Update tracking
        for partition, offset in offsets_to_commit.items():
            # Update last_committed (remember, we committed "next offset")
            last_committed[partition] = offset - 1
            
            # Remove committed offsets from processed set
            # Keep only offsets >= next uncommitted offset
            processed_offsets[partition] = {
                o for o in processed_offsets[partition] if o >= offset
            }
            
            thread_logger.debug(
                "Partition %d: updated last_committed=%d, remaining_processed=%d",
                partition, last_committed[partition], len(processed_offsets[partition])
            )
    
    except Exception as e:
        thread_logger.error(
            "Failed to commit offsets %s: %s",
            offsets_to_commit, e, exc_info=True
        )
        # Note: We don't clear processed_offsets on failure
        # They will be retried in next commit interval
