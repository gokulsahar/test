"""Main Kafka consumer orchestrator.

Coordinates all threads and handles graceful shutdown.
Entry point is the main() function.
"""

import time
import threading
from typing import Dict, Any
import logging
from kafka import KafkaConsumer

from .shared_state import KafkaConsumerConfig, SharedState, validate_config
from .signal_handler import SignalHandler

logger = logging.getLogger(__name__)


def main(config_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Main entry point for Kafka consumer.
    
    Args:
        config_dict: Configuration dictionary (will be unpacked into KafkaConsumerConfig)
        
    Returns:
        Dict with:
            - status: "success" or "error"
            - metrics: Shutdown metrics (if successful)
            - error: Error message (if failed)
    """
    # Parse and validate config
    try:
        config = KafkaConsumerConfig(**config_dict)
        validate_config(config)
    except Exception as e:
        logger.critical("Configuration validation failed: %s", e)
        return {"status": "error", "error": f"Config validation failed: {e}"}
    
    # Create shared state
    shared_state = SharedState(config)
    logger = shared_state.logger  # Use configured logger
    
    logger.info(
        "Starting Kafka consumer: topic=%s, group=%s, workers=%d, bootstrap=%s",
        config.topic, config.group_id, config.worker_count, config.bootstrap_servers
    )
    
    if config.dev_mode:
        logger.warning("DEV MODE ENABLED - Offsets will NOT be committed to Kafka")
    
    # Create Kafka consumer
    try:
        shared_state.kafka_consumer = KafkaConsumer(
            config.topic,
            bootstrap_servers=config.bootstrap_servers,
            group_id=config.group_id,
            enable_auto_commit=False,  # Manual commit only
            auto_offset_reset=config.auto_offset_reset,
            max_poll_records=config.max_poll_records,
            consumer_timeout_ms=config.poll_timeout_ms
        )
        logger.info("Kafka consumer initialized successfully")
    except Exception as e:
        logger.critical("Failed to create Kafka consumer: %s", e, exc_info=True)
        return {"status": "error", "error": f"Kafka init failed: {e}"}
    
    # Register signal handlers
    signal_handler = SignalHandler(callback=shared_state.stop)
    signal_handler.register()
    logger.debug("Signal handlers registered")
    
    try:
        # Start all threads - will raise exception if CSV init fails
        start_all_threads(shared_state)
        
        logger.info("All threads started, consumer is now running")
        logger.info("Waiting for shutdown signal (SIGTERM/SIGINT)...")
        
        # Block until signal received
        signal_handler.wait()
        
        logger.info("Shutdown signal received, initiating graceful shutdown")
        metrics = perform_graceful_shutdown(shared_state)
        
        logger.info("Graceful shutdown complete: %s", metrics)
        return {"status": "success", "metrics": metrics}
    
    except Exception as e:
        logger.critical("Fatal error in main loop: %s", e, exc_info=True)
        shared_state.stop()
        emergency_shutdown(shared_state)
        return {"status": "error", "error": str(e)}
    
    finally:
        signal_handler.restore()
        logger.info("Signal handlers restored")


def start_all_threads(shared_state: SharedState) -> None:
    """Start all consumer threads.
    
    ALL threads are non-daemon (daemon=False) to ensure they complete their work.
    
    Thread startup order:
    1. Polling thread (reads from Kafka)
    2. Worker threads (process messages)
    3. Commit thread (commits offsets)
    4. CSV writer threads (backup and DLQ)
    
    CRITICAL: CSV initialization failures will raise exception and prevent startup.
    This is intentional - if CSV cannot be created, we should not start processing.
    
    Args:
        shared_state: Shared state object
        
    Raises:
        Exception: If any thread fails to start (especially CSV writers)
    """
    config = shared_state.config
    logger = shared_state.logger
    
    # Import thread loop functions
    from .polling_thread import polling_loop
    from .worker_pool import worker_loop
    from .offset_manager import commit_loop
    from .csv_writers import backup_csv_loop, dlq_csv_loop
    
    # Start polling thread
    shared_state.threads["polling"] = threading.Thread(
        target=polling_loop,
        args=(shared_state,),
        name="polling-thread",
        daemon=False  # Must complete its work
    )
    shared_state.threads["polling"].start()
    logger.debug("Polling thread started")
    
    # Start worker threads
    for i in range(config.worker_count):
        worker_thread = threading.Thread(
            target=worker_loop,
            args=(shared_state, i),
            name=f"worker-{i}",
            daemon=False  # CRITICAL: Must complete current message
        )
        worker_thread.start()
        shared_state.threads["workers"].append(worker_thread)
    logger.debug("Started %d worker threads", config.worker_count)
    
    # Start commit thread
    shared_state.threads["commit"] = threading.Thread(
        target=commit_loop,
        args=(shared_state,),
        name="commit-thread",
        daemon=False  # Must complete final commit
    )
    shared_state.threads["commit"].start()
    logger.debug("Commit thread started")
    
    # Start CSV writer threads
    # CRITICAL: If CSV initialization fails, these threads will call shared_state.stop()
    # and trigger graceful shutdown
    if config.backup_enabled:
        shared_state.threads["backup_csv"] = threading.Thread(
            target=backup_csv_loop,
            args=(shared_state,),
            name="backup-csv-thread",
            daemon=False  # Must flush pending writes
        )
        shared_state.threads["backup_csv"].start()
        logger.debug("Backup CSV thread started")
    
    shared_state.threads["dlq_csv"] = threading.Thread(
        target=dlq_csv_loop,
        args=(shared_state,),
        name="dlq-csv-thread",
        daemon=False  # Must flush pending writes
    )
    shared_state.threads["dlq_csv"].start()
    logger.debug("DLQ CSV thread started")
    
    # Calculate total threads
    total_threads = (
        1  # polling
        + config.worker_count  # workers
        + 1  # commit
        + (1 if config.backup_enabled else 0)  # backup CSV
        + 1  # DLQ CSV
    )
    logger.info("Successfully started %d threads", total_threads)


def perform_graceful_shutdown(shared_state: SharedState) -> Dict[str, Any]:
    """Perform graceful shutdown with shared timeout.
    
    CRITICAL: Worker timeout is SHARED across all workers, not per-worker!
    For example, if shutdown_timeout=30s and we have 50 workers, the TOTAL
    time to wait for all workers is 30s, NOT 50*30s = 25 minutes.
    
    Shutdown sequence:
    1. Wait for polling thread (5s timeout)
    2. Wait for all workers (shared config timeout)
    3. Wait for commit thread - final commit (5s timeout)
    4. Wait for CSV writers (5s each)
    5. Close Kafka consumer
    
    Args:
        shared_state: Shared state object
        
    Returns:
        Dict with shutdown metrics:
            - messages_in_queue: Messages remaining in processing queue
            - clean_shutdown: True if all threads stopped cleanly
            - duration_ms: Total shutdown duration in milliseconds
    """
    shutdown_start = time.time()
    config = shared_state.config
    logger = shared_state.logger
    timeout = config.shutdown_timeout_seconds
    
    metrics = {
        "messages_in_queue": shared_state.processing_queue.qsize(),
        "clean_shutdown": True,
        "duration_ms": 0
    }
    
    logger.info("Starting graceful shutdown sequence")
    
    # 1. Wait for polling thread
    if shared_state.threads["polling"]:
        logger.debug("Waiting for polling thread to stop...")
        shared_state.threads["polling"].join(timeout=5)
        if shared_state.threads["polling"].is_alive():
            logger.warning("Polling thread did not stop within 5s timeout")
        else:
            logger.debug("Polling thread stopped")
    
    # 2. Wait for workers - SHARED TIMEOUT
    queue_size = shared_state.processing_queue.qsize()
    if queue_size > 0:
        logger.info("Processing queue has %d messages, waiting for workers to drain", queue_size)
    
    worker_count = len(shared_state.threads["workers"])
    logger.info(
        "Waiting for %d workers to finish (shared timeout=%ds)", 
        worker_count, timeout
    )
    
    worker_wait_start = time.time()
    
    for i, worker_thread in enumerate(shared_state.threads["workers"]):
        # Calculate remaining timeout
        remaining_timeout = timeout - (time.time() - worker_wait_start)
        
        if remaining_timeout <= 0:
            alive_count = sum(1 for w in shared_state.threads["workers"] if w.is_alive())
            logger.warning(
                "Worker timeout exceeded after %ds, %d workers still running",
                timeout, alive_count
            )
            metrics["clean_shutdown"] = False
            break
        
        # join() returns immediately if thread already finished
        worker_thread.join(timeout=remaining_timeout)
        
        # Log progress every 10 workers
        if (i + 1) % 10 == 0:
            alive = sum(1 for w in shared_state.threads["workers"][:i+1] if w.is_alive())
            logger.debug("Worker progress: %d/%d checked, %d still alive", i + 1, worker_count, alive)
    
    # Check final worker status
    alive_workers = sum(1 for w in shared_state.threads["workers"] if w.is_alive())
    elapsed_worker_wait = time.time() - worker_wait_start
    
    if alive_workers == 0:
        logger.info("All %d workers stopped cleanly in %.1fs", worker_count, elapsed_worker_wait)
    else:
        logger.warning(
            "%d/%d workers still running after %.1fs",
            alive_workers, worker_count, elapsed_worker_wait
        )
        metrics["clean_shutdown"] = False
    
    # 3. Wait for commit thread (final commit)
    if shared_state.threads["commit"]:
        logger.debug("Waiting for commit thread (final offset commit)...")
        shared_state.threads["commit"].join(timeout=5)
        if shared_state.threads["commit"].is_alive():
            logger.warning("Commit thread did not stop within 5s timeout")
        else:
            logger.debug("Commit thread stopped (final commit complete)")
    
    # 4. Wait for CSV writers
    if shared_state.threads["backup_csv"]:
        logger.debug("Waiting for backup CSV thread...")
        shared_state.threads["backup_csv"].join(timeout=5)
        if shared_state.threads["backup_csv"].is_alive():
            logger.warning("Backup CSV thread did not stop within 5s timeout")
        else:
            logger.debug("Backup CSV thread stopped")
    
    if shared_state.threads["dlq_csv"]:
        logger.debug("Waiting for DLQ CSV thread...")
        shared_state.threads["dlq_csv"].join(timeout=5)
        if shared_state.threads["dlq_csv"].is_alive():
            logger.warning("DLQ CSV thread did not stop within 5s timeout")
        else:
            logger.debug("DLQ CSV thread stopped")
    
    # 5. Close Kafka consumer
    try:
        if shared_state.kafka_consumer:
            logger.debug("Closing Kafka consumer...")
            shared_state.kafka_consumer.close()
            logger.info("Kafka consumer closed successfully")
    except Exception as e:
        logger.error("Error closing Kafka consumer: %s", e, exc_info=True)
    
    # Calculate total shutdown duration
    metrics["duration_ms"] = (time.time() - shutdown_start) * 1000
    
    logger.info(
        "Graceful shutdown complete: clean=%s, duration=%.0fms",
        metrics["clean_shutdown"], metrics["duration_ms"]
    )
    
    return metrics


def emergency_shutdown(shared_state: SharedState) -> None:
    """Emergency shutdown - last resort.
    
    Called when graceful shutdown fails catastrophically.
    Forces Kafka consumer close and logs critical error.
    
    Args:
        shared_state: Shared state object
    """
    logger = shared_state.logger
    logger.critical("EMERGENCY SHUTDOWN - Forcing Kafka consumer close")
    
    try:
        if shared_state.kafka_consumer:
            shared_state.kafka_consumer.close()
            logger.info("Kafka consumer closed in emergency shutdown")
    except Exception as e:
        logger.error("Error in emergency Kafka close: %s", e, exc_info=True)
