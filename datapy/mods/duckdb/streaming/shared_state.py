"""Shared state and configuration for Kafka consumer.

This module contains:
- KafkaConsumerConfig: All configuration parameters
- SharedState: Thread-safe shared state (queues, events, threads)
- validate_config: Configuration validation
"""

from dataclasses import dataclass
from typing import Callable, Any, Optional, Dict, Tuple
import threading
import queue
import os
import logging


@dataclass
class KafkaConsumerConfig:
    """Configuration for Kafka consumer.
    
    All timeouts and intervals are configurable to avoid hardcoded values.
    """
    
    # Required parameters
    bootstrap_servers: str
    topic: str
    group_id: str
    processor_callable: Callable[[bytes], Any]
    
    # Kafka settings
    poll_timeout_ms: int = 1000
    max_poll_records: int = 100
    auto_offset_reset: str = "latest"
    
    # Processing settings
    worker_count: int = 50
    queue_size: int = 200
    max_message_size: int = 10_485_760  # 10MB
    processing_timeout: Optional[int] = None  # None = no timeout
    
    # Commit settings
    commit_interval_seconds: int = 5
    dev_mode: bool = False  # If True, no commits (testing)
    
    # CSV settings
    backup_enabled: bool = True
    backup_path: str = "kafka_backup.csv"
    dlq_path: str = "kafka_dlq.csv"
    csv_flush_interval_seconds: int = 5
    csv_batch_size: int = 1000
    dlq_csv_batch_size: int = 100
    csv_rotation_check_interval_seconds: int = 60
    
    # Internal timeouts (configurable, not hardcoded)
    queue_get_timeout_seconds: float = 0.1
    processing_queue_put_timeout_seconds: int = 60
    worker_queue_get_timeout_seconds: int = 1
    
    # Shutdown settings
    shutdown_timeout_seconds: int = 30
    stop_at_offset: Optional[Dict[Tuple[str, int], int]] = None
    
    # Logging settings
    log_level: str = "INFO"
    mod_name: str = "kafka_consumer"


class SharedState:
    """Thread-safe shared state for Kafka consumer.
    
    Contains:
    - Configuration
    - Thread-safe queues for message passing
    - Threading events for coordination
    - References to all threads
    - Logger instance
    """
    
    def __init__(self, config: KafkaConsumerConfig):
        """Initialize shared state.
        
        Args:
            config: Consumer configuration
        """
        self.config = config
        
        # Threading events
        self.running = threading.Event()
        self.running.set()  # Initially running
        self.shutdown_event = threading.Event()
        
        # Thread-safe queues
        self.processing_queue: queue.Queue = queue.Queue(maxsize=config.queue_size)
        self.processed_queue: queue.Queue = queue.Queue()  # Unbounded
        self.backup_csv_queue: Optional[queue.Queue] = (
            queue.Queue() if config.backup_enabled else None
        )
        self.dlq_queue: queue.Queue = queue.Queue()  # Unbounded
        
        # Kafka consumer (initialized later)
        self.kafka_consumer = None
        
        # Thread references
        self.threads: Dict[str, Any] = {
            "polling": None,
            "workers": [],
            "commit": None,
            "backup_csv": None,
            "dlq_csv": None
        }
        
        # Setup logger
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger with configured level.
        
        Returns:
            Configured logger instance
        """
        logger = logging.getLogger(self.config.mod_name)
        logger.setLevel(self.config.log_level)
        
        # Add handler only if not already present
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def stop(self) -> None:
        """Signal all threads to stop gracefully."""
        self.running.clear()
        self.shutdown_event.set()


def validate_config(config: KafkaConsumerConfig) -> None:
    """Validate configuration parameters.
    
    Args:
        config: Configuration to validate
        
    Raises:
        AssertionError: If validation fails with descriptive message
    """
    # Type checks
    assert isinstance(config.bootstrap_servers, str), "bootstrap_servers must be string"
    assert isinstance(config.topic, str), "topic must be string"
    assert isinstance(config.group_id, str), "group_id must be string"
    assert callable(config.processor_callable), "processor_callable must be callable"
    
    # Range checks
    assert 1 <= config.worker_count <= 1000, "worker_count must be 1-1000"
    assert config.queue_size >= 10, "queue_size must be >= 10"
    assert config.commit_interval_seconds >= 1, "commit_interval_seconds must be >= 1"
    assert config.max_message_size > 0, "max_message_size must be > 0"
    assert config.shutdown_timeout_seconds > 0, "shutdown_timeout_seconds must be > 0"
    
    # Timeout checks
    assert config.poll_timeout_ms > 0, "poll_timeout_ms must be > 0"
    assert config.queue_get_timeout_seconds > 0, "queue_get_timeout_seconds must be > 0"
    assert config.processing_queue_put_timeout_seconds > 0, "processing_queue_put_timeout_seconds must be > 0"
    assert config.worker_queue_get_timeout_seconds > 0, "worker_queue_get_timeout_seconds must be > 0"
    
    # CSV settings
    assert config.csv_flush_interval_seconds > 0, "csv_flush_interval_seconds must be > 0"
    assert config.csv_batch_size > 0, "csv_batch_size must be > 0"
    assert config.dlq_csv_batch_size > 0, "dlq_csv_batch_size must be > 0"
    assert config.csv_rotation_check_interval_seconds > 0, "csv_rotation_check_interval_seconds must be > 0"
    
    # Stop at offset validation
    if config.stop_at_offset:
        assert isinstance(config.stop_at_offset, dict), "stop_at_offset must be dict"
        for key, offset in config.stop_at_offset.items():
            assert isinstance(key, tuple) and len(key) == 2, (
                f"Key {key} must be tuple (topic, partition)"
            )
            assert isinstance(offset, int) and offset >= 0, (
                f"Offset {offset} must be non-negative integer"
            )
    
    # File path validation - CRITICAL for CSV reliability
    if config.backup_enabled:
        backup_dir = os.path.dirname(config.backup_path) or "."
        assert os.path.exists(backup_dir), f"Backup directory does not exist: {backup_dir}"
        assert os.access(backup_dir, os.W_OK), f"Backup directory not writable: {backup_dir}"
    
    dlq_dir = os.path.dirname(config.dlq_path) or "."
    assert os.path.exists(dlq_dir), f"DLQ directory does not exist: {dlq_dir}"
    assert os.access(dlq_dir, os.W_OK), f"DLQ directory not writable: {dlq_dir}"
    
    # Log level validation
    valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    assert config.log_level.upper() in valid_log_levels, (
        f"log_level must be one of {valid_log_levels}"
    )
