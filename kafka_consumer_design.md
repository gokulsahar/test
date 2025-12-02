# KAFKA CONSUMER - AI IMPLEMENTATION SPECIFICATION

## METADATA
- **Python Version:** 3.12+
- **Kafka Library:** `confluent-kafka==2.6.0` (Confluent Kafka Python client)
- **Framework:** DataPy (use framework's logger)
- **Delivery Guarantee:** At-least-once
- **Concurrency Model:** Multi-threaded
- **Thread Safety:** All threads non-daemon
- **Files:** 7 Python files (~1050 LOC total)

---

## CRITICAL REQUIREMENTS

### 1. THREAD SAFETY
- **ALL threads MUST be non-daemon** (daemon=False)
- Worker threads CANNOT be killed - they must complete in-flight messages
- Use Python `queue.Queue` (thread-safe by default)
- KafkaConsumer operations ONLY in polling thread
- No shared mutable state without proper synchronization

### 2. DATA INTEGRITY
- **Zero message loss** - at-least-once delivery guarantee
- Commit ONLY max contiguous offset per partition
- If worker crashes mid-processing, message goes to DLQ with error details
- All in-flight messages MUST complete before shutdown
- CSV write failures trigger graceful shutdown (don't limp along)

### 3. LOGGING
- Use DataPy framework logger (import from framework)
- **INFO level:** Production (commits, errors, shutdown events)
- **DEBUG level:** Development only (message details, queue sizes)
- Log format: Include partition, offset, timing, error details

### 4. ERROR HANDLING
- Processing exceptions → DLQ (Dead Letter Queue)
- Worker thread crashes → Capture in-flight message to DLQ
- CSV write failures → Trigger graceful shutdown
- Kafka connection errors → Retry, log, continue
- No retries in worker - immediate DLQ on failure

### 5. SHUTDOWN BEHAVIOR
- **Option B Implementation:** Wait for ALL in-flight workers to complete (no hard timeout)
- Stop polling immediately on signal
- Drain processing queue completely
- Workers finish current messages (non-daemon prevents killing)
- Final commit after all workers done
- Close Kafka consumer last
- timeout parameter is for monitoring/logging only

---

## FILE STRUCTURE

```
kafka_consumer/
├── shared_state.py         # Config dataclass + SharedState + validation
├── signal_handler.py        # SIGTERM/SIGINT handler utility
├── kafka_consumer.py        # Main orchestrator + entry point
├── polling_thread.py        # Kafka polling + backpressure logic
├── worker_pool.py           # Message processing + exception handling
├── offset_manager.py        # Offset tracking + commit logic
└── csv_writers.py           # Backup CSV (hourly rotation) + DLQ CSV
```

---

## 1. shared_state.py

### Purpose
Configuration dataclass, shared state, and validation logic.

### Requirements
- **KafkaConsumerConfig dataclass** with all parameters (see Configuration Parameters section)
- **SharedState class** containing:
  - Config instance
  - threading.Event for running flag and shutdown_event
  - queue.Queue instances (processing_queue, processed_queue, backup_csv_queue, dlq_queue)
  - Thread references dict
  - Kafka consumer reference
  - Logger instance from DataPy framework
- **validate_config() function** that checks:
  - Required params present and correct types
  - Numeric ranges (worker_count 1-1000, queue_size >= 10, etc.)
  - File paths exist and are writable
  - stop_at_offset format if provided
- **Logger setup** using DataPy framework logger with configured log_level

### Key Points
- All queues thread-safe by default (stdlib Queue)
- backup_csv_queue is None if backup_enabled=False
- Logger initialized with mod_name for proper namespacing
- Type hints for Python 3.12 (use from `__future__ import annotations` if needed)

---

## 2. signal_handler.py

### Purpose
Reusable signal handling utility for SIGTERM/SIGINT.

### Requirements
- **SignalHandler class** with:
  - Constructor accepting callback function
  - `register()` method - registers signal handlers for SIGTERM, SIGINT
  - `wait()` method - blocks until signal received
  - `restore()` method - restores original handlers
  - Internal threading.Event for shutdown coordination
- Thread-safe callback execution
- Logging of received signals
- Store original handlers for restoration

### Key Points
- Can be used in any Python service (not Kafka-specific)
- Callback should be shared_state.stop()
- Handles signal errors gracefully

---

## 3. kafka_consumer.py

### Purpose
Main orchestrator - entry point, thread management, shutdown coordination.

### Requirements

#### main() function
- **Input:** config_dict (Dict[str, Any])
- **Output:** Dict with status and metrics
- **Logic:**
  1. Create KafkaConsumerConfig from config_dict
  2. Validate config
  3. Create SharedState
  4. Initialize Confluent Kafka Consumer (from confluent_kafka import Consumer)
  5. Register signal handler (callback=shared_state.stop)
  6. Start all threads via start_all_threads()
  7. Wait for shutdown signal
  8. Perform graceful shutdown
  9. Return metrics dict

#### start_all_threads() function
- Start in order:
  1. Polling thread (daemon=False)
  2. Worker threads (worker_count times, daemon=False)
  3. Commit thread (daemon=False)
  4. Backup CSV thread if enabled (daemon=False)
  5. DLQ CSV thread (daemon=False)
- Log each thread startup at DEBUG level
- If CSV init fails, exception propagates → caught in main() → emergency shutdown

#### perform_graceful_shutdown() function
- **CRITICAL:** Wait for ALL in-flight workers to complete (no hard timeout)
- **Steps:**
  1. Log shutdown initiation with queue depth
  2. Wait for polling thread (5s timeout for logging only)
  3. Log queue draining message if queue has messages
  4. Wait for ALL worker threads to complete (join with no timeout, but track elapsed time)
  5. Log progress every 10 workers
  6. Wait for commit thread final commit (5s timeout)
  7. Wait for CSV writer threads (5s timeouts)
  8. Close Kafka consumer
  9. Return metrics dict with duration, clean_shutdown flag
- Calculate metrics: messages_in_queue, duration_ms, clean_shutdown

#### emergency_shutdown() function
- Called only on fatal exceptions
- Force close Kafka consumer
- Log CRITICAL error

### Key Points
- Use Confluent Kafka Consumer, NOT kafka-python
- All threads non-daemon - cannot be killed
- CSV init failures prevent startup (fail fast)
- timeout_seconds parameter used only for logging/metrics, NOT for force-killing workers
- Log with DataPy framework logger at INFO level (DEBUG for thread details)

---

## 4. polling_thread.py

### Purpose
Poll messages from Kafka, handle backpressure, feed queues.

### Requirements

#### polling_loop() function
- **Input:** shared_state
- **Logic:**
  - While shared_state.running is set:
    1. Poll batch from Kafka (consumer.poll(timeout=poll_timeout_ms/1000))
    2. Handle poll errors (log, retry after brief pause)
    3. For each message in batch:
       - Check stop_at_offset condition (if configured)
       - Put in backup_csv_queue (non-blocking with put_nowait, skip if full)
       - Put in processing_queue (blocking with timeout, implement backpressure)
    4. If stop_at_offset reached, call shared_state.stop() and break
  - Exit cleanly when running cleared

#### Backpressure Implementation
- If processing_queue.put() raises queue.Full, log warning
- Then do blocking put() to apply backpressure (polling pauses until space)
- This self-regulates system based on processing speed

#### Stop at Offset Logic
- If config.stop_at_offset is set:
  - Check (msg.topic(), msg.partition()) against config
  - If current offset >= target offset, log INFO and trigger shutdown
  - Graceful shutdown ensures all queued messages processed

### Key Points
- Use Confluent Kafka consumer.poll() (returns list or None)
- Batch size controlled by max_poll_records config (set in consumer config)
- UTF-8 decode errors: handle msg.value() decoding errors, log and put in DLQ
- Log batch sizes at DEBUG level
- Never crash on Kafka errors - log and retry

---

## 5. worker_pool.py

### Purpose
Process messages via user's processor_callable, handle failures, track offsets.

### Requirements

#### worker_loop() function
- **Input:** shared_state, worker_id
- **Logic:**
  - While shared_state.running or queue not empty:
    1. Get message from processing_queue (blocking with timeout)
    2. Check message size against max_message_size
    3. Wrap processing in try-except:
       - Call processor_callable(msg.value())
       - On success: put (partition, offset, "success") in processed_queue
       - On exception: put error details in dlq_queue, then put (partition, offset, "failed") in processed_queue
    4. Log processing result (INFO for errors, DEBUG for success)
  - Exit cleanly when running cleared AND queue empty

#### Worker Crash Handling
- **CRITICAL:** Wrap entire worker_loop in outer try-except
- If worker thread crashes (unexpected exception in worker itself):
  1. Capture the message being processed
  2. Log CRITICAL error with worker_id and traceback
  3. Put message + error to DLQ queue
  4. Mark as processed (to avoid blocking commits)
  5. Thread exits (non-daemon so already committed)

#### Message Size Check
- If len(msg.value()) > max_message_size:
  - Log WARNING
  - Put to DLQ with "MessageTooLargeError"
  - Mark as processed
  - Continue (don't process)

#### Processing Timeout (Optional)
- If processing_timeout configured:
  - Use threading.Timer or signal.alarm (Unix only)
  - If timeout expires, treat as exception → DLQ
  - Implementation detail left to coder

### Key Points
- NO retries in worker - fail immediately to DLQ
- processor_callable receives raw bytes (msg.value())
- Return value of callable ignored
- Log timing for each message (processing_time_ms)
- Use queue.Empty exception for timeout handling

---

## 6. offset_manager.py

### Purpose
Track processed offsets, calculate max contiguous, commit to Kafka.

### Requirements

#### commit_loop() function
- **Input:** shared_state
- **Logic:**
  - Initialize tracking: processed_offsets dict {partition: set([offsets])}, last_committed dict
  - While shared_state.running or processed_queue not empty:
    1. Drain processed_queue for commit_interval_seconds
    2. Add offsets to processed_offsets dict
    3. Calculate max contiguous offset per partition
    4. If dev_mode: log what would be committed, clear tracking, continue (skip actual commit)
    5. If NOT dev_mode: commit offsets to Kafka
    6. Handle commit failures (log ERROR, don't crash)
    7. Remove committed offsets from tracking
  - Exit cleanly

#### Max Contiguous Algorithm
- For each partition:
  - Sort processed offsets
  - Starting from last_committed + 1
  - Find maximum contiguous sequence
  - Stop at first gap
  - Return max contiguous offset
- Commit (max_contiguous + 1) to Kafka (Kafka semantics)

#### Dev Mode Behavior
- If config.dev_mode=True:
  - Skip all Kafka commits
  - Log "Dev mode - would have committed X offsets" at INFO
  - Clear processed_offsets (simulate commit)
  - On restart, all messages reprocessed from last external commit

#### Commit Failure Handling
- If consumer.commit() raises exception:
  - Log ERROR with exception details
  - Don't crash thread
  - Will retry next interval
  - Kafka will redeliver from last successful commit

### Key Points
- Use Confluent Kafka commit API: consumer.commit(offsets=[TopicPartition(...)])
- Commit interval default 5 seconds
- Only commit contiguous offsets (safe at-least-once)
- Gap in offsets = don't commit beyond gap (reprocess on restart)
- Log committed offsets at INFO level

---

## 7. csv_writers.py

### Purpose
Write backup CSV (with hourly rotation) and DLQ CSV.

### Requirements

#### backup_csv_loop() function
- **Input:** shared_state
- **CRITICAL:** Implement hourly rotation
- **CSV Schema:** timestamp, topic, partition, offset, key, value, message_size
- **Logic:**
  1. Initialize first CSV file with timestamp in filename (e.g., backup_2024_01_15_10.csv)
  2. Write header row
  3. Track current_hour
  4. While shared_state.running or backup_csv_queue not empty:
     - Check if hour changed (every csv_rotation_check_interval_seconds)
     - If hour changed:
       - Flush current file
       - Close current file
       - Create new file with new timestamp
       - Write header to new file
       - If file creation fails: Log CRITICAL, call shared_state.stop(), exit thread
     - Drain messages from backup_csv_queue into batch
     - Write batch when: batch size reached OR flush interval elapsed
     - If write fails: Log CRITICAL, call shared_state.stop(), exit thread
  5. Final flush on shutdown

#### dlq_csv_loop() function
- **Input:** shared_state
- **CSV Schema:** timestamp, topic, partition, offset, key, value, error_type, error_message, stack_trace, processing_time_ms, retry_count
- **Logic:**
  1. Open DLQ CSV file (append mode)
  2. Write header if new file
  3. While shared_state.running or dlq_queue not empty:
     - Drain messages from dlq_queue into batch
     - Write batch when: batch size reached OR flush interval elapsed
     - If write fails: Log CRITICAL, call shared_state.stop(), exit thread
  4. Final flush on shutdown

#### Hourly Rotation Implementation
- Filename format: `{backup_path_prefix}_YYYY_MM_DD_HH.csv`
- Check time every csv_rotation_check_interval_seconds (default 60s)
- On hour boundary:
  - Flush and close current file
  - Create new file with new hour in name
  - Log INFO "CSV rotated to {new_filename}"
- If rotation fails (disk full, permissions, etc.):
  - Log CRITICAL error
  - Call shared_state.stop() to trigger graceful shutdown
  - Exit thread

#### Batch Writing
- Accumulate rows in memory (list)
- Write when:
  - Batch size reached (csv_batch_size for backup, dlq_csv_batch_size for DLQ)
  - OR time interval elapsed (csv_flush_interval_seconds)
- Use csv.DictWriter
- Call file.flush() after writerows()

#### CSV Write Failure Handling
- **CRITICAL FOR 24/7 RELIABILITY:**
- If file.open() fails → Log CRITICAL, call shared_state.stop(), exit
- If csv_writer.writerows() fails → Log CRITICAL, call shared_state.stop(), exit
- If file.flush() fails → Log CRITICAL, call shared_state.stop(), exit
- This triggers graceful shutdown → K8s/supervisor restarts consumer

### Key Points
- Backup CSV has hourly rotation IN the consumer code
- DLQ CSV is single append-only file (no rotation)
- Use 'a' mode for append, buffering=1 for line buffering
- Handle special characters in CSV (escaping handled by csv module)
- Log file rotations at INFO level
- CSV failures trigger shutdown (fail cleanly, not silently)

---

## CONFIGURATION PARAMETERS

### Required Parameters
```python
bootstrap_servers: str          # Kafka broker addresses (e.g., "localhost:9092")
topic: str                       # Kafka topic to consume
group_id: str                    # Consumer group ID
processor_callable: Callable[[bytes], Any]  # Function to process messages
```

### Kafka Parameters
```python
poll_timeout_ms: int = 1000             # Kafka poll timeout (milliseconds)
max_poll_records: int = 100             # Max messages per poll
auto_offset_reset: str = "latest"       # "earliest" or "latest"
```

### Processing Parameters
```python
worker_count: int = 50                  # Number of worker threads
queue_size: int = 200                   # Processing queue max size
max_message_size: int = 10_485_760      # 10MB max message size
processing_timeout: Optional[int] = None  # Timeout per message (seconds, None=disabled)
```

### Commit Parameters
```python
commit_interval_seconds: int = 5        # Commit frequency
dev_mode: bool = False                  # Disable commits for testing
```

### CSV Parameters
```python
backup_enabled: bool = True                     # Enable backup CSV
backup_path: str = "kafka_backup.csv"           # Base path (will add timestamp)
dlq_path: str = "kafka_dlq.csv"                 # DLQ CSV path
csv_flush_interval_seconds: int = 5             # Flush frequency
csv_batch_size: int = 1000                      # Backup batch size
dlq_csv_batch_size: int = 100                   # DLQ batch size
csv_rotation_check_interval_seconds: int = 60   # How often to check for hour change
```

### Internal Timeouts (Configurable)
```python
queue_get_timeout_seconds: float = 0.1          # Worker queue.get() timeout
processing_queue_put_timeout_seconds: int = 60  # Queue put timeout (backpressure)
worker_queue_get_timeout_seconds: int = 1       # Worker get timeout
```

### Shutdown Parameters
```python
shutdown_timeout_seconds: int = 30      # For logging/metrics only, NOT hard timeout
stop_at_offset: Optional[Dict[Tuple[str, int], int]] = None  # Stop at specific offsets
```

### Logging Parameters
```python
log_level: str = "INFO"                 # "DEBUG" or "INFO"
mod_name: str = "kafka_consumer"        # Module name for logger
```

---

## DATA FLOW

### Happy Path (Success)
1. Kafka Broker → Polling Thread (poll batch)
2. Polling Thread → Backup CSV Queue → Backup CSV Writer (hourly rotated file)
3. Polling Thread → Processing Queue
4. Processing Queue → Worker Thread (get message)
5. Worker Thread → processor_callable(msg.value()) → Success
6. Worker Thread → Processed Queue (partition, offset, "success")
7. Processed Queue → Commit Thread (drain and track)
8. Commit Thread → Calculate max contiguous → Commit to Kafka (if not dev_mode)

### Failure Path (Processing Error)
1. Kafka Broker → Polling Thread → Backup CSV → Processing Queue
2. Worker Thread → processor_callable(msg.value()) → Exception
3. Worker Thread → DLQ Queue (msg + error details)
4. DLQ Queue → DLQ CSV Writer (append to dlq.csv)
5. Worker Thread → Processed Queue (partition, offset, "failed")
6. Commit Thread → Commit offset anyway (handled via DLQ)

### Worker Crash Path
1. Worker Thread → Processing message → Unexpected exception in worker itself
2. Outer try-except catches crash
3. Put current message to DLQ with error "WorkerCrashError"
4. Mark as processed in Processed Queue
5. Thread exits (another worker takes over)

---

## SHUTDOWN SEQUENCE

1. **Signal Received** (SIGTERM/SIGINT) or stop_at_offset reached
   - shared_state.running.clear()
   - shared_state.shutdown_event.set()

2. **Polling Thread Stops**
   - Exits loop (running is False)
   - No new messages polled
   - Thread joins within 5s

3. **Worker Threads Complete**
   - Process remaining messages in queue
   - Non-daemon prevents killing
   - Wait for ALL workers to finish (no hard timeout)
   - Log progress every 10 workers
   - Track elapsed time for metrics

4. **Final Commit**
   - Commit thread drains processed_queue
   - Calculates final max contiguous offsets
   - Commits to Kafka (if not dev_mode)
   - Thread joins within 5s

5. **CSV Writers Flush**
   - Backup CSV writer flushes final batch
   - DLQ CSV writer flushes final batch
   - Close files
   - Threads join within 5s each

6. **Kafka Consumer Close**
   - consumer.close()
   - Log INFO "Kafka consumer closed"

7. **Exit**
   - Return metrics dict
   - Exit code 0

**CRITICAL:** timeout_seconds is for monitoring/logging elapsed time ONLY. Do NOT force-kill workers. Wait for ALL in-flight messages to complete.

---

## OFFSET COMMIT STRATEGY

### Max Contiguous Algorithm
- Only commit up to first gap in processed offsets
- Safe for at-least-once delivery
- Reprocesses some messages on crash (acceptable trade-off)

### Example
```
Last committed: 1000
Processed: {1001, 1002, 1003, 1005, 1006}

Max contiguous: 1003 (gap at 1004)
Commit to Kafka: 1004 (next offset)
Uncommitted: {1005, 1006} (will retry next interval)
```

### Dev Mode
- Skips all Kafka commits
- Logs what would be committed
- On restart, all messages reprocessed
- For testing/development only

---

## ERROR HANDLING MATRIX

| Error Type | Action | Routing | Logging |
|------------|--------|---------|---------|
| Processing exception | DLQ + continue | DLQ CSV | ERROR |
| Worker thread crash | DLQ + exit thread | DLQ CSV | CRITICAL |
| Message too large | DLQ + skip | DLQ CSV | WARNING |
| CSV write failure | Graceful shutdown | - | CRITICAL |
| CSV rotation failure | Graceful shutdown | - | CRITICAL |
| Kafka connection lost | Retry + continue | - | ERROR |
| Commit failure | Retry next interval | - | ERROR |
| Disk full (CSV) | Graceful shutdown | - | CRITICAL |
| UTF-8 decode error | DLQ + continue | DLQ CSV | ERROR |

---

## LOGGING REQUIREMENTS

### Use DataPy Framework Logger
- Import and initialize logger from DataPy framework
- Set level based on config.log_level
- Use mod_name for proper namespacing

### INFO Level (Production)
- Consumer start/stop with config summary
- Offset commits (partition, offset, count)
- CSV file rotations
- Message processing errors (not individual successes)
- Graceful shutdown progress
- Worker crashes
- Dev mode warnings
- Stop at offset triggers

### DEBUG Level (Development)
- Individual message processing (offset, timing)
- Queue sizes and depths
- Thread lifecycle events
- Batch poll details
- Backpressure events

### Log Fields to Include
- timestamp (automatic)
- level (automatic)
- mod_name (configured)
- partition (for message-level logs)
- offset (for message-level logs)
- worker_id (for worker logs)
- processing_time_ms (for performance tracking)
- error_type (for exceptions)
- error_message (for exceptions)
- stack_trace (for CRITICAL errors)

---

## THREAD INVENTORY

| Thread Name | Count | Purpose | Daemon | Exit Condition |
|-------------|-------|---------|--------|----------------|
| Main | 1 | Orchestration, signals | N/A | Signal or exception |
| Polling | 1 | Poll Kafka messages | False | running=False |
| Workers | 50 (default) | Process messages | False | running=False AND queue empty |
| Commit | 1 | Offset commits | False | running=False AND processed_queue empty |
| Backup CSV | 1 | Write backup CSV | False | running=False AND backup_csv_queue empty |
| DLQ CSV | 1 | Write DLQ CSV | False | running=False AND dlq_queue empty |

**Total:** 54 threads (default with 50 workers)

**CRITICAL:** ALL threads non-daemon - they complete their work, cannot be killed

---

## PERFORMANCE CONSIDERATIONS

### Throughput Formula
```
Throughput (msg/s) = worker_count / avg_processing_time(s)
```

### Bottlenecks
- **Primary:** Message processing time (user's processor_callable)
- **Secondary:** Kafka poll (negligible)
- **Tertiary:** CSV writes (async, batched, negligible)
- **Commits:** Infrequent (5s default), negligible

### Memory Estimate
```
Memory = Base + (workers × 8MB) + (queue_size × avg_message_size)

Example (50 workers, 200 queue, 10KB avg message):
= 50MB + 400MB + 2MB = ~450MB
Recommendation: 1GB allocated
```

### Tuning Parameters
- **Increase throughput:** More workers (if CPU available)
- **Reduce latency:** Smaller batch sizes, faster commits
- **Reduce memory:** Smaller queue_size, smaller max_message_size
- **Improve reliability:** More frequent commits (trade-off: more overhead)

---

## CONFLUENT KAFKA SPECIFICS

### Consumer Configuration
```python
from confluent_kafka import Consumer, TopicPartition, KafkaError

consumer_config = {
    'bootstrap.servers': config.bootstrap_servers,
    'group.id': config.group_id,
    'enable.auto.commit': False,
    'auto.offset.reset': config.auto_offset_reset,
    'max.poll.interval.ms': 300000,  # 5 minutes
    'session.timeout.ms': 10000,
    'api.version.request': True
}

consumer = Consumer(consumer_config)
consumer.subscribe([config.topic])
```

### Polling Messages
```python
msg = consumer.poll(timeout=1.0)  # timeout in seconds
if msg is None:
    continue
if msg.error():
    # Handle error
    continue
# Process msg.value(), msg.partition(), msg.offset()
```

### Committing Offsets
```python
from confluent_kafka import TopicPartition

offsets = [
    TopicPartition(topic, partition, offset + 1)  # +1 for next offset
    for partition, offset in offsets_to_commit.items()
]
consumer.commit(offsets=offsets, asynchronous=False)
```

### Closing Consumer
```python
consumer.close()
```

---

## VALIDATION CHECKLIST

### Before Implementation
- [ ] All 7 files created with correct names
- [ ] All imports use relative imports (from . import X)
- [ ] All functions have type hints
- [ ] All threads created with daemon=False
- [ ] Confluent Kafka library used (not kafka-python)
- [ ] DataPy framework logger used (not stdlib logging directly)

### After Implementation
- [ ] Config validation catches all invalid inputs
- [ ] Worker crashes go to DLQ
- [ ] CSV write failures trigger graceful shutdown
- [ ] Hourly CSV rotation works correctly
- [ ] Dev mode skips commits and logs appropriately
- [ ] Stop at offset triggers shutdown
- [ ] All threads join without hard timeout
- [ ] Offset commits are max contiguous only
- [ ] UTF-8 decode errors handled
- [ ] No hardcoded timeouts (all configurable)

### Production Readiness
- [ ] INFO level logging in production
- [ ] DEBUG level logging in development
- [ ] All errors logged with proper severity
- [ ] Graceful shutdown waits for all workers
- [ ] CSV files created with proper permissions
- [ ] Kafka consumer properly closed
- [ ] Memory usage within estimates
- [ ] No daemon threads (all complete work)

---

## TESTING SCENARIOS

### Unit Tests
1. Max contiguous offset algorithm (gaps, no gaps, single element)
2. Config validation (missing params, invalid types, out of range)
3. Message size validation (normal, at limit, over limit)
4. Stop at offset format validation

### Integration Tests
1. **Happy path:** 100 messages, all succeed, offsets committed
2. **Processing failures:** 50 valid, 50 invalid → 50 in DLQ
3. **Backpressure:** Slow workers, fast producer → queue fills
4. **Graceful shutdown:** Signal during processing → all complete, final commit
5. **Worker crash:** Exception in worker → message in DLQ
6. **CSV rotation:** Run for 2+ hours → multiple CSV files created
7. **Dev mode:** No commits made, restart reprocesses all
8. **Stop at offset:** Stops at configured offset, graceful shutdown

### Failure Tests
1. Kafka connection loss → retry, recover
2. Disk full (CSV) → graceful shutdown
3. Worker thread crash → DLQ entry, continue
4. Processing timeout → DLQ entry, continue
5. CSV rotation failure → graceful shutdown
6. Out of memory → reject large messages → DLQ

---

## OPERATIONAL NOTES

### Kubernetes Deployment
- Set `terminationGracePeriodSeconds` high enough (e.g., 300s) for workers to complete
- Use liveness probe on /health endpoint (if added)
- Resource limits: 1 CPU, 1GB RAM minimum
- Restart policy: Always (auto-restart on CSV failures)

### Monitoring
- Alert on CRITICAL logs (CSV failures, worker crashes)
- Track processing lag (Kafka consumer group lag)
- Track DLQ file size growth
- Track CSV rotation events

### Maintenance
- External cron to delete old CSV files (e.g., >7 days)
- Separate DLQ reprocessing job
- Monitor disk space for CSV writes

### Configuration Examples
See Configuration Parameters section for all defaults and ranges.

---

## IMPLEMENTATION ORDER

### Recommended Coding Sequence
1. **shared_state.py** - Foundation (config, state, validation)
2. **signal_handler.py** - Independent utility
3. **offset_manager.py** - Core offset logic
4. **worker_pool.py** - Core processing logic
5. **polling_thread.py** - Kafka integration
6. **csv_writers.py** - CSV logic with rotation
7. **kafka_consumer.py** - Orchestration (uses all above)

### Testing Order
1. Unit tests for offset algorithm
2. Unit tests for config validation
3. Integration test with mock Kafka
4. Integration test with real Kafka (single partition)
5. Integration test with multiple partitions
6. Integration test with worker crashes
7. Integration test with CSV rotation
8. Load test with high throughput

---

## FINAL CHECKLIST

### Code Quality
- [ ] Type hints on all functions and classes
- [ ] Docstrings on all public functions
- [ ] No hardcoded values (all configurable)
- [ ] Proper exception handling (no bare excepts)
- [ ] Resource cleanup (close files, consumers)
- [ ] Thread-safe operations (proper Queue usage)

### Data Integrity
- [ ] At-least-once guarantee (max contiguous commits)
- [ ] No message loss (proper offset management)
- [ ] Worker crashes captured (DLQ entries)
- [ ] CSV failures trigger shutdown (fail cleanly)
- [ ] All in-flight messages complete before shutdown

### Production Readiness
- [ ] Framework logger integration
- [ ] Proper log levels (INFO/DEBUG)
- [ ] CSV hourly rotation implemented
- [ ] Non-daemon threads (complete work)
- [ ] Confluent Kafka library used
- [ ] Graceful shutdown with no hard timeout
- [ ] Dev mode for testing without commits
- [ ] Stop at offset for controlled testing

---

**END OF SPECIFICATION - READY FOR IMPLEMENTATION**