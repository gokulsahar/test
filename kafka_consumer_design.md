# KAFKA CONSUMER MOD - PRODUCTION DESIGN DOCUMENT

## DOCUMENT METADATA

| Property | Value |
|----------|-------|
| **Mod Type** | `kafka_consumer` |
| **Version** | 1.1.0 |
| **Framework** | DataPy |
| **Consistency Model** | AP (Availability Priority) |
| **Delivery Guarantee** | At-least-once |
| **Concurrency** | Multi-threaded (configurable workers) |
| **Date** | 2024-11-27 |

---

## TABLE OF CONTENTS

1. [Executive Summary](#1-executive-summary)
2. [Architecture Overview](#2-architecture-overview)
3. [Component Specifications](#3-component-specifications)
4. [Data Flow](#4-data-flow)
5. [Offset Management Strategy](#5-offset-management-strategy)
6. [CSV Backup System](#6-csv-backup-system)
7. [Error Handling & DLQ](#7-error-handling--dlq)
8. [Graceful Shutdown](#8-graceful-shutdown)
9. [Thread Safety](#9-thread-safety)
10. [Configuration Parameters](#10-configuration-parameters)
11. [Logging Strategy](#11-logging-strategy)
12. [Performance & Throughput](#12-performance--throughput)
13. [Testing Strategy](#13-testing-strategy)
14. [Operational Runbooks](#14-operational-runbooks)
15. [Complete Parameter Reference](#15-complete-parameter-reference)

---

## 1. EXECUTIVE SUMMARY

### 1.1 Purpose

The `kafka_consumer` mod is a production-ready, high-throughput Kafka consumer designed for the DataPy framework. It implements an AP (Availability Priority) model with at-least-once delivery guarantees, zero message loss, and automatic failure recovery.

### 1.2 Key Features

| Feature | Description |
|---------|-------------|
| **Zero Message Loss** | Safe offset commits ensure no messages are lost on crash |
| **High Throughput** | Batch polling + worker pool maximizes processing speed |
| **Fault Tolerance** | Workers auto-recover, DLQ captures failures |
| **CSV Backup** | All messages backed up for replay (configurable) |
| **Graceful Shutdown** | Clean shutdown with configurable timeout |
| **Pluggable Processing** | User provides callable for message processing |
| **Comprehensive Logging** | Detailed logs via framework's logger.py |
| **Dev Mode** | No commits for testing/development scenarios |
| **Controlled Processing** | Stop after specific offset for testing |

### 1.3 Design Philosophy

**CORE PRINCIPLES:**
1. Consumer NEVER stops (AP model)
2. Failures go to DLQ, not crash
3. Simple CSV backup (no rotation in consumer)
4. Everything is configurable
5. Thread-safe by design
6. Observable via structured logs

---

## 2. ARCHITECTURE OVERVIEW

### 2.1 High-Level Architecture

**KAFKA CONSUMER MOD - 8 Threads Total**

```
Main Thread (Orchestrator)
├── Signal handling (SIGTERM, SIGINT)
├── Component lifecycle management
└── Shutdown coordination
    │
    ├─── Polling Thread
    │    ├── Poll Kafka
    │    ├── Batch 100 msgs
    │    └── Feed queues
    │
    ├─── Commit Thread
    │    ├── Track processed
    │    ├── Calc max contiguous
    │    └── Commit every 5s (if dev_mode=False)
    │
    ├─── Processing Queue (200 messages)
    │
    ├─── Backup CSV Queue (Unbounded)
    │
    ├─── Worker Pool (50 threads)
    │    ├── Process via user's callable
    │    ├── Success → processed queue
    │    └── Failure → DLQ queue
    │
    ├─── Backup CSV Writer Thread
    │    ├── Batch writes
    │    ├── Flush every 5s
    │    └── Single file
    │
    └─── DLQ Queue + Writer Thread
         ├── Batch writes
         ├── Flush every 5s
         └── Failure details
```

### 2.2 Thread Inventory

| Thread # | Name | Count | Purpose | Can Block? |
|----------|------|-------|---------|-----------|
| 1 | Main | 1 | Orchestration, signal handling | No |
| 2 | Polling | 1 | Kafka consumer.poll() | Yes (timeout=1s) |
| 3 | Workers | 50 (configurable) | Process messages | Yes (processing) |
| 4 | Commit | 1 | Offset commits | Yes (commit call) |
| 5 | Backup CSV Writer | 1 | Write backup CSV | Yes (I/O) |
| 6 | DLQ CSV Writer | 1 | Write DLQ CSV | Yes (I/O) |

**Total Threads:** 54 (default with 50 workers)

---

## 3. COMPONENT SPECIFICATIONS

### 3.1 Main Thread (Orchestrator)

**Responsibilities:**
- Initialize all components
- Start all threads
- Register signal handlers (SIGTERM, SIGINT)
- Coordinate graceful shutdown
- Join all threads on exit

**Pseudo-logic:**
```python
def main():
    # 1. Setup
    config = load_config()
    logger = setup_logger(__name__, mod_type="kafka_consumer", mod_name="main")
    
    # 2. Initialize components
    kafka_consumer = create_kafka_consumer(config)
    processing_queue = Queue(maxsize=config.queue_size)
    processed_queue = Queue()
    backup_csv_queue = Queue() if config.backup_enabled else None
    dlq_queue = Queue()
    
    # 3. Create threads
    polling_thread = Thread(target=poll_messages, args=(...))
    worker_threads = [Thread(target=worker, args=(...)) for _ in range(config.worker_count)]
    commit_thread = Thread(target=commit_offsets, args=(...))
    backup_writer = Thread(target=write_backup_csv, args=(...)) if config.backup_enabled else None
    dlq_writer = Thread(target=write_dlq_csv, args=(...))
    
    # 4. Register signal handlers
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)
    
    # 5. Start all threads
    polling_thread.start()
    for worker in worker_threads:
        worker.start()
    commit_thread.start()
    if backup_writer:
        backup_writer.start()
    dlq_writer.start()
    
    # 6. Wait for shutdown signal
    shutdown_event.wait()
    
    # 7. Coordinate shutdown
    logger.info("Initiating graceful shutdown")
    perform_graceful_shutdown()
    
    # 8. Join all threads
    polling_thread.join(timeout=config.shutdown_timeout)
    for worker in worker_threads:
        worker.join(timeout=config.shutdown_timeout)
    commit_thread.join(timeout=5)
    if backup_writer:
        backup_writer.join(timeout=5)
    dlq_writer.join(timeout=5)
    
    # 9. Final commit
    final_commit()
    
    logger.info("Shutdown complete")
```

---

### 3.2 Polling Thread

**Responsibilities:**
- Poll messages from Kafka in batches
- Write to backup CSV queue (if enabled)
- Put messages in processing queue
- Handle backpressure (queue full)
- Check stop_at_offset condition (if configured)

**Configuration:**
- `poll_timeout_ms`: Kafka poll timeout (default: 1000ms)
- `max_poll_records`: Batch size (default: 100)
- `queue_size`: Processing queue capacity (default: 200)
- `stop_at_offset`: Stop consuming after this offset per partition (default: None)

**Pseudo-logic:**
```python
def poll_messages():
    logger = setup_logger(__name__, mod_type="kafka_consumer", mod_name="polling")
    
    while running:
        try:
            # Poll batch from Kafka
            messages = consumer.poll(
                timeout_ms=config.poll_timeout_ms,
                max_records=config.max_poll_records
            )
            
            if not messages:
                continue
            
            batch_size = len(messages)
            logger.debug(f"Polled batch", extra={"batch_size": batch_size})
            
            for msg in messages:
                # Check if stop_at_offset configured and reached
                if config.stop_at_offset:
                    partition_key = (msg.topic, msg.partition)
                    target_offset = config.stop_at_offset.get(partition_key)
                    
                    if target_offset is not None and msg.offset >= target_offset:
                        logger.info(f"Reached stop_at_offset", extra={
                            "topic": msg.topic,
                            "partition": msg.partition,
                            "offset": msg.offset,
                            "target_offset": target_offset
                        })
                        # Trigger graceful shutdown
                        running = False
                        shutdown_event.set()
                        break
                
                # Write to backup CSV queue (async, non-blocking)
                if config.backup_enabled:
                    try:
                        backup_csv_queue.put_nowait({
                            "timestamp": datetime.utcnow().isoformat(),
                            "topic": msg.topic,
                            "partition": msg.partition,
                            "offset": msg.offset,
                            "key": msg.key,
                            "value": msg.value,
                            "message_size": len(msg.value)
                        })
                    except Full:
                        logger.warning("Backup CSV queue full, dropping backup entry")
                
                # Put in processing queue (blocks if full - backpressure)
                try:
                    processing_queue.put(msg, timeout=config.queue_put_timeout)
                except Full:
                    logger.warning("Processing queue full, backpressure applied")
                    processing_queue.put(msg)  # Block until space
            
        except KafkaException as e:
            logger.error(f"Kafka poll error: {e}")
            time.sleep(1)  # Brief pause before retry
            
        except Exception as e:
            logger.error(f"Unexpected polling error: {e}", extra={"stack_trace": traceback.format_exc()})
            time.sleep(1)
```

**Key Design Decisions:**

1. **Batch Polling:** Polls up to `max_poll_records` at once (default: 100)
   - Reduces Kafka API calls
   - Amortizes network latency
   - 5-10x throughput improvement vs single-message polling

2. **Backpressure:** If processing queue is full, polling blocks
   - Prevents memory overflow
   - Self-regulating system
   - Workers control flow rate

3. **Backup CSV Non-Blocking:** Uses `put_nowait()`
   - Never blocks polling on CSV backup
   - If backup queue full, log warning and continue
   - Processing priority > backup priority

4. **Stop at Offset:** Optional controlled stop
   - Useful for testing/development
   - Per-partition offset targets
   - Triggers graceful shutdown when reached

---

### 3.3 Worker Pool

**Responsibilities:**
- Pull messages from processing queue
- Call user's processing callable
- Handle success/failure
- Track processed offsets
- Write failures to DLQ queue

**Configuration:**
- `worker_count`: Number of worker threads (default: 50)
- `processing_timeout`: Max time per message (default: None - no timeout)
- `max_message_size`: Reject messages larger than this (default: 10MB)

**Pseudo-logic:**
```python
def worker(worker_id):
    logger = setup_logger(__name__, mod_type="kafka_consumer", mod_name=f"worker_{worker_id}")
    
    while running:
        try:
            # Get message from queue (blocks with timeout)
            msg = processing_queue.get(timeout=1)
            
            start_time = time.time()
            
            # Check message size
            if config.max_message_size and len(msg.value) > config.max_message_size:
                logger.warning(f"Message too large, skipping", extra={
                    "offset": msg.offset,
                    "size": len(msg.value),
                    "max_size": config.max_message_size
                })
                dlq_queue.put({
                    "msg": msg,
                    "error": "MessageTooLargeError",
                    "error_message": f"Message size {len(msg.value)} exceeds max {config.max_message_size}",
                    "processing_time_ms": 0
                })
                processed_queue.put((msg.partition, msg.offset, "failed"))
                continue
            
            # Process message via user's callable
            try:
                result = config.processor_callable(msg.value)
                
                processing_time = (time.time() - start_time) * 1000
                
                logger.info("Message processed successfully", extra={
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "processing_time_ms": processing_time,
                    "worker_id": worker_id
                })
                
                processed_queue.put((msg.partition, msg.offset, "success"))
                
            except Exception as e:
                processing_time = (time.time() - start_time) * 1000
                
                logger.error("Message processing failed", extra={
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "processing_time_ms": processing_time,
                    "worker_id": worker_id,
                    "stack_trace": traceback.format_exc()
                })
                
                # Write to DLQ
                dlq_queue.put({
                    "msg": msg,
                    "error": type(e).__name__,
                    "error_message": str(e),
                    "stack_trace": traceback.format_exc(),
                    "processing_time_ms": processing_time
                })
                
                # Still mark as processed (handled via DLQ)
                processed_queue.put((msg.partition, msg.offset, "failed"))
        
        except Empty:
            # Queue empty, that's fine
            continue
            
        except Exception as e:
            # Worker-level exception (shouldn't happen)
            logger.critical(f"Worker {worker_id} crashed: {e}", extra={
                "stack_trace": traceback.format_exc()
            })
            # Thread will exit, main thread should detect and restart
            break
```

**Worker Exception Recovery:**

Each worker is wrapped in a recovery wrapper:

```python
def worker_wrapper(worker_id):
    retry_count = 0
    max_retries = 3
    
    while running and retry_count < max_retries:
        try:
            worker(worker_id)  # Main worker logic
            # If exits normally, we're shutting down
            break
            
        except Exception as e:
            retry_count += 1
            logger.error(f"Worker {worker_id} crashed, retry {retry_count}/{max_retries}", extra={
                "error": str(e),
                "stack_trace": traceback.format_exc()
            })
            
            if retry_count >= max_retries:
                logger.critical(f"Worker {worker_id} failed permanently after {max_retries} retries")
                # TODO: Spawn replacement worker?
                break
            
            time.sleep(1)  # Brief pause before retry
```

**Key Design Decisions:**

1. **No Retries in Worker:** If processing fails, immediately goes to DLQ
   - User requested: "no matter what we put in DLQ and move on"
   - Keeps consumer moving forward
   - Retries handled externally (separate DLQ reprocessing job)

2. **Timeout Handling:** Optional `processing_timeout` parameter
   - If set, wraps processing call in timeout
   - If timeout exceeded, treated as failure → DLQ

3. **Message Size Limit:** Configurable max message size
   - Prevents OOM from huge messages
   - Large messages rejected immediately → DLQ

---

### 3.4 Commit Thread

**Responsibilities:**
- Track processed offsets from `processed_queue`
- Calculate max contiguous offset per partition
- Commit offsets to Kafka at configured interval
- Handle commit failures
- Skip commits if dev_mode enabled

**Configuration:**
- `commit_interval_seconds`: How often to commit (default: 5)
- `enable_auto_commit`: Use Kafka's auto-commit (default: False)
- `dev_mode`: Disable offset commits for testing (default: False)

**Pseudo-logic:**
```python
def commit_offsets():
    logger = setup_logger(__name__, mod_type="kafka_consumer", mod_name="commit")
    
    # Check if dev_mode enabled
    if config.dev_mode:
        logger.warning("Dev mode enabled - offsets will NOT be committed")
    
    # Track processed offsets per partition
    processed_offsets = {}  # {partition: set([offsets])}
    last_committed = {}     # {partition: offset}
    
    while running:
        # Drain processed_queue
        batch_start = time.time()
        drained_count = 0
        
        while time.time() - batch_start < config.commit_interval_seconds:
            try:
                partition, offset, status = processed_queue.get(timeout=0.1)
                
                if partition not in processed_offsets:
                    processed_offsets[partition] = set()
                
                processed_offsets[partition].add(offset)
                drained_count += 1
                
            except Empty:
                # No more processed messages yet
                time.sleep(0.1)
                continue
        
        # If dev_mode, skip actual commit but log what would be committed
        if config.dev_mode:
            if processed_offsets:
                logger.info("Dev mode - would have committed offsets", extra={
                    "partition_offsets": {p: max(offsets) for p, offsets in processed_offsets.items()},
                    "messages_processed": drained_count
                })
                # Clear tracking (simulate commit)
                processed_offsets.clear()
            continue
        
        # Calculate max contiguous offset per partition
        offsets_to_commit = {}
        
        for partition, offsets in processed_offsets.items():
            if not offsets:
                continue
            
            sorted_offsets = sorted(offsets)
            
            # Find max contiguous offset
            last_committed_offset = last_committed.get(partition, -1)
            max_contiguous = last_committed_offset
            
            for offset in sorted_offsets:
                if offset == max_contiguous + 1:
                    max_contiguous = offset
                else:
                    # Gap found, stop here
                    break
            
            if max_contiguous > last_committed_offset:
                offsets_to_commit[partition] = max_contiguous + 1  # Kafka commits "next offset"
        
        # Commit to Kafka
        if offsets_to_commit:
            try:
                commit_payload = {
                    TopicPartition(config.topic, partition): OffsetAndMetadata(offset, None)
                    for partition, offset in offsets_to_commit.items()
                }
                
                consumer.commit(offsets=commit_payload)
                
                logger.info("Offsets committed", extra={
                    "partition_offsets": offsets_to_commit,
                    "messages_committed": drained_count
                })
                
                # Update last committed
                for partition, offset in offsets_to_commit.items():
                    last_committed[partition] = offset - 1  # Store actual offset, not "next"
                
                # Remove committed offsets from tracking
                for partition, committed_offset in offsets_to_commit.items():
                    processed_offsets[partition] = {
                        o for o in processed_offsets[partition] 
                        if o >= committed_offset
                    }
                
            except KafkaException as e:
                logger.error(f"Commit failed: {e}")
                # Don't crash, will retry next interval
            
        else:
            logger.debug("No offsets to commit this interval")
```

**Max Contiguous Offset Algorithm:**

```python
def find_max_contiguous(offsets: Set[int], last_committed: int) -> int:
    """
    Find the maximum contiguous offset starting from last_committed.
    
    Example:
        last_committed = 1000
        offsets = {1001, 1002, 1003, 1005, 1006, 1007}
        
        Result: 1003 (gap at 1004)
    
    Args:
        offsets: Set of processed offsets
        last_committed: Last committed offset (or -1 if none)
    
    Returns:
        Maximum contiguous offset
    """
    sorted_offsets = sorted(offsets)
    max_contiguous = last_committed
    
    for offset in sorted_offsets:
        if offset == max_contiguous + 1:
            max_contiguous = offset
        else:
            break  # Gap found
    
    return max_contiguous
```

**Key Design Decisions:**

1. **Max Contiguous Only:** Only commit up to first gap
   - Safe: Never commit unprocessed messages
   - Trade-off: Some reprocessing on crash if gaps exist
   - Acceptable in at-least-once model

2. **Batch Interval:** Commits every N seconds (configurable)
   - Reduces Kafka API calls
   - Balances safety vs performance
   - Default: 5 seconds (industry standard)

3. **Commit Failure Handling:** If commit fails, log and retry next interval
   - Don't crash consumer
   - Kafka will redeliver from last successful commit

4. **Dev Mode:** Completely skip commits when enabled
   - Useful for development/testing
   - Messages still processed
   - On restart, all messages reprocessed
   - Logs what would have been committed

---

### 3.5 Backup CSV Writer Thread

**Responsibilities:**
- Read from `backup_csv_queue`
- Batch messages in memory
- Write to CSV file periodically
- Flush on shutdown

**Configuration:**
- `backup_enabled`: Enable/disable backup (default: True)
- `backup_path`: CSV file path (required if enabled)
- `csv_flush_interval_seconds`: Write frequency (default: 5)
- `csv_batch_size`: Write after N messages (default: 1000)

**CSV Schema:**
```csv
timestamp,topic,partition,offset,key,value,message_size
2024-01-15T10:00:01.123Z,orders,0,1000,user123,"{...}",5120
2024-01-15T10:00:01.125Z,orders,0,1001,user124,"{...}",4890
```

**Pseudo-logic:**
```python
def write_backup_csv():
    logger = setup_logger(__name__, mod_type="kafka_consumer", mod_name="backup_csv_writer")
    
    csv_file = open(config.backup_path, 'a', newline='', buffering=1)
    csv_writer = csv.DictWriter(csv_file, fieldnames=[
        'timestamp', 'topic', 'partition', 'offset', 'key', 'value', 'message_size'
    ])
    
    # Write header if file is new
    if csv_file.tell() == 0:
        csv_writer.writeheader()
    
    batch = []
    last_flush = time.time()
    
    while running or not backup_csv_queue.empty():
        try:
            # Get message from queue
            msg_data = backup_csv_queue.get(timeout=0.1)
            batch.append(msg_data)
            
            # Check if should flush
            should_flush = (
                len(batch) >= config.csv_batch_size or
                (time.time() - last_flush) >= config.csv_flush_interval_seconds
            )
            
            if should_flush:
                # Write batch
                csv_writer.writerows(batch)
                csv_file.flush()
                
                logger.debug(f"Backup CSV batch written", extra={
                    "messages_written": len(batch),
                    "file_path": config.backup_path
                })
                
                batch = []
                last_flush = time.time()
        
        except Empty:
            # Check if should flush anyway (time-based)
            if batch and (time.time() - last_flush) >= config.csv_flush_interval_seconds:
                csv_writer.writerows(batch)
                csv_file.flush()
                logger.debug(f"Backup CSV time-based flush", extra={
                    "messages_written": len(batch)
                })
                batch = []
                last_flush = time.time()
            continue
            
        except Exception as e:
            logger.error(f"Backup CSV write error: {e}", extra={
                "stack_trace": traceback.format_exc()
            })
            # Don't crash, continue trying
    
    # Final flush on shutdown
    if batch:
        csv_writer.writerows(batch)
        csv_file.flush()
        logger.info(f"Final backup CSV flush", extra={"messages_written": len(batch)})
    
    csv_file.close()
```

**Key Design Decisions:**

1. **Append Mode:** Opens file in append mode (`'a'`)
   - Allows multiple consumer instances (different files)
   - Survives consumer restarts
   - External rotation via logrotate

2. **Batch Writing:** Buffers messages, writes in batches
   - Reduces I/O syscalls
   - Better performance
   - Flush triggers: batch size OR time interval

3. **Error Handling:** If write fails, log and continue
   - Don't crash consumer over backup failure
   - Backup is safety net, not critical path

---

### 3.6 DLQ CSV Writer Thread

**Responsibilities:**
- Read from `dlq_queue`
- Write failed messages with error details
- Batch writes for performance
- Flush on shutdown

**Configuration:**
- `dlq_path`: DLQ CSV file path (required)
- `csv_flush_interval_seconds`: Write frequency (default: 5)
- `csv_batch_size`: Write after N messages (default: 100)

**CSV Schema:**
```csv
timestamp,topic,partition,offset,key,value,error_type,error_message,stack_trace,processing_time_ms,retry_count
2024-01-15T10:05:23.456Z,orders,0,1005,user125,"{...}",TimeoutError,"API timeout after 30s","Traceback...",30123,0
2024-01-15T10:06:45.789Z,orders,1,2103,user126,"{...}",ValidationError,"Invalid schema","...",234,0
```

**Pseudo-logic:**
```python
def write_dlq_csv():
    logger = setup_logger(__name__, mod_type="kafka_consumer", mod_name="dlq_csv_writer")
    
    csv_file = open(config.dlq_path, 'a', newline='', buffering=1)
    csv_writer = csv.DictWriter(csv_file, fieldnames=[
        'timestamp', 'topic', 'partition', 'offset', 'key', 'value',
        'error_type', 'error_message', 'stack_trace', 'processing_time_ms', 'retry_count'
    ])
    
    if csv_file.tell() == 0:
        csv_writer.writeheader()
    
    batch = []
    last_flush = time.time()
    
    while running or not dlq_queue.empty():
        try:
            dlq_entry = dlq_queue.get(timeout=0.1)
            
            msg = dlq_entry["msg"]
            
            row = {
                'timestamp': datetime.utcnow().isoformat(),
                'topic': msg.topic,
                'partition': msg.partition,
                'offset': msg.offset,
                'key': msg.key,
                'value': msg.value,
                'error_type': dlq_entry["error"],
                'error_message': dlq_entry["error_message"],
                'stack_trace': dlq_entry.get("stack_trace", ""),
                'processing_time_ms': dlq_entry["processing_time_ms"],
                'retry_count': 0  # Initial entry
            }
            
            batch.append(row)
            
            should_flush = (
                len(batch) >= config.csv_batch_size or
                (time.time() - last_flush) >= config.csv_flush_interval_seconds
            )
            
            if should_flush:
                csv_writer.writerows(batch)
                csv_file.flush()
                
                logger.info(f"DLQ CSV batch written", extra={
                    "messages_written": len(batch),
                    "file_path": config.dlq_path
                })
                
                batch = []
                last_flush = time.time()
        
        except Empty:
            if batch and (time.time() - last_flush) >= config.csv_flush_interval_seconds:
                csv_writer.writerows(batch)
                csv_file.flush()
                batch = []
                last_flush = time.time()
            continue
            
        except Exception as e:
            logger.error(f"DLQ CSV write error: {e}", extra={
                "stack_trace": traceback.format_exc()
            })
            # Continue, don't crash
    
    # Final flush
    if batch:
        csv_writer.writerows(batch)
        csv_file.flush()
        logger.info(f"Final DLQ CSV flush", extra={"messages_written": len(batch)})
    
    csv_file.close()
```

**Key Design Decisions:**

1. **Always Append:** Never overwrite existing DLQ entries
   - Preserves history
   - External job handles deduplication/cleanup

2. **Include Stack Trace:** Full traceback for debugging
   - Helps identify root cause
   - Can be long, but DLQ should be small

3. **Retry Count:** Initially 0, incremented by external reprocessing job
   - Tracks how many times reprocessed
   - Can implement max retry logic externally

---

## 4. DATA FLOW

### 4.1 Happy Path (Success)

**Message Flow (Success Scenario):**

```
STEP 1: Kafka Broker
    |
    | (poll)
    v
STEP 2: Polling Thread
    |
    ├─→ Backup CSV Queue → Backup CSV Writer → CSV File
    |
    └─→ Processing Queue
        |
        | (get)
        v
STEP 3: Worker Thread
    |
    ├─→ Call processor_callable(msg.value)
    |
    ├─→ Success!
    |
    └─→ Processed Queue (partition, offset, "success")
        |
        | (get)
        v
STEP 4: Commit Thread
    |
    ├─→ Calculate max contiguous offset
    |
    └─→ Commit to Kafka (if dev_mode=False)
        |
        v
STEP 5: Offset Committed ✅ (or skipped in dev_mode)
```

### 4.2 Failure Path (Processing Error)

**Message Flow (Failure Scenario):**

```
STEP 1: Kafka Broker
    |
    | (poll)
    v
STEP 2: Polling Thread
    |
    ├─→ Backup CSV Queue → Backup CSV Writer → CSV File
    |
    └─→ Processing Queue
        |
        | (get)
        v
STEP 3: Worker Thread
    |
    ├─→ Call processor_callable(msg.value)
    |
    ├─→ Exception raised! ❌
    |
    ├─→ DLQ Queue (msg + error details)
    |   |
    |   └─→ DLQ CSV Writer → DLQ CSV File
    |
    └─→ Processed Queue (partition, offset, "failed")
        |
        | (get)
        v
STEP 4: Commit Thread
    |
    ├─→ Calculate max contiguous offset
    |
    └─→ Commit to Kafka (if dev_mode=False)
        |
        v
STEP 5: Offset Committed ✅ (even though failed, it's handled via DLQ)
       (or skipped in dev_mode)
```

### 4.3 Crash Recovery Flow

**Scenario: Consumer crashes at t=10s**

```
BEFORE CRASH:
├─ Polled offsets 1-100
├─ Processed offsets 1-50 (success)
├─ Processing offsets 51-70 (in-flight)
├─ Queue offsets 71-100 (not started)
├─ Last committed offset: 50 (if dev_mode=False)
└─ CRASH! 💥

AFTER RESTART (Production Mode):
├─ Consumer starts
├─ Reads committed offset from Kafka: 50
├─ Kafka redelivers offsets 51-100
├─ Some duplicates (51-70 might have partially processed)
├─ At-least-once guarantee ✅
└─ No messages lost ✅

AFTER RESTART (Dev Mode):
├─ Consumer starts
├─ No commits were made (dev_mode=True)
├─ Kafka redelivers ALL messages from beginning or last manual commit
├─ All messages reprocessed
└─ Expected behavior for testing ✅
```

---

## 5. OFFSET MANAGEMENT STRATEGY

### 5.1 Commit Strategy Summary

| Aspect | Strategy |
|--------|----------|
| **When to commit** | Every N seconds (time-based), unless dev_mode=True |
| **What to commit** | Max contiguous processed offset per partition |
| **Failure handling** | Mark as processed (via DLQ), commit anyway |
| **Gap handling** | Commit up to first gap, rest uncommitted |
| **Crash recovery** | Replay from last committed offset |
| **Dev mode** | No commits - all messages reprocessed on restart |

### 5.2 Max Contiguous Algorithm (Detailed)

**Input:**
- `processed_offsets`: Set of offsets that have been processed
- `last_committed`: Last offset committed to Kafka

**Output:**
- Maximum contiguous offset starting from `last_committed + 1`

**Algorithm:**
```python
def find_max_contiguous(processed_offsets: Set[int], last_committed: int) -> int:
    sorted_offsets = sorted(processed_offsets)
    max_contiguous = last_committed
    
    for offset in sorted_offsets:
        if offset == max_contiguous + 1:
            max_contiguous = offset
        else:
            # Gap detected, stop
            break
    
    return max_contiguous
```

**Examples:**

**Example 1: No Gaps**
```
last_committed = 1000
processed = {1001, 1002, 1003, 1004, 1005}

Result: 1005 (all contiguous)
Commit: 1006 (next offset)
```

**Example 2: With Gap**
```
last_committed = 1000
processed = {1001, 1002, 1003, 1005, 1006}

Result: 1003 (gap at 1004)
Commit: 1004 (next offset)
Uncommitted: {1005, 1006} (will retry next interval)
```

**Example 3: Multiple Gaps**
```
last_committed = 1000
processed = {1001, 1003, 1005, 1007}

Result: 1001 (gap at 1002)
Commit: 1002
Uncommitted: {1003, 1005, 1007}
```

### 5.3 Why Gaps Occur

**Causes:**
1. Variable processing times (some messages faster than others)
2. Worker threads process out of order
3. Processing failures (goes to DLQ immediately)

**Example Timeline:**
```
t=0s:  Poll offsets 1-5, put in queue
t=1s:  Worker A starts msg 1 (will take 10s)
       Worker B starts msg 2 (will take 1s)
       Worker C starts msg 3 (will take 5s)
       Worker D starts msg 4 (will take 2s)
       Worker E starts msg 5 (will take 3s)

t=2s:  Worker B finishes msg 2 → processed = {2}
t=3s:  Worker D finishes msg 4 → processed = {2, 4}
t=4s:  Worker E finishes msg 5 → processed = {2, 4, 5}
t=5s:  COMMIT TIME
       Max contiguous from offset 0: None (msg 1 not done)
       Commit: offset 1 (nothing new)
       
t=6s:  Worker C finishes msg 3 → processed = {2, 3, 4, 5}
t=11s: Worker A finishes msg 1 → processed = {1, 2, 3, 4, 5}
t=10s: COMMIT TIME
       Max contiguous: 5
       Commit: offset 6 ✅
```

### 5.4 Commit Interval Trade-offs

| Interval | Pros | Cons |
|----------|------|------|
| **1 second** | Minimal reprocessing on crash | High Kafka API calls, commit overhead |
| **5 seconds** | ✅ **Balanced (recommended)** | Some reprocessing (5s worth) |
| **10 seconds** | Fewer API calls, less overhead | More reprocessing on crash |
| **30+ seconds** | Minimal overhead | Significant reprocessing, not recommended |
| **Dev mode (disabled)** | Complete reprocessing control | All messages reprocessed on restart |

**Recommendation:** 5 seconds (industry standard), dev_mode for testing

---

## 6. CSV BACKUP SYSTEM

### 6.1 Purpose

**Why Backup?**
- Kafka retention < data lifetime requirement
- In some orgs, retention is < 1 day
- CSV backup = replay capability after Kafka expires

**What's Backed Up?**
- ALL polled messages (success + failure)
- Raw message content
- Metadata (topic, partition, offset, timestamp)

### 6.2 Backup CSV Format

**File Location:** Configurable via `backup_path`

**Schema:**
```csv
timestamp,topic,partition,offset,key,value,message_size
```

**Field Descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | ISO-8601 | When message was polled (UTC) |
| `topic` | string | Kafka topic name |
| `partition` | int | Partition number |
| `offset` | int | Message offset within partition |
| `key` | string | Message key (null if none) |
| `value` | string/json | Message payload |
| `message_size` | int | Payload size in bytes |

**Example:**
```csv
timestamp,topic,partition,offset,key,value,message_size
2024-01-15T10:00:01.123Z,orders,0,1000,order_123,"{""customer"":""john"",""amount"":99.99}",45
2024-01-15T10:00:01.125Z,orders,0,1001,order_124,"{""customer"":""jane"",""amount"":149.50}",46
```

### 6.3 File Rotation Strategy

**NOT handled by consumer.** Use external tools:

**Option A: logrotate (Linux)**
```bash
# /etc/logrotate.d/kafka-consumer-backup

/data/backups/kafka_backup.csv {
    daily                    # Rotate daily
    rotate 7                 # Keep 7 days
    compress                 # Compress old files
    delaycompress           # Compress after 1 day
    missingok               # Don't error if file missing
    notifempty              # Don't rotate empty files
    copytruncate            # Copy then truncate (consumer keeps writing)
    maxsize 500M            # Also rotate if > 500MB
}
```

**Option B: Cron script**
```bash
#!/bin/bash
# rotate_backup_csv.sh

DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/data/backups"
CURRENT_FILE="$BACKUP_DIR/kafka_backup.csv"
ARCHIVE_FILE="$BACKUP_DIR/archive/kafka_backup_$DATE.csv.gz"

# Copy current file
cp "$CURRENT_FILE" "$BACKUP_DIR/kafka_backup_temp.csv"

# Truncate original (consumer continues writing)
> "$CURRENT_FILE"

# Compress and archive
gzip -c "$BACKUP_DIR/kafka_backup_temp.csv" > "$ARCHIVE_FILE"
rm "$BACKUP_DIR/kafka_backup_temp.csv"

# Cleanup old archives (> 7 days)
find "$BACKUP_DIR/archive" -name "kafka_backup_*.csv.gz" -mtime +7 -delete
```

### 6.4 CSV Replay (External Tool)

**Not implemented in consumer.** Separate script:

```python
# replay_from_backup.py (example)

def replay_csv(csv_path, processor_callable):
    """Replay messages from backup CSV"""
    
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            msg_value = row['value']
            
            try:
                processor_callable(msg_value)
                print(f"✅ Replayed offset {row['offset']}")
            except Exception as e:
                print(f"❌ Failed offset {row['offset']}: {e}")

# Usage:
# replay_csv('/data/backups/kafka_backup_20240115.csv.gz', my_processor)
```

---

## 7. ERROR HANDLING & DLQ

### 7.1 Error Philosophy

**Core Principle:** Consumer NEVER stops for processing errors

**ERROR HANDLING HIERARCHY:**
1. Processing Error → DLQ, continue
2. CSV Write Error → Log, continue
3. Kafka Connection Error → Retry, continue
4. Worker Thread Crash → Restart worker
5. Fatal System Error → Graceful shutdown

### 7.2 DLQ CSV Format

**File Location:** Configurable via `dlq_path`

**Schema:**
```csv
timestamp,topic,partition,offset,key,value,error_type,error_message,stack_trace,processing_time_ms,retry_count
```

**Field Descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | ISO-8601 | When failure occurred |
| `topic` | string | Kafka topic |
| `partition` | int | Partition number |
| `offset` | int | Message offset |
| `key` | string | Message key |
| `value` | string/json | Message payload |
| `error_type` | string | Exception class name |
| `error_message` | string | Exception message |
| `stack_trace` | text | Full Python traceback |
| `processing_time_ms` | float | Time before failure |
| `retry_count` | int | Times reprocessed (0 initially) |

**Example:**
```csv
timestamp,topic,partition,offset,key,value,error_type,error_message,stack_trace,processing_time_ms,retry_count
2024-01-15T10:05:23.456Z,orders,0,1005,order_125,"{...}",TimeoutError,"API timeout after 30s","Traceback (most recent call last)...",30123.4,0
2024-01-15T10:06:45.789Z,orders,1,2103,order_126,"{...}",ValidationError,"Invalid email format","Traceback...",234.5,0
```

### 7.3 Error Categories

**Category 1: Processing Errors (Expected)**
- User's processor raises exception
- API timeouts, validation errors, etc.
- **Action:** Write to DLQ, mark processed, continue

**Category 2: System Errors (Unexpected)**
- Disk full (CSV write fails)
- Kafka connection lost
- Worker thread crashes
- **Action:** Log, retry, don't stop consumer

**Category 3: Fatal Errors (Rare)**
- Out of memory
- Corrupted Kafka consumer state
- **Action:** Graceful shutdown, alert

### 7.4 DLQ Reprocessing (External)

**Not in consumer.** Separate job:

```python
# dlq_reprocessor.py (example)

def reprocess_dlq(dlq_path, processor_callable, max_retries=3):
    """Reprocess messages from DLQ"""
    
    with open(dlq_path, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    reprocessed = []
    permanent_failures = []
    
    for row in rows:
        retry_count = int(row['retry_count'])
        
        if retry_count >= max_retries:
            permanent_failures.append(row)
            continue
        
        try:
            processor_callable(row['value'])
            print(f"✅ Reprocessed offset {row['offset']}")
            # Don't add back to DLQ
            
        except Exception as e:
            row['retry_count'] = retry_count + 1
            row['error_message'] = str(e)
            row['timestamp'] = datetime.utcnow().isoformat()
            reprocessed.append(row)
    
    # Rewrite DLQ with only failures
    with open(dlq_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(reprocessed)
    
    # Write permanent failures separately
    if permanent_failures:
        with open('permanent_failures.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(permanent_failures)

# Run via cron: 0 * * * * python dlq_reprocessor.py
```

---

## 8. GRACEFUL SHUTDOWN

### 8.1 Shutdown Triggers

**Signals:**
- `SIGTERM` (Kubernetes pod termination)
- `SIGINT` (Ctrl+C)
- User calls `consumer.stop()`
- Reached `stop_at_offset` (if configured)

### 8.2 Shutdown Sequence

**GRACEFUL SHUTDOWN SEQUENCE (30 seconds timeout):**

```
STEP 1: Signal Received (SIGTERM/SIGINT) or stop_at_offset reached
    |
    v
STEP 2: Set running = False (all threads check this)
    |
    v
STEP 3: Stop Polling Thread
    - Stop polling new messages
    - Current poll completes
    Timeout: 5 seconds
    |
    v
STEP 4: Drain Processing Queue
    - Workers process remaining messages
    - No new messages added
    Timeout: 30 seconds
    |
    v
STEP 5: Wait for Workers
    - All workers finish current message
    - Or timeout after 30 seconds
    |
    v
STEP 6: Final Commit
    - Drain processed_queue
    - Calculate final offsets
    - Commit to Kafka (if dev_mode=False)
    Timeout: 5 seconds
    |
    v
STEP 7: Flush CSV Writers
    - Backup CSV writer flushes buffer
    - DLQ CSV writer flushes buffer
    Timeout: 5 seconds
    |
    v
STEP 8: Close Resources
    - Close Kafka consumer
    - Close CSV files
    - Join all threads
    |
    v
STEP 9: Exit
    - Log shutdown complete
    - Exit code 0
```

### 8.3 Timeout Behavior

**Configuration:**
- `shutdown_timeout_seconds`: Max time to wait (default: 30)

**Scenario 1: Clean Shutdown (< 30s)**
```
t=0s:  SIGTERM received
t=1s:  Polling stopped
t=5s:  Queue drained (50 messages processed)
t=6s:  All workers idle
t=7s:  Final commit (50 offsets) - skipped if dev_mode=True
t=8s:  CSV writers flushed
t=9s:  Exit ✅

Messages in flight: 0
Reprocessed on restart: 0 (or all if dev_mode=True)
```

**Scenario 2: Timeout Exceeded (> 30s)**
```
t=0s:  SIGTERM received
t=1s:  Polling stopped
t=30s: TIMEOUT! (100 messages still in queue, 20 being processed)
t=31s: Force commit processed so far (50 messages) - skipped if dev_mode=True
t=32s: Force flush CSV writers
t=33s: Force exit

Messages in flight: 20 (not committed)
Messages in queue: 100 (not processed)
Reprocessed on restart: 120 (at-least-once) or all if dev_mode=True
```

**Scenario 3: Stop at Offset Reached**
```
t=0s:  Polling, offset 1000 reached (stop_at_offset configured)
t=1s:  Polling thread triggers shutdown
t=2s:  Queue has 50 remaining messages
t=10s: All 50 processed
t=11s: Final commit (if dev_mode=False)
t=12s: Exit ✅

Result: Controlled stop at exact offset for testing
```

### 8.4 Shutdown Logging

```python
# Shutdown initiated
logger.info("Shutdown initiated", extra={
    "signal": "SIGTERM",
    "queue_depth": processing_queue.qsize(),
    "active_workers": count_active_workers(),
    "reason": "signal" or "stop_at_offset"
})

# Polling stopped
logger.info("Polling thread stopped")

# Queue draining
logger.info("Draining processing queue", extra={
    "messages_remaining": processing_queue.qsize()
})

# Workers stopped
logger.info("All workers stopped", extra={
    "duration_ms": (time.time() - shutdown_start) * 1000
})

# Final commit
if not config.dev_mode:
    logger.info("Final commit", extra={
        "partition_offsets": final_offsets,
        "messages_committed": count
    })
else:
    logger.info("Dev mode - final commit skipped", extra={
        "messages_processed": count
    })

# Shutdown complete
logger.info("Shutdown complete", extra={
    "total_duration_ms": (time.time() - shutdown_start) * 1000,
    "clean_shutdown": timeout_exceeded == False
})
```

---

## 9. THREAD SAFETY

### 9.1 Thread Safety Principles

**Rule 1: KafkaConsumer is NOT thread-safe**
- ALL Kafka operations in same thread (polling thread)
- Exception: Commit can be in separate thread WITH LOCK

**Rule 2: Queues are thread-safe**
- Use `queue.Queue` (thread-safe by default)
- No additional locking needed

**Rule 3: CSV writers are single-threaded**
- Each CSV file has ONE writer thread
- No concurrent writes

### 9.2 Shared Resources & Locks

| Resource | Access Pattern | Thread Safety |
|----------|----------------|---------------|
| `KafkaConsumer` | Polling thread only | ✅ Safe (single thread) |
| `processing_queue` | Producer: polling; Consumer: workers | ✅ Safe (Queue is thread-safe) |
| `processed_queue` | Producer: workers; Consumer: commit | ✅ Safe (Queue is thread-safe) |
| `backup_csv_queue` | Producer: polling; Consumer: CSV writer | ✅ Safe (Queue is thread-safe) |
| `dlq_queue` | Producer: workers; Consumer: CSV writer | ✅ Safe (Queue is thread-safe) |
| Backup CSV file | CSV writer thread only | ✅ Safe (single thread) |
| DLQ CSV file | CSV writer thread only | ✅ Safe (single thread) |
| `running` flag | All threads read; Main writes | ✅ Safe (atomic bool) |

### 9.3 KafkaConsumer Thread Safety (Critical)

**Problem:**
```python
# ❌ WRONG: Multiple threads touching consumer
Thread 1: consumer.poll()
Thread 2: consumer.commit()
Thread 3: consumer.seek()

Result: Undefined behavior, crashes, data corruption
```

**Solution:**
```python
# ✅ CORRECT: Single thread OR use lock

# Option A: All operations in polling thread
def polling_thread():
    while running:
        msgs = consumer.poll()
        # ... process ...
        consumer.commit()  # Same thread

# Option B: Separate commit thread WITH LOCK
consumer_lock = threading.Lock()

def polling_thread():
    while running:
        with consumer_lock:
            msgs = consumer.poll()

def commit_thread():
    while running:
        with consumer_lock:
            consumer.commit(offsets)
```

**Recommendation:** Option A (all in polling thread) is simpler

**Our Design:** Commit in separate thread, but polling thread doesn't commit
- Polling thread: Only polls
- Commit thread: Only commits
- No overlap, no lock needed

---

## 10. CONFIGURATION PARAMETERS

### 10.1 Complete Parameter List

#### **REQUIRED PARAMETERS**

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `bootstrap_servers` | string | Kafka broker addresses | `"localhost:9092"` |
| `topics` | list[string] | Topics to consume | `["orders", "payments"]` |
| `group_id` | string | Consumer group ID | `"order-processor-group"` |
| `processor_callable` | callable | Function to process messages | `process_message` |

---

#### **OPTIONAL PARAMETERS - KAFKA**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `poll_timeout_ms` | int | `1000` | Kafka poll timeout (milliseconds) |
| `max_poll_records` | int | `100` | Max messages per poll |
| `session_timeout_ms` | int | `10000` | Consumer session timeout |
| `heartbeat_interval_ms` | int | `3000` | Heartbeat interval |
| `max_poll_interval_ms` | int | `300000` | Max time between polls |
| `auto_offset_reset` | string | `"latest"` | Where to start if no offset (`"earliest"`, `"latest"`) |
| `enable_auto_commit` | bool | `False` | Use Kafka's auto-commit (NOT RECOMMENDED) |
| `isolation_level` | string | `"read_uncommitted"` | Transaction isolation level |

---

#### **OPTIONAL PARAMETERS - PROCESSING**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `worker_count` | int | `50` | Number of worker threads |
| `queue_size` | int | `200` | Processing queue capacity |
| `max_message_size` | int | `10485760` | Max message size in bytes (10MB) |
| `processing_timeout` | int | `None` | Timeout per message (seconds, None = no timeout) |
| `queue_put_timeout` | int | `60` | Timeout when queue full (seconds) |

---

#### **OPTIONAL PARAMETERS - COMMITS**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `commit_interval_seconds` | int | `5` | How often to commit offsets |
| `commit_on_shutdown` | bool | `True` | Commit during graceful shutdown |
| `dev_mode` | bool | `False` | **NEW:** Disable offset commits for testing/development |

---

#### **OPTIONAL PARAMETERS - CSV BACKUP**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `backup_enabled` | bool | `True` | Enable CSV backup |
| `backup_path` | string | `"kafka_backup.csv"` | Backup CSV file path |
| `csv_flush_interval_seconds` | int | `5` | How often to flush CSV |
| `csv_batch_size` | int | `1000` | Write after N messages |

---

#### **OPTIONAL PARAMETERS - DLQ**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dlq_path` | string | `"kafka_dlq.csv"` | DLQ CSV file path |
| `dlq_csv_batch_size` | int | `100` | DLQ batch size |

---

#### **OPTIONAL PARAMETERS - SHUTDOWN & CONTROL**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `shutdown_timeout_seconds` | int | `30` | Max time for graceful shutdown |
| `shutdown_drain_queue` | bool | `True` | Wait for queue to drain on shutdown |
| `stop_at_offset` | dict | `None` | **NEW:** Stop after reaching offsets: `{("topic", partition): offset}` |

---

#### **OPTIONAL PARAMETERS - LOGGING**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `log_level` | string | `"INFO"` | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `mod_name` | string | `"kafka_consumer"` | Mod name for logging context |

---

### 10.2 Configuration Examples

#### **Example 1: Minimal Configuration**

```python
# Absolute minimum required
result = run_mod("kafka_consumer", {
    "bootstrap_servers": "localhost:9092",
    "topics": ["orders"],
    "group_id": "order-processor",
    "processor_callable": process_order_message
})
```

#### **Example 2: Production Configuration**

```python
result = run_mod("kafka_consumer", {
    # Required
    "bootstrap_servers": "kafka1:9092,kafka2:9092,kafka3:9092",
    "topics": ["orders", "payments"],
    "group_id": "order-processor-prod",
    "processor_callable": process_message,
    
    # Kafka settings
    "poll_timeout_ms": 1000,
    "max_poll_records": 100,
    "auto_offset_reset": "earliest",
    
    # Processing settings
    "worker_count": 100,  # More workers for high throughput
    "queue_size": 500,
    "max_message_size": 5242880,  # 5MB
    "processing_timeout": 30,
    
    # Commit settings
    "commit_interval_seconds": 5,
    "dev_mode": False,  # Production mode - commits enabled
    
    # CSV backup
    "backup_enabled": True,
    "backup_path": "/data/backups/kafka_backup.csv",
    "csv_flush_interval_seconds": 10,
    
    # DLQ
    "dlq_path": "/data/dlq/kafka_dlq.csv",
    
    # Shutdown
    "shutdown_timeout_seconds": 60,
    
    # Logging
    "log_level": "INFO"
})
```

#### **Example 3: Development/Testing Configuration**

```python
result = run_mod("kafka_consumer", {
    # Required
    "bootstrap_servers": "localhost:9092",
    "topics": ["orders-test"],
    "group_id": "test-consumer",
    "processor_callable": test_processor,
    
    # Dev mode - no commits
    "dev_mode": True,  # Messages will be reprocessed on restart
    
    # Stop after processing first 100 messages
    "stop_at_offset": {
        ("orders-test", 0): 100,  # Stop at offset 100 for partition 0
        ("orders-test", 1): 50,   # Stop at offset 50 for partition 1
    },
    
    # Smaller configuration for testing
    "worker_count": 5,
    "queue_size": 20,
    
    # Keep backup for inspection
    "backup_enabled": True,
    "backup_path": "/tmp/test_backup.csv",
    
    # DLQ for test failures
    "dlq_path": "/tmp/test_dlq.csv",
    
    # Quick shutdown
    "shutdown_timeout_seconds": 10,
    
    # Verbose logging
    "log_level": "DEBUG"
})
```

#### **Example 4: High-Throughput Configuration**

```python
result = run_mod("kafka_consumer", {
    # Required
    "bootstrap_servers": "kafka:9092",
    "topics": ["high-volume-topic"],
    "group_id": "high-throughput-consumer",
    "processor_callable": fast_processor,
    
    # Maximize throughput
    "worker_count": 200,  # Many workers
    "queue_size": 1000,   # Large queue
    "max_poll_records": 500,  # Large batches
    "commit_interval_seconds": 10,  # Less frequent commits
    
    # Disable backup for speed (rely on Kafka retention)
    "backup_enabled": False,
    
    # Larger timeouts
    "processing_timeout": None,  # No timeout
    "shutdown_timeout_seconds": 120,
    
    # Production mode
    "dev_mode": False
})
```

#### **Example 5: Low-Latency Configuration**

```python
result = run_mod("kafka_consumer", {
    # Required
    "bootstrap_servers": "kafka:9092",
    "topics": ["realtime-events"],
    "group_id": "realtime-processor",
    "processor_callable": realtime_processor,
    
    # Minimize latency
    "worker_count": 20,  # Fewer workers (less contention)
    "queue_size": 50,    # Small queue
    "max_poll_records": 10,  # Small batches
    "commit_interval_seconds": 1,  # Frequent commits
    
    # CSV settings
    "csv_flush_interval_seconds": 1,  # Frequent flushes
    "csv_batch_size": 100,
    
    # Production mode
    "dev_mode": False
})
```

---

### 10.3 Configuration Validation

**Validation Rules:**

```python
def validate_config(config):
    # Required parameters
    required = ["bootstrap_servers", "topics", "group_id", "processor_callable"]
    for param in required:
        if param not in config:
            raise ValueError(f"Missing required parameter: {param}")
    
    # Type checks
    assert isinstance(config["bootstrap_servers"], str)
    assert isinstance(config["topics"], list) and len(config["topics"]) > 0
    assert isinstance(config["group_id"], str)
    assert callable(config["processor_callable"])
    
    # Range checks
    if "worker_count" in config:
        assert 1 <= config["worker_count"] <= 1000, "worker_count must be 1-1000"
    
    if "queue_size" in config:
        assert config["queue_size"] >= 10, "queue_size must be >= 10"
    
    if "commit_interval_seconds" in config:
        assert config["commit_interval_seconds"] >= 1, "commit_interval must be >= 1"
    
    # Dev mode validation
    if config.get("dev_mode", False):
        logger.warning("Dev mode enabled - offsets will NOT be committed. "
                      "All messages will be reprocessed on restart.")
    
    # Stop at offset validation
    if "stop_at_offset" in config and config["stop_at_offset"] is not None:
        assert isinstance(config["stop_at_offset"], dict), "stop_at_offset must be dict"
        for key, offset in config["stop_at_offset"].items():
            assert isinstance(key, tuple) and len(key) == 2, \
                "stop_at_offset keys must be (topic, partition) tuples"
            assert isinstance(offset, int) and offset >= 0, \
                "stop_at_offset values must be non-negative integers"
    
    # File path checks
    if config.get("backup_enabled", True):
        if "backup_path" not in config:
            raise ValueError("backup_path required when backup_enabled=True")
        
        # Check directory exists and is writable
        backup_dir = os.path.dirname(config["backup_path"])
        if not os.path.exists(backup_dir):
            raise ValueError(f"Backup directory does not exist: {backup_dir}")
        if not os.access(backup_dir, os.W_OK):
            raise ValueError(f"Backup directory not writable: {backup_dir}")
    
    # DLQ path check
    if "dlq_path" in config:
        dlq_dir = os.path.dirname(config["dlq_path"])
        if not os.path.exists(dlq_dir):
            raise ValueError(f"DLQ directory does not exist: {dlq_dir}")
```

---

## 11. LOGGING STRATEGY

### 11.1 Logging Levels

| Level | Usage | Examples |
|-------|-------|----------|
| **DEBUG** | Detailed flow, polling details | "Polled batch of 100 messages" |
| **INFO** | Important events, milestones | "Committed offsets", "Worker started" |
| **WARNING** | Recoverable issues | "Queue full, backpressure applied", "Dev mode enabled" |
| **ERROR** | Processing failures, retryable errors | "Message processing failed" |
| **CRITICAL** | System failures, fatal errors | "Worker crashed permanently" |

### 11.2 Key Log Events

#### **Startup Logs**

```python
logger.info("Kafka consumer starting", extra={
    "bootstrap_servers": config.bootstrap_servers,
    "topics": config.topics,
    "group_id": config.group_id,
    "worker_count": config.worker_count,
    "dev_mode": config.dev_mode,
    "stop_at_offset": config.stop_at_offset
})

if config.dev_mode:
    logger.warning("Dev mode enabled - offsets will NOT be committed")

if config.stop_at_offset:
    logger.info("Stop at offset configured", extra={
        "targets": config.stop_at_offset
    })

logger.info("All components started", extra={
    "total_threads": thread_count
})
```

#### **Polling Logs**

```python
# Successful poll
logger.debug("Batch polled", extra={
    "batch_size": len(messages),
    "partitions": list(set(msg.partition for msg in messages)),
    "offset_range": f"{min_offset}-{max_offset}",
    "poll_time_ms": poll_duration * 1000
})

# Stop at offset reached
if stop_at_offset_reached:
    logger.info("Stop at offset reached, triggering shutdown", extra={
        "topic": msg.topic,
        "partition": msg.partition,
        "offset": msg.offset,
        "target_offset": target_offset
    })

# Backpressure
logger.warning("Processing queue full, backpressure applied", extra={
    "queue_size": processing_queue.qsize(),
    "queue_capacity": config.queue_size
})
```

#### **Processing Logs**

```python
# Success
logger.info("Message processed successfully", extra={
    "partition": msg.partition,
    "offset": msg.offset,
    "processing_time_ms": duration * 1000,
    "worker_id": worker_id
})

# Failure
logger.error("Message processing failed", extra={
    "partition": msg.partition,
    "offset": msg.offset,
    "error_type": type(e).__name__,
    "error_message": str(e),
    "processing_time_ms": duration * 1000,
    "worker_id": worker_id,
    "stack_trace": traceback.format_exc()
})
```

#### **Commit Logs**

```python
# Successful commit (production mode)
if not config.dev_mode:
    logger.info("Offsets committed", extra={
        "partition_offsets": {p: o for p, o in partition_offsets.items()},
        "messages_committed": count,
        "commit_time_ms": commit_duration * 1000
    })
else:
    # Dev mode
    logger.info("Dev mode - would have committed offsets", extra={
        "partition_offsets": {p: o for p, o in partition_offsets.items()},
        "messages_processed": count
    })

# Gap warning
logger.warning("Gap in processed offsets", extra={
    "partition": partition,
    "committed_offset": committed,
    "gap_at": first_gap,
    "uncommitted_count": len(uncommitted_offsets)
})
```

#### **CSV Logs**

```python
# Backup CSV
logger.debug("Backup CSV batch written", extra={
    "messages_written": len(batch),
    "file_path": config.backup_path,
    "write_time_ms": duration * 1000
})

# DLQ CSV
logger.info("DLQ CSV batch written", extra={
    "failures_written": len(batch),
    "file_path": config.dlq_path
})
```

#### **Shutdown Logs**

```python
logger.info("Shutdown initiated", extra={
    "signal": signal_name,
    "queue_depth": processing_queue.qsize(),
    "active_workers": active_count,
    "reason": "signal" or "stop_at_offset" or "error"
})

logger.info("Shutdown complete", extra={
    "total_duration_ms": total_duration * 1000,
    "messages_committed": final_commit_count if not config.dev_mode else "skipped",
    "clean_shutdown": not timeout_exceeded,
    "dev_mode": config.dev_mode
})
```

### 11.3 Log Analysis Queries

**Using your tab-delimited format:**

**Query 1: Find slow messages**
```bash
grep "Message processed successfully" consumer.log | \
  awk -F'\t' '{print $7}' | \
  jq 'select(.processing_time_ms > 10000) | {offset: .offset, time: .processing_time_ms}'
```

**Query 2: Calculate throughput**
```bash
grep "Batch polled" consumer.log | \
  awk -F'\t' '{print $1, $7}' | \
  jq -r '[.timestamp, .batch_size] | @tsv' | \
  # Further processing to calculate msg/s
```

**Query 3: Error rate**
```bash
total=$(grep "Message processed" consumer.log | wc -l)
errors=$(grep "Message processing failed" consumer.log | wc -l)
echo "scale=2; $errors * 100 / $total" | bc
```

**Query 4: Top error types**
```bash
grep "Message processing failed" consumer.log | \
  awk -F'\t' '{print $7}' | \
  jq -r '.error_type' | \
  sort | uniq -c | sort -rn
```

**Query 5: Check if running in dev mode**
```bash
grep "Dev mode enabled" consumer.log
```

---

## 12. PERFORMANCE & THROUGHPUT

### 12.1 Throughput Formula

```
Throughput (msg/s) = Worker Count / Avg Processing Time (s)
```

**Example:**
- 50 workers
- Avg processing time: 5 seconds
- Throughput: 50 / 5 = **10 msg/s**

### 12.2 Bottleneck Analysis

| Component | Typical Performance | Bottleneck? |
|-----------|---------------------|-------------|
| Kafka polling | 1-5ms per poll | ❌ NO (very fast) |
| Queue operations | <0.1ms | ❌ NO (memory operation) |
| Message processing | 1-30 seconds | ✅ **YES** (main bottleneck) |
| Offset commits | 10-50ms | ❌ NO (infrequent) |
| CSV writes | 10-100ms per batch | ❌ NO (async) |

**Conclusion:** Processing time is the bottleneck

**Note:** Dev mode has zero commit overhead but doesn't improve throughput

### 12.3 Optimization Strategies

#### **Strategy 1: Increase Workers**

```python
# Before: 50 workers, 5s processing = 10 msg/s
"worker_count": 50

# After: 100 workers, 5s processing = 20 msg/s
"worker_count": 100

# Impact: 2x throughput
```

**Trade-off:** More memory (each worker ~8MB) and CPU context switching

---

#### **Strategy 2: Optimize Processing Logic**

```python
# Before: No connection pooling
def process_message(msg):
    response = requests.post(url, json=msg)  # New connection each time
    # Takes 5 seconds

# After: Connection pooling
session = requests.Session()

def process_message(msg):
    response = session.post(url, json=msg)  # Reuse connection
    # Takes 3 seconds

# Impact: 5/3 = 1.67x throughput
```

---

#### **Strategy 3: Batch Polling**

```python
# Before: max_poll_records = 1
"max_poll_records": 1  # 5ms per message

# After: max_poll_records = 100
"max_poll_records": 100  # 5ms per 100 messages

# Impact: Negligible (polling not bottleneck)
```

---

#### **Strategy 4: Reduce Processing Time**

```python
# Async I/O (if using asyncio workers)
# Parallel API calls
# Caching
# Pre-computed lookups
```

---

### 12.4 Expected Throughput

**Assumptions:**
- Kafka: 3 partitions, 3 replicas
- Network: 1Gbps
- Processing: REST API call (average 3 seconds)

| Worker Count | Avg Processing Time | Expected Throughput |
|--------------|---------------------|---------------------|
| 10 | 3s | 3.3 msg/s |
| 25 | 3s | 8.3 msg/s |
| 50 | 3s | 16.7 msg/s |
| 100 | 3s | 33.3 msg/s |
| 200 | 3s | 66.7 msg/s |

**Reality Check:**
- CSV writes: ~100 msg/s capacity (not bottleneck)
- Kafka consumer: ~1000 msg/s capacity (not bottleneck)
- **Processing is always the limit**

**Dev Mode Impact:** No performance difference (commits are async and infrequent)

---

### 12.5 Memory Usage Estimation

```
Total Memory = Base + (Workers × Worker Memory) + Queue Memory

Base Memory: ~50 MB (Python runtime, Kafka consumer)
Worker Memory: ~8 MB per thread
Queue Memory: queue_size × avg_message_size

Example (50 workers, 200 queue, 10KB messages):
= 50 MB + (50 × 8 MB) + (200 × 10 KB)
= 50 MB + 400 MB + 2 MB
= 452 MB

Recommendation: Allocate 1GB for safety
```

---

## 13. TESTING STRATEGY

### 13.1 Unit Tests

**Test Coverage:**

1. **Max Contiguous Offset Algorithm**
   - No gaps
   - Single gap
   - Multiple gaps
   - Empty input
   - Single element

2. **Configuration Validation**
   - Missing required params
   - Invalid types
   - Out of range values
   - Invalid file paths
   - Dev mode flag
   - Stop at offset format

3. **Message Size Validation**
   - Normal size
   - Exactly at limit
   - Over limit

4. **CSV Row Formatting**
   - Standard message
   - Message with special characters
   - Null key
   - Large payload

5. **Stop at Offset Logic**
   - Single partition stop
   - Multi-partition stop
   - Offset boundary conditions

---

### 13.2 Integration Tests

**Test Scenarios:**

1. **Happy Path**
   - Start consumer
   - Produce 100 messages
   - Verify all processed
   - Verify offsets committed (if not dev mode)
   - Verify backup CSV written

2. **Processing Failures**
   - Produce 100 messages (50 valid, 50 invalid)
   - Verify 50 succeed, 50 go to DLQ
   - Verify all offsets committed (if not dev mode)
   - Verify DLQ CSV has 50 entries

3. **Backpressure**
   - Queue size: 10
   - Slow workers (10s per message)
   - Fast producer (100 msg/s)
   - Verify queue fills, polling pauses

4. **Graceful Shutdown**
   - Start consumer
   - Process 50 messages
   - Send SIGTERM
   - Verify queue drains
   - Verify final commit (if not dev mode)
   - Verify clean exit

5. **Timeout Shutdown**
   - Start consumer
   - Slow workers (60s per message)
   - Queue has 100 messages
   - Send SIGTERM
   - Wait 30s (timeout)
   - Verify forced shutdown
   - Verify partial commit (if not dev mode)

6. **Dev Mode Test**
   - Start consumer with dev_mode=True
   - Process 100 messages
   - Verify no commits made
   - Restart consumer
   - Verify all 100 messages reprocessed

7. **Stop at Offset Test**
   - Configure stop_at_offset={("topic", 0): 50}
   - Start consumer
   - Verify stops after offset 50
   - Verify messages up to 50 processed
   - Verify graceful shutdown triggered

---

### 13.3 Failure Injection Tests

**Test Scenarios:**

1. **Kafka Connection Loss**
   - Start consumer
   - Kill Kafka broker
   - Verify consumer retries
   - Bring broker back up
   - Verify consumer recovers

2. **Disk Full (CSV Writes)**
   - Start consumer
   - Fill disk
   - Verify CSV writes fail gracefully
   - Verify consumer continues (logs error)

3. **Worker Thread Crash**
   - Start consumer
   - Inject exception in worker
   - Verify worker restarts
   - Verify processing continues

4. **Processing Timeout**
   - Start consumer with processing_timeout=5
   - Slow processor (10s)
   - Verify timeout, message goes to DLQ
   - Verify consumer continues

5. **Out of Memory**
   - Start consumer
   - Huge messages (>100MB)
   - Verify message rejected
   - Verify consumer continues

6. **Dev Mode Restart Test**
   - Start with dev_mode=True
   - Process 50 messages
   - Crash consumer
   - Restart
   - Verify all 50 reprocessed (no commits were made)

---

## 14. OPERATIONAL RUNBOOKS

### 14.1 Runbook: Consumer Lag Increasing

**Symptoms:**
- Messages piling up in Kafka
- Consumer lag metrics climbing

**Diagnosis:**
1. Check worker count vs processing time
2. Check if workers are idle or busy
3. Check for slow messages (p99 processing time)

**Resolution:**
1. Increase worker count
2. Optimize processing logic
3. Add more consumer instances (scale horizontally)

---

### 14.2 Runbook: High DLQ Rate

**Symptoms:**
- Many messages in DLQ CSV
- DLQ write logs frequent

**Diagnosis:**
1. Check top error types in logs
2. Check if specific message pattern failing
3. Check downstream API health

**Resolution:**
1. Fix root cause (API timeout, validation, etc.)
2. Reprocess DLQ via separate job
3. If transient issue, DLQ will drain naturally

---

### 14.3 Runbook: CSV Disk Full

**Symptoms:**
- CSV write errors in logs
- Disk usage at 100%

**Diagnosis:**
1. Check CSV file sizes
2. Check if rotation is working
3. Check if archives are being cleaned

**Resolution:**
1. Immediate: Delete old archives
2. Short-term: Set up proper logrotate
3. Long-term: Increase disk or move to S3

---

### 14.4 Runbook: Consumer Won't Stop

**Symptoms:**
- SIGTERM sent, consumer still running after 60s
- K8s pod stuck terminating

**Diagnosis:**
1. Check shutdown logs
2. Check if workers are stuck on slow messages
3. Check if queue is draining

**Resolution:**
1. Wait for timeout (30-60s)
2. If still stuck, SIGKILL (force kill)
3. Investigate why workers were stuck
4. Adjust shutdown_timeout if needed

---

### 14.5 Runbook: Offset Committed But Messages Not Processed

**Symptoms:**
- Offset committed
- But data missing in downstream system
- No errors in logs

**Diagnosis:**
1. Check if consumer crashed between commit and processing
2. Check backup CSV for messages
3. Check if processing silently failed

**Resolution:**
1. Replay from backup CSV
2. Fix processing logic (add better error handling)
3. Consider increasing commit interval

---

### 14.6 Runbook: Dev Mode Running in Production

**Symptoms:**
- Messages being reprocessed on every restart
- No offset commits in logs
- Log shows "Dev mode enabled" warning

**Diagnosis:**
1. Check configuration: `dev_mode` parameter
2. Verify environment (dev vs prod)

**Resolution:**
1. IMMEDIATE: Stop consumer
2. Change config: Set `dev_mode=False`
3. Restart consumer
4. Verify commits are happening
5. Review why dev config was used in production

---

### 14.7 Runbook: Stop at Offset Not Triggering

**Symptoms:**
- Consumer configured with stop_at_offset
- Consumer continues past target offset

**Diagnosis:**
1. Check stop_at_offset configuration format
2. Verify (topic, partition) tuple matches actual topic/partition
3. Check if offset target already passed

**Resolution:**
1. Verify configuration: `stop_at_offset={("topic", partition): offset}`
2. Check current consumer position
3. If target already passed, consumer will not stop
4. Restart consumer with new target offset if needed

---

## 15. COMPLETE PARAMETER REFERENCE

### Quick Reference Table

| Parameter | Required? | Default | Min | Max | Type |
|-----------|-----------|---------|-----|-----|------|
| `bootstrap_servers` | ✅ | - | - | - | string |
| `topics` | ✅ | - | 1 topic | - | list[string] |
| `group_id` | ✅ | - | - | - | string |
| `processor_callable` | ✅ | - | - | - | callable |
| `poll_timeout_ms` | ❌ | 1000 | 100 | 60000 | int |
| `max_poll_records` | ❌ | 100 | 1 | 10000 | int |
| `worker_count` | ❌ | 50 | 1 | 1000 | int |
| `queue_size` | ❌ | 200 | 10 | 100000 | int |
| `max_message_size` | ❌ | 10485760 | 1024 | 1GB | int |
| `processing_timeout` | ❌ | None | 1 | 3600 | int/None |
| `commit_interval_seconds` | ❌ | 5 | 1 | 300 | int |
| `dev_mode` | ❌ | **False** | - | - | bool |
| `backup_enabled` | ❌ | True | - | - | bool |
| `backup_path` | Conditional | "kafka_backup.csv" | - | - | string |
| `dlq_path` | ❌ | "kafka_dlq.csv" | - | - | string |
| `csv_flush_interval_seconds` | ❌ | 5 | 1 | 300 | int |
| `shutdown_timeout_seconds` | ❌ | 30 | 5 | 300 | int |
| `stop_at_offset` | ❌ | **None** | - | - | dict |
| `log_level` | ❌ | "INFO" | - | - | enum |

---

### New Parameters Detail

#### **dev_mode**
- **Type:** bool
- **Default:** False
- **Description:** When True, disables offset commits. Messages will be reprocessed on every restart. Useful for development and testing.
- **Use Cases:**
  - Testing message processing logic
  - Debugging without affecting committed offsets
  - Development environments
  - Idempotency testing
- **Warning:** Never use in production - will cause infinite reprocessing

#### **stop_at_offset**
- **Type:** dict or None
- **Default:** None
- **Format:** `{(topic, partition): offset, ...}`
- **Description:** Stop consuming when reaching specified offsets for each partition. Triggers graceful shutdown.
- **Example:**
  ```python
  {
      ("orders", 0): 1000,  # Stop at offset 1000 for orders partition 0
      ("orders", 1): 500,   # Stop at offset 500 for orders partition 1
  }
  ```
- **Use Cases:**
  - Testing with specific data ranges
  - Controlled processing for data migration
  - Debugging specific offset ranges
  - Integration testing with known datasets
- **Behavior:**
  - Consumer stops when ANY configured offset is reached
  - Graceful shutdown is triggered
  - All in-flight messages are processed
  - Final commit performed (if not dev_mode)

---

## DOCUMENT END

---

**Document Version:** 1.1.0  
**Last Updated:** 2024-11-27  
**Status:** Ready for Implementation  
**Next Steps:** Code implementation based on this design  

**Changes from v1.0.0:**
- Added `dev_mode` parameter for testing without commits
- Added `stop_at_offset` parameter for controlled processing
- Updated commit thread logic to handle dev_mode
- Updated polling thread logic to handle stop_at_offset
- Updated shutdown sequence for stop_at_offset trigger
- Added new runbooks for dev_mode and stop_at_offset scenarios
- Updated logging to reflect new parameters