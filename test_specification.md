# KAFKA CONSUMER - TEST SPECIFICATION

## OVERVIEW

This document specifies all test scenarios, edge cases, and requirements for achieving **95% code coverage** of the Kafka consumer implementation.

**Target:** 95% code coverage across all 7 modules

**Testing Framework:** pytest with coverage.py

**Test Structure:**
```
tests/
├── unit/                       # Unit tests (isolated components)
│   ├── test_shared_state.py
│   ├── test_signal_handler.py
│   ├── test_offset_manager.py
│   ├── test_worker_pool.py
│   └── test_csv_writers.py
├── integration/                # Integration tests (with mock Kafka)
│   ├── test_polling_thread.py
│   ├── test_commit_flow.py
│   ├── test_csv_rotation.py
│   └── test_dlq_flow.py
├── e2e/                        # End-to-end tests (with real Kafka)
│   ├── test_happy_path.py
│   ├── test_failure_scenarios.py
│   ├── test_shutdown.py
│   └── test_performance.py
└── conftest.py                 # Shared fixtures
```

---

## TEST REQUIREMENTS

### Coverage Goals

| Module | Target Coverage | Critical Paths |
|--------|----------------|----------------|
| shared_state.py | 95% | Config validation, logger setup |
| signal_handler.py | 100% | All signal handling paths |
| kafka_consumer.py | 95% | Shutdown sequences, error handling |
| polling_thread.py | 95% | Backpressure, stop_at_offset |
| worker_pool.py | 95% | Success/failure paths, worker crashes |
| offset_manager.py | 100% | Max contiguous algorithm, dev_mode |
| csv_writers.py | 95% | Hourly rotation, final flush, write failures |

**Overall Target: 95%+**

---

## UNIT TESTS

### 1. test_shared_state.py

#### Test: Config Validation - Valid Inputs
```
Purpose: Ensure valid configs pass validation
Setup: Create config with all valid parameters
Test: Call validate_config(), should not raise
Coverage: config validation success path
```

#### Test: Config Validation - Missing Required Params
```
Purpose: Catch missing required parameters
Test Cases:
  - Missing bootstrap_servers → AssertionError
  - Missing topic → AssertionError
  - Missing group_id → AssertionError
  - Missing processor_callable → AssertionError
Coverage: validation error paths
```

#### Test: Config Validation - Invalid Types
```
Purpose: Catch type errors
Test Cases:
  - bootstrap_servers as int → AssertionError
  - topic as list → AssertionError
  - processor_callable not callable → AssertionError
Coverage: type checking
```

#### Test: Config Validation - Out of Range Values
```
Purpose: Catch invalid ranges
Test Cases:
  - worker_count = 0 → AssertionError
  - worker_count = 2000 → AssertionError
  - queue_size = 5 → AssertionError
  - commit_interval_seconds = 0 → AssertionError
Coverage: range validation
```

#### Test: Config Validation - Invalid File Paths
```
Purpose: Catch filesystem issues
Setup: Create temp directory
Test Cases:
  - backup_path directory doesn't exist → AssertionError
  - backup_path directory not writable → AssertionError
  - dlq_path directory doesn't exist → AssertionError
Coverage: file path validation
```

#### Test: Config Validation - stop_at_offset Format
```
Purpose: Validate stop_at_offset structure
Test Cases:
  - Valid: {("topic", 0): 100} → Pass
  - Invalid: {("topic",): 100} → AssertionError (wrong tuple size)
  - Invalid: {"topic": 100} → AssertionError (not tuple key)
  - Invalid: {("topic", 0): -1} → AssertionError (negative offset)
Coverage: stop_at_offset validation
```

#### Test: SharedState Initialization
```
Purpose: Ensure SharedState initializes correctly
Setup: Create config
Test: 
  - Create SharedState(config)
  - Verify running event is set
  - Verify shutdown_event is not set
  - Verify queues created
  - Verify logger initialized
Coverage: SharedState.__init__
```

#### Test: SharedState.stop()
```
Purpose: Verify stop() clears running flag
Setup: Create SharedState
Test:
  - Call stop()
  - Verify running.is_set() == False
  - Verify shutdown_event.is_set() == True
Coverage: SharedState.stop()
```

#### Test: Logger Setup - INFO Level
```
Purpose: Verify logger configured correctly
Setup: Config with log_level="INFO"
Test: Verify logger.level == logging.INFO
Coverage: _setup_logger()
```

#### Test: Logger Setup - DEBUG Level
```
Purpose: Verify DEBUG level works
Setup: Config with log_level="DEBUG"
Test: Verify logger.level == logging.DEBUG
Coverage: _setup_logger()
```

---

### 2. test_signal_handler.py

#### Test: Signal Handler Registration
```
Purpose: Verify signals registered
Setup: Create SignalHandler
Test:
  - Call register()
  - Verify handlers registered for SIGTERM, SIGINT
  - Verify original handlers stored
Coverage: register()
```

#### Test: Signal Handler Callback Execution
```
Purpose: Verify callback called on signal
Setup: 
  - Create mock callback
  - Create SignalHandler(callback=mock)
  - Register handlers
Test:
  - Send SIGTERM
  - Verify callback called once
  - Verify shutdown_event set
Coverage: _handle_signal()
```

#### Test: Signal Handler Without Callback
```
Purpose: Verify works without callback
Setup: SignalHandler(callback=None)
Test:
  - Send SIGTERM
  - Verify no exception
  - Verify shutdown_event set
Coverage: _handle_signal() with None callback
```

#### Test: Signal Handler Callback Exception
```
Purpose: Verify exceptions in callback handled
Setup:
  - Callback that raises exception
  - Create SignalHandler(callback=bad_callback)
Test:
  - Send SIGTERM
  - Verify exception logged
  - Verify shutdown_event still set
Coverage: exception handling in _handle_signal()
```

#### Test: Signal Handler Wait
```
Purpose: Verify wait() blocks until signal
Setup: 
  - Create SignalHandler
  - Register handlers
Test:
  - Start wait() in thread
  - Send SIGTERM
  - Verify wait() returns
Coverage: wait()
```

#### Test: Signal Handler Restore
```
Purpose: Verify original handlers restored
Setup:
  - Store original handlers
  - Create and register SignalHandler
Test:
  - Call restore()
  - Verify original handlers back in place
Coverage: restore()
```

---

### 3. test_offset_manager.py

#### Test: Max Contiguous - Perfect Sequence
```
Purpose: Test all contiguous offsets
Input:
  last_committed = 1000
  processed = {1001, 1002, 1003, 1004, 1005}
Expected: 1005
Coverage: find_max_contiguous() success path
```

#### Test: Max Contiguous - Single Gap
```
Purpose: Test stop at gap
Input:
  last_committed = 1000
  processed = {1001, 1002, 1003, 1005, 1006}
Expected: 1003 (gap at 1004)
Coverage: gap detection
```

#### Test: Max Contiguous - Multiple Gaps
```
Purpose: Test stop at first gap
Input:
  last_committed = 1000
  processed = {1001, 1003, 1005, 1007}
Expected: 1001 (gap at 1002)
Coverage: first gap stops scan
```

#### Test: Max Contiguous - Empty Set
```
Purpose: Test with no processed offsets
Input:
  last_committed = 1000
  processed = {}
Expected: 1000 (no change)
Coverage: empty set handling
```

#### Test: Max Contiguous - Single Offset
```
Purpose: Test with one offset
Input:
  last_committed = 1000
  processed = {1001}
Expected: 1001
Coverage: single element
```

#### Test: Max Contiguous - Gap at Start
```
Purpose: Test immediate gap
Input:
  last_committed = 1000
  processed = {1002, 1003, 1004}
Expected: 1000 (gap at 1001)
Coverage: gap at beginning
```

#### Test: Max Contiguous - Out of Order Input
```
Purpose: Test sorting works
Input:
  last_committed = 1000
  processed = {1005, 1001, 1003, 1002, 1004} (unsorted)
Expected: 1005 (should sort internally)
Coverage: sorting logic
```

#### Test: Commit Thread - Normal Operation
```
Purpose: Test commit loop logic
Setup: Mock processed_queue with offsets
Test:
  - Drain queue
  - Calculate max contiguous
  - Verify correct offsets to commit
Coverage: commit_loop() main logic
```

#### Test: Commit Thread - Dev Mode Skip
```
Purpose: Verify dev_mode skips commits
Setup: config.dev_mode = True
Test:
  - Process offsets
  - Verify no actual commit called
  - Verify log message about dev mode
Coverage: dev_mode branch
```

#### Test: Commit Thread - Commit Failure
```
Purpose: Test commit error handling
Setup: Mock consumer.commit() to raise exception
Test:
  - Attempt commit
  - Verify exception logged
  - Verify thread continues (no crash)
Coverage: commit exception handling
```

#### Test: Commit Thread - Multi-Partition
```
Purpose: Test multiple partitions
Setup: Offsets for partitions 0, 1, 2
Test:
  - Calculate max contiguous per partition
  - Verify correct commit for each
Coverage: multi-partition logic
```

#### Test: Commit Thread - Remove Committed from Tracking
```
Purpose: Verify offsets removed after commit
Setup: 
  - processed_offsets = {1001, 1002, 1003, 1005}
  - Commit up to 1003
Test:
  - Verify processed_offsets = {1005} after commit
Coverage: offset cleanup
```

---

### 4. test_worker_pool.py

#### Test: Worker - Successful Processing
```
Purpose: Test success path
Setup: Mock processor_callable returns success
Test:
  - Put message in processing_queue
  - Verify worker processes
  - Verify (partition, offset, "success") in processed_queue
Coverage: success path
```

#### Test: Worker - Processing Exception
```
Purpose: Test failure path
Setup: Mock processor_callable raises ValueError
Test:
  - Put message in processing_queue
  - Verify exception caught
  - Verify message in dlq_queue
  - Verify (partition, offset, "failed") in processed_queue
Coverage: exception handling
```

#### Test: Worker - Message Too Large
```
Purpose: Test size validation
Setup: 
  - config.max_message_size = 1000
  - Message size = 2000
Test:
  - Verify message rejected
  - Verify in dlq_queue with "MessageTooLargeError"
  - Verify marked as processed
Coverage: size check branch
```

#### Test: Worker - Processing Timeout (if implemented)
```
Purpose: Test timeout handling
Setup: 
  - config.processing_timeout = 5
  - processor_callable sleeps 10 seconds
Test:
  - Verify timeout triggered
  - Verify message in dlq_queue
Coverage: timeout logic
```

#### Test: Worker - Worker Thread Crash
```
Purpose: Test worker crash handling
Setup: Simulate exception in worker loop itself (not in processor)
Test:
  - Trigger worker crash
  - Verify message in dlq_queue
  - Verify marked as processed
  - Verify CRITICAL log
Coverage: outer exception handler
```

#### Test: Worker - Queue Empty Handling
```
Purpose: Test Empty exception handling
Setup: Empty processing_queue
Test:
  - Worker attempts get()
  - Verify queue.Empty caught
  - Verify worker continues (no crash)
Coverage: Empty exception branch
```

#### Test: Worker - UTF-8 Decode Error
```
Purpose: Test invalid UTF-8 handling
Setup: Message with invalid UTF-8 bytes
Test:
  - Verify decode error caught
  - Verify message in dlq_queue
Coverage: decode error handling
```

#### Test: Worker - Shutdown While Processing
```
Purpose: Test running flag check
Setup:
  - Start worker
  - Set running = False
Test:
  - Verify worker finishes current message
  - Verify worker exits after queue empty
Coverage: shutdown logic
```

---

### 5. test_csv_writers.py

#### Test: Backup CSV - Initial Creation
```
Purpose: Test CSV file created
Setup: New backup_path
Test:
  - Start backup_csv_loop
  - Verify file created
  - Verify header written
Coverage: file initialization
```

#### Test: Backup CSV - Batch Writing
```
Purpose: Test batch logic
Setup: csv_batch_size = 10
Test:
  - Add 15 messages to queue
  - Verify write triggered at 10
  - Verify remaining 5 in batch
Coverage: batch size trigger
```

#### Test: Backup CSV - Time-Based Flush
```
Purpose: Test flush interval
Setup: csv_flush_interval_seconds = 5
Test:
  - Add 5 messages
  - Wait 5 seconds
  - Verify flush triggered
Coverage: time-based flush
```

#### Test: Backup CSV - Hourly Rotation
```
Purpose: Test rotation logic
Setup: Start at 10:59:50
Test:
  - Advance time to 11:00:10
  - Verify old file closed
  - Verify new file created with new hour
  - Verify header written to new file
Coverage: rotation logic
```

#### Test: Backup CSV - Rotation Failure Triggers Shutdown
```
Purpose: Test rotation error handling
Setup: Mock file open to fail on rotation
Test:
  - Trigger rotation
  - Verify CRITICAL log
  - Verify shared_state.stop() called
Coverage: rotation error branch
```

#### Test: Backup CSV - Write Failure Triggers Shutdown
```
Purpose: Test write error handling
Setup: Mock csv_writer.writerows() to raise IOError
Test:
  - Attempt write
  - Verify CRITICAL log
  - Verify shared_state.stop() called
Coverage: write error branch
```

#### Test: Backup CSV - Final Flush on Shutdown
```
Purpose: Test final flush
Setup:
  - Add 5 messages to batch
  - Set running = False
Test:
  - Verify final flush executed
  - Verify all 5 messages written
  - Verify file closed
Coverage: final flush logic
```

#### Test: DLQ CSV - Normal Operation
```
Purpose: Test DLQ writing
Setup: Add failures to dlq_queue
Test:
  - Verify rows written with error details
  - Verify stack trace included
Coverage: DLQ write logic
```

#### Test: DLQ CSV - Batch Writing
```
Purpose: Test DLQ batch logic
Setup: dlq_csv_batch_size = 100
Test:
  - Add 150 failures
  - Verify batch write at 100
Coverage: DLQ batch logic
```

#### Test: DLQ CSV - Write Failure Triggers Shutdown
```
Purpose: Test DLQ write error
Setup: Mock write to fail
Test:
  - Verify CRITICAL log
  - Verify shutdown triggered
Coverage: DLQ error handling
```

---

## INTEGRATION TESTS

### 1. test_polling_thread.py

#### Test: Polling - Normal Message Flow
```
Purpose: Test polling and distribution
Setup: Mock Kafka with 100 messages
Test:
  - Start polling thread
  - Verify messages polled in batches
  - Verify messages in processing_queue
  - Verify messages in backup_csv_queue
Coverage: polling_loop() main logic
```

#### Test: Polling - Backpressure
```
Purpose: Test queue full handling
Setup:
  - processing_queue maxsize = 10
  - Slow workers (don't drain queue)
Test:
  - Poll 50 messages
  - Verify polling blocks when queue full
  - Verify backpressure applied
Coverage: queue.put() timeout branch
```

#### Test: Polling - Backup Queue Full (Non-Blocking)
```
Purpose: Test backup queue non-blocking
Setup: backup_csv_queue full (artificially)
Test:
  - Poll messages
  - Verify put_nowait() skips if full
  - Verify warning logged
  - Verify processing continues
Coverage: backup queue Full exception
```

#### Test: Polling - Stop at Offset Trigger
```
Purpose: Test stop_at_offset functionality
Setup: config.stop_at_offset = {("topic", 0): 100}
Test:
  - Poll messages up to offset 100
  - Verify shared_state.stop() called
  - Verify INFO log about reaching target
Coverage: stop_at_offset logic
```

#### Test: Polling - Kafka Poll Error
```
Purpose: Test Kafka error handling
Setup: Mock consumer.poll() to raise KafkaException
Test:
  - Trigger poll error
  - Verify error logged
  - Verify thread continues (no crash)
  - Verify retry after pause
Coverage: exception handling
```

#### Test: Polling - UTF-8 Decode Error
```
Purpose: Test invalid message handling
Setup: Message with invalid UTF-8 in value
Test:
  - Poll invalid message
  - Verify decode error caught
  - Verify message in DLQ
  - Verify polling continues
Coverage: decode error branch
```

---

### 2. test_commit_flow.py

#### Test: Commit Flow - End to End
```
Purpose: Test complete commit flow
Setup:
  - Start all threads
  - Process 100 messages
Test:
  - Verify offsets collected in processed_queue
  - Verify commit thread calculates max contiguous
  - Verify commit to Kafka
  - Verify offsets updated
Coverage: full commit flow
```

#### Test: Commit Flow - With Failures
```
Purpose: Test commit with failed messages
Setup: 50 success, 50 failures
Test:
  - Verify all 100 in processed_queue
  - Verify all 100 committed (failures included)
  - Verify 50 in DLQ
Coverage: failure commit behavior
```

#### Test: Commit Flow - Dev Mode
```
Purpose: Test dev_mode skips commits
Setup: config.dev_mode = True
Test:
  - Process 100 messages
  - Verify no Kafka commit called
  - Verify log message about dev mode
  - Verify offsets cleared (simulated commit)
Coverage: dev_mode branch
```

---

### 3. test_csv_rotation.py

#### Test: CSV Rotation - Hour Boundary
```
Purpose: Test rotation at hour change
Setup: 
  - Start at 10:59:50
  - Mock time advancement
Test:
  - Process messages
  - Advance to 11:00:10
  - Verify old file (10.csv) closed
  - Verify new file (11.csv) created
  - Verify messages split correctly
Coverage: rotation logic
```

#### Test: CSV Rotation - Multiple Rotations
```
Purpose: Test multiple rotations
Setup: Run for 3 hours (mocked time)
Test:
  - Verify 3 files created (hour_10, hour_11, hour_12)
  - Verify each has correct data
  - Verify headers in each
Coverage: multiple rotations
```

#### Test: CSV Rotation - Rotation During Shutdown
```
Purpose: Test rotation coincides with shutdown
Setup: Trigger rotation and shutdown simultaneously
Test:
  - Verify rotation completes
  - Verify new file gets final flush
  - Verify clean shutdown
Coverage: edge case
```

---

### 4. test_dlq_flow.py

#### Test: DLQ Flow - End to End
```
Purpose: Test complete DLQ flow
Setup: Messages that fail processing
Test:
  - Verify exception caught in worker
  - Verify message in dlq_queue
  - Verify DLQ CSV writer writes entry
  - Verify all error details present
Coverage: DLQ flow
```

#### Test: DLQ Flow - Multiple Error Types
```
Purpose: Test different exceptions
Setup: 
  - ValueError
  - KeyError
  - TimeoutError
  - Custom exception
Test:
  - Verify all captured in DLQ
  - Verify error_type field correct
  - Verify stack traces present
Coverage: error type handling
```

---

## END-TO-END TESTS

### 1. test_happy_path.py

#### Test: E2E - Complete Happy Path
```
Purpose: Test full system with real Kafka
Setup:
  - Start real Kafka container (testcontainers)
  - Produce 1000 messages
  - Start consumer
Test:
  - Process all 1000 messages
  - Verify all committed
  - Verify backup CSV has 1000 entries
  - Verify no DLQ entries
  - Verify consumer lag = 0
Duration: ~30 seconds
Coverage: complete integration
```

#### Test: E2E - Multiple Partitions
```
Purpose: Test multi-partition handling
Setup:
  - Topic with 3 partitions
  - Produce 300 messages (100 per partition)
Test:
  - Verify all partitions consumed
  - Verify offsets committed per partition
  - Verify correct distribution
Coverage: partition handling
```

#### Test: E2E - High Throughput
```
Purpose: Test performance
Setup: Produce 10,000 messages rapidly
Test:
  - Measure throughput (msg/s)
  - Verify all processed
  - Verify no errors
  - Verify memory stable
Target: > 100 msg/s
Coverage: performance
```

---

### 2. test_failure_scenarios.py

#### Test: E2E - Mixed Success and Failure
```
Purpose: Test realistic failure rate
Setup: 
  - Produce 1000 messages
  - 900 valid, 100 invalid (will fail)
Test:
  - Verify 900 succeed
  - Verify 100 in DLQ
  - Verify all 1000 committed
  - Verify consumer continues
Coverage: failure handling
```

#### Test: E2E - All Messages Fail
```
Purpose: Test high failure rate
Setup: All messages invalid
Test:
  - Verify all go to DLQ
  - Verify all committed
  - Verify consumer doesn't crash
Coverage: extreme failure
```

#### Test: E2E - Slow Processing
```
Purpose: Test backpressure
Setup:
  - processor_callable sleeps 1s per message
  - Produce 100 messages rapidly
Test:
  - Verify queue fills up
  - Verify backpressure applied
  - Verify no message loss
  - Verify eventual processing
Coverage: backpressure
```

#### Test: E2E - Worker Crash Recovery
```
Purpose: Test worker crash handling
Setup: Inject exception in worker thread
Test:
  - Verify message in DLQ
  - Verify worker marked crashed
  - Verify processing continues with other workers
Coverage: worker crash
```

#### Test: E2E - Kafka Connection Loss
```
Purpose: Test Kafka disconnect
Setup:
  - Start processing
  - Stop Kafka broker mid-stream
Test:
  - Verify poll errors logged
  - Verify consumer retries
  - Restart Kafka
  - Verify consumer recovers
Coverage: connection loss
```

#### Test: E2E - Disk Full (CSV Write)
```
Purpose: Test disk full handling
Setup: Fill disk to capacity (mocked)
Test:
  - Trigger CSV write
  - Verify write fails
  - Verify CRITICAL log
  - Verify graceful shutdown triggered
Coverage: disk full handling
```

---

### 3. test_shutdown.py

#### Test: E2E - Clean Shutdown
```
Purpose: Test graceful shutdown
Setup:
  - Start consumer
  - Process 100 messages
  - 50 in queue
Test:
  - Send SIGTERM
  - Verify polling stops
  - Verify 50 messages processed
  - Verify final commit
  - Verify CSV flush
  - Verify clean exit
Duration: ~30 seconds
Coverage: shutdown sequence
```

#### Test: E2E - Shutdown with Slow Workers
```
Purpose: Test timeout behavior
Setup:
  - processor_callable sleeps 60s
  - 5 messages in progress
Test:
  - Send SIGTERM
  - Verify timeout warning at 30s
  - Verify workers continue anyway
  - Verify all finish eventually
  - Verify commit after finish
Duration: ~60 seconds
Coverage: timeout handling
```

#### Test: E2E - Shutdown During Commit
```
Purpose: Test shutdown timing edge case
Setup: Trigger shutdown during commit call
Test:
  - Verify commit completes or fails atomically
  - Verify no partial commits
  - Verify clean state on restart
Coverage: commit race condition
```

#### Test: E2E - Shutdown During CSV Rotation
```
Purpose: Test rotation during shutdown
Setup: Trigger shutdown at hour boundary
Test:
  - Verify rotation completes
  - Verify new file closed cleanly
  - Verify no data loss
Coverage: rotation edge case
```

#### Test: E2E - Double Shutdown (Multiple SIGTERMs)
```
Purpose: Test signal idempotency
Setup: Send SIGTERM multiple times
Test:
  - First SIGTERM: starts shutdown
  - Second SIGTERM: ignored
  - Third SIGTERM: ignored
  - Verify single clean shutdown
Coverage: signal handling
```

#### Test: E2E - Stop at Offset
```
Purpose: Test stop_at_offset feature
Setup: 
  - config.stop_at_offset = {("topic", 0): 100}
  - Produce 200 messages
Test:
  - Verify stops at offset 100
  - Verify messages up to 100 processed
  - Verify graceful shutdown triggered
  - Verify final commit includes 100
Coverage: stop_at_offset
```

#### Test: E2E - Dev Mode Restart
```
Purpose: Test dev_mode behavior
Setup:
  - config.dev_mode = True
  - Process 100 messages
Test:
  - Verify no commits
  - Stop consumer
  - Restart consumer
  - Verify all 100 reprocessed
Coverage: dev_mode restart
```

---

### 4. test_performance.py

#### Test: Performance - Throughput Benchmark
```
Purpose: Measure max throughput
Setup:
  - Simple processor (no-op)
  - 10,000 messages
Test:
  - Measure msg/s
  - Verify > 1000 msg/s (baseline)
Metric: Messages per second
```

#### Test: Performance - Latency Measurement
```
Purpose: Measure processing latency
Setup: Track timestamp from poll to commit
Test:
  - Measure p50, p95, p99 latency
  - Verify p95 < 100ms
Metric: Latency percentiles
```

#### Test: Performance - Memory Stability
```
Purpose: Verify no memory leaks
Setup: Process 100,000 messages
Test:
  - Monitor memory usage
  - Verify stable (no growth)
  - Verify < 1GB used
Metric: Memory usage over time
```

#### Test: Performance - Worker Utilization
```
Purpose: Measure worker efficiency
Setup: 50 workers, varying message rates
Test:
  - Measure active vs idle workers
  - Verify optimal utilization
Metric: Worker busy percentage
```

---

## STRESS TESTS

### 1. Test: Stress - Long Running (24 Hours)
```
Purpose: Test stability over time
Setup:
  - Run consumer for 24 hours
  - Continuous message flow
Test:
  - Verify no crashes
  - Verify no memory leaks
  - Verify commit rate stable
  - Verify CSV rotation works
Duration: 24 hours
```

### 2. Test: Stress - High Message Rate
```
Purpose: Test maximum throughput
Setup: Produce 100,000 messages rapidly
Test:
  - Verify consumer keeps up
  - Verify no message loss
  - Verify no crashes
```

### 3. Test: Stress - Large Messages
```
Purpose: Test near-limit messages
Setup:
  - Messages at 9.5MB (near 10MB limit)
  - 1,000 messages
Test:
  - Verify all processed
  - Verify memory stable
  - Verify no OOM
```

### 4. Test: Stress - High Failure Rate
```
Purpose: Test DLQ under load
Setup: 50% failure rate, 10,000 messages
Test:
  - Verify 5,000 in DLQ
  - Verify DLQ file manageable size
  - Verify no performance degradation
```

---

## CHAOS TESTS

### 1. Test: Chaos - Random Worker Kills
```
Purpose: Test resilience to worker crashes
Setup: Randomly kill workers during processing
Test:
  - Verify messages not lost
  - Verify processing continues
  - Verify eventual completion
```

### 2. Test: Chaos - Network Partitions
```
Purpose: Test network failure handling
Setup: Randomly disconnect from Kafka
Test:
  - Verify retries
  - Verify recovery
  - Verify no data loss
```

### 3. Test: Chaos - Resource Exhaustion
```
Purpose: Test resource limits
Setup: 
  - Limit CPU to 50%
  - Limit memory to 512MB
Test:
  - Verify consumer adapts
  - Verify no crashes
  - Verify slower but stable
```

---

## MOCK HELPERS

### Mock Kafka Consumer
```python
class MockKafkaConsumer:
    """Mock for confluent_kafka.Consumer"""
    def __init__(self, messages=None):
        self.messages = messages or []
        self.committed_offsets = {}
        self.poll_count = 0
    
    def poll(self, timeout=1.0):
        if self.messages:
            return self.messages.pop(0)
        return None
    
    def commit(self, offsets=None):
        self.committed_offsets.update(offsets)
    
    def close(self):
        pass
```

### Mock Processor Callable
```python
def mock_processor_success(msg_bytes):
    """Always succeeds"""
    return True

def mock_processor_failure(msg_bytes):
    """Always fails"""
    raise ValueError("Processing failed")

def mock_processor_slow(msg_bytes):
    """Slow processing"""
    time.sleep(5)
    return True

def mock_processor_selective(msg_bytes):
    """Fails on specific pattern"""
    data = json.loads(msg_bytes)
    if data.get("invalid"):
        raise ValueError("Invalid message")
    return True
```

### Mock Message
```python
class MockMessage:
    def __init__(self, topic, partition, offset, key, value):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key
        self._value = value
    
    def topic(self):
        return self._topic
    
    def partition(self):
        return self._partition
    
    def offset(self):
        return self._offset
    
    def key(self):
        return self._key
    
    def value(self):
        return self._value
    
    def error(self):
        return None
```

---

## FIXTURES (conftest.py)

### Fixture: temp_dir
```python
@pytest.fixture
def temp_dir():
    """Temporary directory for CSV files"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir
```

### Fixture: mock_config
```python
@pytest.fixture
def mock_config(temp_dir):
    """Standard test configuration"""
    return {
        "bootstrap_servers": "localhost:9092",
        "topic": "test-topic",
        "group_id": "test-group",
        "processor_callable": mock_processor_success,
        "backup_path": f"{temp_dir}/backup.csv",
        "dlq_path": f"{temp_dir}/dlq.csv",
        "worker_count": 5,
        "queue_size": 20,
        "log_level": "DEBUG"
    }
```

### Fixture: kafka_container
```python
@pytest.fixture(scope="session")
def kafka_container():
    """Real Kafka for E2E tests"""
    from testcontainers.kafka import KafkaContainer
    
    kafka = KafkaContainer()
    kafka.start()
    
    yield kafka.get_bootstrap_server()
    
    kafka.stop()
```

### Fixture: shared_state
```python
@pytest.fixture
def shared_state(mock_config):
    """SharedState instance"""
    config = KafkaConsumerConfig(**mock_config)
    return SharedState(config)
```

---

## COVERAGE STRATEGY

### Target: 95% Overall

**Prioritized Coverage:**
1. **100% Critical Paths:**
   - Offset calculation (max contiguous algorithm)
   - Worker exception handling (success/failure routing)
   - Signal handling (shutdown trigger)
   - CSV failure handling (shutdown trigger)

2. **95% Main Logic:**
   - Polling loop
   - Worker loop
   - Commit loop
   - CSV writers

3. **90% Edge Cases:**
   - Error recovery
   - Timeout handling
   - Resource cleanup

**Excluded from Coverage (< 5%):**
- Unreachable code (if any)
- Defensive assertions that can't fail
- Some exception handlers for impossible states

### Coverage Commands
```bash
# Run all tests with coverage
pytest --cov=kafka_consumer --cov-report=html --cov-report=term-missing

# Target: 95%+ coverage
# Fail if < 95%
pytest --cov=kafka_consumer --cov-fail-under=95

# Generate detailed report
coverage html
# Open htmlcov/index.html
```

---

## TEST ORGANIZATION

### Quick Tests (< 1 second each)
- All unit tests
- Should complete in < 30 seconds total

### Integration Tests (1-10 seconds each)
- Mock Kafka interactions
- Should complete in < 2 minutes total

### E2E Tests (10-60 seconds each)
- Real Kafka (testcontainers)
- Should complete in < 10 minutes total

### Stress Tests (minutes to hours)
- Run separately
- Not part of CI pipeline
- Run nightly or weekly

### Test Markers
```python
# Mark tests by type
@pytest.mark.unit
@pytest.mark.integration
@pytest.mark.e2e
@pytest.mark.stress
@pytest.mark.slow

# Run specific markers
pytest -m unit           # Fast unit tests only
pytest -m "not slow"     # Skip slow tests
pytest -m e2e            # E2E tests only
```

---

## COVERAGE REPORT FORMAT

### Expected Coverage Report
```
Name                        Stmts   Miss  Cover   Missing
---------------------------------------------------------
shared_state.py               120      5    96%   45, 78-81
signal_handler.py              50      0   100%
kafka_consumer.py             200     10    95%   156-160, 189-193
polling_thread.py             140      7    95%   98-100, 125-128
worker_pool.py                180      9    95%   145-148, 167-171
offset_manager.py             160      0   100%
csv_writers.py                200     10    95%   178-182, 195-199
---------------------------------------------------------
TOTAL                        1050     41    96%
```

### Minimum Acceptable
```
- Overall: 95%+
- Per module: 90%+ (except offset_manager which should be 100%)
- Critical paths: 100%
```

---

## CI/CD INTEGRATION

### GitHub Actions Workflow
```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v2
      
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.12'
      
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest pytest-cov
      
      - name: Run unit tests
        run: pytest -m unit --cov=kafka_consumer
      
      - name: Run integration tests
        run: pytest -m integration --cov=kafka_consumer --cov-append
      
      - name: Run E2E tests
        run: pytest -m e2e --cov=kafka_consumer --cov-append
      
      - name: Check coverage
        run: pytest --cov=kafka_consumer --cov-fail-under=95
      
      - name: Upload coverage report
        uses: codecov/codecov-action@v2
```

---

## TEST EXECUTION ORDER

### 1. Development
```bash
# Fast feedback loop
pytest -m unit -v

# Quick integration check
pytest -m "unit or integration" -v

# Full suite before commit
pytest --cov=kafka_consumer
```

### 2. CI Pipeline
```bash
# Stage 1: Unit tests (fast)
pytest -m unit --cov=kafka_consumer

# Stage 2: Integration tests
pytest -m integration --cov=kafka_consumer --cov-append

# Stage 3: E2E tests
pytest -m e2e --cov=kafka_consumer --cov-append

# Stage 4: Coverage check (fail if < 95%)
pytest --cov=kafka_consumer --cov-fail-under=95
```

### 3. Nightly
```bash
# Full suite including stress tests
pytest --cov=kafka_consumer

# Long-running tests
pytest -m stress
```

---

## SPECIAL TEST CASES

### Test: Concurrency Race Conditions
```
Purpose: Test thread safety
Method:
  - Start 100 threads accessing shared state
  - Verify no race conditions
  - Verify no deadlocks
Tools: threading, concurrent.futures
```

### Test: Memory Leak Detection
```
Purpose: Detect memory leaks
Method:
  - Process 100,000 messages
  - Monitor memory with memory_profiler
  - Verify stable memory usage
Tools: memory_profiler, tracemalloc
```

### Test: CSV File Integrity
```
Purpose: Verify CSV files not corrupted
Method:
  - Process messages
  - Crash consumer randomly
  - Verify CSV files can be parsed
  - Verify no partial writes
Tools: csv.DictReader
```

---

## DEBUGGING TESTS

### Enable Debug Logging
```python
@pytest.fixture
def debug_logging():
    logging.basicConfig(level=logging.DEBUG)
```

### Capture Logs in Tests
```python
def test_something(caplog):
    with caplog.at_level(logging.INFO):
        # Test code
        pass
    
    assert "Expected log message" in caplog.text
```

### Temporary Test Files
```python
def test_csv_writing(tmp_path):
    csv_file = tmp_path / "test.csv"
    # Test writes to csv_file
    assert csv_file.exists()
```

---

## SUCCESS CRITERIA

### Test Suite Must:
1. ✅ Achieve 95%+ code coverage
2. ✅ Pass all unit tests (< 30s)
3. ✅ Pass all integration tests (< 2m)
4. ✅ Pass all E2E tests (< 10m)
5. ✅ Detect all critical bugs (offset, DLQ, shutdown)
6. ✅ Validate all edge cases
7. ✅ Run in CI/CD pipeline
8. ✅ Generate coverage report

### Must Catch:
- ✅ Offset calculation errors (data loss)
- ✅ Worker crash data loss
- ✅ CSV corruption
- ✅ Shutdown data loss
- ✅ Memory leaks
- ✅ Race conditions
- ✅ Deadlocks
- ✅ Resource leaks

---

## IMPLEMENTATION CHECKLIST

For the AI implementing tests:

- [ ] Create test directory structure
- [ ] Implement all unit tests (shared_state, signal_handler, offset_manager, worker_pool, csv_writers)
- [ ] Implement integration tests (polling, commit, CSV, DLQ)
- [ ] Implement E2E tests (happy path, failures, shutdown, performance)
- [ ] Create mock helpers
- [ ] Create fixtures in conftest.py
- [ ] Run tests and verify 95%+ coverage
- [ ] Fix any failing tests
- [ ] Document test results
- [ ] Create CI/CD workflow file

**GOAL: 95%+ CODE COVERAGE WITH COMPREHENSIVE TEST SUITE**

---

**END OF TEST SPECIFICATION**
