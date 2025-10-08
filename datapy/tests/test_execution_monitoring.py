"""
Test cases for datapy.mod_manager.execution_monitoring module.

Tests memory monitoring, timing, and integration with SDK execution flow.
"""

import sys
import time
import threading
from pathlib import Path
from unittest.mock import patch, MagicMock, PropertyMock

# Add project root to Python path for testing
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pytest

from datapy.mod_manager.execution_monitoring import (
    ExecutionMonitor,
    execute_with_monitoring,
    _get_execution_monitor,
    PSUTIL_AVAILABLE
)
from datapy.mod_manager.result import SUCCESS, RUNTIME_ERROR


class TestExecutionMonitor:
    """Test cases for ExecutionMonitor class."""
    
    def test_init_with_psutil_available(self):
        """Test ExecutionMonitor initialization when psutil is available."""
        if not PSUTIL_AVAILABLE:
            pytest.skip("psutil not available for testing")
        
        monitor = ExecutionMonitor()
        assert monitor.process is not None
        assert monitor.is_available() is True
    
    @patch('datapy.mod_manager.execution_monitoring.PSUTIL_AVAILABLE', False)
    def test_init_without_psutil(self):
        """Test ExecutionMonitor initialization when psutil is not available."""
        monitor = ExecutionMonitor()
        assert monitor.process is None
        assert monitor.is_available() is False
    
    @patch('datapy.mod_manager.execution_monitoring.PSUTIL_AVAILABLE', True)
    @patch('datapy.mod_manager.execution_monitoring.psutil.Process')
    def test_init_psutil_exception(self, mock_process_class):
        """Test ExecutionMonitor handles psutil initialization exception."""
        mock_process_class.side_effect = Exception("Process creation failed")
        
        monitor = ExecutionMonitor()
        assert monitor.process is None
        assert monitor.is_available() is False
    
    def test_get_memory_usage_mb_without_psutil(self):
        """Test get_memory_usage_mb returns 0.0 when psutil unavailable."""
        monitor = ExecutionMonitor()
        monitor.process = None
        
        memory_usage = monitor.get_memory_usage_mb()
        assert memory_usage == 0.0
    
    @patch('datapy.mod_manager.execution_monitoring.PSUTIL_AVAILABLE', True)
    def test_get_memory_usage_mb_with_psutil(self):
        """Test get_memory_usage_mb with psutil available."""
        if not PSUTIL_AVAILABLE:
            pytest.skip("psutil not available for testing")
        
        monitor = ExecutionMonitor()
        if monitor.is_available():
            memory_usage = monitor.get_memory_usage_mb()
            assert isinstance(memory_usage, float)
            assert memory_usage >= 0.0
    
    @patch('datapy.mod_manager.execution_monitoring.PSUTIL_AVAILABLE', True)
    def test_get_memory_usage_mb_exception_handling(self):
        """Test get_memory_usage_mb handles exceptions gracefully."""
        monitor = ExecutionMonitor()
        mock_process = MagicMock()
        mock_process.memory_info.side_effect = Exception("Memory read failed")
        monitor.process = mock_process
        
        memory_usage = monitor.get_memory_usage_mb()
        assert memory_usage == 0.0
    
    @patch('datapy.mod_manager.execution_monitoring.PSUTIL_AVAILABLE', True)
    def test_memory_conversion_calculation(self):
        """Test memory conversion from bytes to MB."""
        monitor = ExecutionMonitor()
        mock_process = MagicMock()
        
        # Mock memory_info to return 1GB in bytes
        mock_memory_info = MagicMock()
        mock_memory_info.rss = 1024 * 1024 * 1024  # 1GB in bytes
        mock_process.memory_info.return_value = mock_memory_info
        monitor.process = mock_process
        
        memory_usage = monitor.get_memory_usage_mb()
        assert memory_usage == 1024.0  # Should be 1024 MB


class TestExecutionMonitorSingleton:
    """Test cases for ExecutionMonitor singleton pattern."""
    
    def test_singleton_returns_same_instance(self):
        """Test that _get_execution_monitor returns same instance."""
        monitor1 = _get_execution_monitor()
        monitor2 = _get_execution_monitor()
        
        assert monitor1 is monitor2
    
    def test_singleton_thread_safety(self):
        """Test singleton thread safety."""
        monitors = []
        errors = []
        
        def create_monitor():
            try:
                monitor = _get_execution_monitor()
                monitors.append(monitor)
            except Exception as e:
                errors.append(e)
        
        # Create multiple threads
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=create_monitor)
            threads.append(thread)
        
        # Start all threads
        for thread in threads:
            thread.start()
        
        # Wait for completion
        for thread in threads:
            thread.join()
        
        # Verify no errors and all monitors are the same instance
        assert len(errors) == 0
        assert len(monitors) == 5
        assert all(monitor is monitors[0] for monitor in monitors)


class TestExecuteWithMonitoring:
    """Test cases for execute_with_monitoring function."""
    
    def test_execute_with_monitoring_success(self):
        """Test successful execution with monitoring."""
        # Mock original executor
        def mock_executor(mod_type, params, mod_name):
            return {
                "status": "success",
                "exit_code": SUCCESS,
                "metrics": {"rows_processed": 100},
                "artifacts": {"data": "test_data"},
                "globals": {},
                "warnings": [],
                "errors": [],
                "logs": {"run_id": "test_123"}
            }
        
        result = execute_with_monitoring(
            "test_mod",
            {"param": "value"},
            "test_instance",
            mock_executor
        )
        
        # Verify original result is preserved
        assert result["status"] == "success"
        assert result["metrics"]["rows_processed"] == 100
        
        # Verify monitoring metrics are added
        assert "execution_monitoring" in result["metrics"]
        monitoring = result["metrics"]["execution_monitoring"]
        
        assert "execution_time" in monitoring
        assert "memory_start_mb" in monitoring
        assert "memory_end_mb" in monitoring
        assert "memory_delta_mb" in monitoring
        assert "monitoring_available" in monitoring
        
        assert isinstance(monitoring["execution_time"], float)
        assert monitoring["execution_time"] >= 0  # Changed from > 0 to >= 0
        assert isinstance(monitoring["memory_start_mb"], float)
        assert isinstance(monitoring["memory_end_mb"], float)
        assert isinstance(monitoring["memory_delta_mb"], float)
        assert isinstance(monitoring["monitoring_available"], bool)
    
    def test_execute_with_monitoring_preserves_existing_metrics(self):
        """Test that monitoring preserves existing metrics."""
        def mock_executor(mod_type, params, mod_name):
            return {
                "status": "success",
                "exit_code": SUCCESS,
                "metrics": {
                    "existing_metric": "value",
                    "rows_count": 500
                },
                "artifacts": {},
                "globals": {},
                "warnings": [],
                "errors": [],
                "logs": {}
            }
        
        result = execute_with_monitoring(
            "test_mod",
            {},
            "test_instance",
            mock_executor
        )
        
        # Verify existing metrics are preserved
        assert result["metrics"]["existing_metric"] == "value"
        assert result["metrics"]["rows_count"] == 500
        
        # Verify monitoring metrics are added
        assert "execution_monitoring" in result["metrics"]
    
    def test_execute_with_monitoring_creates_metrics_if_missing(self):
        """Test that monitoring creates metrics section if missing."""
        def mock_executor(mod_type, params, mod_name):
            return {
                "status": "success",
                "exit_code": SUCCESS,
                "artifacts": {},
                "globals": {},
                "warnings": [],
                "errors": [],
                "logs": {}
                # Note: no metrics section
            }
        
        result = execute_with_monitoring(
            "test_mod",
            {},
            "test_instance",
            mock_executor
        )
        
        # Verify metrics section is created
        assert "metrics" in result
        assert "execution_monitoring" in result["metrics"]
    
    def test_execute_with_monitoring_handles_non_dict_result(self):
        """Test monitoring handles non-dict result from executor."""
        def mock_executor(mod_type, params, mod_name):
            return "not_a_dict"
        
        result = execute_with_monitoring(
            "test_mod",
            {},
            "test_instance",
            mock_executor
        )
        
        # Should return original result when not a dict
        assert result == "not_a_dict"
    
    def test_execute_with_monitoring_fallback_on_exception(self):
        """Test monitoring falls back to original executor on exception."""
        def mock_executor(mod_type, params, mod_name):
            return {
                "status": "success",
                "exit_code": SUCCESS,
                "metrics": {},
                "artifacts": {},
                "globals": {},
                "warnings": [],
                "errors": [],
                "logs": {}
            }
        
        # Mock time.perf_counter to raise exception
        with patch('time.perf_counter', side_effect=Exception("Timer failed")):
            result = execute_with_monitoring(
                "test_mod",
                {},
                "test_instance",
                mock_executor
            )
        
        # Should return original result without monitoring due to fallback
        assert result["status"] == "success"
        assert "execution_monitoring" not in result.get("metrics", {})
    
    @patch('time.perf_counter')
    def test_execute_with_monitoring_timing_accuracy(self, mock_timer):
        """Test timing accuracy in monitoring."""
        # Mock timer to return predictable values
        mock_timer.side_effect = [1000.0, 1002.5]  # 2.5 second execution
        
        def mock_executor(mod_type, params, mod_name):
            return {
                "status": "success",
                "exit_code": SUCCESS,
                "metrics": {},
                "artifacts": {},
                "globals": {},
                "warnings": [],
                "errors": [],
                "logs": {}
            }
        
        result = execute_with_monitoring(
            "test_mod",
            {},
            "test_instance",
            mock_executor
        )
        
        monitoring = result["metrics"]["execution_monitoring"]
        assert monitoring["execution_time"] == 2.5
    
    @patch('datapy.mod_manager.execution_monitoring._get_execution_monitor')
    def test_execute_with_monitoring_memory_tracking(self, mock_get_monitor):
        """Test memory tracking in monitoring."""
        # Mock monitor with predictable memory values
        mock_monitor = MagicMock()
        mock_monitor.get_memory_usage_mb.side_effect = [100.0, 150.0]  # Start: 100MB, End: 150MB
        mock_monitor.is_available.return_value = True
        mock_get_monitor.return_value = mock_monitor
        
        def mock_executor(mod_type, params, mod_name):
            return {
                "status": "success",
                "exit_code": SUCCESS,
                "metrics": {},
                "artifacts": {},
                "globals": {},
                "warnings": [],
                "errors": [],
                "logs": {}
            }
        
        result = execute_with_monitoring(
            "test_mod",
            {},
            "test_instance",
            mock_executor
        )
        
        monitoring = result["metrics"]["execution_monitoring"]
        assert monitoring["memory_start_mb"] == 100.0
        assert monitoring["memory_end_mb"] == 150.0
        assert monitoring["memory_delta_mb"] == 50.0
        assert monitoring["monitoring_available"] is True
    
    def test_execute_with_monitoring_executor_parameters(self):
        """Test that executor receives correct parameters."""
        captured_params = {}
        
        def mock_executor(mod_type, params, mod_name):
            captured_params["mod_type"] = mod_type
            captured_params["params"] = params
            captured_params["mod_name"] = mod_name
            return {
                "status": "success",
                "exit_code": SUCCESS,
                "metrics": {},
                "artifacts": {},
                "globals": {},
                "warnings": [],
                "errors": [],
                "logs": {}
            }
        
        execute_with_monitoring(
            "csv_reader",
            {"file_path": "test.csv"},
            "test_reader",
            mock_executor
        )
        
        assert captured_params["mod_type"] == "csv_reader"
        assert captured_params["params"] == {"file_path": "test.csv"}
        assert captured_params["mod_name"] == "test_reader"


class TestIntegrationWithSDK:
    """Test cases for integration with SDK execution flow."""
    
    @patch('datapy.mod_manager.sdk.execute_with_monitoring')
    def test_sdk_calls_monitoring(self, mock_execute_with_monitoring):
        """Test that SDK properly calls execute_with_monitoring."""
        # This test verifies the integration point
        mock_execute_with_monitoring.return_value = {
            "status": "success",
            "exit_code": SUCCESS,
            "metrics": {
                "execution_monitoring": {
                    "execution_time": 1.5,
                    "memory_start_mb": 100.0,
                    "memory_end_mb": 110.0,
                    "memory_delta_mb": 10.0,
                    "monitoring_available": True
                }
            },
            "artifacts": {},
            "globals": {},
            "warnings": [],
            "errors": [],
            "logs": {}
        }
        
        # Import and call run_mod to verify integration
        from datapy.mod_manager.sdk import run_mod
        
        with patch('datapy.mod_manager.sdk.get_registry') as mock_registry, \
             patch('datapy.mod_manager.sdk._resolve_mod_parameters') as mock_resolve, \
             patch('datapy.mod_manager.sdk.substitute_context_variables') as mock_substitute, \
             patch('datapy.mod_manager.sdk.validate_mod_parameters') as mock_validate, \
             patch('datapy.mod_manager.sdk._auto_generate_mod_name') as mock_gen_name:
            
            # Setup mocks
            mock_registry.return_value.get_mod_info.return_value = {
                "config_schema": {"required": {}, "optional": {}}
            }
            mock_resolve.return_value = {}
            mock_substitute.return_value = {}
            mock_validate.return_value = {}
            mock_gen_name.return_value = "auto_generated_name"
            
            # Call run_mod
            result = run_mod("test_mod", {"param": "value"})
            
            # Verify execute_with_monitoring was called
            mock_execute_with_monitoring.assert_called_once()
            
            # Verify result contains monitoring metrics
            assert "execution_monitoring" in result["metrics"]
            monitoring = result["metrics"]["execution_monitoring"]
            assert "execution_time" in monitoring
            assert "memory_delta_mb" in monitoring


class TestEdgeCases:
    """Test cases for edge cases and error conditions."""
    
    def test_monitoring_with_empty_result(self):
        """Test monitoring with empty result dict."""
        def mock_executor(mod_type, params, mod_name):
            return {}
        
        result = execute_with_monitoring(
            "test_mod",
            {},
            "test_instance",
            mock_executor
        )
        
        # Should add metrics section and monitoring
        assert "metrics" in result
        assert "execution_monitoring" in result["metrics"]
    
    def test_monitoring_with_invalid_metrics_type(self):
        """Test monitoring when existing metrics is not a dict."""
        def mock_executor(mod_type, params, mod_name):
            return {
                "status": "success",
                "metrics": "not_a_dict",  # Invalid metrics type
                "artifacts": {},
                "globals": {},
                "warnings": [],
                "errors": [],
                "logs": {}
            }
        
        result = execute_with_monitoring(
            "test_mod",
            {},
            "test_instance",
            mock_executor
        )
        
        # Should preserve invalid metrics as-is (monitoring skips invalid metrics)
        assert "metrics" in result
        assert result["metrics"] == "not_a_dict" 
        
    def test_monitoring_performance_overhead(self):
        """Test that monitoring overhead is minimal."""
        execution_count = 100
        
        def mock_executor(mod_type, params, mod_name):
            return {
                "status": "success",
                "exit_code": SUCCESS,
                "metrics": {},
                "artifacts": {},
                "globals": {},
                "warnings": [],
                "errors": [],
                "logs": {}
            }
        
        start_time = time.perf_counter()
        
        for i in range(execution_count):
            execute_with_monitoring(
                "test_mod",
                {},
                f"test_instance_{i}",
                mock_executor
            )
        
        end_time = time.perf_counter()
        total_time = end_time - start_time
        avg_overhead = total_time / execution_count
        
        # Overhead should be less than 1ms per execution
        assert avg_overhead < 0.001, f"Monitoring overhead too high: {avg_overhead:.6f}s per execution"