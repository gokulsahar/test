"""
Test cases for datapy.mods.duckdb.duckdb_init module.

Tests DuckDB initialization with memory limits, custom configuration,
and integration with ModResult pattern.
"""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock, call

# Add project root to Python path for testing
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pytest

from datapy.mods.duckdb.duckdb_init import (
    run,
    METADATA,
    CONFIG_SCHEMA
)
from datapy.mod_manager.result import SUCCESS, RUNTIME_ERROR


class TestMetadata:
    """Test cases for module metadata."""
    
    def test_metadata_type(self):
        """Test metadata has correct type."""
        assert METADATA.type == "duckdb_init"
    
    def test_metadata_version(self):
        """Test metadata has version."""
        assert METADATA.version == "1.0.0"
    
    def test_metadata_description(self):
        """Test metadata has description."""
        assert "DuckDB" in METADATA.description
        assert len(METADATA.description) > 0
    
    def test_metadata_category(self):
        """Test metadata has correct category."""
        assert METADATA.category == "duckdb"
    
    def test_metadata_ports(self):
        """Test metadata has correct ports."""
        assert METADATA.input_ports == []
        assert METADATA.output_ports == ["connection"]
    
    def test_metadata_globals(self):
        """Test metadata has empty globals."""
        assert METADATA.globals == []
    
    def test_metadata_packages(self):
        """Test metadata has required packages."""
        assert "duckdb>=1.0.0" in METADATA.packages


class TestConfigSchema:
    """Test cases for configuration schema."""
    
    def test_config_schema_required_empty(self):
        """Test config schema has no required parameters."""
        assert CONFIG_SCHEMA.required == {}
    
    def test_config_schema_memory_limit(self):
        """Test config schema has memory_limit parameter."""
        assert "memory_limit" in CONFIG_SCHEMA.optional
        assert CONFIG_SCHEMA.optional["memory_limit"]["type"] == "str"
        assert CONFIG_SCHEMA.optional["memory_limit"]["default"] == "4GB"
    
    def test_config_schema_config(self):
        """Test config schema has config parameter."""
        assert "config" in CONFIG_SCHEMA.optional
        assert CONFIG_SCHEMA.optional["config"]["type"] == "dict"
        assert CONFIG_SCHEMA.optional["config"]["default"] is None


class TestRunFunctionBasic:
    """Test cases for basic run function behavior."""
    
    @patch('datapy.mods.duckdb.duckdb_init.duckdb.connect')
    def test_run_with_default_memory_limit(self, mock_connect):
        """Test run with default memory_limit."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        params = {}
        result = run(params)
        
        assert result["status"] == "success"
        assert result["exit_code"] == SUCCESS
        mock_connect.assert_called_once_with(
            ":memory:",
            config={"memory_limit": "4GB"}
        )
        assert "connection" in result["artifacts"]
        assert result["artifacts"]["connection"] == mock_connection
    
    @patch('datapy.mods.duckdb.duckdb_init.duckdb.connect')
    def test_run_with_custom_memory_limit(self, mock_connect):
        """Test run with custom memory_limit."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        params = {"memory_limit": "2GB"}
        result = run(params)
        
        assert result["status"] == "success"
        assert result["exit_code"] == SUCCESS
        mock_connect.assert_called_once_with(
            ":memory:",
            config={"memory_limit": "2GB"}
        )
        assert result["metrics"]["memory_limit"] == "2GB"
    
    @patch('datapy.mods.duckdb.duckdb_init.duckdb.connect')
    def test_run_with_custom_config_dict(self, mock_connect):
        """Test run with custom config dictionary."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        custom_config = {
            "threads": 4,
            "max_memory": "1GB"
        }
        params = {"config": custom_config}
        result = run(params)
        
        assert result["status"] == "success"
        expected_config = {
            "memory_limit": "4GB",
            "threads": 4,
            "max_memory": "1GB"
        }
        mock_connect.assert_called_once_with(
            ":memory:",
            config=expected_config
        )
    
    @patch('datapy.mods.duckdb.duckdb_init.duckdb.connect')
    def test_run_with_both_memory_limit_and_config(self, mock_connect):
        """Test run with both memory_limit and custom config."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        custom_config = {"threads": 8}
        params = {
            "memory_limit": "8GB",
            "config": custom_config
        }
        result = run(params)
        
        assert result["status"] == "success"
        expected_config = {
            "memory_limit": "8GB",
            "threads": 8
        }
        mock_connect.assert_called_once_with(
            ":memory:",
            config=expected_config
        )
        assert result["metrics"]["memory_limit"] == "8GB"
        assert result["metrics"]["config_applied"] == expected_config


class TestRunFunctionEdgeCases:
    """Test cases for edge cases and error handling."""
    
    @patch('datapy.mods.duckdb.duckdb_init.duckdb.connect')
    def test_run_with_none_config(self, mock_connect):
        """Test run with None as config (should be ignored)."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        params = {"config": None}
        result = run(params)
        
        assert result["status"] == "success"
        mock_connect.assert_called_once_with(
            ":memory:",
            config={"memory_limit": "4GB"}
        )
    
    @patch('datapy.mods.duckdb.duckdb_init.duckdb.connect')
    def test_run_with_empty_config_dict(self, mock_connect):
        """Test run with empty config dictionary."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        params = {"config": {}}
        result = run(params)
        
        assert result["status"] == "success"
        mock_connect.assert_called_once_with(
            ":memory:",
            config={"memory_limit": "4GB"}
        )
    
    @patch('datapy.mods.duckdb.duckdb_init.duckdb.connect')
    def test_run_with_non_dict_config(self, mock_connect):
        """Test run with non-dict config (should be ignored)."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        params = {"config": "not_a_dict"}
        result = run(params)
        
        assert result["status"] == "success"
        # Non-dict config should be ignored, only memory_limit used
        mock_connect.assert_called_once_with(
            ":memory:",
            config={"memory_limit": "4GB"}
        )
    
    @patch('datapy.mods.duckdb.duckdb_init.duckdb.connect')
    def test_run_with_mod_name_in_params(self, mock_connect):
        """Test run with _mod_name in params."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        params = {"_mod_name": "custom_duckdb"}
        result = run(params)
        
        assert result["status"] == "success"
        assert result["logs"]["mod_name"] == "custom_duckdb"
    
    @patch('datapy.mods.duckdb.duckdb_init.duckdb.connect')
    def test_run_connection_failure(self, mock_connect):
        """Test run when DuckDB connection fails."""
        mock_connect.side_effect = Exception("Connection failed")
        
        params = {}
        result = run(params)
        
        assert result["status"] == "error"
        assert result["exit_code"] == RUNTIME_ERROR
        assert len(result["errors"]) == 1
        assert "Failed to initialize DuckDB connection" in result["errors"][0]["message"]
        assert "Connection failed" in result["errors"][0]["message"]
    
    @patch('datapy.mods.duckdb.duckdb_init.duckdb.connect')
    def test_run_memory_error(self, mock_connect):
        """Test run when memory allocation fails."""
        mock_connect.side_effect = MemoryError("Out of memory")
        
        params = {"memory_limit": "1TB"}
        result = run(params)
        
        assert result["status"] == "error"
        assert result["exit_code"] == RUNTIME_ERROR
        assert len(result["errors"]) == 1
        assert "Failed to initialize DuckDB connection" in result["errors"][0]["message"]


class TestResultArtifacts:
    """Test cases for result artifacts."""
    
    @patch('datapy.mods.duckdb.duckdb_init.duckdb.connect')
    def test_connection_artifact_added(self, mock_connect):
        """Test that connection is added to artifacts."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        params = {}
        result = run(params)
        
        assert "connection" in result["artifacts"]
        assert result["artifacts"]["connection"] == mock_connection
    
    @patch('datapy.mods.duckdb.duckdb_init.duckdb.connect')
    def test_connection_artifact_type(self, mock_connect):
        """Test that connection artifact is the DuckDB connection object."""
        mock_connection = MagicMock()
        mock_connection.__class__.__name__ = "DuckDBPyConnection"
        mock_connect.return_value = mock_connection
        
        params = {}
        result = run(params)
        
        connection = result["artifacts"]["connection"]
        assert connection.__class__.__name__ == "DuckDBPyConnection"


class TestResultMetrics:
    """Test cases for result metrics."""
    
    @patch('datapy.mods.duckdb.duckdb_init.duckdb.connect')
    def test_memory_limit_metric(self, mock_connect):
        """Test that memory_limit metric is added."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        params = {"memory_limit": "2GB"}
        result = run(params)
        
        assert "memory_limit" in result["metrics"]
        assert result["metrics"]["memory_limit"] == "2GB"
    
    @patch('datapy.mods.duckdb.duckdb_init.duckdb.connect')
    def test_config_applied_metric(self, mock_connect):
        """Test that config_applied metric is added."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        params = {
            "memory_limit": "4GB",
            "config": {"threads": 4}
        }
        result = run(params)
        
        assert "config_applied" in result["metrics"]
        expected_config = {
            "memory_limit": "4GB",
            "threads": 4
        }
        assert result["metrics"]["config_applied"] == expected_config
    
    @patch('datapy.mods.duckdb.duckdb_init.duckdb.connect')
    def test_default_memory_limit_metric(self, mock_connect):
        """Test that default memory_limit is in metrics."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        params = {}
        result = run(params)
        
        assert result["metrics"]["memory_limit"] == "4GB"


class TestLogging:
    """Test cases for logging behavior."""
    
    @patch('datapy.mods.duckdb.duckdb_init.duckdb.connect')
    @patch('datapy.mods.duckdb.duckdb_init.logger')
    def test_debug_logging_initialization(self, mock_logger, mock_connect):
        """Test debug logging during initialization."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        params = {"memory_limit": "2GB"}
        result = run(params)
        
        # Check that debug logging was called
        mock_logger.debug.assert_any_call(
            "Initializing DuckDB connection with memory_limit=2GB"
        )
    
    @patch('datapy.mods.duckdb.duckdb_init.duckdb.connect')
    @patch('datapy.mods.duckdb.duckdb_init.logger')
    def test_debug_logging_custom_config(self, mock_logger, mock_connect):
        """Test debug logging for custom config."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        custom_config = {"threads": 4, "max_memory": "1GB"}
        params = {"config": custom_config}
        result = run(params)
        
        # Check that custom config logging was called
        mock_logger.debug.assert_any_call(
            "Merging 2 custom config settings"
        )
    
    @patch('datapy.mods.duckdb.duckdb_init.duckdb.connect')
    @patch('datapy.mods.duckdb.duckdb_init.logger')
    def test_debug_logging_success(self, mock_logger, mock_connect):
        """Test debug logging on successful connection."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        params = {}
        result = run(params)
        
        # Check success logging
        mock_logger.debug.assert_any_call(
            "DuckDB connection created successfully"
        )


class TestIntegrationScenarios:
    """Integration test cases for complete workflows."""
    
    @patch('datapy.mods.duckdb.duckdb_init.duckdb.connect')
    def test_complete_successful_initialization(self, mock_connect):
        """Test complete successful initialization workflow."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        params = {
            "_mod_name": "my_duckdb",
            "memory_limit": "8GB",
            "config": {
                "threads": 8,
                "max_memory": "8GB"
            }
        }
        
        result = run(params)
        
        # Verify success
        assert result["status"] == "success"
        assert result["exit_code"] == SUCCESS
        
        # Verify logs
        assert result["logs"]["mod_name"] == "my_duckdb"
        assert result["logs"]["mod_type"] == "duckdb_init"
        
        # Verify artifacts
        assert "connection" in result["artifacts"]
        assert result["artifacts"]["connection"] == mock_connection
        
        # Verify metrics
        assert result["metrics"]["memory_limit"] == "8GB"
        assert "config_applied" in result["metrics"]
        
        # Verify no errors or warnings
        assert len(result["errors"]) == 0
        assert len(result["warnings"]) == 0
    
    @patch('datapy.mods.duckdb.duckdb_init.duckdb.connect')
    def test_minimal_params_initialization(self, mock_connect):
        """Test initialization with minimal parameters."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        params = {}
        result = run(params)
        
        assert result["status"] == "success"
        assert "connection" in result["artifacts"]
        assert result["metrics"]["memory_limit"] == "4GB"