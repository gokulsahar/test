"""
Test cases for datapy.mod_manager.params module.

Tests project configuration discovery, parameter resolution chain,
and YAML configuration management across the DataPy framework.
"""

import sys
import os
import tempfile
import yaml
from pathlib import Path
from unittest.mock import patch, mock_open, MagicMock

# Add project root to Python path for testing
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pytest

from datapy.mod_manager.params import (
    ProjectConfig,
    ParameterResolver,
    get_project_config,
    clear_project_config,
    load_job_config,
    create_resolver,
    _global_project_config,
    _get_script_directory
)


class TestGetScriptDirectoryParams:
    """Test cases for _get_script_directory helper function in params module."""
    
    def test_get_script_directory_normal_execution(self):
        """Test getting script directory in normal execution."""
        result = _get_script_directory()
        
        # Verify it's a Path object
        assert isinstance(result, Path)
        assert result is not None
    
    def test_get_script_directory_with_valid_script_path(self, tmp_path, monkeypatch):
        """Test script directory resolution with valid script path."""
        # Create a fake script in a directory
        script_dir = tmp_path / "my_project" / "scripts"
        script_dir.mkdir(parents=True)
        
        fake_script = script_dir / "data_pipeline.py"
        fake_script.write_text("#!/usr/bin/env python3\n# data pipeline")
        
        # Mock sys.argv[0] to point to our fake script
        monkeypatch.setattr(sys, 'argv', [str(fake_script)])
        
        result = _get_script_directory()
        
        # Should return the parent directory (script_dir)
        assert result == script_dir
        assert result.name == "scripts"
    
    def test_get_script_directory_with_absolute_path(self, tmp_path, monkeypatch):
        """Test script directory with absolute path in sys.argv."""
        # Create nested structure
        project_dir = tmp_path / "project"
        jobs_dir = project_dir / "jobs"
        jobs_dir.mkdir(parents=True)
        
        script_file = jobs_dir / "etl_job.py"
        script_file.write_text("# ETL job script")
        
        # Use absolute path
        monkeypatch.setattr(sys, 'argv', [str(script_file.resolve())])
        
        result = _get_script_directory()
        
        assert result == jobs_dir
        assert result.is_absolute()
    
    def test_get_script_directory_with_relative_path(self, tmp_path, monkeypatch):
        """Test script directory with relative path in sys.argv."""
        # Create script structure
        work_dir = tmp_path / "workspace"
        work_dir.mkdir()
        
        script_file = work_dir / "process.py"
        script_file.write_text("# processing script")
        
        # Change to work_dir and use relative path
        monkeypatch.chdir(work_dir)
        monkeypatch.setattr(sys, 'argv', ['process.py'])
        
        result = _get_script_directory()
        
        # Should resolve to absolute path
        assert result == work_dir
        assert result.is_absolute()
    
    def test_get_script_directory_with_nested_relative_path(self, tmp_path, monkeypatch):
        """Test script directory with nested relative path."""
        # Create structure: workspace/tools/scripts/run.py
        workspace = tmp_path / "workspace"
        scripts_dir = workspace / "tools" / "scripts"
        scripts_dir.mkdir(parents=True)
        
        script_file = scripts_dir / "run.py"
        script_file.write_text("# run script")
        
        # Change to workspace and use relative path
        monkeypatch.chdir(workspace)
        monkeypatch.setattr(sys, 'argv', ['tools/scripts/run.py'])
        
        result = _get_script_directory()
        
        assert result == scripts_dir
    
    def test_get_script_directory_empty_argv_fallback(self, monkeypatch, tmp_path):
        """Test fallback to CWD when sys.argv is empty (IndexError)."""
        # Mock sys.argv to be empty list
        monkeypatch.setattr(sys, 'argv', [])
        monkeypatch.chdir(tmp_path)
        
        result = _get_script_directory()
        
        # Should fallback to current working directory
        assert result == tmp_path
    
    def test_get_script_directory_argv_none_fallback(self, monkeypatch, tmp_path):
        """Test fallback when sys.argv[0] access raises IndexError."""
        # Create a mock that raises IndexError on index access
        class MockArgv:
            def __getitem__(self, index):
                raise IndexError("list index out of range")
        
        monkeypatch.setattr(sys, 'argv', MockArgv())
        monkeypatch.chdir(tmp_path)
        
        result = _get_script_directory()
        
        # Should fallback to CWD
        assert result == tmp_path
    
    def test_get_script_directory_oserror_fallback(self, monkeypatch, tmp_path):
        """Test fallback to CWD when OSError occurs during path resolution."""
        monkeypatch.chdir(tmp_path)
        
        # Mock sys.argv with a path
        monkeypatch.setattr(sys, 'argv', ['test_script.py'])
        
        # Mock Path class to raise OSError during resolve()
        with patch('datapy.mod_manager.params.Path') as mock_path_cls:
            # Create mock instance that raises OSError on resolve
            mock_instance = MagicMock(spec=Path)
            mock_instance.resolve.side_effect = OSError("Cannot resolve path")
            
            # Path() constructor returns mock instance
            mock_path_cls.return_value = mock_instance
            
            # Path.cwd() returns tmp_path
            mock_path_cls.cwd.return_value = tmp_path
            
            result = _get_script_directory()
            
            # Should fallback to CWD
            assert result == tmp_path
    
    def test_get_script_directory_permission_error_fallback(self, monkeypatch, tmp_path):
        """Test fallback when PermissionError occurs (also caught as OSError)."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(sys, 'argv', ['restricted_script.py'])
        
        # Mock Path to raise PermissionError (subclass of OSError)
        with patch('datapy.mod_manager.params.Path') as mock_path_cls:
            mock_instance = MagicMock()
            mock_instance.resolve.side_effect = PermissionError("Access denied")
            
            mock_path_cls.return_value = mock_instance
            mock_path_cls.cwd.return_value = tmp_path
            
            result = _get_script_directory()
            
            assert result == tmp_path
    
    def test_get_script_directory_with_special_characters(self, tmp_path, monkeypatch):
        """Test script directory with special characters in path."""
        # Create directory with spaces and special chars
        special_dir = tmp_path / "my scripts" / "data-processing (v2)"
        special_dir.mkdir(parents=True)
        
        script_file = special_dir / "process.py"
        script_file.write_text("# script")
        
        monkeypatch.setattr(sys, 'argv', [str(script_file)])
        
        result = _get_script_directory()
        
        assert result == special_dir
        assert "my scripts" in str(result)
    
    def test_get_script_directory_deeply_nested_path(self, tmp_path, monkeypatch):
        """Test script directory with deeply nested path."""
        # Create deep nesting: level1/level2/.../level10
        deep_path = tmp_path
        for i in range(10):
            deep_path = deep_path / f"level{i+1}"
        deep_path.mkdir(parents=True)
        
        script_file = deep_path / "deep_script.py"
        script_file.write_text("# deep script")
        
        monkeypatch.setattr(sys, 'argv', [str(script_file)])
        
        result = _get_script_directory()
        
        assert result == deep_path
        assert result.name == "level10"
    
    def test_get_script_directory_returns_parent_not_file(self, tmp_path, monkeypatch):
        """Test that function returns parent directory, not the script file."""
        script_dir = tmp_path / "scripts"
        script_dir.mkdir()
        
        script_file = script_dir / "my_script.py"
        script_file.write_text("# script")
        
        monkeypatch.setattr(sys, 'argv', [str(script_file)])
        
        result = _get_script_directory()
        
        # Should be directory, not file
        assert result.is_dir()
        assert not result.is_file()
        assert result == script_dir
        assert result != script_file
    
    def test_get_script_directory_with_symlink(self, tmp_path, monkeypatch):
        """Test script directory resolves symlinks correctly."""
        # Create actual location
        actual_dir = tmp_path / "actual_scripts"
        actual_dir.mkdir()
        actual_script = actual_dir / "script.py"
        actual_script.write_text("# actual script")
        
        try:
            # Create symlink
            link_dir = tmp_path / "linked_scripts"
            link_dir.mkdir()
            link_script = link_dir / "script.py"
            link_script.symlink_to(actual_script)
            
            monkeypatch.setattr(sys, 'argv', [str(link_script)])
            
            result = _get_script_directory()
            
            # Path.resolve() should follow symlink to actual location
            assert result == actual_dir
            
        except (OSError, NotImplementedError):
            # Skip on systems that don't support symlinks
            pytest.skip("Symlinks not supported on this system")
    
    def test_get_script_directory_cwd_is_preserved(self, monkeypatch, tmp_path):
        """Test that fallback returns actual CWD without side effects."""
        # Create specific working directory
        work_dir = tmp_path / "working" / "directory"
        work_dir.mkdir(parents=True)
        
        # Force fallback by empty argv
        monkeypatch.setattr(sys, 'argv', [])
        monkeypatch.chdir(work_dir)
        
        result = _get_script_directory()
        
        assert result == work_dir
        assert result.name == "directory"
        
        # Verify CWD hasn't changed
        assert Path.cwd() == work_dir
    
    def test_get_script_directory_multiple_calls_consistent(self, tmp_path, monkeypatch):
        """Test that multiple calls return consistent results."""
        script_dir = tmp_path / "scripts"
        script_dir.mkdir()
        script_file = script_dir / "test.py"
        script_file.write_text("# test")
        
        monkeypatch.setattr(sys, 'argv', [str(script_file)])
        
        # Call multiple times
        result1 = _get_script_directory()
        result2 = _get_script_directory()
        result3 = _get_script_directory()
        
        # All calls should return the same directory
        assert result1 == result2 == result3 == script_dir
    
    def test_get_script_directory_with_windows_path_separator(self, tmp_path, monkeypatch):
        """Test handling of Windows-style path separators."""
        script_dir = tmp_path / "project" / "scripts"
        script_dir.mkdir(parents=True)
        script_file = script_dir / "run.py"
        script_file.write_text("# script")
        
        # Use backslashes (will be handled by Path automatically)
        monkeypatch.setattr(sys, 'argv', [str(script_file)])
        
        result = _get_script_directory()
        
        # Should work regardless of path separator
        assert result == script_dir
    
    def test_get_script_directory_with_trailing_slash(self, tmp_path, monkeypatch):
        """Test script path with trailing slash doesn't affect result."""
        script_dir = tmp_path / "scripts"
        script_dir.mkdir()
        script_file = script_dir / "test.py"
        script_file.write_text("# test")
        
        # Add trailing slash to the path (Path should normalize it)
        path_with_slash = str(script_file) + os.sep
        monkeypatch.setattr(sys, 'argv', [path_with_slash])
        
        result = _get_script_directory()
        
        # Should still get the correct parent directory
        assert result == script_dir



class TestProjectConfig:
    """Test cases for ProjectConfig class."""
    
    def teardown_method(self):
        """Clean up after each test."""
        clear_project_config()
    
    def test_init_with_valid_parent_config(self, tmp_path):
        """Test initialization with config in parent directory."""
        # Create project structure
        project_dir = tmp_path / "project"
        work_dir = project_dir / "work"
        work_dir.mkdir(parents=True)
        
        config_data = {
            "project_name": "test_project",
            "project_version": "1.0.0",
            "globals": {"log_level": "INFO"},
            "mod_defaults": {
                "csv_reader": {"encoding": "utf-8"}
            }
        }
        
        config_file = project_dir / "project_defaults.yaml"
        config_file.write_text(yaml.dump(config_data))
        
        # Initialize from work directory (should find parent config)
        config = ProjectConfig(str(work_dir))
        
        assert config.project_name == "test_project"
        assert config.project_version == "1.0.0"
        assert config.project_path == project_dir
        assert config.config_data["globals"]["log_level"] == "INFO"
    
    def test_init_with_current_dir_config(self, tmp_path):
        """Test initialization with config in current directory."""
        config_data = {
            "project_name": "current_project",
            "globals": {"debug": True}
        }
        
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text(yaml.dump(config_data))
        
        config = ProjectConfig(str(tmp_path))
        
        assert config.project_name == "current_project"
        assert config.project_path == tmp_path
        assert config.config_data["globals"]["debug"] is True
    
    def test_init_no_config_file(self, tmp_path):
        """Test initialization with no config file found."""
        config = ProjectConfig(str(tmp_path))
        
        assert config.config_data == {}
        assert config.project_name is None
        assert config.project_path is None
    
    def test_init_invalid_yaml_raises_error(self, tmp_path):
        """Test initialization with invalid YAML raises error."""
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text("invalid: yaml: content: [")
        
        with pytest.raises(RuntimeError, match="Invalid YAML in project config"):
            ProjectConfig(str(tmp_path))
    
    def test_init_non_dict_yaml_raises_error(self, tmp_path):
        """Test initialization with non-dict YAML raises error."""
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text("- not a dictionary")
        
        with pytest.raises(RuntimeError, match="must contain a YAML dictionary"):
            ProjectConfig(str(tmp_path))
    
    def test_init_permission_error_raises_runtime_error(self, tmp_path):
        """Test initialization with permission error raises RuntimeError."""
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text("project_name: test")
        
        with patch('builtins.open', side_effect=PermissionError("Access denied")):
            with pytest.raises(RuntimeError, match="Cannot read project config"):
                ProjectConfig(str(tmp_path))
    
    def test_default_project_name_from_directory(self, tmp_path):
        """Test default project name is set from directory name."""
        config_data = {"project_version": "1.0.0"}
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text(yaml.dump(config_data))
        
        config = ProjectConfig(str(tmp_path))
        
        assert config.project_name == tmp_path.name
        assert config.config_data["project_name"] == tmp_path.name
    
    def test_get_mod_defaults_valid_mod(self, tmp_path):
        """Test getting mod defaults for existing mod."""
        config_data = {
            "mod_defaults": {
                "csv_reader": {
                    "encoding": "utf-8",
                    "delimiter": ",",
                    "header": 0
                },
                "csv_writer": {
                    "encoding": "utf-8"
                }
            }
        }
        
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text(yaml.dump(config_data))
        config = ProjectConfig(str(tmp_path))
        
        defaults = config.get_mod_defaults("csv_reader")
        
        assert defaults == {
            "encoding": "utf-8",
            "delimiter": ",",
            "header": 0
        }
        
        # Test returns copy
        defaults["new_key"] = "new_value"
        assert "new_key" not in config.get_mod_defaults("csv_reader")
    
    def test_get_mod_defaults_nonexistent_mod(self, tmp_path):
        """Test getting mod defaults for non-existent mod."""
        config_data = {
            "mod_defaults": {
                "csv_reader": {"encoding": "utf-8"}
            }
        }
        
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text(yaml.dump(config_data))
        config = ProjectConfig(str(tmp_path))
        
        defaults = config.get_mod_defaults("nonexistent_mod")
        assert defaults == {}
    
    def test_get_mod_defaults_no_mod_defaults_section(self, tmp_path):
        """Test getting mod defaults when no mod_defaults section exists."""
        config_data = {"project_name": "test"}
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text(yaml.dump(config_data))
        config = ProjectConfig(str(tmp_path))
        
        defaults = config.get_mod_defaults("csv_reader")
        assert defaults == {}
    
    def test_get_mod_defaults_invalid_mod_defaults_type(self, tmp_path):
        """Test getting mod defaults when mod_defaults is not a dict."""
        config_data = {"mod_defaults": "not_a_dict"}
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text(yaml.dump(config_data))
        config = ProjectConfig(str(tmp_path))
        
        defaults = config.get_mod_defaults("csv_reader")
        assert defaults == {}
    
    def test_get_mod_defaults_empty_mod_name_raises_error(self, tmp_path):
        """Test get_mod_defaults with empty mod_name raises ValueError."""
        config = ProjectConfig(str(tmp_path))
        
        with pytest.raises(ValueError, match="mod_name must be a non-empty string"):
            config.get_mod_defaults("")
        
        with pytest.raises(ValueError, match="mod_name must be a non-empty string"):
            config.get_mod_defaults(None)
    
    def test_get_globals_valid_globals(self, tmp_path):
        """Test getting global settings."""
        config_data = {
            "globals": {
                "log_level": "DEBUG",
                "base_path": "/data",
                "timeout": 30,
                "debug_mode": True
            }
        }
        
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text(yaml.dump(config_data))
        config = ProjectConfig(str(tmp_path))
        
        globals_dict = config.get_globals()
        
        assert globals_dict == {
            "log_level": "DEBUG",
            "base_path": "/data",
            "timeout": 30,
            "debug_mode": True
        }
        
        # Test returns copy
        globals_dict["new_key"] = "new_value"
        assert "new_key" not in config.get_globals()
    
    def test_get_globals_no_globals_section(self, tmp_path):
        """Test getting globals when no globals section exists."""
        config_data = {"project_name": "test"}
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text(yaml.dump(config_data))
        config = ProjectConfig(str(tmp_path))
        
        globals_dict = config.get_globals()
        assert globals_dict == {}
    
    def test_get_globals_invalid_globals_type(self, tmp_path):
        """Test getting globals when globals is not a dict."""
        config_data = {"globals": ["not", "a", "dict"]}
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text(yaml.dump(config_data))
        config = ProjectConfig(str(tmp_path))
        
        globals_dict = config.get_globals()
        assert globals_dict == {}


class TestParameterResolver:
    """Test cases for ParameterResolver class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        clear_project_config()
    
    def teardown_method(self):
        """Clean up after each test."""
        clear_project_config()
    
    def test_init_with_project_config(self, tmp_path):
        """Test initialization with explicit project config."""
        config_data = {"project_name": "test"}
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text(yaml.dump(config_data))
        
        project_config = ProjectConfig(str(tmp_path))
        resolver = ParameterResolver(project_config)
        
        assert resolver.project_config is project_config
    
    def test_init_without_project_config(self):
        """Test initialization without explicit project config."""
        with patch('datapy.mod_manager.params.get_project_config') as mock_get:
            mock_config = MagicMock()
            mock_get.return_value = mock_config
            
            resolver = ParameterResolver()
            
            assert resolver.project_config is mock_config
            mock_get.assert_called_once()
    
    def test_resolve_mod_params_with_project_defaults(self, tmp_path):
        """Test parameter resolution with project defaults."""
        config_data = {
            "mod_defaults": {
                "csv_reader": {
                    "encoding": "utf-8",
                    "delimiter": ",",
                    "header": 0
                }
            }
        }
        
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text(yaml.dump(config_data))
        project_config = ProjectConfig(str(tmp_path))
        resolver = ParameterResolver(project_config)
        
        job_params = {
            "file_path": "/data/test.csv",
            "encoding": "latin-1"  # Override project default
        }
        
        resolved = resolver.resolve_mod_params("csv_reader", job_params)
        
        expected = {
            "encoding": "latin-1",  # Job param overrides project default
            "delimiter": ",",       # From project defaults
            "header": 0,           # From project defaults
            "file_path": "/data/test.csv"  # From job params
        }
        
        assert resolved == expected
    
    def test_resolve_mod_params_job_params_only(self, tmp_path):
        """Test parameter resolution with job params only (no project defaults)."""
        config_data = {"project_name": "test"}  # No mod_defaults
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text(yaml.dump(config_data))
        project_config = ProjectConfig(str(tmp_path))
        resolver = ParameterResolver(project_config)
        
        job_params = {
            "file_path": "/data/test.csv",
            "encoding": "utf-8"
        }
        
        resolved = resolver.resolve_mod_params("csv_reader", job_params)
        
        assert resolved == job_params
    
    def test_resolve_mod_params_no_job_params(self, tmp_path):
        """Test parameter resolution with project defaults only."""
        config_data = {
            "mod_defaults": {
                "csv_reader": {
                    "encoding": "utf-8",
                    "delimiter": ","
                }
            }
        }
        
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text(yaml.dump(config_data))
        project_config = ProjectConfig(str(tmp_path))
        resolver = ParameterResolver(project_config)
        
        resolved = resolver.resolve_mod_params("csv_reader", {})
        
        expected = {
            "encoding": "utf-8",
            "delimiter": ","
        }
        
        assert resolved == expected
    
    def test_resolve_mod_params_empty_mod_name_raises_error(self, tmp_path):
        """Test resolve_mod_params with empty mod_name raises ValueError."""
        project_config = ProjectConfig(str(tmp_path))
        resolver = ParameterResolver(project_config)
        
        with pytest.raises(ValueError, match="mod_name must be a non-empty string"):
            resolver.resolve_mod_params("", {})
        
        with pytest.raises(ValueError, match="mod_name must be a non-empty string"):
            resolver.resolve_mod_params(None, {})
    
    def test_resolve_mod_params_non_dict_job_params_raises_error(self, tmp_path):
        """Test resolve_mod_params with non-dict job_params raises ValueError."""
        project_config = ProjectConfig(str(tmp_path))
        resolver = ParameterResolver(project_config)
        
        with pytest.raises(ValueError, match="job_params must be a dictionary"):
            resolver.resolve_mod_params("csv_reader", "not_a_dict")
        
        with pytest.raises(ValueError, match="job_params must be a dictionary"):
            resolver.resolve_mod_params("csv_reader", None)
    
    def test_resolve_mod_params_with_project_config_error(self, tmp_path):
        """Test parameter resolution when project config access fails."""
        project_config = ProjectConfig(str(tmp_path))
        resolver = ParameterResolver(project_config)
        
        # Mock get_mod_defaults to raise exception
        with patch.object(project_config, 'get_mod_defaults', side_effect=Exception("Config error")):
            # Should continue and return job params despite project config error
            resolved = resolver.resolve_mod_params("csv_reader", {"file_path": "test.csv"})
            assert resolved == {"file_path": "test.csv"}


class TestModuleFunctions:
    """Test cases for module-level functions."""
    
    def teardown_method(self):
        """Clean up after each test."""
        clear_project_config()
    
    def test_get_project_config_creates_singleton(self, tmp_path):
        """Test get_project_config creates and caches singleton."""
        config_data = {"project_name": "singleton_test"}
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text(yaml.dump(config_data))
        
        # First call creates instance
        config1 = get_project_config(str(tmp_path))
        assert config1.project_name == "singleton_test"
        
        # Second call returns same instance
        config2 = get_project_config()
        assert config1 is config2
    
    def test_get_project_config_raises_error_on_failure(self, tmp_path):
        """Test get_project_config raises RuntimeError on failure."""
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text("invalid: yaml: [")
        
        with pytest.raises(RuntimeError, match="Failed to create project config"):
            get_project_config(str(tmp_path))
    
    def test_clear_project_config_resets_singleton(self, tmp_path):
        """Test clear_project_config resets global singleton."""
        config_data = {"project_name": "clear_test"}
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text(yaml.dump(config_data))
        
        # Create singleton
        config1 = get_project_config(str(tmp_path))
        assert config1.project_name == "clear_test"
        
        # Clear singleton
        clear_project_config()
        
        # Verify global is reset
        from datapy.mod_manager.params import _global_project_config
        assert _global_project_config is None
        
        # New call creates new instance
        config2 = get_project_config(str(tmp_path))
        assert config1 is not config2
    
    def test_load_job_config_valid_yaml(self, tmp_path):
        """Test loading valid job configuration."""
        job_data = {
            "globals": {"log_level": "INFO"},
            "mods": {
                "extract_data": {
                    "_type": "csv_reader",
                    "file_path": "/data/input.csv"
                }
            }
        }
        
        config_file = tmp_path / "job_config.yaml"
        config_file.write_text(yaml.dump(job_data))
        
        loaded_config = load_job_config(str(config_file))
        assert loaded_config == job_data
    
    def test_load_job_config_empty_file_returns_empty_dict(self, tmp_path):
        """Test loading empty job config returns empty dict."""
        config_file = tmp_path / "empty_config.yaml"
        config_file.write_text("")
        
        loaded_config = load_job_config(str(config_file))
        assert loaded_config == {}
    
    def test_load_job_config_file_not_found_raises_error(self, tmp_path):
        """Test loading non-existent config raises FileNotFoundError."""
        missing_file = tmp_path / "missing.yaml"
        
        with pytest.raises(FileNotFoundError, match="Job config file not found"):
            load_job_config(str(missing_file))
    
    def test_load_job_config_directory_raises_error(self, tmp_path):
        """Test loading directory path raises FileNotFoundError."""
        directory = tmp_path / "config_dir"
        directory.mkdir()
        
        with pytest.raises(FileNotFoundError, match="Path is not a file"):
            load_job_config(str(directory))
    
    def test_load_job_config_invalid_yaml_raises_error(self, tmp_path):
        """Test loading invalid YAML raises RuntimeError."""
        config_file = tmp_path / "invalid.yaml"
        config_file.write_text("invalid: yaml: content: [")
        
        with pytest.raises(RuntimeError, match="Invalid YAML"):
            load_job_config(str(config_file))
    
    def test_load_job_config_non_dict_yaml_raises_error(self, tmp_path):
        """Test loading non-dict YAML raises RuntimeError."""
        config_file = tmp_path / "list_config.yaml"
        config_file.write_text("- item1\n- item2")
        
        with pytest.raises(RuntimeError, match="must contain a YAML dictionary"):
            load_job_config(str(config_file))
    
    def test_load_job_config_permission_error_raises_runtime_error(self, tmp_path):
        """Test loading config with permission error raises RuntimeError."""
        config_file = tmp_path / "permission_config.yaml"
        config_file.write_text("test: config")
        
        with patch('builtins.open', side_effect=PermissionError("Access denied")):
            with pytest.raises(RuntimeError, match="Cannot read job config"):
                load_job_config(str(config_file))
    
    def test_load_job_config_empty_path_raises_error(self):
        """Test loading config with empty path raises ValueError."""
        with pytest.raises(ValueError, match="config_path must be a non-empty string"):
            load_job_config("")
        
        with pytest.raises(ValueError, match="config_path must be a non-empty string"):
            load_job_config(None)
    
    def test_create_resolver_success(self, tmp_path):
        """Test create_resolver returns configured resolver."""
        config_data = {"project_name": "resolver_test"}
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text(yaml.dump(config_data))
        
        resolver = create_resolver(str(tmp_path))
        
        assert isinstance(resolver, ParameterResolver)
        assert resolver.project_config.project_name == "resolver_test"
    
    def test_create_resolver_raises_error_on_failure(self, tmp_path):
        """Test create_resolver raises RuntimeError on failure."""
        with patch('datapy.mod_manager.params.get_project_config', side_effect=Exception("Config error")):
            with pytest.raises(RuntimeError, match="Failed to create parameter resolver"):
                create_resolver(str(tmp_path))


class TestIntegrationScenarios:
    """Integration test cases for complete parameter workflows."""
    
    def teardown_method(self):
        """Clean up after each test."""
        clear_project_config()
    
    def test_complete_parameter_resolution_workflow(self, tmp_path):
        """Test complete parameter resolution from project config to job execution."""
        # Create project structure
        project_dir = tmp_path / "project"
        jobs_dir = project_dir / "jobs"
        jobs_dir.mkdir(parents=True)
        
        # Project configuration
        project_config_data = {
            "project_name": "ETL Pipeline",
            "project_version": "2.1.0",
            "globals": {
                "log_level": "INFO",
                "base_data_path": "/data"
            },
            "mod_defaults": {
                "csv_reader": {
                    "encoding": "utf-8",
                    "delimiter": ",",
                    "header": 0,
                    "skip_empty_lines": True
                },
                "csv_filter": {
                    "drop_duplicates": True,
                    "sort_results": False
                },
                "csv_writer": {
                    "encoding": "utf-8",
                    "create_directories": True
                }
            }
        }
        
        project_config_file = project_dir / "project_defaults.yaml"
        project_config_file.write_text(yaml.dump(project_config_data))
        
        # Job configuration
        job_config_data = {
            "globals": {
                "log_level": "DEBUG"  # Override project default
            },
            "mods": {
                "extract_customers": {
                    "_type": "csv_reader",
                    "file_path": "/data/customers.csv",
                    "encoding": "latin-1"  # Override project default
                },
                "filter_adults": {
                    "_type": "csv_filter",
                    "filter_conditions": {"age": {"gte": 18}},
                    "sort_results": True  # Override project default
                },
                "save_results": {
                    "_type": "csv_writer",
                    "output_path": "/output/filtered_customers.csv"
                }
            }
        }
        
        job_config_file = jobs_dir / "customer_pipeline.yaml"
        job_config_file.write_text(yaml.dump(job_config_data))
        
        # Execute parameter resolution workflow
        resolver = create_resolver(str(jobs_dir))  # Start from jobs directory
        job_config = load_job_config(str(job_config_file))
        
        # Test csv_reader parameter resolution
        csv_reader_params = job_config["mods"]["extract_customers"]
        resolved_reader = resolver.resolve_mod_params("csv_reader", csv_reader_params)
        
        expected_reader = {
            "_type": "csv_reader",
            "file_path": "/data/customers.csv",
            "encoding": "latin-1",  # Job override
            "delimiter": ",",       # From project defaults
            "header": 0,           # From project defaults
            "skip_empty_lines": True  # From project defaults
        }
        
        assert resolved_reader == expected_reader
        
        # Test csv_filter parameter resolution
        csv_filter_params = job_config["mods"]["filter_adults"]
        resolved_filter = resolver.resolve_mod_params("csv_filter", csv_filter_params)
        
        expected_filter = {
            "_type": "csv_filter",
            "filter_conditions": {"age": {"gte": 18}},
            "drop_duplicates": True,  # From project defaults
            "sort_results": True     # Job override
        }
        
        assert resolved_filter == expected_filter
        
        # Test csv_writer parameter resolution
        csv_writer_params = job_config["mods"]["save_results"]
        resolved_writer = resolver.resolve_mod_params("csv_writer", csv_writer_params)
        
        expected_writer = {
            "_type": "csv_writer",
            "output_path": "/output/filtered_customers.csv",
            "encoding": "utf-8",        # From project defaults
            "create_directories": True  # From project defaults
        }
        
        assert resolved_writer == expected_writer
    
    def test_project_config_discovery_hierarchy(self, tmp_path):
        """Test project config discovery follows proper hierarchy."""
        # Create nested directory structure  
        project_dir = tmp_path / "pipeline"
        jobs_dir = project_dir / "jobs"
        jobs_dir.mkdir(parents=True)
        
        # Create project config at project level
        project_config_data = {
            "project_name": "ETL Pipeline",
            "mod_defaults": {
                "csv_reader": {"encoding": "utf-8"}
            }
        }
        
        project_config_file = project_dir / "project_defaults.yaml"
        project_config_file.write_text(yaml.dump(project_config_data))
        
        # Clear global state first
        clear_project_config()
        
        # Test discovery from jobs directory (should find parent config)
        resolver = create_resolver(str(jobs_dir))
        assert resolver.project_config.project_name == "ETL Pipeline"
        assert resolver.project_config.project_path == project_dir


class TestErrorHandling:
    """Test cases for comprehensive error handling."""
    
    def teardown_method(self):
        """Clean up after each test."""
        clear_project_config()
    
    def test_yaml_parsing_errors(self, tmp_path):
        """Test various YAML parsing error scenarios."""
        # Create a file that will cause YAML parsing to fail in load_job_config
        invalid_yaml_content = "key: value\n[invalid syntax without proper mapping"
        
        config_file = tmp_path / "invalid.yaml"
        config_file.write_text(invalid_yaml_content)
        
        # Test load_job_config - this should definitely raise error for invalid YAML
        with pytest.raises(RuntimeError, match="Invalid YAML"):
            load_job_config(str(config_file))
        
        # Test ProjectConfig with a different invalid YAML that causes parsing error
        project_config_file = tmp_path / "project_defaults.yaml"
        project_config_file.write_text("key: value\n{invalid: syntax")
        
        clear_project_config()
        with pytest.raises(RuntimeError, match="Invalid YAML"):
            ProjectConfig(str(tmp_path))
    
    def test_file_system_error_scenarios(self, tmp_path):
        """Test various file system error scenarios."""
        # Test permission errors - create a valid config first
        config_file = tmp_path / "project_defaults.yaml"  
        config_file.write_text("test: config")
        
        # Clear global state
        clear_project_config()
        
        # Mock file open to simulate permission error
        with patch('builtins.open', side_effect=PermissionError("Access denied")):
            with pytest.raises(RuntimeError, match="Cannot read project config"):
                ProjectConfig(str(tmp_path))
        
        # Test general I/O errors
        clear_project_config()
        with patch('builtins.open', side_effect=IOError("Disk error")):
            with pytest.raises(RuntimeError, match="Failed to load project config"):
                ProjectConfig(str(tmp_path))
    
    def test_invalid_configuration_structures(self, tmp_path):
        """Test handling of invalid configuration structures."""
        # Test non-dict at root level
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text("not a dictionary")
        
        clear_project_config()
        with pytest.raises(RuntimeError, match="must contain a YAML dictionary"):
            ProjectConfig(str(tmp_path))
        
        # Test invalid mod_defaults type
        clear_project_config()
        config_file.write_text(yaml.dump({"mod_defaults": "should_be_dict"}))
        config = ProjectConfig(str(tmp_path))
        assert config.get_mod_defaults("any_mod") == {}
        
        # Test invalid globals type  
        clear_project_config()
        config_file.write_text(yaml.dump({"globals": ["should", "be", "dict"]}))
        config = ProjectConfig(str(tmp_path))
        assert config.get_globals() == {}
    
    def test_parameter_resolution_with_corrupted_project_config(self, tmp_path):
        """Test parameter resolution when project config is corrupted."""
        # Create initially valid config
        config_data = {
            "project_name": "test",
            "mod_defaults": {
                "csv_reader": {"encoding": "utf-8"}
            }
        }
        
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text(yaml.dump(config_data))
        
        # Create resolver with valid config
        resolver = create_resolver(str(tmp_path))
        
        # Simulate project config method failure during resolution
        with patch.object(resolver.project_config, 'get_mod_defaults', side_effect=Exception("Corruption error")):
            # Should still return job params despite project config failure
            job_params = {"file_path": "test.csv"}
            resolved = resolver.resolve_mod_params("csv_reader", job_params)
            assert resolved == {"file_path": "test.csv"}
    
    def test_edge_case_inputs_actual_validation(self, tmp_path):
        """Test edge case input validation - checking actual behavior."""
        clear_project_config()
        config = ProjectConfig(str(tmp_path))
        resolver = ParameterResolver(config)
        
        # The implementation validates some cases strictly, test actual behavior
        
        # Test empty string - should raise ValueError
        with pytest.raises(ValueError, match="mod_name must be a non-empty string"):
            config.get_mod_defaults("")
        
        # Test whitespace - implementation might not strip, so just check it returns empty dict
        result_whitespace = config.get_mod_defaults("   ")
        assert result_whitespace == {}  # Should return empty dict for non-existent mod
        
        # Test None - should raise ValueError
        with pytest.raises(ValueError, match="mod_name must be a non-empty string"):
            config.get_mod_defaults(None)
        
        # Test non-string inputs - should raise ValueError
        for invalid_input in [123, [], {}]:
            with pytest.raises(ValueError, match="mod_name must be a non-empty string"):
                config.get_mod_defaults(invalid_input)
        
        # Test resolver with invalid job_params (this validation should work)
        invalid_job_params = [
            "string",     # String instead of dict
            123,          # Number
            None,         # None
            [],           # List
        ]
        
        for invalid_params in invalid_job_params:
            with pytest.raises(ValueError, match="job_params must be a dictionary"):
                resolver.resolve_mod_params("csv_reader", invalid_params)
   
    def test_concurrent_access_safety(self, tmp_path):
       """Test thread safety of global singleton access."""
       import threading
       
       config_data = {"project_name": "concurrent_test"}
       config_file = tmp_path / "project_defaults.yaml"
       config_file.write_text(yaml.dump(config_data))
       
       # Clear global state first
       clear_project_config()
       
       results = []
       errors = []
       
       def worker():
           try:
               config = get_project_config(str(tmp_path))
               results.append(config)
           except Exception as e:
               errors.append(e)
       
       # Create multiple threads accessing singleton
       threads = []
       for _ in range(5):  # Reduce thread count for more reliable test
           thread = threading.Thread(target=worker)
           threads.append(thread)
       
       # Start all threads
       for thread in threads:
           thread.start()
       
       # Wait for completion
       for thread in threads:
           thread.join()
       
       # Verify no errors
       assert len(errors) == 0, f"Errors occurred: {errors}"
       assert len(results) == 5
       
       # Verify all results are valid ProjectConfig instances
       for config in results:
           assert isinstance(config, ProjectConfig)
           assert config.project_name == "concurrent_test"


class TestMemoryAndResourceManagement:
    """Test cases for memory usage and resource cleanup."""
   
    def teardown_method(self):
       """Clean up after each test."""
       clear_project_config()
   
    def test_global_singleton_cleanup(self, tmp_path):
       """Test proper cleanup of global singleton."""
       config_data = {"project_name": "cleanup_test"}
       config_file = tmp_path / "project_defaults.yaml" 
       config_file.write_text(yaml.dump(config_data))
       
       # Clear global state first
       clear_project_config()
       
       # Create singleton
       config1 = get_project_config(str(tmp_path))
       assert config1.project_name == "cleanup_test"
       
       # Verify global state exists
       from datapy.mod_manager.params import _global_project_config
       assert _global_project_config is not None
       
       # Clear and verify cleanup
       clear_project_config()
       
       # Import again to get current state
       from datapy.mod_manager.params import _global_project_config as current_global
       assert current_global is None
       
       # New instance should be different
       config2 = get_project_config(str(tmp_path))
       assert config1 is not config2
       assert config2.project_name == "cleanup_test"
   
    def test_large_configuration_handling(self, tmp_path):
       """Test handling of large configuration files."""
       # Create large configuration
       large_config = {
           "project_name": "large_config_test",
           "mod_defaults": {}
       }
       
       # Add many mod defaults
       for i in range(100):
           mod_name = f"mod_{i}"
           large_config["mod_defaults"][mod_name] = {
               f"param_{j}": f"value_{i}_{j}" for j in range(50)
           }
       
       config_file = tmp_path / "project_defaults.yaml"
       config_file.write_text(yaml.dump(large_config))
       
       # Clear global state
       clear_project_config()
       
       # Should handle large config without issues
       config = ProjectConfig(str(tmp_path))
       assert config.project_name == "large_config_test"
       assert len(config.config_data["mod_defaults"]) == 100
       
       # Test accessing specific mod defaults
       mod_50_defaults = config.get_mod_defaults("mod_50")
       assert len(mod_50_defaults) == 50
       assert mod_50_defaults["param_25"] == "value_50_25"
   
    def test_special_characters_in_configs(self, tmp_path):
       """Test handling of special characters in configs."""
       config_with_special_chars = {
           "project_name": "test_project",
           "globals": {
               "message": "Hello, world!",
               "path": "/data/study",
               "special_chars": "!@#$%^&*()+={}[]|\\:;\"'<>,.?/"
           },
           "mod_defaults": {
               "csv_reader": {
                   "encoding": "utf-8",
                   "error_message": "Error reading file: file not found"
               }
           }
       }
       
       config_file = tmp_path / "project_defaults.yaml"
       config_file.write_text(yaml.dump(config_with_special_chars, allow_unicode=True), encoding='utf-8')
       
       # Clear global state
       clear_project_config()
       
       config = ProjectConfig(str(tmp_path))
       
       assert config.project_name == "test_project"
       
       globals_dict = config.get_globals()
       assert globals_dict["message"] == "Hello, world!"
       assert globals_dict["path"] == "/data/study"
       assert "!@#$%^&*()" in globals_dict["special_chars"]
       
       mod_defaults = config.get_mod_defaults("csv_reader")
       assert mod_defaults["error_message"] == "Error reading file: file not found"


class TestBackwardCompatibility:
    """Test cases for backward compatibility and version handling."""
   
    def teardown_method(self):
       """Clean up after each test."""
       clear_project_config()
   
    def test_minimal_project_config(self, tmp_path):
       """Test minimal project configuration (only required fields)."""
       minimal_config = {
           "project_name": "minimal_project"
       }
       
       config_file = tmp_path / "project_defaults.yaml"
       config_file.write_text(yaml.dump(minimal_config))
       
       config = ProjectConfig(str(tmp_path))
       
       assert config.project_name == "minimal_project"
       assert config.project_version is None
       assert config.get_globals() == {}
       assert config.get_mod_defaults("any_mod") == {}
   
    def test_legacy_config_structure_compatibility(self, tmp_path):
       """Test compatibility with potential legacy config structures."""
       # Test config with extra/unknown fields
       legacy_config = {
           "project_name": "legacy_project",
           "project_version": "1.0.0",
           "globals": {"log_level": "INFO"},
           "mod_defaults": {"csv_reader": {"encoding": "utf-8"}},
           
           # Legacy/unknown fields (should be ignored gracefully)
           "deprecated_field": "old_value",
           "old_section": {"old_key": "old_value"}
       }
       
       config_file = tmp_path / "project_defaults.yaml"
       config_file.write_text(yaml.dump(legacy_config))
       
       config = ProjectConfig(str(tmp_path))
       
       # Should work with known fields
       assert config.project_name == "legacy_project"
       assert config.project_version == "1.0.0"
       assert config.get_globals()["log_level"] == "INFO"
       assert config.get_mod_defaults("csv_reader")["encoding"] == "utf-8"
       
       # Unknown fields should be preserved in config_data but not interfere
       assert "deprecated_field" in config.config_data
       assert config.config_data["deprecated_field"] == "old_value"
   
    def test_empty_config_file_handling_actual_behavior(self, tmp_path):
        """Test handling of empty configuration files - check actual behavior."""
        # Clear global state
        clear_project_config()
        
        # Completely empty file
        empty_config_file = tmp_path / "project_defaults.yaml"
        empty_config_file.write_text("")
        
        config = ProjectConfig(str(tmp_path))
        
        # The implementation sets project_name to directory name when not in config
        expected_project_name = tmp_path.name
        assert config.project_name == expected_project_name
        
        # The config_data should contain the auto-generated project_name
        assert config.config_data.get("project_name") == expected_project_name
        
        # File with only comments
        clear_project_config()
        comment_only_file = tmp_path / "project_defaults.yaml"
        comment_only_file.write_text("# This is a comment\n# Another comment\n")
        
        config2 = ProjectConfig(str(tmp_path))
        assert config2.project_name == tmp_path.name
        
        # File with only spaces (no tabs to avoid YAML parsing issues)
        clear_project_config()
        whitespace_file = tmp_path / "project_defaults.yaml"
        whitespace_file.write_text("   \n   \n   ")
        
        config3 = ProjectConfig(str(tmp_path))
        assert config3.project_name == tmp_path.name


class TestAdditionalCoverage:
    """Additional test cases to improve coverage."""
   
    def teardown_method(self):
       """Clean up after each test."""
       clear_project_config()
   
    def test_project_config_find_config_file_edge_cases(self, tmp_path):
        """Test edge cases in config file discovery."""
        # Test when both parent and current have config files
        parent_dir = tmp_path / "parent"
        child_dir = parent_dir / "child"
        child_dir.mkdir(parents=True)
        
        # Create config in parent
        parent_config = {"project_name": "parent_project"}
        parent_config_file = parent_dir / "project_defaults.yaml"
        parent_config_file.write_text(yaml.dump(parent_config))
        
        # Create config in child
        child_config = {"project_name": "child_project"}
        child_config_file = child_dir / "project_defaults.yaml"
        child_config_file.write_text(yaml.dump(child_config))
        
        # Initialize from child directory
        config = ProjectConfig(str(child_dir))
        
        # CORRECTED EXPECTATION: The implementation searches from current directory first
        # So it should find CHILD config first, not parent
        assert config.project_name == "child_project"
        assert config.project_path == child_dir
        
        # Test that parent config would be found if child didn't have one
        clear_project_config()
        child_config_file.unlink()  # Remove child config
        
        config2 = ProjectConfig(str(child_dir), max_depth=1)
        assert config2.project_name == "parent_project"
        assert config2.project_path == parent_dir
   
    def test_project_config_properties(self, tmp_path):
        """Test project config property accessors."""
        config_data = {
            "project_name": "property_test",
            "project_version": "2.5.1"
        }
        
        config_file = tmp_path / "project_defaults.yaml"
        config_file.write_text(yaml.dump(config_data))
        
        config = ProjectConfig(str(tmp_path))
        
        # Test property accessors
        assert config.project_name == "property_test"
        assert config.project_version == "2.5.1"
        
        # Test when properties don't exist - create completely separate temp directory
        import tempfile
        with tempfile.TemporaryDirectory() as temp_dir:
            clear_project_config()
            empty_config = ProjectConfig(temp_dir)
            # When no config file exists, project_name and project_version should be None
            assert empty_config.project_name is None
            assert empty_config.project_version is None
   
    def test_resolver_with_complex_nested_params(self, tmp_path):
       """Test parameter resolver with complex nested parameter structures."""
       config_data = {
           "mod_defaults": {
               "complex_mod": {
                   "nested": {
                       "level1": {
                           "level2": "deep_value"
                       },
                       "list_param": [1, 2, 3]
                   },
                   "simple": "value"
               }
           }
       }
       
       config_file = tmp_path / "project_defaults.yaml"
       config_file.write_text(yaml.dump(config_data))
       
       resolver = create_resolver(str(tmp_path))
       
       # Job params that override nested structures
       job_params = {
           "nested": {
               "level1": {
                   "level2": "overridden"
               },
               "new_param": "added"
           },
           "additional": "param"
       }
       
       resolved = resolver.resolve_mod_params("complex_mod", job_params)
       
       # Job params should completely replace, not merge nested structures
       expected = {
           "nested": {
               "level1": {
                   "level2": "overridden"
               },
               "new_param": "added"
           },
           "simple": "value",  # From project defaults
           "additional": "param"  # From job params
       }
       
       assert resolved == expected
   
    def test_load_job_config_with_various_yaml_formats(self, tmp_path):
       """Test load_job_config with various YAML formats."""
       # Test with complex YAML structures
       complex_yaml = {
           "globals": {
               "nested": {"deep": {"value": "test"}},
               "list": [1, 2, {"nested_in_list": True}]
           },
           "mods": {
               "mod1": {
                   "_type": "test_mod",
                   "complex_param": {
                       "sub_param": [1, 2, 3],
                       "another_sub": {"key": "value"}
                   }
               }
           }
       }
       
       config_file = tmp_path / "complex_config.yaml"
       config_file.write_text(yaml.dump(complex_yaml))
       
       loaded = load_job_config(str(config_file))
       assert loaded == complex_yaml
       assert loaded["globals"]["nested"]["deep"]["value"] == "test"
       assert loaded["mods"]["mod1"]["complex_param"]["sub_param"] == [1, 2, 3]
   
    def test_error_message_formatting(self, tmp_path):
       """Test that error messages contain helpful information."""
       # Test file not found error message
       missing_file = tmp_path / "missing.yaml"
       
       try:
           load_job_config(str(missing_file))
           assert False, "Should have raised FileNotFoundError"
       except FileNotFoundError as e:
           assert "not found" in str(e)
           assert str(missing_file) in str(e) or "missing.yaml" in str(e)
       
       # Test invalid YAML error message  
       invalid_file = tmp_path / "invalid.yaml"
       invalid_file.write_text("invalid: yaml: [content")
       
       try:
           load_job_config(str(invalid_file))
           assert False, "Should have raised RuntimeError"
       except RuntimeError as e:
           assert "Invalid YAML" in str(e)
           assert str(invalid_file) in str(e) or "invalid.yaml" in str(e)