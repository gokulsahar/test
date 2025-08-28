"""
Test 07: Parameter System
Tests the parameter management system including project configuration discovery,
parameter resolution chain, and parameter validation.
"""

import sys
import os
import yaml
import json
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory, NamedTemporaryFile

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datapy.mod_manager.params import (
    ProjectConfig, ParameterResolver, load_job_config, create_resolver
)
from datapy.mod_manager.parameter_validation import validate_mod_parameters
from datapy.mod_manager.registry import ModRegistry
from datapy.mod_manager.base import ModMetadata, ConfigSchema


def create_test_project_config(config_data: dict) -> str:
    """Create temporary project_defaults.yaml file."""
    with NamedTemporaryFile(mode='w', suffix='_project_defaults.yaml', delete=False, encoding='utf-8') as f:
        yaml.dump(config_data, f, default_flow_style=False)
        return f.name


def create_test_job_config(config_data: dict) -> str:
    """Create temporary job config YAML file."""
    with NamedTemporaryFile(mode='w', suffix='_job.yaml', delete=False, encoding='utf-8') as f:
        yaml.dump(config_data, f, default_flow_style=False)
        return f.name


def create_test_registry_with_mod() -> str:
    """Create test registry with a sample mod for validation testing."""
    registry_data = {
        "_metadata": {
            "version": "1.0.0",
            "created": "2024-01-01",
            "description": "Test Registry",
            "last_updated": "2024-01-01"
        },
        "mods": {
            "test_validator_mod": {
                "module_path": "test.validator.mod",
                "type": "test_validator_mod",
                "version": "1.0.0",
                "description": "Test mod for parameter validation",
                "category": "solo",
                "input_ports": [],
                "output_ports": ["result"],
                "globals": ["test_global"],
                "packages": [],
                "python_version": ">=3.8",
                "config_schema": {
                    "required": {
                        "required_param": {
                            "type": "str",
                            "description": "Required string parameter"
                        },
                        "required_int": {
                            "type": "int",
                            "description": "Required integer parameter"
                        }
                    },
                    "optional": {
                        "optional_param": {
                            "type": "str",
                            "default": "default_value",
                            "description": "Optional string parameter"
                        },
                        "optional_bool": {
                            "type": "bool",
                            "default": True,
                            "description": "Optional boolean parameter"
                        }
                    }
                },
                "registered_at": "2024-01-01T00:00:00"
            }
        }
    }
    
    with NamedTemporaryFile(mode='w', suffix='_registry.json', delete=False, encoding='utf-8') as f:
        json.dump(registry_data, f, indent=2)
        return f.name


def cleanup_file(file_path: str) -> None:
    """Clean up temporary file."""
    try:
        if os.path.exists(file_path):
            os.unlink(file_path)
    except:
        pass


def test_project_config_discovery():
    """Test project configuration discovery in different directory structures."""
    print("=== Test: Project Config Discovery ===")
    
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create project structure: project_root/jobs/current_dir
        project_root = temp_path / "project_root"
        jobs_dir = project_root / "jobs"
        current_dir = jobs_dir / "current_job"
        
        project_root.mkdir()
        jobs_dir.mkdir()
        current_dir.mkdir()
        
        # Test 1: No project config found
        config = ProjectConfig(str(current_dir))
        assert config.project_path is None
        assert config.config_data == {}
        assert config.project_name is None
        
        print("PASS: No project config case handled")
        
        # Test 2: Project config in parent directory (typical case)
        project_config_data = {
            "project_name": "test_project",
            "project_version": "1.0.0",
            "mod_defaults": {
                "csv_reader": {
                    "encoding": "utf-8",
                    "delimiter": ","
                }
            },
            "globals": {
                "base_path": "/data",
                "debug_mode": True
            }
        }
        
        project_config_file = project_root / "project_defaults.yaml"
        with open(project_config_file, 'w', encoding='utf-8') as f:
            yaml.dump(project_config_data, f)
        
        # Search from jobs directory should find parent config
        config = ProjectConfig(str(jobs_dir))
        assert config.project_path == project_root
        assert config.project_name == "test_project"
        assert config.project_version == "1.0.0"
        
        print("PASS: Parent directory project config discovery works")
        
        # Test 3: Project config in current directory (fallback)
        current_config_file = current_dir / "project_defaults.yaml"
        with open(current_config_file, 'w', encoding='utf-8') as f:
            yaml.dump({"project_name": "current_project"}, f)
        
        config = ProjectConfig(str(current_dir))
        assert config.project_name == "current_project"
        
        print("PASS: Current directory project config discovery works")


def test_project_config_validation():
    """Test project config validation and error handling."""
    print("\n=== Test: Project Config Validation ===")
    
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Test invalid YAML
        invalid_yaml_file = temp_path / "project_defaults.yaml"
        with open(invalid_yaml_file, 'w') as f:
            f.write("invalid: yaml: content: {")
        
        try:
            ProjectConfig(str(temp_path))
            assert False, "Should fail with invalid YAML"
        except RuntimeError as e:
            assert "Invalid YAML" in str(e)
            print("PASS: Invalid YAML rejected")
        
        # Test non-dictionary content
        non_dict_file = temp_path / "project_defaults.yaml"
        with open(non_dict_file, 'w') as f:
            yaml.dump(["not", "a", "dictionary"], f)
        
        try:
            ProjectConfig(str(temp_path))
            assert False, "Should fail with non-dictionary YAML"
        except RuntimeError as e:
            assert "must contain a YAML dictionary" in str(e)
            print("PASS: Non-dictionary YAML rejected")


def test_project_config_methods():
    """Test ProjectConfig methods for getting defaults and globals."""
    print("\n=== Test: ProjectConfig Methods ===")
    
    config_data = {
        "project_name": "test_project",
        "project_version": "2.0.0",
        "mod_defaults": {
            "csv_reader": {
                "encoding": "utf-8",
                "delimiter": ",",
                "skip_rows": 0
            },
            "data_transformer": {
                "batch_size": 1000,
                "validation": True
            }
        },
        "globals": {
            "environment": "test",
            "debug_level": "INFO",
            "max_retries": 3
        }
    }
    
    config_file = create_test_project_config(config_data)
    
    try:
        # Create config by copying to temp directory structure
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            target_file = temp_path / "project_defaults.yaml"
            shutil.copy2(config_file, target_file)
            
            config = ProjectConfig(str(temp_path))
            
            # Test mod defaults retrieval
            csv_defaults = config.get_mod_defaults("csv_reader")
            expected_csv = {
                "encoding": "utf-8",
                "delimiter": ",", 
                "skip_rows": 0
            }
            assert csv_defaults == expected_csv
            
            transformer_defaults = config.get_mod_defaults("data_transformer")
            expected_transformer = {
                "batch_size": 1000,
                "validation": True
            }
            assert transformer_defaults == expected_transformer
            
            # Test non-existent mod
            empty_defaults = config.get_mod_defaults("nonexistent_mod")
            assert empty_defaults == {}
            
            print("PASS: Mod defaults retrieval works")
            
            # Test globals retrieval
            globals_config = config.get_globals()
            expected_globals = {
                "environment": "test",
                "debug_level": "INFO",
                "max_retries": 3
            }
            assert globals_config == expected_globals
            
            print("PASS: Globals retrieval works")
            
            # Test project metadata
            assert config.project_name == "test_project"
            assert config.project_version == "2.0.0"
            
            print("PASS: Project metadata access works")
            
    finally:
        cleanup_file(config_file)


def test_parameter_resolver_creation():
    """Test ParameterResolver creation and configuration."""
    print("\n=== Test: Parameter Resolver Creation ===")
    
    # Test with no project config
    resolver = ParameterResolver()
    assert resolver.project_config is not None
    
    print("PASS: Default resolver creation works")
    
    # Test with explicit project config
    config_data = {"project_name": "explicit_project"}
    config_file = create_test_project_config(config_data)
    
    try:
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            target_file = temp_path / "project_defaults.yaml"
            shutil.copy2(config_file, target_file)
            
            project_config = ProjectConfig(str(temp_path))
            resolver = ParameterResolver(project_config)
            
            assert resolver.project_config.project_name == "explicit_project"
            
            print("PASS: Explicit project config resolver creation works")
            
    finally:
        cleanup_file(config_file)
    
    # Test convenience function
    resolver = create_resolver()
    assert isinstance(resolver, ParameterResolver)
    
    print("PASS: Convenience function works")


def test_mod_parameter_resolution():
    """Test mod parameter resolution with inheritance chain."""
    print("\n=== Test: Mod Parameter Resolution ===")
    
    config_data = {
        "project_name": "param_test_project",
        "mod_defaults": {
            "test_mod": {
                "project_param": "from_project",
                "shared_param": "project_value",
                "project_only": "project_only_value"
            }
        }
    }
    
    config_file = create_test_project_config(config_data)
    
    try:
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            target_file = temp_path / "project_defaults.yaml"
            shutil.copy2(config_file, target_file)
            
            project_config = ProjectConfig(str(temp_path))
            resolver = ParameterResolver(project_config)
            
            # Test parameter resolution chain
            job_params = {
                "job_param": "from_job",
                "shared_param": "job_value",  # Should override project
                "job_only": "job_only_value"
            }
            
            resolved = resolver.resolve_mod_params("test_mod", job_params)
            
            # Verify resolution order: project defaults + job params (job wins conflicts)
            expected = {
                "project_param": "from_project",    # From project defaults
                "shared_param": "job_value",        # Job overrides project
                "project_only": "project_only_value", # Project only
                "job_param": "from_job",            # Job only
                "job_only": "job_only_value"        # Job only
            }
            
            assert resolved == expected
            
            print("PASS: Parameter resolution inheritance chain works")
            
            # Test mod without project defaults
            no_defaults_resolved = resolver.resolve_mod_params("unknown_mod", job_params)
            assert no_defaults_resolved == job_params
            
            print("PASS: Mod without project defaults handled correctly")
            
    finally:
        cleanup_file(config_file)


def test_job_config_loading():
    """Test job configuration file loading and validation."""
    print("\n=== Test: Job Config Loading ===")
    
    # Test valid job config
    job_config_data = {
        "mods": {
            "extract_data": {
                "_type": "csv_reader",
                "file_path": "/data/input.csv",
                "encoding": "utf-8"
            },
            "process_data": {
                "_type": "data_processor",
                "batch_size": 1000
            }
        },
        "globals": {
            "environment": "test",
            "debug": True
        }
    }
    
    job_config_file = create_test_job_config(job_config_data)
    
    try:
        loaded_config = load_job_config(job_config_file)
        assert loaded_config == job_config_data
        
        print("PASS: Valid job config loading works")
        
    finally:
        cleanup_file(job_config_file)
    
    # Test non-existent file
    try:
        load_job_config("/path/that/does/not/exist.yaml")
        assert False, "Should fail for non-existent file"
    except FileNotFoundError as e:
        assert "not found" in str(e)
        print("PASS: Non-existent file error handled")
    
    # Test invalid YAML
    with NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8') as f:
        f.write("invalid: yaml: {")
        invalid_file = f.name
    
    try:
        try:
            load_job_config(invalid_file)
            assert False, "Should fail with invalid YAML"
        except RuntimeError as e:
            assert "Invalid YAML" in str(e)
            print("PASS: Invalid YAML job config error handled")
    finally:
        cleanup_file(invalid_file)
    
    # Test non-dictionary content
    with NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8') as f:
        yaml.dump(["not", "a", "dictionary"], f)
        non_dict_file = f.name
    
    try:
        try:
            load_job_config(non_dict_file)
            assert False, "Should fail with non-dictionary YAML"
        except RuntimeError as e:
            assert "must contain a YAML dictionary" in str(e)
            print("PASS: Non-dictionary job config error handled")
    finally:
        cleanup_file(non_dict_file)


def test_parameter_validation():
    """Test parameter validation using registry mod info."""
    print("\n=== Test: Parameter Validation ===")
    
    registry_file = create_test_registry_with_mod()
    
    try:
        registry = ModRegistry(registry_file)
        mod_info = registry.get_mod_info("test_validator_mod")
        
        # Test valid parameters with all required fields
        valid_params = {
            "required_param": "test_value",
            "required_int": 42,
            "optional_param": "custom_value",
            "optional_bool": False
        }
        
        validated = validate_mod_parameters(mod_info, valid_params)
        assert validated == valid_params
        
        print("PASS: Valid parameter validation works")
        
        # Test parameters with defaults applied
        minimal_params = {
            "required_param": "test_value",
            "required_int": 123
            # optional parameters missing - should get defaults
        }
        
        validated = validate_mod_parameters(mod_info, minimal_params)
        expected = {
            "required_param": "test_value",
            "required_int": 123,
            "optional_param": "default_value",  # Default applied
            "optional_bool": True  # Default applied
        }
        
        assert validated == expected
        
        print("PASS: Parameter defaults application works")
        
        # Test missing required parameter
        incomplete_params = {
            "required_param": "test_value"
            # missing required_int
        }
        
        try:
            validate_mod_parameters(mod_info, incomplete_params)
            assert False, "Should fail with missing required parameter"
        except ValueError as e:
            assert "Missing required parameters" in str(e)
            assert "required_int" in str(e)
            print("PASS: Missing required parameter validation works")
        
        # Test mod with no config schema
        mod_info_no_schema = {"type": "test_mod"}  # No config_schema
        
        validated = validate_mod_parameters(mod_info_no_schema, {"any": "params"})
        assert validated == {"any": "params"}
        
        print("PASS: Mod without config schema handled correctly")
        
    finally:
        cleanup_file(registry_file)


def test_parameter_validation_edge_cases():
    """Test edge cases in parameter validation."""
    print("\n=== Test: Parameter Validation Edge Cases ===")
    
    # Test empty config schema
    mod_info_empty = {
        "type": "empty_mod",
        "config_schema": {
            "required": {},
            "optional": {}
        }
    }
    
    validated = validate_mod_parameters(mod_info_empty, {"any": "value"})
    assert validated == {"any": "value"}
    
    print("PASS: Empty config schema handled")
    
    # Test config schema with only required fields
    mod_info_required_only = {
        "type": "required_only_mod",
        "config_schema": {
            "required": {
                "must_have": {
                    "type": "str",
                    "description": "Required field"
                }
            },
            "optional": {}
        }
    }
    
    validated = validate_mod_parameters(mod_info_required_only, {"must_have": "provided"})
    assert validated == {"must_have": "provided"}
    
    print("PASS: Required-only config schema works")
    
    # Test config schema with only optional fields
    mod_info_optional_only = {
        "type": "optional_only_mod", 
        "config_schema": {
            "required": {},
            "optional": {
                "maybe": {
                    "type": "str",
                    "default": "default_val",
                    "description": "Optional field"
                }
            }
        }
    }
    
    validated = validate_mod_parameters(mod_info_optional_only, {})
    assert validated == {"maybe": "default_val"}
    
    print("PASS: Optional-only config schema works")


def test_integration_parameter_workflow():
    """Test complete parameter workflow integration."""
    print("\n=== Test: Integration Parameter Workflow ===")
    
    # Create complete test setup
    project_config_data = {
        "project_name": "integration_test",
        "mod_defaults": {
            "test_integration_mod": {
                "project_default": "from_project",
                "override_me": "project_version"
            }
        },
        "globals": {
            "project_global": "global_value"
        }
    }
    
    job_config_data = {
        "mods": {
            "test_instance": {
                "_type": "test_integration_mod",
                "job_param": "from_job",
                "override_me": "job_version"
            }
        }
    }
    
    registry_data = {
        "_metadata": {"version": "1.0.0"},
        "mods": {
            "test_integration_mod": {
                "type": "test_integration_mod",
                "config_schema": {
                    "required": {
                        "job_param": {
                            "type": "str",
                            "description": "Job parameter"
                        }
                    },
                    "optional": {
                        "project_default": {
                            "type": "str",
                            "default": "registry_default",
                            "description": "Project defaulted parameter"
                        },
                        "registry_default": {
                            "type": "str",
                            "default": "from_registry",
                            "description": "Registry default parameter"
                        }
                    }
                }
            }
        }
    }
    
    project_file = create_test_project_config(project_config_data)
    job_file = create_test_job_config(job_config_data)
    
    with NamedTemporaryFile(mode='w', suffix='_registry.json', delete=False, encoding='utf-8') as f:
        json.dump(registry_data, f)
        registry_file = f.name
    
    try:
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            target_file = temp_path / "project_defaults.yaml"
            shutil.copy2(project_file, target_file)
            
            # Step 1: Load job config
            job_config = load_job_config(job_file)
            mod_config = job_config["mods"]["test_instance"]
            mod_type = mod_config["_type"]
            job_params = {k: v for k, v in mod_config.items() if k != "_type"}
            
            # Step 2: Create resolver and resolve parameters
            project_config = ProjectConfig(str(temp_path))
            resolver = ParameterResolver(project_config)
            resolved_params = resolver.resolve_mod_params(mod_type, job_params)
            
            # Step 3: Get mod info and validate parameters
            registry = ModRegistry(registry_file)
            mod_info = registry.get_mod_info(mod_type)
            final_params = validate_mod_parameters(mod_info, resolved_params)
            
            # Verify complete resolution chain
            expected_final = {
                "job_param": "from_job",           # Required from job
                "project_default": "from_project", # Project overrides registry default
                "override_me": "job_version",      # Job overrides project
                "registry_default": "from_registry" # Registry default applied
            }
            
            assert final_params == expected_final
            
            print("PASS: Complete parameter workflow integration works")
            
    finally:
        cleanup_file(project_file)
        cleanup_file(job_file)
        cleanup_file(registry_file)


def test_parameter_input_validation():
    """Test input validation for parameter functions."""
    print("\n=== Test: Parameter Input Validation ===")
    
    # Test ParameterResolver input validation
    resolver = ParameterResolver()
    
    try:
        resolver.resolve_mod_params("", {})
        assert False, "Should fail with empty mod_name"
    except ValueError as e:
        assert "must be a non-empty string" in str(e)
        print("PASS: Empty mod_name validation works")
    
    try:
        resolver.resolve_mod_params("test_mod", "not a dict")
        assert False, "Should fail with non-dict job_params"
    except ValueError as e:
        assert "must be a dictionary" in str(e)
        print("PASS: Non-dict job_params validation works")
    
    # Test load_job_config input validation
    try:
        load_job_config("")
        assert False, "Should fail with empty config_path"
    except ValueError as e:
        assert "must be a non-empty string" in str(e)
        print("PASS: Empty config_path validation works")
    
    # Test ProjectConfig input validation for methods
    config = ProjectConfig()
    
    try:
        config.get_mod_defaults("")
        assert False, "Should fail with empty mod_name"
    except ValueError as e:
        assert "must be a non-empty string" in str(e)
        print("PASS: ProjectConfig mod_name validation works")


def main():
    """Run all parameter system tests."""
    print("Starting Parameter System Tests...")
    print("=" * 50)
    
    try:
        test_project_config_discovery()
        test_project_config_validation()
        test_project_config_methods()
        test_parameter_resolver_creation()
        test_mod_parameter_resolution()
        test_job_config_loading()
        test_parameter_validation()
        test_parameter_validation_edge_cases()
        test_integration_parameter_workflow()
        test_parameter_input_validation()
        
        print("\n" + "=" * 50)
        print("ALL PARAMETER SYSTEM TESTS PASSED")
        return True
        
    except Exception as e:
        print(f"\nFAIL: Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)