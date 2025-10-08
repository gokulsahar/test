"""
Test cases for datapy.mod_manager.parameter_validation module.

Tests parameter validation logic including input validation, required parameter
checking, default value application, and error handling scenarios.
"""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add project root to Python path for testing
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pytest

from datapy.mod_manager.parameter_validation import validate_mod_parameters


class TestValidateModParametersBasic:
    """Test cases for basic validate_mod_parameters functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.simple_mod_info = {
            "config_schema": {
                "required": {
                    "input_path": {
                        "type": "str",
                        "description": "Path to input file"
                    }
                },
                "optional": {
                    "encoding": {
                        "type": "str",
                        "default": "utf-8",
                        "description": "File encoding"
                    },
                    "debug": {
                        "type": "bool",
                        "default": False,
                        "description": "Enable debug mode"
                    }
                }
            }
        }
        
        self.complex_mod_info = {
            "config_schema": {
                "required": {
                    "database_url": {
                        "type": "str",
                        "description": "Database connection URL"
                    },
                    "table_name": {
                        "type": "str", 
                        "description": "Target table name"
                    }
                },
                "optional": {
                    "batch_size": {
                        "type": "int",
                        "default": 1000,
                        "description": "Batch processing size"
                    },
                    "timeout": {
                        "type": "float",
                        "default": 30.0,
                        "description": "Connection timeout in seconds"
                    },
                    "columns": {
                        "type": "list",
                        "default": [],
                        "description": "List of columns to process"
                    },
                    "options": {
                        "type": "dict",
                        "default": {"ssl": True},
                        "description": "Connection options"
                    },
                    "metadata": {
                        "type": "object",
                        "default": None,
                        "description": "Additional metadata"
                    }
                }
            }
        }
    
    def test_valid_parameters_with_defaults_applied(self):
        """Test successful validation with defaults applied."""
        params = {"input_path": "/data/test.csv"}
        
        result = validate_mod_parameters(self.simple_mod_info, params)
        
        assert result["input_path"] == "/data/test.csv"
        assert result["encoding"] == "utf-8"  # Default applied
        assert result["debug"] is False  # Default applied
        assert len(result) == 3
    
    def test_valid_parameters_override_defaults(self):
        """Test validation when user overrides default values."""
        params = {
            "input_path": "/data/custom.csv",
            "encoding": "latin-1",
            "debug": True
        }
        
        result = validate_mod_parameters(self.simple_mod_info, params)
        
        assert result["input_path"] == "/data/custom.csv"
        assert result["encoding"] == "latin-1"  # User value used
        assert result["debug"] is True  # User value used
        assert len(result) == 3
    
    def test_valid_parameters_partial_override(self):
        """Test validation with partial default override."""
        params = {
            "input_path": "/data/partial.csv",
            "debug": True  # Override only debug, keep encoding default
        }
        
        result = validate_mod_parameters(self.simple_mod_info, params)
        
        assert result["input_path"] == "/data/partial.csv"
        assert result["encoding"] == "utf-8"  # Default kept
        assert result["debug"] is True  # User override
    
    def test_complex_parameters_all_defaults(self):
        """Test complex mod info with all default types."""
        params = {
            "database_url": "postgresql://user:pass@host/db",
            "table_name": "test_table"
        }
        
        result = validate_mod_parameters(self.complex_mod_info, params)
        
        assert result["database_url"] == "postgresql://user:pass@host/db"
        assert result["table_name"] == "test_table"
        assert result["batch_size"] == 1000  # int default
        assert result["timeout"] == 30.0  # float default
        assert result["columns"] == []  # list default
        assert result["options"] == {"ssl": True}  # dict default
        assert result["metadata"] is None  # None default
    
    def test_extra_parameters_preserved(self):
        """Test that extra parameters not in schema are preserved."""
        params = {
            "input_path": "/data/test.csv",
            "extra_param": "preserved",
            "another_extra": 123
        }
        
        result = validate_mod_parameters(self.simple_mod_info, params)
        
        assert result["input_path"] == "/data/test.csv"
        assert result["encoding"] == "utf-8"  # Default applied
        assert result["debug"] is False  # Default applied
        assert result["extra_param"] == "preserved"  # Extra preserved
        assert result["another_extra"] == 123  # Extra preserved
    
    def test_empty_optional_section(self):
        """Test mod info with empty optional section."""
        mod_info = {
            "config_schema": {
                "required": {
                    "name": {
                        "type": "str",
                        "description": "Required name"
                    }
                },
                "optional": {}
            }
        }
        
        params = {"name": "test"}
        result = validate_mod_parameters(mod_info, params)
        
        assert result["name"] == "test"
        assert len(result) == 1


class TestValidateModParametersErrorHandling:
    """Test cases for error handling scenarios."""
    
    def setup_method(self):
        """Set up test fixtures for error cases."""
        self.valid_mod_info = {
            "config_schema": {
                "required": {
                    "required_param": {
                        "type": "str",
                        "description": "Required parameter"
                    }
                },
                "optional": {
                    "optional_param": {
                        "type": "int",
                        "default": 42,
                        "description": "Optional parameter"
                    }
                }
            }
        }
    
    def test_invalid_mod_info_type_raises_error(self):
        """Test that non-dict mod_info raises TypeError."""
        with pytest.raises(TypeError, match="mod_info must be a dictionary"):
            validate_mod_parameters("not_a_dict", {})
        
        with pytest.raises(TypeError, match="mod_info must be a dictionary"):
            validate_mod_parameters(123, {})
        
        with pytest.raises(TypeError, match="mod_info must be a dictionary"):
            validate_mod_parameters(None, {})
    
    def test_invalid_params_type_raises_error(self):
        """Test that non-dict params raises TypeError."""
        with pytest.raises(TypeError, match="params must be a dictionary"):
            validate_mod_parameters(self.valid_mod_info, "not_a_dict")
        
        with pytest.raises(TypeError, match="params must be a dictionary"):
            validate_mod_parameters(self.valid_mod_info, [1, 2, 3])
        
        with pytest.raises(TypeError, match="params must be a dictionary"):
            validate_mod_parameters(self.valid_mod_info, None)
    
    def test_missing_required_parameter_raises_error(self):
        """Test that missing required parameter raises ValueError."""
        params = {"optional_param": 100}  # Missing required_param
        
        with pytest.raises(ValueError, match="Missing required parameters: required_param"):
            validate_mod_parameters(self.valid_mod_info, params)
    
    def test_multiple_missing_required_parameters_raises_error(self):
        """Test error message with multiple missing required parameters."""
        mod_info = {
            "config_schema": {
                "required": {
                    "param1": {"type": "str", "description": "First param"},
                    "param2": {"type": "str", "description": "Second param"},
                    "param3": {"type": "int", "description": "Third param"}
                },
                "optional": {}
            }
        }
        
        params = {"param2": "present"}  # Missing param1 and param3
        
        with pytest.raises(ValueError, match="Missing required parameters: param1, param3"):
            validate_mod_parameters(mod_info, params)
    
    def test_no_config_schema_returns_params_copy(self):
        """Test behavior when mod_info has no config_schema."""
        mod_info = {"other_field": "value"}
        params = {"test": "value"}
        
        result = validate_mod_parameters(mod_info, params)
        
        assert result == params
        assert result is not params  # Should be a copy
    
    def test_invalid_config_schema_type_warning(self):
        """Test warning logged when config_schema is not a dict."""
        mod_info = {"config_schema": "not_a_dict"}
        params = {"test": "value"}
        
        with patch('datapy.mod_manager.parameter_validation.logger') as mock_logger:
            result = validate_mod_parameters(mod_info, params)
            
            mock_logger.warning.assert_called_with("Invalid config_schema type: str, using empty schema")
            assert result == params
    
    def test_invalid_required_section_type_warning(self):
        """Test warning when required section is not a dict."""
        mod_info = {
            "config_schema": {
                "required": "not_a_dict",
                "optional": {}
            }
        }
        params = {"test": "value"}
        
        with patch('datapy.mod_manager.parameter_validation.logger') as mock_logger:
            result = validate_mod_parameters(mod_info, params)
            
            mock_logger.warning.assert_called_with("Invalid required section type: str, skipping required validation")
            assert result == params
    
    def test_invalid_optional_section_type_warning(self):
        """Test warning when optional section is not a dict."""
        mod_info = {
            "config_schema": {
                "required": {},
                "optional": ["not", "a", "dict"]
            }
        }
        params = {"test": "value"}
        
        with patch('datapy.mod_manager.parameter_validation.logger') as mock_logger:
            result = validate_mod_parameters(mod_info, params)
            
            mock_logger.warning.assert_called_with("Invalid optional section type: list, skipping defaults")
            assert result == params


class TestValidateModParametersEdgeCases:
    """Test cases for edge cases and boundary conditions."""
    
    def test_empty_mod_info(self):
        """Test validation with completely empty mod_info."""
        mod_info = {}
        params = {"any": "value"}
        
        result = validate_mod_parameters(mod_info, params)
        
        assert result == params
        assert result is not params
    
    def test_empty_params(self):
        """Test validation with empty params."""
        mod_info = {
            "config_schema": {
                "required": {},
                "optional": {
                    "default_only": {
                        "type": "str",
                        "default": "applied",
                        "description": "Default only param"
                    }
                }
            }
        }
        params = {}
        
        result = validate_mod_parameters(mod_info, params)
        
        assert result["default_only"] == "applied"
        assert len(result) == 1
    
    def test_none_default_values(self):
        """Test various None default values."""
        mod_info = {
            "config_schema": {
                "required": {"req": {"type": "str", "description": "Required"}},
                "optional": {
                    "none_str": {"type": "str", "default": None, "description": "None string"},
                    "none_int": {"type": "int", "default": None, "description": "None int"},
                    "none_list": {"type": "list", "default": None, "description": "None list"},
                    "none_dict": {"type": "dict", "default": None, "description": "None dict"},
                    "none_obj": {"type": "object", "default": None, "description": "None object"}
                }
            }
        }
        params = {"req": "required_value"}
        
        result = validate_mod_parameters(mod_info, params)
        
        assert result["req"] == "required_value"
        assert result["none_str"] is None
        assert result["none_int"] is None
        assert result["none_list"] is None
        assert result["none_dict"] is None
        assert result["none_obj"] is None
    
    def test_complex_nested_defaults(self):
        """Test complex nested default values."""
        mod_info = {
            "config_schema": {
                "required": {"name": {"type": "str", "description": "Name"}},
                "optional": {
                    "nested_dict": {
                        "type": "dict",
                        "default": {
                            "level1": {
                                "level2": ["item1", "item2"],
                                "config": {"enabled": True, "timeout": 30}
                            }
                        },
                        "description": "Nested configuration"
                    },
                    "complex_list": {
                        "type": "list",
                        "default": [
                            {"name": "first", "value": 1},
                            {"name": "second", "value": 2}
                        ],
                        "description": "Complex list structure"
                    }
                }
            }
        }
        params = {"name": "test"}
        
        result = validate_mod_parameters(mod_info, params)
        
        assert result["name"] == "test"
        assert result["nested_dict"]["level1"]["level2"] == ["item1", "item2"]
        assert result["nested_dict"]["level1"]["config"]["enabled"] is True
        assert result["complex_list"][0]["name"] == "first"
        assert result["complex_list"][1]["value"] == 2
    
    def test_invalid_parameter_definition_handling(self):
        """Test handling of invalid parameter definitions."""
        mod_info = {
            "config_schema": {
                "required": {"req": {"type": "str", "description": "Required"}},
                "optional": {
                    "valid_param": {
                        "type": "str",
                        "default": "valid",
                        "description": "Valid parameter"
                    },
                    "invalid_param": "not_a_dict",  # Invalid definition
                    "another_invalid": {
                        # Missing required fields
                        "default": "missing_type_and_desc"
                    }
                }
            }
        }
        params = {"req": "required_value"}
        
        with patch('datapy.mod_manager.parameter_validation.logger') as mock_logger:
            result = validate_mod_parameters(mod_info, params)
            
            # Should apply valid defaults and skip invalid ones with warnings
            assert result["req"] == "required_value"
            assert result["valid_param"] == "valid"
            
            # Non-dict param definition should be skipped with warning
            assert "invalid_param" not in result
            
            # Dict with default but missing other fields should still be applied
            # (the current implementation only checks for dict type and 'default' key)
            assert result["another_invalid"] == "missing_type_and_desc"
            
            # Should log warning for non-dict definition
            mock_logger.warning.assert_called_with("Invalid parameter definition for 'invalid_param': str, skipping")
    
    def test_parameter_with_default_but_no_description(self):
        """Test parameter definition with missing description field."""
        mod_info = {
            "config_schema": {
                "required": {},
                "optional": {
                    "missing_desc": {
                        "type": "str",
                        "default": "value"
                        # Missing description - but current implementation doesn't validate this
                    }
                }
            }
        }
        params = {}
        
        with patch('datapy.mod_manager.parameter_validation.logger') as mock_logger:
            result = validate_mod_parameters(mod_info, params)
            
            # Current implementation doesn't validate presence of 'description' field
            # It only checks if param_def is dict and has 'default' key
            assert result["missing_desc"] == "value"
            
            # Should not log any warnings since the param_def is a valid dict with default
            mock_logger.warning.assert_not_called()


class TestValidateModParametersLogging:
    """Test cases for logging behavior."""
    
    def test_debug_logging_on_success(self):
        """Test debug logging messages on successful validation."""
        mod_info = {
            "config_schema": {
                "required": {"req": {"type": "str", "description": "Required"}},
                "optional": {
                    "opt": {"type": "int", "default": 42, "description": "Optional"}
                }
            }
        }
        params = {"req": "value"}
        
        with patch('datapy.mod_manager.parameter_validation.logger') as mock_logger:
            result = validate_mod_parameters(mod_info, params)
            
            # Should log debug messages about defaults and completion
            assert mock_logger.debug.call_count >= 2
            debug_calls = [call[0][0] for call in mock_logger.debug.call_args_list]
            assert any("Applied default for opt: 42" in msg for msg in debug_calls)
            assert any("Parameter validation completed" in msg for msg in debug_calls)
    
    def test_no_schema_debug_logging(self):
        """Test debug logging when no config schema present."""
        mod_info = {}
        params = {"test": "value"}
        
        with patch('datapy.mod_manager.parameter_validation.logger') as mock_logger:
            validate_mod_parameters(mod_info, params)
            
            mock_logger.debug.assert_called_with("No config schema found - returning params as-is")
    
    @pytest.mark.parametrize("defaults_count,expected_message", [
        (0, "0 defaults applied"),
        (1, "1 defaults applied"),
        (3, "3 defaults applied")
    ])
    def test_defaults_applied_logging(self, defaults_count, expected_message):
        """Test logging of defaults applied count."""
        # Create mod_info with specified number of optional parameters
        optional = {}
        for i in range(defaults_count):
            optional[f"opt_{i}"] = {
                "type": "str",
                "default": f"default_{i}",
                "description": f"Optional param {i}"
            }
        
        mod_info = {
            "config_schema": {
                "required": {"req": {"type": "str", "description": "Required"}},
                "optional": optional
            }
        }
        params = {"req": "value"}  # Only provide required param
        
        with patch('datapy.mod_manager.parameter_validation.logger') as mock_logger:
            validate_mod_parameters(mod_info, params)
            
            # Check final completion message includes correct count
            completion_calls = [
                call for call in mock_logger.debug.call_args_list
                if "Parameter validation completed" in call[0][0]
            ]
            assert len(completion_calls) == 1
            assert expected_message in completion_calls[0][0][0]


class TestValidateModParametersIntegration:
    """Integration test cases with realistic scenarios."""
    
    def test_realistic_csv_reader_scenario(self):
        """Test validation with realistic CSV reader-like parameters."""
        mod_info = {
            "config_schema": {
                "required": {
                    "file_path": {
                        "type": "str",
                        "description": "Path to CSV file to read"
                    }
                },
                "optional": {
                    "encoding": {
                        "type": "str",
                        "default": "utf-8",
                        "description": "File encoding"
                    },
                    "delimiter": {
                        "type": "str",
                        "default": ",",
                        "description": "CSV delimiter character"
                    },
                    "header": {
                        "type": "int",
                        "default": 0,
                        "description": "Row number for column headers"
                    },
                    "skip_rows": {
                        "type": "int",
                        "default": 0,
                        "description": "Number of rows to skip"
                    },
                    "max_rows": {
                        "type": "int",
                        "default": None,
                        "description": "Maximum number of rows to read"
                    }
                }
            }
        }
        
        params = {
            "file_path": "/data/customers.csv",
            "delimiter": "|",  # Override default
            "skip_rows": 2     # Override default
        }
        
        result = validate_mod_parameters(mod_info, params)
        
        assert result["file_path"] == "/data/customers.csv"
        assert result["encoding"] == "utf-8"  # Default
        assert result["delimiter"] == "|"     # Overridden
        assert result["header"] == 0          # Default
        assert result["skip_rows"] == 2       # Overridden
        assert result["max_rows"] is None     # Default
    
    def test_realistic_database_writer_scenario(self):
        """Test validation with realistic database writer-like parameters."""
        mod_info = {
            "config_schema": {
                "required": {
                    "data": {
                        "type": "object",
                        "description": "Input DataFrame to write"
                    },
                    "connection_string": {
                        "type": "str", 
                        "description": "Database connection string"
                    },
                    "table_name": {
                        "type": "str",
                        "description": "Target table name"
                    }
                },
                "optional": {
                    "batch_size": {
                        "type": "int",
                        "default": 1000,
                        "description": "Batch size for inserts"
                    },
                    "if_exists": {
                        "type": "str",
                        "default": "replace",
                        "description": "What to do if table exists"
                    },
                    "create_table": {
                        "type": "bool",
                        "default": True,
                        "description": "Create table if not exists"
                    },
                    "indexes": {
                        "type": "list",
                        "default": [],
                        "description": "List of columns to index"
                    },
                    "connection_options": {
                        "type": "dict",
                        "default": {"timeout": 30, "pool_size": 5},
                        "description": "Additional connection options"
                    }
                }
            }
        }
        
        # Mock DataFrame object
        mock_dataframe = MagicMock()
        params = {
            "data": mock_dataframe,
            "connection_string": "postgresql://user:pass@host:5432/db",
            "table_name": "processed_data",
            "if_exists": "append",  # Override default
            "indexes": ["id", "timestamp"]  # Override default
        }
        
        result = validate_mod_parameters(mod_info, params)
        
        assert result["data"] is mock_dataframe
        assert result["connection_string"] == "postgresql://user:pass@host:5432/db"
        assert result["table_name"] == "processed_data"
        assert result["batch_size"] == 1000         # Default
        assert result["if_exists"] == "append"      # Overridden
        assert result["create_table"] is True       # Default
        assert result["indexes"] == ["id", "timestamp"]  # Overridden
        assert result["connection_options"]["timeout"] == 30    # Default dict
        assert result["connection_options"]["pool_size"] == 5   # Default dict
    
    def test_parameter_preservation_with_mod_context(self):
        """Test that framework-added parameters are preserved."""
        mod_info = {
            "config_schema": {
                "required": {
                    "input": {"type": "str", "description": "Input parameter"}
                },
                "optional": {
                    "debug": {"type": "bool", "default": False, "description": "Debug mode"}
                }
            }
        }
        
        # Simulate parameters that framework might add
        params = {
            "input": "test_value",
            "_mod_name": "test_instance",      # Framework-added
            "_mod_type": "test_mod",          # Framework-added
            "_execution_id": "exec_12345",   # Framework-added
            "custom_extra": "preserved"       # User-added extra
        }
        
        result = validate_mod_parameters(mod_info, params)
        
        # Schema parameters should be validated
        assert result["input"] == "test_value"
        assert result["debug"] is False  # Default applied
        
        # Framework and extra parameters should be preserved
        assert result["_mod_name"] == "test_instance"
        assert result["_mod_type"] == "test_mod" 
        assert result["_execution_id"] == "exec_12345"
        assert result["custom_extra"] == "preserved"