"""
Test 03: Base Classes
Tests the ModMetadata and ConfigSchema base classes for mod definitions.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datapy.mod_manager.base import ModMetadata, ConfigSchema
from pydantic import ValidationError


def test_valid_mod_metadata():
    """Test valid ModMetadata creation."""
    print("=== Test: Valid ModMetadata ===")
    
    # Create valid metadata
    metadata = ModMetadata(
        type="csv_reader",
        version="1.0.0",
        description="Reads CSV files with configurable options",
        category="source",
        input_ports=[],
        output_ports=["data"],
        globals=["row_count", "column_count"],
        packages=["pandas>=1.5.0"],
        python_version=">=3.8"
    )
    
    assert metadata.type == "csv_reader"
    assert metadata.version == "1.0.0"
    assert metadata.description == "Reads CSV files with configurable options"
    assert metadata.category == "source"
    assert metadata.input_ports == []
    assert metadata.output_ports == ["data"]
    assert metadata.globals == ["row_count", "column_count"]
    assert metadata.packages == ["pandas>=1.5.0"]
    assert metadata.python_version == ">=3.8"
    
    print("PASS: Valid ModMetadata created successfully")


def test_mod_metadata_validation():
    """Test ModMetadata validation rules."""
    print("\n=== Test: ModMetadata Validation ===")
    
    # Test invalid type (empty)
    try:
        ModMetadata(
            type="",
            version="1.0.0",
            description="Test description that is long enough",
            category="source"
        )
        assert False, "Should fail with empty type"
    except ValidationError as e:
        assert "type cannot be empty" in str(e)
        print("PASS: Empty type rejected")
    
    # Test invalid type (too short)
    try:
        ModMetadata(
            type="a",
            version="1.0.0",
            description="Test description that is long enough",
            category="source"
        )
        assert False, "Should fail with too short type"
    except ValidationError as e:
        assert "at least 2 characters" in str(e)
        print("PASS: Too short type rejected")
    
    # Test invalid version (not semver)
    try:
        ModMetadata(
            type="test_mod",
            version="1.0",
            description="Test description that is long enough",
            category="source"
        )
        assert False, "Should fail with invalid version format"
    except ValidationError as e:
        assert "must follow format" in str(e)
        print("PASS: Invalid version format rejected")
    
    # Test invalid description (too short)
    try:
        ModMetadata(
            type="test_mod",
            version="1.0.0",
            description="Short",
            category="source"
        )
        assert False, "Should fail with too short description"
    except ValidationError as e:
        assert "at least 10 characters" in str(e)
        print("PASS: Too short description rejected")
    
    # Test invalid category
    try:
        ModMetadata(
            type="test_mod",
            version="1.0.0",
            description="Test description that is long enough",
            category="invalid_category"
        )
        assert False, "Should fail with invalid category"
    except ValidationError as e:
        assert "must be one of" in str(e)
        print("PASS: Invalid category rejected")


def test_mod_metadata_categories():
    """Test all valid ModMetadata categories."""
    print("\n=== Test: ModMetadata Categories ===")
    
    valid_categories = ["source", "transformer", "sink", "solo"]
    
    for category in valid_categories:
        metadata = ModMetadata(
            type="test_mod",
            version="1.0.0",
            description="Test description that is long enough",
            category=category
        )
        assert metadata.category == category
        print(f"PASS: Category '{category}' accepted")


def test_mod_metadata_defaults():
    """Test ModMetadata default values."""
    print("\n=== Test: ModMetadata Defaults ===")
    
    # Create minimal metadata
    metadata = ModMetadata(
        type="test_mod",
        version="1.0.0",
        description="Test description that is long enough",
        category="source"
    )
    
    # Check defaults
    assert metadata.input_ports == []
    assert metadata.output_ports == []
    assert metadata.globals == []
    assert metadata.packages == []
    assert metadata.python_version == ">=3.8"
    
    print("PASS: Default values set correctly")


def test_valid_config_schema():
    """Test valid ConfigSchema creation."""
    print("\n=== Test: Valid ConfigSchema ===")
    
    # Create valid config schema
    schema = ConfigSchema(
        required={
            "file_path": {
                "type": "str",
                "description": "Path to CSV file"
            },
            "columns": {
                "type": "list",
                "description": "List of columns to read"
            }
        },
        optional={
            "encoding": {
                "type": "str",
                "default": "utf-8",
                "description": "File encoding"
            },
            "delimiter": {
                "type": "str",
                "default": ",",
                "description": "CSV delimiter"
            }
        }
    )
    
    assert "file_path" in schema.required
    assert schema.required["file_path"]["type"] == "str"
    assert schema.required["file_path"]["description"] == "Path to CSV file"
    
    assert "encoding" in schema.optional
    assert schema.optional["encoding"]["default"] == "utf-8"
    assert schema.optional["delimiter"]["default"] == ","
    
    print("PASS: Valid ConfigSchema created successfully")


def test_config_schema_validation():
    """Test ConfigSchema validation rules."""
    print("\n=== Test: ConfigSchema Validation ===")
    
    # Test missing type in required parameter
    try:
        ConfigSchema(
            required={
                "file_path": {
                    "description": "Path to CSV file"
                    # Missing "type" field
                }
            }
        )
        assert False, "Should fail with missing type field"
    except ValidationError as e:
        assert "missing required 'type' field" in str(e)
        print("PASS: Missing type field rejected")
    
    # Test missing description in required parameter
    try:
        ConfigSchema(
            required={
                "file_path": {
                    "type": "str"
                    # Missing "description" field
                }
            }
        )
        assert False, "Should fail with missing description field"
    except ValidationError as e:
        assert "missing required 'description' field" in str(e)
        print("PASS: Missing description field rejected")
    
    # Test missing type in optional parameter
    try:
        ConfigSchema(
            optional={
                "encoding": {
                    "default": "utf-8",
                    "description": "File encoding"
                    # Missing "type" field
                }
            }
        )
        assert False, "Should fail with missing type field in optional"
    except ValidationError as e:
        assert "missing required 'type' field" in str(e)
        print("PASS: Missing type field in optional rejected")


def test_config_schema_defaults():
    """Test ConfigSchema default values."""
    print("\n=== Test: ConfigSchema Defaults ===")
    
    # Create minimal schema
    schema = ConfigSchema()
    
    # Check defaults
    assert schema.required == {}
    assert schema.optional == {}
    
    print("PASS: Default values set correctly")


def test_config_schema_parameter_types():
    """Test different parameter types in ConfigSchema."""
    print("\n=== Test: ConfigSchema Parameter Types ===")
    
    schema = ConfigSchema(
        required={
            "string_param": {
                "type": "str",
                "description": "String parameter"
            },
            "int_param": {
                "type": "int", 
                "description": "Integer parameter"
            },
            "float_param": {
                "type": "float",
                "description": "Float parameter"
            },
            "bool_param": {
                "type": "bool",
                "description": "Boolean parameter"
            },
            "list_param": {
                "type": "list",
                "description": "List parameter"
            },
            "dict_param": {
                "type": "dict",
                "description": "Dictionary parameter"
            }
        }
    )
    
    expected_params = {
        "string_param": "str",
        "int_param": "int", 
        "float_param": "float",
        "bool_param": "bool",
        "list_param": "list",
        "dict_param": "dict"
    }
    
    for param_name, expected_type in expected_params.items():
        assert schema.required[param_name]["type"] == expected_type
        print(f"PASS: Parameter type '{expected_type}' accepted")


def test_complex_mod_example():
    """Test a complete, complex mod definition."""
    print("\n=== Test: Complex Mod Example ===")
    
    # Create complex metadata
    metadata = ModMetadata(
        type="advanced_data_processor",
        version="2.1.3",
        description="Advanced data processing with multiple transformation options and validation",
        category="transformer",
        input_ports=["raw_data", "config_data"],
        output_ports=["processed_data", "validation_report"],
        globals=["total_records", "error_count", "processing_time"],
        packages=["pandas>=1.5.0", "numpy>=1.20.0", "scikit-learn>=1.0.0"],
        python_version=">=3.9"
    )
    
    # Create complex schema
    schema = ConfigSchema(
        required={
            "input_columns": {
                "type": "list",
                "description": "List of input column names to process"
            },
            "output_path": {
                "type": "str",
                "description": "Path where processed data will be saved"
            },
            "processing_mode": {
                "type": "str",
                "description": "Processing mode (batch, streaming, or interactive)"
            }
        },
        optional={
            "batch_size": {
                "type": "int",
                "default": 1000,
                "description": "Number of records to process in each batch"
            },
            "validation_rules": {
                "type": "dict",
                "default": {},
                "description": "Custom validation rules for data quality checks"
            },
            "enable_logging": {
                "type": "bool",
                "default": True,
                "description": "Enable detailed processing logs"
            },
            "timeout_seconds": {
                "type": "float",
                "default": 300.0,
                "description": "Maximum processing time in seconds"
            }
        }
    )
    
    # Verify all fields
    assert metadata.type == "advanced_data_processor"
    assert len(metadata.input_ports) == 2
    assert len(metadata.output_ports) == 2
    assert len(metadata.globals) == 3
    assert len(metadata.packages) == 3
    
    assert len(schema.required) == 3
    assert len(schema.optional) == 4
    assert schema.optional["batch_size"]["default"] == 1000
    assert schema.optional["enable_logging"]["default"] is True
    
    print("PASS: Complex mod definition created successfully")


def main():
    """Run all base classes tests."""
    print("Starting Base Classes Tests...")
    print("=" * 50)
    
    try:
        test_valid_mod_metadata()
        test_mod_metadata_validation()
        test_mod_metadata_categories()
        test_mod_metadata_defaults()
        test_valid_config_schema()
        test_config_schema_validation()
        test_config_schema_defaults()
        test_config_schema_parameter_types()
        test_complex_mod_example()
        
        print("\n" + "=" * 50)
        print("ALL BASE CLASSES TESTS PASSED")
        return True
        
    except Exception as e:
        print(f"\nFAIL: Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)