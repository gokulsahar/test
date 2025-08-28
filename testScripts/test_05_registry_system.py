"""
Test 05: Registry System
Tests the mod registry functionality including registration, lookup, and validation.
"""

import sys
import json
import os
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datapy.mod_manager.registry import ModRegistry, get_registry
from datapy.mod_manager.base import ModMetadata, ConfigSchema


def create_test_mod_file(temp_dir: str, mod_name: str) -> str:
    """Create a temporary test mod file."""
    mod_content = f'''"""
Test mod for registry testing.
"""

from datapy.mod_manager.result import ModResult
from datapy.mod_manager.base import ModMetadata, ConfigSchema

METADATA = ModMetadata(
    type="{mod_name}",
    version="1.0.0",
    description="Test mod for registry system testing",
    category="solo",
    input_ports=[],
    output_ports=["result"],
    globals=["test_value"],
    packages=[],
    python_version=">=3.8"
)

CONFIG_SCHEMA = ConfigSchema(
    required={{
        "input_value": {{
            "type": "str",
            "description": "Test input value"
        }}
    }},
    optional={{}}
)

def run(params):
    """Test run function."""
    mod_name = params.get("_mod_name", "{mod_name}")
    result = ModResult("{mod_name}", mod_name)
    
    result.add_metric("test_metric", 42)
    result.add_artifact("result", "test_output")
    result.add_global("test_value", "test")
    
    return result.success()
'''
    
    mod_file = Path(temp_dir) / f"{mod_name}.py"
    with open(mod_file, 'w') as f:
        f.write(mod_content)
    
    return str(mod_file)


def create_test_registry_file() -> str:
    """Create a temporary registry file."""
    registry_data = {
        "_metadata": {
            "version": "1.0.0",
            "created": "2024-01-01",
            "description": "Test Registry",
            "last_updated": "2024-01-01"
        },
        "mods": {}
    }
    
    with TemporaryDirectory() as temp_dir:
        registry_file = Path(temp_dir) / "test_registry.json"
        with open(registry_file, 'w') as f:
            json.dump(registry_data, f, indent=2)
        
        # Copy to permanent location for testing
        test_registry = Path(__file__).parent / "test_registry.json"
        shutil.copy2(registry_file, test_registry)
        
    return str(test_registry)


def cleanup_registry_file(registry_path: str):
    """Clean up test registry file."""
    try:
        if os.path.exists(registry_path):
            os.unlink(registry_path)
    except:
        pass


def test_registry_creation():
    """Test registry creation and loading."""
    print("=== Test: Registry Creation ===")
    
    # Create test registry
    registry_path = create_test_registry_file()
    
    try:
        registry = ModRegistry(registry_path)
        
        assert registry.registry_path == registry_path
        assert isinstance(registry.registry_data, dict)
        assert "mods" in registry.registry_data
        assert "_metadata" in registry.registry_data
        
        print("PASS: Registry created and loaded successfully")
        
    finally:
        cleanup_registry_file(registry_path)


def test_empty_registry_operations():
    """Test operations on empty registry."""
    print("\n=== Test: Empty Registry Operations ===")
    
    registry_path = create_test_registry_file()
    
    try:
        registry = ModRegistry(registry_path)
        
        # Test list_available_mods on empty registry
        mods = registry.list_available_mods()
        assert mods == []
        
        print("PASS: Empty registry list works")
        
        # Test get_mod_info on non-existent mod
        try:
            registry.get_mod_info("nonexistent_mod")
            assert False, "Should fail for non-existent mod"
        except ValueError as e:
            assert "not found in registry" in str(e)
            print("PASS: Non-existent mod lookup fails correctly")
        
        # Test validation on empty registry
        errors = registry.validate_registry()
        assert errors == []
        
        print("PASS: Empty registry validation passes")
        
    finally:
        cleanup_registry_file(registry_path)


def test_mod_registration():
    """Test mod registration functionality."""
    print("\n=== Test: Mod Registration ===")
    
    registry_path = create_test_registry_file()
    
    with TemporaryDirectory() as temp_dir:
        # Add temp dir to Python path
        sys.path.insert(0, temp_dir)
        
        try:
            # Create test mod
            mod_file = create_test_mod_file(temp_dir, "test_registration_mod")
            mod_module_name = Path(mod_file).stem
            
            registry = ModRegistry(registry_path)
            
            # Register the mod
            success = registry.register_mod(mod_module_name)
            assert success is True
            
            print("PASS: Mod registration successful")
            
            # Verify mod is in registry
            mods = registry.list_available_mods()
            assert "test_registration_mod" in mods
            
            print("PASS: Registered mod appears in list")
            
            # Get mod info
            mod_info = registry.get_mod_info("test_registration_mod")
            assert mod_info["type"] == "test_registration_mod"
            assert mod_info["version"] == "1.0.0"
            assert mod_info["category"] == "solo"
            assert mod_info["module_path"] == mod_module_name
            
            print("PASS: Mod info retrieval works")
            
        finally:
            sys.path.remove(temp_dir)
            cleanup_registry_file(registry_path)


def test_duplicate_registration():
    """Test duplicate mod registration handling."""
    print("\n=== Test: Duplicate Registration ===")
    
    registry_path = create_test_registry_file()
    
    with TemporaryDirectory() as temp_dir:
        sys.path.insert(0, temp_dir)
        
        try:
            # Create test mod
            create_test_mod_file(temp_dir, "duplicate_test_mod")
            
            registry = ModRegistry(registry_path)
            
            # Register mod first time
            success1 = registry.register_mod("duplicate_test_mod")
            assert success1 is True
            
            # Try to register same mod again
            try:
                registry.register_mod("duplicate_test_mod")
                assert False, "Should fail on duplicate registration"
            except ValueError as e:
                assert "already registered" in str(e)
                print("PASS: Duplicate registration rejected")
            
        finally:
            sys.path.remove(temp_dir)
            cleanup_registry_file(registry_path)


def test_invalid_mod_registration():
    """Test registration of invalid mods."""
    print("\n=== Test: Invalid Mod Registration ===")
    
    registry_path = create_test_registry_file()
    
    try:
        registry = ModRegistry(registry_path)
        
        # Test non-existent module
        try:
            registry.register_mod("nonexistent.module.path")
            assert False, "Should fail for non-existent module"
        except ValueError as e:
            assert "Cannot import mod" in str(e)
            print("PASS: Non-existent module registration rejected")
        
        # Test empty module path
        try:
            registry.register_mod("")
            assert False, "Should fail for empty module path"
        except ValueError as e:
            assert "must be a non-empty string" in str(e)
            print("PASS: Empty module path rejected")
        
    finally:
        cleanup_registry_file(registry_path)


def test_mod_deletion():
    """Test mod deletion functionality."""
    print("\n=== Test: Mod Deletion ===")
    
    registry_path = create_test_registry_file()
    
    with TemporaryDirectory() as temp_dir:
        sys.path.insert(0, temp_dir)
        
        try:
            # Create and register test mod
            create_test_mod_file(temp_dir, "deletion_test_mod")
            
            registry = ModRegistry(registry_path)
            registry.register_mod("deletion_test_mod")
            
            # Verify mod exists
            mods_before = registry.list_available_mods()
            assert "deletion_test_mod" in mods_before
            
            # Delete mod
            success = registry.delete_mod("deletion_test_mod")
            assert success is True
            
            print("PASS: Mod deletion successful")
            
            # Verify mod is gone
            mods_after = registry.list_available_mods()
            assert "deletion_test_mod" not in mods_after
            
            print("PASS: Mod removed from registry")
            
            # Try to delete non-existent mod
            try:
                registry.delete_mod("nonexistent_mod")
                assert False, "Should fail for non-existent mod"
            except ValueError as e:
                assert "not found in registry" in str(e)
                print("PASS: Non-existent mod deletion rejected")
            
        finally:
            sys.path.remove(temp_dir)
            cleanup_registry_file(registry_path)


def test_category_filtering():
    """Test mod listing with category filtering."""
    print("\n=== Test: Category Filtering ===")
    
    registry_path = create_test_registry_file()
    
    with TemporaryDirectory() as temp_dir:
        sys.path.insert(0, temp_dir)
        
        try:
            registry = ModRegistry(registry_path)
            
            # Create mods with different categories
            categories = ["source", "transformer", "sink", "solo"]
            for category in categories:
                mod_content = f'''
from datapy.mod_manager.result import ModResult
from datapy.mod_manager.base import ModMetadata, ConfigSchema

METADATA = ModMetadata(
    type="{category}_mod",
    version="1.0.0",
    description="Test {category} mod",
    category="{category}",
    input_ports=[],
    output_ports=[],
    globals=[],
    packages=[],
    python_version=">=3.8"
)

CONFIG_SCHEMA = ConfigSchema(required={{}}, optional={{}})

def run(params):
    mod_name = params.get("_mod_name", "{category}_mod")
    result = ModResult("{category}_mod", mod_name)
    return result.success()
'''
                mod_file = Path(temp_dir) / f"{category}_mod.py"
                with open(mod_file, 'w') as f:
                    f.write(mod_content)
                
                registry.register_mod(f"{category}_mod")
            
            # Test listing all mods
            all_mods = registry.list_available_mods()
            assert len(all_mods) == 4
            
            print("PASS: All mods listed correctly")
            
            # Test category filtering
            for category in categories:
                filtered_mods = registry.list_available_mods(category)
                assert len(filtered_mods) == 1
                assert f"{category}_mod" in filtered_mods
                
            print("PASS: Category filtering works")
            
            # Test invalid category
            invalid_mods = registry.list_available_mods("invalid_category")
            assert len(invalid_mods) == 0
            
            print("PASS: Invalid category returns empty list")
            
        finally:
            sys.path.remove(temp_dir)
            cleanup_registry_file(registry_path)


def test_registry_validation():
    """Test registry validation functionality."""
    print("\n=== Test: Registry Validation ===")
    
    registry_path = create_test_registry_file()
    
    with TemporaryDirectory() as temp_dir:
        sys.path.insert(0, temp_dir)
        
        try:
            registry = ModRegistry(registry_path)
            
            # Create valid mod
            create_test_mod_file(temp_dir, "valid_test_mod")
            registry.register_mod("valid_test_mod")
            
            # Validate registry
            errors = registry.validate_registry()
            assert errors == []
            
            print("PASS: Valid registry validation passes")
            
            # Manually add invalid entry to test validation
            registry.registry_data["mods"]["invalid_mod"] = {
                "module_path": "nonexistent.module",
                "type": "invalid_mod",
                "version": "1.0.0"
            }
            
            # Validate registry with invalid entry
            errors = registry.validate_registry()
            assert len(errors) > 0
            assert any("import failed" in error for error in errors)
            
            print("PASS: Invalid mod detected in validation")
            
        finally:
            sys.path.remove(temp_dir)
            cleanup_registry_file(registry_path)


def test_global_registry():
    """Test global registry singleton."""
    print("\n=== Test: Global Registry ===")
    
    # Get global registry instance
    registry1 = get_registry()
    registry2 = get_registry()
    
    # Should be same instance (singleton)
    assert registry1 is registry2
    
    print("PASS: Global registry singleton works")


def test_registry_persistence():
    """Test that registry changes persist to file."""
    print("\n=== Test: Registry Persistence ===")
    
    registry_path = create_test_registry_file()
    
    with TemporaryDirectory() as temp_dir:
        sys.path.insert(0, temp_dir)
        
        try:
            # Create test mod and register
            create_test_mod_file(temp_dir, "persistence_test_mod")
            
            registry = ModRegistry(registry_path)
            registry.register_mod("persistence_test_mod")
            
            # Create new registry instance from same file
            registry2 = ModRegistry(registry_path)
            
            # Should have the registered mod
            mods = registry2.list_available_mods()
            assert "persistence_test_mod" in mods
            
            print("PASS: Registry changes persist to file")
            
        finally:
            sys.path.remove(temp_dir)
            cleanup_registry_file(registry_path)


def main():
    """Run all registry system tests."""
    print("Starting Registry System Tests...")
    print("=" * 50)
    
    try:
        test_registry_creation()
        test_empty_registry_operations()
        test_mod_registration()
        test_duplicate_registration()
        test_invalid_mod_registration()
        test_mod_deletion()
        test_category_filtering()
        test_registry_validation()
        test_global_registry()
        test_registry_persistence()
        
        print("\n" + "=" * 50)
        print("ALL REGISTRY SYSTEM TESTS PASSED")
        return True
        
    except Exception as e:
        print(f"\nFAIL: Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)