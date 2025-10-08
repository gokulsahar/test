"""
Test cases for datapy.mod_manager.registry module.

Tests ModRegistry class including initialization, mod registration,
validation, retrieval, deletion, and singleton pattern.
"""

import sys
import json
import os
from pathlib import Path
from unittest.mock import patch, mock_open, MagicMock
from datetime import datetime

# Add project root to Python path for testing
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pytest

from datapy.mod_manager.registry import (
    ModRegistry,
    get_registry,
    _global_registry
)
from datapy.mod_manager.base import ModMetadata, ConfigSchema


class TestModRegistryInit:
    """Test cases for ModRegistry initialization."""
    
    def test_init_with_valid_registry_file(self, tmp_path):
        """Test initialization with valid registry file."""
        registry_file = tmp_path / "test_registry.json"
        registry_data = {
            "mods": {
                "test_mod": {
                    "module_path": "test.module",
                    "type": "test_mod",
                    "version": "1.0.0"
                }
            }
        }
        registry_file.write_text(json.dumps(registry_data))
        
        registry = ModRegistry(str(registry_file))
        
        assert registry.registry_path == str(registry_file)
        assert "test_mod" in registry.registry_data["mods"]
    
    def test_init_auto_discover_registry(self):
        """Test initialization with auto-discovery of registry file."""
        # This will use the actual registry file in the framework
        registry = ModRegistry()
        
        assert registry.registry_path is not None
        assert Path(registry.registry_path).exists()
        assert "mods" in registry.registry_data
    
    def test_init_with_invalid_json(self, tmp_path):
        """Test initialization with invalid JSON raises error."""
        registry_file = tmp_path / "invalid.json"
        registry_file.write_text("{ invalid json }")
        
        with pytest.raises(RuntimeError, match="Invalid JSON"):
            ModRegistry(str(registry_file))
    
    def test_init_with_missing_mods_section(self, tmp_path):
        """Test initialization with missing 'mods' section."""
        registry_file = tmp_path / "no_mods.json"
        registry_file.write_text(json.dumps({"other": "data"}))
        
        with pytest.raises(RuntimeError, match="missing 'mods' section"):
            ModRegistry(str(registry_file))
    
    def test_init_with_non_dict_registry(self, tmp_path):
        """Test initialization with non-dictionary registry."""
        registry_file = tmp_path / "list_registry.json"
        registry_file.write_text(json.dumps(["item1", "item2"]))
        
        with pytest.raises(RuntimeError, match="must contain a JSON dictionary"):
            ModRegistry(str(registry_file))
    
    def test_init_with_non_dict_mods_section(self, tmp_path):
        """Test initialization with non-dictionary mods section."""
        registry_file = tmp_path / "bad_mods.json"
        registry_file.write_text(json.dumps({"mods": ["mod1", "mod2"]}))
        
        with pytest.raises(RuntimeError, match="'mods' section must be a dictionary"):
            ModRegistry(str(registry_file))
    
    def test_init_with_nonexistent_file(self):
        """Test initialization with nonexistent file raises error."""
        with pytest.raises(RuntimeError, match="Registry file not found"):
            ModRegistry("/nonexistent/registry.json")


class TestFindRegistryFile:
    """Test cases for _find_registry_file method."""
    
    def test_find_registry_file_success(self):
        """Test finding registry file at framework level."""
        registry = ModRegistry()
        
        # Registry should be found at datapy/mod_registry.json
        assert registry.registry_path.endswith("mod_registry.json")
        assert Path(registry.registry_path).exists()
    
    def test_find_registry_file_not_found(self):
        """Test error when registry file not found."""
        with patch('pathlib.Path.exists', return_value=False):
            with pytest.raises(RuntimeError, match="Registry file not found"):
                registry = ModRegistry()


class TestLoadRegistry:
    """Test cases for _load_registry method."""
    
    def test_load_registry_success(self, tmp_path):
        """Test successful registry loading."""
        registry_file = tmp_path / "test.json"
        data = {
            "mods": {
                "mod1": {"type": "mod1"},
                "mod2": {"type": "mod2"}
            },
            "_metadata": {"last_updated": "2024-01-01"}
        }
        registry_file.write_text(json.dumps(data))
        
        registry = ModRegistry(str(registry_file))
        
        assert len(registry.registry_data["mods"]) == 2
        assert "mod1" in registry.registry_data["mods"]
        assert "mod2" in registry.registry_data["mods"]
    
    def test_load_registry_empty_mods(self, tmp_path):
        """Test loading registry with empty mods section."""
        registry_file = tmp_path / "empty_mods.json"
        registry_file.write_text(json.dumps({"mods": {}}))
        
        registry = ModRegistry(str(registry_file))
        
        assert len(registry.registry_data["mods"]) == 0


class TestSaveRegistry:
    """Test cases for _save_registry method."""
    
    def test_save_registry_success(self, tmp_path):
        """Test successful registry saving."""
        registry_file = tmp_path / "save_test.json"
        initial_data = {"mods": {"mod1": {"type": "mod1"}}}
        registry_file.write_text(json.dumps(initial_data))
        
        registry = ModRegistry(str(registry_file))
        registry.registry_data["mods"]["mod2"] = {"type": "mod2"}
        registry._save_registry()
        
        # Read back and verify
        with open(registry_file) as f:
            saved_data = json.load(f)
        
        assert "mod2" in saved_data["mods"]
        assert "_metadata" in saved_data
        assert "last_updated" in saved_data["_metadata"]
    
    def test_save_registry_atomic_write(self, tmp_path):
        """Test atomic write using temporary file."""
        registry_file = tmp_path / "atomic_test.json"
        registry_file.write_text(json.dumps({"mods": {}}))
        
        registry = ModRegistry(str(registry_file))
        registry.registry_data["mods"]["new_mod"] = {"type": "new"}
        registry._save_registry()
        
        # Verify no .tmp or .backup files remain
        assert not (tmp_path / "atomic_test.json.tmp").exists()
        assert not (tmp_path / "atomic_test.json.backup").exists()
        
        # Verify content saved
        with open(registry_file) as f:
            data = json.load(f)
        assert "new_mod" in data["mods"]
    
    def test_save_registry_permission_error(self, tmp_path):
        """Test handling of permission errors during save."""
        registry_file = tmp_path / "perm_test.json"
        registry_file.write_text(json.dumps({"mods": {}}))
        
        registry = ModRegistry(str(registry_file))
        
        # Mock os.rename to raise PermissionError after temp file is created
        with patch('os.rename', side_effect=PermissionError("Access denied")):
            with pytest.raises(RuntimeError, match="Failed to save registry"):
                registry._save_registry()


class TestGetModInfo:
    """Test cases for get_mod_info method."""
    
    def test_get_mod_info_success(self, tmp_path):
        """Test successful mod info retrieval."""
        registry_file = tmp_path / "info_test.json"
        mod_data = {
            "module_path": "test.module",
            "type": "test_mod",
            "version": "1.0.0",
            "description": "Test description"
        }
        registry_file.write_text(json.dumps({"mods": {"test_mod": mod_data}}))
        
        registry = ModRegistry(str(registry_file))
        info = registry.get_mod_info("test_mod")
        
        assert info["type"] == "test_mod"
        assert info["version"] == "1.0.0"
        assert info["description"] == "Test description"
    
    def test_get_mod_info_returns_copy(self, tmp_path):
        """Test that get_mod_info returns a copy not reference."""
        registry_file = tmp_path / "copy_test.json"
        registry_file.write_text(json.dumps({"mods": {"mod1": {"type": "mod1"}}}))
        
        registry = ModRegistry(str(registry_file))
        info1 = registry.get_mod_info("mod1")
        info1["modified"] = True
        info2 = registry.get_mod_info("mod1")
        
        assert "modified" in info1
        assert "modified" not in info2
    
    def test_get_mod_info_not_found(self, tmp_path):
        """Test error when mod not found."""
        registry_file = tmp_path / "not_found.json"
        registry_file.write_text(json.dumps({"mods": {"mod1": {"type": "mod1"}}}))
        
        registry = ModRegistry(str(registry_file))
        
        with pytest.raises(ValueError, match="not found in registry"):
            registry.get_mod_info("nonexistent_mod")
    
    def test_get_mod_info_empty_mod_type(self, tmp_path):
        """Test error with empty mod_type."""
        registry_file = tmp_path / "empty_type.json"
        registry_file.write_text(json.dumps({"mods": {}}))
        
        registry = ModRegistry(str(registry_file))
        
        with pytest.raises(ValueError, match="must be a non-empty string"):
            registry.get_mod_info("")
    
    def test_get_mod_info_none_mod_type(self, tmp_path):
        """Test error with None mod_type."""
        registry_file = tmp_path / "none_type.json"
        registry_file.write_text(json.dumps({"mods": {}}))
        
        registry = ModRegistry(str(registry_file))
        
        with pytest.raises(ValueError, match="must be a non-empty string"):
            registry.get_mod_info(None)


class TestDeleteMod:
    """Test cases for delete_mod method."""
    
    def test_delete_mod_success(self, tmp_path):
        """Test successful mod deletion."""
        registry_file = tmp_path / "delete_test.json"
        registry_file.write_text(json.dumps({
            "mods": {
                "mod1": {"type": "mod1"},
                "mod2": {"type": "mod2"}
            }
        }))
        
        registry = ModRegistry(str(registry_file))
        result = registry.delete_mod("mod1")
        
        assert result is True
        assert "mod1" not in registry.registry_data["mods"]
        assert "mod2" in registry.registry_data["mods"]
        
        # Verify saved to file
        with open(registry_file) as f:
            data = json.load(f)
        assert "mod1" not in data["mods"]
    
    def test_delete_mod_not_found(self, tmp_path):
        """Test error when deleting non-existent mod."""
        registry_file = tmp_path / "delete_not_found.json"
        registry_file.write_text(json.dumps({"mods": {"mod1": {"type": "mod1"}}}))
        
        registry = ModRegistry(str(registry_file))
        
        with pytest.raises(ValueError, match="not found in registry"):
            registry.delete_mod("nonexistent")
    
    def test_delete_mod_empty_mod_type(self, tmp_path):
        """Test error with empty mod_type."""
        registry_file = tmp_path / "delete_empty.json"
        registry_file.write_text(json.dumps({"mods": {}}))
        
        registry = ModRegistry(str(registry_file))
        
        with pytest.raises(ValueError, match="must be a non-empty string"):
            registry.delete_mod("")
    
    def test_delete_mod_save_failure(self, tmp_path):
        """Test handling of save failure during deletion."""
        registry_file = tmp_path / "delete_fail.json"
        registry_file.write_text(json.dumps({"mods": {"mod1": {"type": "mod1"}}}))
        
        registry = ModRegistry(str(registry_file))
        
        with patch.object(registry, '_save_registry', side_effect=Exception("Save failed")):
            with pytest.raises(RuntimeError, match="Failed to delete mod"):
                registry.delete_mod("mod1")


class TestListAvailableMods:
    """Test cases for list_available_mods method."""
    
    def test_list_all_mods(self, tmp_path):
        """Test listing all mods without filter."""
        registry_file = tmp_path / "list_all.json"
        registry_file.write_text(json.dumps({
            "mods": {
                "mod1": {"type": "mod1", "category": "sources"},
                "mod2": {"type": "mod2", "category": "transformers"},
                "mod3": {"type": "mod3", "category": "sinks"}
            }
        }))
        
        registry = ModRegistry(str(registry_file))
        mods = registry.list_available_mods()
        
        assert len(mods) == 3
        assert "mod1" in mods
        assert "mod2" in mods
        assert "mod3" in mods
    
    def test_list_mods_by_category(self, tmp_path):
        """Test listing mods filtered by category."""
        registry_file = tmp_path / "list_category.json"
        registry_file.write_text(json.dumps({
            "mods": {
                "csv_reader": {"type": "csv_reader", "category": "sources"},
                "csv_filter": {"type": "csv_filter", "category": "transformers"},
                "excel_reader": {"type": "excel_reader", "category": "sources"}
            }
        }))
        
        registry = ModRegistry(str(registry_file))
        sources = registry.list_available_mods(category="sources")
        
        assert len(sources) == 2
        assert "csv_reader" in sources
        assert "excel_reader" in sources
        assert "csv_filter" not in sources
    
    def test_list_mods_empty_registry(self, tmp_path):
        """Test listing mods from empty registry."""
        registry_file = tmp_path / "list_empty.json"
        registry_file.write_text(json.dumps({"mods": {}}))
        
        registry = ModRegistry(str(registry_file))
        mods = registry.list_available_mods()
        
        assert len(mods) == 0
    
    def test_list_mods_no_matching_category(self, tmp_path):
        """Test listing with category that has no matches."""
        registry_file = tmp_path / "list_no_match.json"
        registry_file.write_text(json.dumps({
            "mods": {
                "mod1": {"type": "mod1", "category": "sources"}
            }
        }))
        
        registry = ModRegistry(str(registry_file))
        mods = registry.list_available_mods(category="transformers")
        
        assert len(mods) == 0


class TestRegisterMod:
    """Test cases for register_mod method."""
    
    def test_register_mod_success(self, tmp_path):
        """Test successful mod registration."""
        registry_file = tmp_path / "register_test.json"
        registry_file.write_text(json.dumps({"mods": {}}))
        
        # Create mock mod module with valid description (at least 10 chars)
        mock_metadata = ModMetadata(
            type="new_mod",
            version="1.0.0",
            description="Test mod for registration testing",
            category="sources"
        )
        mock_config = ConfigSchema()
        
        mock_module = MagicMock()
        mock_module.METADATA = mock_metadata
        mock_module.CONFIG_SCHEMA = mock_config
        mock_module.run = MagicMock()
        
        registry = ModRegistry(str(registry_file))
        
        with patch('importlib.import_module', return_value=mock_module):
            result = registry.register_mod("test.new_mod")
        
        assert result is True
        assert "new_mod" in registry.registry_data["mods"]
        assert registry.registry_data["mods"]["new_mod"]["type"] == "new_mod"
        assert registry.registry_data["mods"]["new_mod"]["version"] == "1.0.0"
    
    def test_register_mod_already_registered(self, tmp_path):
        """Test error when registering already registered mod."""
        registry_file = tmp_path / "register_duplicate.json"
        registry_file.write_text(json.dumps({
            "mods": {"existing_mod": {"type": "existing_mod"}}
        }))
        
        # Create mock mod module
        mock_metadata = ModMetadata(
            type="existing_mod",
            version="1.0.0",
            description="Existing mod description",
            category="sources"
        )
        mock_config = ConfigSchema()
        mock_module = MagicMock()
        mock_module.METADATA = mock_metadata
        mock_module.CONFIG_SCHEMA = mock_config
        mock_module.run = MagicMock()
        
        registry = ModRegistry(str(registry_file))
        
        with patch('importlib.import_module', return_value=mock_module):
            with pytest.raises(ValueError, match="already registered"):
                registry.register_mod("test.existing_mod")
    
    def test_register_mod_import_failure(self, tmp_path):
        """Test error when mod import fails."""
        registry_file = tmp_path / "register_import_fail.json"
        registry_file.write_text(json.dumps({"mods": {}}))
        
        registry = ModRegistry(str(registry_file))
        
        with patch('importlib.import_module', side_effect=ImportError("Module not found")):
            with pytest.raises(ValueError, match="Cannot import mod"):
                registry.register_mod("nonexistent.module")
    
    def test_register_mod_missing_metadata(self, tmp_path):
        """Test error when mod missing METADATA."""
        registry_file = tmp_path / "register_no_metadata.json"
        registry_file.write_text(json.dumps({"mods": {}}))
        
        mock_module = MagicMock()
        del mock_module.METADATA  # Remove METADATA
        mock_module.run = MagicMock()
        
        registry = ModRegistry(str(registry_file))
        
        with patch('importlib.import_module', return_value=mock_module):
            with pytest.raises(ValueError, match="missing required 'METADATA'"):
                registry.register_mod("test.no_metadata")
    
    def test_register_mod_empty_module_path(self, tmp_path):
        """Test error with empty module path."""
        registry_file = tmp_path / "register_empty_path.json"
        registry_file.write_text(json.dumps({"mods": {}}))
        
        registry = ModRegistry(str(registry_file))
        
        with pytest.raises(ValueError, match="must be a non-empty string"):
            registry.register_mod("")


class TestValidateRegistry:
    """Test cases for validate_registry method."""
    
    def test_validate_registry_all_valid(self, tmp_path):
        """Test validation with all valid mods."""
        registry_file = tmp_path / "validate_all_valid.json"
        
        mock_metadata = ModMetadata(
            type="valid_mod",
            version="1.0.0",
            description="Valid mod for testing validation",
            category="sources"
        )
        mock_config = ConfigSchema()
        mock_module = MagicMock()
        mock_module.METADATA = mock_metadata
        mock_module.CONFIG_SCHEMA = mock_config
        mock_module.run = MagicMock()
        
        registry_file.write_text(json.dumps({
            "mods": {
                "valid_mod": {
                    "module_path": "test.valid_mod",
                    "type": "valid_mod"
                }
            }
        }))
        
        registry = ModRegistry(str(registry_file))
        
        with patch('importlib.import_module', return_value=mock_module):
            errors = registry.validate_registry()
        
        assert len(errors) == 0
    
    def test_validate_registry_import_error(self, tmp_path):
        """Test validation catches import errors."""
        registry_file = tmp_path / "validate_import_error.json"
        registry_file.write_text(json.dumps({
            "mods": {
                "bad_mod": {
                    "module_path": "nonexistent.module",
                    "type": "bad_mod"
                }
            }
        }))
        
        registry = ModRegistry(str(registry_file))
        
        with patch('importlib.import_module', side_effect=ImportError("Not found")):
            errors = registry.validate_registry()
        
        assert len(errors) > 0
        assert any("import failed" in err for err in errors)
    
    def test_validate_registry_missing_run(self, tmp_path):
        """Test validation catches missing run function."""
        registry_file = tmp_path / "validate_no_run.json"
        registry_file.write_text(json.dumps({
            "mods": {
                "no_run_mod": {
                    "module_path": "test.no_run",
                    "type": "no_run_mod"
                }
            }
        }))
        
        mock_module = MagicMock()
        del mock_module.run  # Remove run function
        
        registry = ModRegistry(str(registry_file))
        
        with patch('importlib.import_module', return_value=mock_module):
            errors = registry.validate_registry()
        
        assert len(errors) > 0
        assert any("missing run()" in err for err in errors)
    
    def test_validate_registry_empty(self, tmp_path):
        """Test validation of empty registry."""
        registry_file = tmp_path / "validate_empty.json"
        registry_file.write_text(json.dumps({"mods": {}}))
        
        registry = ModRegistry(str(registry_file))
        errors = registry.validate_registry()
        
        assert len(errors) == 0


class TestGetRegistry:
    """Test cases for get_registry singleton function."""
    
    def test_get_registry_returns_instance(self):
        """Test get_registry returns ModRegistry instance."""
        registry = get_registry()
        
        assert isinstance(registry, ModRegistry)
        assert hasattr(registry, 'registry_data')
    
    def test_get_registry_singleton_pattern(self):
        """Test get_registry returns same instance (singleton)."""
        # Clear global first
        import datapy.mod_manager.registry as registry_module
        registry_module._global_registry = None
        
        registry1 = get_registry()
        registry2 = get_registry()
        
        assert registry1 is registry2
    
    def test_get_registry_creates_on_first_call(self):
        """Test get_registry creates instance on first call."""
        import datapy.mod_manager.registry as registry_module
        registry_module._global_registry = None
        
        assert registry_module._global_registry is None
        
        registry = get_registry()
        
        assert registry_module._global_registry is not None
        assert registry_module._global_registry is registry


class TestIntegrationScenarios:
    """Integration test cases for complete registry workflows."""
    
    def test_complete_mod_lifecycle(self, tmp_path):
        """Test complete mod lifecycle: register, list, get info, delete."""
        registry_file = tmp_path / "lifecycle.json"
        registry_file.write_text(json.dumps({"mods": {}}))
        
        # Create mock mod with valid description
        mock_metadata = ModMetadata(
            type="lifecycle_mod",
            version="1.0.0",
            description="Lifecycle test module for testing",
            category="sources"
        )
        mock_config = ConfigSchema()
        mock_module = MagicMock()
        mock_module.METADATA = mock_metadata
        mock_module.CONFIG_SCHEMA = mock_config
        mock_module.run = MagicMock()
        
        registry = ModRegistry(str(registry_file))
        
        # Register
        with patch('importlib.import_module', return_value=mock_module):
            result = registry.register_mod("test.lifecycle_mod")
        assert result is True
        
        # List
        mods = registry.list_available_mods()
        assert "lifecycle_mod" in mods
        
        # Get info
        info = registry.get_mod_info("lifecycle_mod")
        assert info["version"] == "1.0.0"
        
        # Delete
        result = registry.delete_mod("lifecycle_mod")
        assert result is True
        
        # Verify deleted
        mods = registry.list_available_mods()
        assert "lifecycle_mod" not in mods
    
    def test_multiple_mods_different_categories(self, tmp_path):
        """Test managing multiple mods with different categories."""
        registry_file = tmp_path / "multi_cat.json"
        registry_file.write_text(json.dumps({"mods": {}}))
        
        registry = ModRegistry(str(registry_file))
        
        # Add mods of different categories
        for i, category in enumerate(["sources", "transformers", "sinks"]):
            mock_metadata = ModMetadata(
                type=f"mod{i}",
                version="1.0.0",
                description=f"Test module for {category} category",
                category=category
            )
            mock_module = MagicMock()
            mock_module.METADATA = mock_metadata
            mock_module.CONFIG_SCHEMA = ConfigSchema()
            mock_module.run = MagicMock()
            
            with patch('importlib.import_module', return_value=mock_module):
                registry.register_mod(f"test.mod{i}")
        
        # Test category filtering
        sources = registry.list_available_mods(category="sources")
        assert len(sources) == 1
        
        all_mods = registry.list_available_mods()
        assert len(all_mods) == 3


class TestRegisterModValidation:
    """Test cases for mod registration validation."""
    
    def test_register_mod_missing_run_function(self, tmp_path):
        """Test error when mod missing run function."""
        registry_file = tmp_path / "register_no_run.json"
        registry_file.write_text(json.dumps({"mods": {}}))
        
        mock_module = MagicMock()
        del mock_module.run  # Remove run function
        mock_module.METADATA = ModMetadata(
            type="no_run_mod",
            version="1.0.0",
            description="Mod without run function",
            category="sources"
        )
        mock_module.CONFIG_SCHEMA = ConfigSchema()
        
        registry = ModRegistry(str(registry_file))
        
        with patch('importlib.import_module', return_value=mock_module):
            with pytest.raises(ValueError, match="missing required 'run' function"):
                registry.register_mod("test.no_run_mod")
    
    def test_register_mod_missing_config_schema(self, tmp_path):
        """Test error when mod missing CONFIG_SCHEMA."""
        registry_file = tmp_path / "register_no_config.json"
        registry_file.write_text(json.dumps({"mods": {}}))
        
        mock_module = MagicMock()
        mock_module.run = MagicMock()
        mock_module.METADATA = ModMetadata(
            type="no_config_mod",
            version="1.0.0",
            description="Mod without config schema",
            category="sources"
        )
        del mock_module.CONFIG_SCHEMA  # Remove CONFIG_SCHEMA
        
        registry = ModRegistry(str(registry_file))
        
        with patch('importlib.import_module', return_value=mock_module):
            with pytest.raises(ValueError, match="missing required 'CONFIG_SCHEMA'"):
                registry.register_mod("test.no_config_mod")
    
    def test_register_mod_whitespace_module_path(self, tmp_path):
        """Test registration with module path that has whitespace."""
        registry_file = tmp_path / "register_whitespace.json"
        registry_file.write_text(json.dumps({"mods": {}}))
        
        mock_metadata = ModMetadata(
            type="whitespace_mod",
            version="1.0.0",
            description="Mod with whitespace in path",
            category="sources"
        )
        mock_module = MagicMock()
        mock_module.METADATA = mock_metadata
        mock_module.CONFIG_SCHEMA = ConfigSchema()
        mock_module.run = MagicMock()
        
        registry = ModRegistry(str(registry_file))
        
        with patch('importlib.import_module', return_value=mock_module):
            result = registry.register_mod("  test.whitespace_mod  ")
        
        assert result is True
        assert "whitespace_mod" in registry.registry_data["mods"]


class TestValidateModStructure:
    """Test cases for _validate_mod_structure method."""
    
    def test_validate_mod_structure_non_callable_run(self, tmp_path):
        """Test validation catches non-callable run attribute."""
        registry_file = tmp_path / "validate_non_callable.json"
        registry_file.write_text(json.dumps({
            "mods": {
                "bad_run_mod": {
                    "module_path": "test.bad_run",
                    "type": "bad_run_mod"
                }
            }
        }))
        
        mock_module = MagicMock()
        mock_module.run = "not_a_function"  # Not callable
        mock_module.METADATA = MagicMock()
        mock_module.CONFIG_SCHEMA = MagicMock()
        
        registry = ModRegistry(str(registry_file))
        
        with patch('importlib.import_module', return_value=mock_module):
            errors = registry.validate_registry()
        
        assert len(errors) > 0
        assert any("run is not callable" in err for err in errors)
    
    def test_validate_mod_structure_missing_module_path(self, tmp_path):
        """Test validation handles missing module_path."""
        registry_file = tmp_path / "validate_no_path.json"
        registry_file.write_text(json.dumps({
            "mods": {
                "no_path_mod": {
                    "type": "no_path_mod"
                    # Missing module_path
                }
            }
        }))
        
        registry = ModRegistry(str(registry_file))
        errors = registry.validate_registry()
        
        assert len(errors) > 0
        assert any("missing module_path" in err for err in errors)


class TestValidateModMetadata:
    """Test cases for _validate_mod_metadata method."""
    
    def test_validate_metadata_not_modmetadata_instance(self, tmp_path):
        """Test validation catches incorrect METADATA type."""
        registry_file = tmp_path / "validate_bad_metadata.json"
        registry_file.write_text(json.dumps({
            "mods": {
                "bad_metadata_mod": {
                    "module_path": "test.bad_metadata",
                    "type": "bad_metadata_mod"
                }
            }
        }))
        
        mock_module = MagicMock()
        mock_module.run = MagicMock()
        mock_module.METADATA = {"not": "ModMetadata"}  # Wrong type
        mock_module.CONFIG_SCHEMA = ConfigSchema()
        
        registry = ModRegistry(str(registry_file))
        
        with patch('importlib.import_module', return_value=mock_module):
            errors = registry.validate_registry()
        
        assert len(errors) > 0
        assert any("METADATA is not ModMetadata instance" in err for err in errors)
    
    def test_validate_metadata_not_configschema_instance(self, tmp_path):
        """Test validation catches incorrect CONFIG_SCHEMA type."""
        registry_file = tmp_path / "validate_bad_config.json"
        registry_file.write_text(json.dumps({
            "mods": {
                "bad_config_mod": {
                    "module_path": "test.bad_config",
                    "type": "bad_config_mod"
                }
            }
        }))
        
        mock_metadata = ModMetadata(
            type="bad_config_mod",
            version="1.0.0",
            description="Mod with bad config",
            category="sources"
        )
        
        mock_module = MagicMock()
        mock_module.run = MagicMock()
        mock_module.METADATA = mock_metadata
        mock_module.CONFIG_SCHEMA = {"not": "ConfigSchema"}  # Wrong type
        
        registry = ModRegistry(str(registry_file))
        
        with patch('importlib.import_module', return_value=mock_module):
            errors = registry.validate_registry()
        
        assert len(errors) > 0
        assert any("CONFIG_SCHEMA is not ConfigSchema instance" in err for err in errors)


class TestSaveRegistryEdgeCases:
    """Test cases for edge cases in _save_registry method."""
    
    def test_save_registry_creates_new_file(self, tmp_path):
        """Test saving registry when file doesn't exist."""
        registry_file = tmp_path / "new_registry.json"
        # Don't create the file - let _save_registry create it
        
        # Create a registry with a valid existing file first
        initial_file = tmp_path / "initial.json"
        initial_file.write_text(json.dumps({"mods": {}}))
        registry = ModRegistry(str(initial_file))
        
        # Change the registry path to non-existent file
        registry.registry_path = str(registry_file)
        registry.registry_data["mods"]["new_mod"] = {"type": "new"}
        
        # Should create new file
        registry._save_registry()
        
        assert registry_file.exists()
        with open(registry_file) as f:
            data = json.load(f)
        assert "new_mod" in data["mods"]
    
    def test_save_registry_updates_metadata_timestamp(self, tmp_path):
        """Test that save updates last_updated timestamp."""
        registry_file = tmp_path / "timestamp_test.json"
        registry_file.write_text(json.dumps({"mods": {}}))
        
        registry = ModRegistry(str(registry_file))
        registry._save_registry()
        
        with open(registry_file) as f:
            data = json.load(f)
        
        assert "_metadata" in data
        assert "last_updated" in data["_metadata"]
        # Verify it's a valid ISO format timestamp
        datetime.fromisoformat(data["_metadata"]["last_updated"])


class TestDeleteModEdgeCases:
    """Test cases for edge cases in delete_mod method."""
    
    def test_delete_mod_whitespace_mod_type(self, tmp_path):
        """Test delete with whitespace in mod_type."""
        registry_file = tmp_path / "delete_whitespace.json"
        registry_file.write_text(json.dumps({
            "mods": {"test_mod": {"type": "test_mod"}}
        }))
        
        registry = ModRegistry(str(registry_file))
        result = registry.delete_mod("  test_mod  ")
        
        assert result is True
        assert "test_mod" not in registry.registry_data["mods"]


class TestGetModInfoEdgeCases:
    """Test cases for edge cases in get_mod_info method."""
    
    def test_get_mod_info_whitespace_mod_type(self, tmp_path):
        """Test get_mod_info with whitespace in mod_type."""
        registry_file = tmp_path / "info_whitespace.json"
        registry_file.write_text(json.dumps({
            "mods": {"test_mod": {"type": "test_mod", "version": "1.0.0"}}
        }))
        
        registry = ModRegistry(str(registry_file))
        info = registry.get_mod_info("  test_mod  ")
        
        assert info["type"] == "test_mod"
        assert info["version"] == "1.0.0"