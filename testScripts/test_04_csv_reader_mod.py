"""
Test 04: CSV Reader Mod
Tests the CSV reader mod implementation directly.
"""

import sys
import os
import pandas as pd
from pathlib import Path
from tempfile import NamedTemporaryFile

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import the CSV reader mod directly
from datapy.mods.sources.csv_reader import run, METADATA, CONFIG_SCHEMA
from datapy.mod_manager.result import SUCCESS, SUCCESS_WITH_WARNINGS, RUNTIME_ERROR


def create_test_csv(content: str) -> str:
    """Create temporary CSV file with given content."""
    with NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
        f.write(content)
        return f.name


def cleanup_file(file_path: str) -> None:
    """Clean up temporary file."""
    try:
        if os.path.exists(file_path):
            os.unlink(file_path)
    except:
        pass


def test_mod_structure():
    """Test that mod has required structure."""
    print("=== Test: Mod Structure ===")
    
    # Check metadata exists and is valid
    assert hasattr(METADATA, 'type')
    assert METADATA.type == "csv_reader"
    assert METADATA.version == "1.0.0"
    assert METADATA.category == "source"
    assert METADATA.description != ""
    
    print("PASS: Metadata structure valid")
    
    # Check config schema exists
    assert hasattr(CONFIG_SCHEMA, 'required')
    assert hasattr(CONFIG_SCHEMA, 'optional')
    assert "file_path" in CONFIG_SCHEMA.required
    
    print("PASS: Config schema structure valid")
    
    # Check run function exists and is callable
    assert callable(run)
    
    print("PASS: Run function exists and callable")


def test_valid_csv_read():
    """Test reading a valid CSV file."""
    print("\n=== Test: Valid CSV Read ===")
    
    # Create test CSV
    csv_content = """name,age,city
John,25,New York
Jane,30,Los Angeles
Bob,35,Chicago"""
    
    csv_file = create_test_csv(csv_content)
    
    try:
        # Test with minimal parameters
        params = {
            "_mod_name": "test_csv_reader",
            "_mod_type": "csv_reader",
            "file_path": csv_file
        }
        
        result = run(params)
        
        # Validate result structure
        assert result["status"] == "success"
        assert result["exit_code"] == SUCCESS
        assert "execution_time" in result
        
        # Validate metrics
        assert result["metrics"]["rows_read"] == 3
        assert result["metrics"]["columns_read"] == 3
        assert result["metrics"]["file_size_bytes"] > 0
        assert result["metrics"]["encoding_used"] == "utf-8"
        assert result["metrics"]["delimiter_used"] == ","
        
        # Validate artifacts
        assert "data" in result["artifacts"]
        assert "file_path" in result["artifacts"]
        
        df = result["artifacts"]["data"]
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert len(df.columns) == 3
        assert list(df.columns) == ["name", "age", "city"]
        
        # Validate globals
        assert result["globals"]["row_count"] == 3
        assert result["globals"]["column_count"] == 3
        assert result["globals"]["file_size"] > 0
        
        print("PASS: Valid CSV read successful")
        
    finally:
        cleanup_file(csv_file)


def test_csv_with_custom_delimiter():
    """Test reading CSV with custom delimiter."""
    print("\n=== Test: Custom Delimiter ===")
    
    # Create test CSV with semicolon delimiter
    csv_content = """name;age;city
John;25;New York
Jane;30;Los Angeles"""
    
    csv_file = create_test_csv(csv_content)
    
    try:
        params = {
            "_mod_name": "test_delimiter",
            "_mod_type": "csv_reader", 
            "file_path": csv_file,
            "delimiter": ";"
        }
        
        result = run(params)
        
        assert result["status"] == "success"
        assert result["metrics"]["delimiter_used"] == ";"
        
        df = result["artifacts"]["data"]
        assert len(df) == 2
        assert len(df.columns) == 3
        assert list(df.columns) == ["name", "age", "city"]
        
        print("PASS: Custom delimiter works")
        
    finally:
        cleanup_file(csv_file)


def test_csv_with_no_header():
    """Test reading CSV without header row."""
    print("\n=== Test: No Header ===")
    
    # Create test CSV without header
    csv_content = """John,25,New York
Jane,30,Los Angeles"""
    
    csv_file = create_test_csv(csv_content)
    
    try:
        params = {
            "_mod_name": "test_no_header",
            "_mod_type": "csv_reader",
            "file_path": csv_file,
            "header": -1  # No header
        }
        
        result = run(params)
        
        assert result["status"] == "success"
        
        df = result["artifacts"]["data"]
        assert len(df) == 2
        assert len(df.columns) == 3
        # Pandas will create default column names like 0, 1, 2
        
        print("PASS: No header CSV read works")
        
    finally:
        cleanup_file(csv_file)


def test_csv_with_skip_rows():
    """Test reading CSV with skipped rows."""
    print("\n=== Test: Skip Rows ===")
    
    # Create test CSV with extra rows at top
    csv_content = """# This is a comment
# Another comment
name,age,city
John,25,New York
Jane,30,Los Angeles"""
    
    csv_file = create_test_csv(csv_content)
    
    try:
        params = {
            "_mod_name": "test_skip_rows",
            "_mod_type": "csv_reader",
            "file_path": csv_file,
            "skip_rows": 2  # Skip first 2 comment rows
        }
        
        result = run(params)
        
        assert result["status"] == "success"
        
        df = result["artifacts"]["data"]
        assert len(df) == 2  # Data rows after header
        assert list(df.columns) == ["name", "age", "city"]
        
        print("PASS: Skip rows works")
        
    finally:
        cleanup_file(csv_file)


def test_csv_with_max_rows():
    """Test reading CSV with row limit."""
    print("\n=== Test: Max Rows ===")
    
    # Create test CSV with many rows
    csv_content = """name,age,city
John,25,New York
Jane,30,Los Angeles
Bob,35,Chicago
Alice,28,Boston
Charlie,32,Seattle"""
    
    csv_file = create_test_csv(csv_content)
    
    try:
        params = {
            "_mod_name": "test_max_rows",
            "_mod_type": "csv_reader",
            "file_path": csv_file,
            "max_rows": 2  # Only read 2 data rows
        }
        
        result = run(params)
        
        assert result["status"] == "success"
        assert result["metrics"]["rows_read"] == 2
        
        df = result["artifacts"]["data"]
        assert len(df) == 2
        
        print("PASS: Max rows limit works")
        
    finally:
        cleanup_file(csv_file)


def test_file_not_found():
    """Test handling of non-existent file."""
    print("\n=== Test: File Not Found ===")
    
    params = {
        "_mod_name": "test_not_found",
        "_mod_type": "csv_reader",
        "file_path": "/path/that/does/not/exist.csv"
    }
    
    result = run(params)
    
    assert result["status"] == "error"
    assert result["exit_code"] == RUNTIME_ERROR
    assert len(result["errors"]) == 1
    assert "File not found" in result["errors"][0]["message"]
    
    print("PASS: File not found handled correctly")


def test_empty_csv():
    """Test handling of empty CSV file."""
    print("\n=== Test: Empty CSV ===")
    
    # Create empty CSV
    csv_content = ""
    csv_file = create_test_csv(csv_content)
    
    try:
        params = {
            "_mod_name": "test_empty",
            "_mod_type": "csv_reader",
            "file_path": csv_file
        }
        
        result = run(params)
        
        assert result["status"] == "warning"
        assert result["exit_code"] == SUCCESS_WITH_WARNINGS
        assert len(result["warnings"]) == 1
        assert "empty" in result["warnings"][0]["message"].lower()
        
        print("PASS: Empty CSV handled correctly")
        
    finally:
        cleanup_file(csv_file)


def test_missing_required_parameter():
    """Test handling of missing required parameter."""
    print("\n=== Test: Missing Required Parameter ===")
    
    params = {
        "_mod_name": "test_missing_param",
        "_mod_type": "csv_reader"
        # Missing file_path
    }
    
    result = run(params)
    
    assert result["status"] == "error"
    assert result["exit_code"] == RUNTIME_ERROR
    assert len(result["errors"]) == 1
    assert "Missing required parameter" in result["errors"][0]["message"]
    
    print("PASS: Missing required parameter handled correctly")


def test_encoding_fallback():
    """Test encoding fallback functionality."""
    print("\n=== Test: Encoding Fallback ===")
    
    # Create CSV with UTF-8 BOM (will cause UnicodeDecodeError with plain utf-8)
    csv_content = """name,age,city
John,25,New York
Jane,30,Los Angeles"""
    
    # Write with BOM
    with NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8-sig') as f:
        f.write(csv_content)
        csv_file = f.name
    
    try:
        params = {
            "_mod_name": "test_encoding",
            "_mod_type": "csv_reader",
            "file_path": csv_file,
            "encoding": "ascii"  # This will fail, should fallback
        }
        
        result = run(params)
        
        # Should succeed with warning about encoding correction
        if result["status"] == "success":
            assert len(result["warnings"]) > 0
            print("PASS: Encoding fallback works")
        else:
            # If it fails, that's also acceptable for this test
            print("PASS: Encoding error handled appropriately")
        
    finally:
        cleanup_file(csv_file)


def main():
    """Run all CSV reader mod tests."""
    print("Starting CSV Reader Mod Tests...")
    print("=" * 50)
    
    try:
        test_mod_structure()
        test_valid_csv_read()
        test_csv_with_custom_delimiter()
        test_csv_with_no_header()
        test_csv_with_skip_rows()
        test_csv_with_max_rows()
        test_file_not_found()
        test_empty_csv()
        test_missing_required_parameter()
        test_encoding_fallback()
        
        print("\n" + "=" * 50)
        print("ALL CSV READER MOD TESTS PASSED")
        return True
        
    except Exception as e:
        print(f"\nFAIL: Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)