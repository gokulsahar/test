"""
Test 09: CLI System
Tests the command-line interface functionality with cross-platform support.
"""

import sys
import json
import os
import subprocess
import shutil
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import NamedTuple

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


class CommandResult(NamedTuple):
    """Result of running a CLI command."""
    exit_code: int
    stdout: str
    stderr: str
    output: str  # Combined stdout + stderr for convenience


def run_datapy_command(*args, timeout: int = 30, input_data: str = None, cwd: str = None) -> CommandResult:
    """
    Run a DataPy CLI command using direct script execution.
    
    Args:
        *args: Command arguments (e.g., 'list-registry', '--category', 'sources')
        timeout: Command timeout in seconds
        input_data: Data to send to stdin
        cwd: Working directory
        
    Returns:
        CommandResult with exit code and output
    """
    # Get project root and CLI script path
    project_root = Path(__file__).parent.parent
    cli_script = project_root / "datapy" / "__main__.py"
    
    if not cli_script.exists():
        return CommandResult(
            exit_code=-1,
            stdout="",
            stderr=f"CLI script not found: {cli_script}",
            output=f"CLI script not found: {cli_script}"
        )
    
    # Use direct script execution
    python_executable = sys.executable
    cmd = [python_executable, str(cli_script)] + list(args)
    
    # Set up environment to ensure imports work
    env = os.environ.copy()
    env['PYTHONPATH'] = str(project_root)
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            input=input_data,
            cwd=cwd or project_root,
            env=env,
            encoding='utf-8',
            errors='replace'
        )
        
        combined_output = ""
        if result.stdout:
            combined_output += result.stdout
        if result.stderr:
            combined_output += "\n" + result.stderr
        
        return CommandResult(
            exit_code=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
            output=combined_output.strip()
        )
        
    except subprocess.TimeoutExpired:
        return CommandResult(
            exit_code=-1,
            stdout="",
            stderr=f"Command timed out after {timeout} seconds",
            output=f"Command timed out after {timeout} seconds"
        )
    except Exception as e:
        return CommandResult(
            exit_code=-1,
            stdout="",
            stderr=f"Command execution failed: {e}",
            output=f"Command execution failed: {e}"
        )


def create_test_yaml(content: dict) -> str:
    """Create temporary YAML file with given content."""
    import yaml
    with NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8') as f:
        yaml.dump(content, f, default_flow_style=False)
        return f.name


def cleanup_file(file_path: str) -> None:
    """Clean up temporary file."""
    try:
        if os.path.exists(file_path):
            os.unlink(file_path)
    except:
        pass


def test_cli_help():
    """Test CLI help command."""
    print("=== Test: CLI Help Command ===")
    
    result = run_datapy_command("--help")
    
    # Should show help and exit successfully
    assert result.exit_code == 0 or result.exit_code == None, f"Help should succeed, got {result.exit_code}"
    assert "DataPy Framework" in result.output or "Usage:" in result.output, "Help should show usage info"
    
    print("PASS: CLI help works correctly")


def test_list_registry():
    """Test list-registry command."""
    print("\n=== Test: List Registry Command ===")
    
    result = run_datapy_command("list-registry")
    
    # Should succeed even if registry is empty
    assert result.exit_code == 0, f"list-registry should succeed, got {result.exit_code}. Output: {result.output}"
    
    # Should either show "No mods found" or list mods
    assert ("No mods found" in result.output or 
            "Registered Mods" in result.output or
            result.output == ""), f"Unexpected output: {result.output}"
    
    print("PASS: list-registry command works correctly")


def test_validate_registry():
    """Test validate-registry command."""
    print("\n=== Test: Validate Registry Command ===")
    
    result = run_datapy_command("validate-registry")
    
    # Should succeed (empty registry is valid)
    assert result.exit_code == 0, f"validate-registry should succeed, got {result.exit_code}. Output: {result.output}"
    
    print("PASS: validate-registry command works correctly")


def test_run_mod_missing_file():
    """Test run-mod with missing file."""
    print("\n=== Test: Run-Mod Missing File Error ===")
    
    result = run_datapy_command("run-mod", "test_mod", "--params", "/nonexistent/file.yaml")
    
    # Should fail with appropriate error code
    assert result.exit_code != 0, f"run-mod should fail with missing file"
    assert ("not found" in result.output.lower() or 
            "no such file" in result.output.lower() or
            "does not exist" in result.output.lower()), f"Should mention file not found: {result.output}"
    
    print("PASS: run-mod missing file error handled correctly")


def test_run_mod_invalid_yaml():
    """Test run-mod with invalid YAML."""
    print("\n=== Test: Run-Mod Invalid YAML Error ===")
    
    # Create invalid YAML file
    with NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8') as f:
        f.write("invalid: yaml: content: {")
        invalid_yaml = f.name
    
    try:
        result = run_datapy_command("run-mod", "test_mod", "--params", invalid_yaml)
        
        # Should fail
        assert result.exit_code != 0, f"run-mod should fail with invalid YAML"
        
        print("PASS: run-mod correctly failed with invalid YAML")
        
    finally:
        cleanup_file(invalid_yaml)


def test_run_mod_missing_mod_in_yaml():
    """Test run-mod with valid YAML but missing mod."""
    print("\n=== Test: Run-Mod Missing Mod in YAML ===")
    
    # Create valid YAML without the requested mod
    yaml_content = {
        "globals": {"log_level": "INFO"},
        "mods": {
            "other_mod": {
                "_type": "csv_reader",
                "file_path": "/test.csv"
            }
        }
    }
    
    yaml_file = create_test_yaml(yaml_content)
    
    try:
        result = run_datapy_command("run-mod", "missing_mod", "--params", yaml_file)
        
        # Should fail
        assert result.exit_code != 0, f"run-mod should fail when mod not found in YAML"
        
        print("PASS: run-mod correctly failed when mod not found in YAML")
        
    finally:
        cleanup_file(yaml_file)


def test_invalid_command():
    """Test invalid DataPy command."""
    print("\n=== Test: Invalid Command ===")
    
    result = run_datapy_command("invalid-command")
    
    # Should fail with usage info
    assert result.exit_code != 0, f"Invalid command should fail"
    
    print("PASS: Invalid command handled correctly")


def test_log_level_option():
    """Test --log-level option."""
    print("\n=== Test: Log Level Option ===")
    
    # Test with DEBUG log level
    result = run_datapy_command("--log-level", "DEBUG", "list-registry")
    
    # Should succeed (log level is just a modifier)
    assert result.exit_code == 0, f"Command with log level should succeed, got {result.exit_code}"
    
    print("PASS: Log level option works")


def main():
    """Run all CLI system tests."""
    print("Starting CLI System Tests...")
    print("=" * 50)
    
    try:
        test_cli_help()
        test_list_registry()
        test_validate_registry()
        test_run_mod_missing_file()
        test_run_mod_invalid_yaml()
        test_run_mod_missing_mod_in_yaml()
        test_invalid_command()
        test_log_level_option()
        
        print("\n" + "=" * 50)
        print("ALL CLI SYSTEM TESTS PASSED")
        return True
        
    except Exception as e:
        print(f"\nFAIL: Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)