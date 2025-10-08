"""
Test cases for datapy.mod_manager.scaffold_cli module.

Tests job scaffolding CLI commands including create-job functionality,
file generation, validation, and error handling.
"""

import sys
import json
from pathlib import Path
from unittest.mock import patch

# Add project root to Python path for testing
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pytest
from click.testing import CliRunner

from datapy.mod_manager.scaffold_cli import (
    create_job_command,
    _validate_job_name,
    _create_job_directory,
    _create_pipeline_file,
    _create_context_file,
    PIPELINE_TEMPLATE,
    CONTEXT_TEMPLATE
)
from datapy.mod_manager.result import VALIDATION_ERROR, RUNTIME_ERROR, SUCCESS


class TestValidateJobName:
    """Test cases for _validate_job_name function."""
    
    def test_valid_job_names(self):
        """Test validation with valid job names."""
        valid_names = [
            "customer_etl",
            "sales_report",
            "data_cleanup_v2",
            "process123",
            "_private_job",
            "Job_With_Caps"
        ]
        
        for name in valid_names:
            result = _validate_job_name(name)
            assert result == name
    
    def test_invalid_job_name_with_hyphen(self):
        """Test validation rejects hyphens."""
        with pytest.raises(SystemExit) as exc_info:
            _validate_job_name("data-cleanup")
        assert exc_info.value.code == VALIDATION_ERROR
    
    def test_invalid_job_name_with_space(self):
        """Test validation rejects spaces."""
        with pytest.raises(SystemExit) as exc_info:
            _validate_job_name("customer etl")
        assert exc_info.value.code == VALIDATION_ERROR
    
    def test_invalid_job_name_starts_with_number(self):
        """Test validation rejects names starting with numbers."""
        with pytest.raises(SystemExit) as exc_info:
            _validate_job_name("123_job")
        assert exc_info.value.code == VALIDATION_ERROR
    
    def test_invalid_job_name_empty_string(self):
        """Test validation rejects empty strings."""
        with pytest.raises(SystemExit) as exc_info:
            _validate_job_name("")
        assert exc_info.value.code == VALIDATION_ERROR
    
    def test_invalid_job_name_none(self):
        """Test validation rejects None."""
        with pytest.raises(SystemExit) as exc_info:
            _validate_job_name(None)
        assert exc_info.value.code == VALIDATION_ERROR
    
    def test_invalid_job_name_whitespace_only(self):
        """Test validation rejects whitespace-only strings."""
        with pytest.raises(SystemExit) as exc_info:
            _validate_job_name("   ")
        assert exc_info.value.code == VALIDATION_ERROR
    
    def test_valid_job_name_with_leading_trailing_spaces(self):
        """Test validation strips leading/trailing spaces."""
        result = _validate_job_name("  job_name  ")
        assert result == "job_name"
    
    def test_invalid_job_name_special_characters(self):
        """Test validation rejects special characters."""
        invalid_names = ["job@name", "job#name", "job$name", "job%name"]
        
        for name in invalid_names:
            with pytest.raises(SystemExit) as exc_info:
                _validate_job_name(name)
            assert exc_info.value.code == VALIDATION_ERROR


class TestCreateJobDirectory:
    """Test cases for _create_job_directory function."""
    
    def test_create_job_directory_success(self, tmp_path):
        """Test successful job directory creation."""
        job_path = _create_job_directory("test_job", False, str(tmp_path))
        
        assert job_path.exists()
        assert job_path.is_dir()
        assert job_path.name == "test_job"
    
    def test_create_job_directory_in_current_dir(self, tmp_path, monkeypatch):
        """Test creating job directory in current directory."""
        monkeypatch.chdir(tmp_path)
        
        job_path = _create_job_directory("current_job", False, None)
        
        assert job_path.exists()
        assert job_path.parent == tmp_path
    
    def test_create_job_directory_already_exists_no_force(self, tmp_path):
        """Test creating job when directory exists without force flag."""
        existing_dir = tmp_path / "existing_job"
        existing_dir.mkdir()
        
        with pytest.raises(SystemExit) as exc_info:
            _create_job_directory("existing_job", False, str(tmp_path))
        assert exc_info.value.code == VALIDATION_ERROR
    
    def test_create_job_directory_already_exists_with_force(self, tmp_path):
        """Test creating job when directory exists with force flag."""
        existing_dir = tmp_path / "existing_job"
        existing_dir.mkdir()
        
        job_path = _create_job_directory("existing_job", True, str(tmp_path))
        
        assert job_path.exists()
        assert job_path == existing_dir
    
    def test_create_job_directory_invalid_output_dir(self):
        """Test creating job with non-existent output directory."""
        with pytest.raises(SystemExit) as exc_info:
            _create_job_directory("test_job", False, "/nonexistent/path")
        assert exc_info.value.code == VALIDATION_ERROR
    
    def test_create_job_directory_nested_creation(self, tmp_path):
        """Test creating nested directory structure."""
        output_dir = tmp_path / "projects" / "etl"
        output_dir.mkdir(parents=True)
        
        job_path = _create_job_directory("nested_job", False, str(output_dir))
        
        assert job_path.exists()
        assert job_path == output_dir / "nested_job"


class TestCreatePipelineFile:
    """Test cases for _create_pipeline_file function."""
    
    def test_create_pipeline_file_success(self, tmp_path):
        """Test successful pipeline file creation."""
        job_path = tmp_path / "test_job"
        job_path.mkdir()
        
        _create_pipeline_file(job_path, "test_job")
        
        pipeline_file = job_path / "test_job_pipeline.py"
        assert pipeline_file.exists()
        
        content = pipeline_file.read_text(encoding='utf-8')
        assert "Pipeline Name: test_job" in content
        assert "from datapy.mod_manager.sdk import" in content
        assert "from datapy.utils.script_monitor import monitor_execution" in content
        assert "def pre_run():" in content
        assert "def run_pipeline(logger):" in content
        assert "def post_run(logger, result):" in content
        assert '@monitor_execution("test_job")' in content
        assert "def main():" in content
    
    def test_create_pipeline_file_with_different_job_names(self, tmp_path):
        """Test pipeline file creation with various job names."""
        job_names = ["customer_etl", "sales_report_v2", "data_cleanup"]
        
        for job_name in job_names:
            job_path = tmp_path / job_name
            job_path.mkdir()
            
            _create_pipeline_file(job_path, job_name)
            
            pipeline_file = job_path / f"{job_name}_pipeline.py"
            assert pipeline_file.exists()
            
            content = pipeline_file.read_text(encoding='utf-8')
            assert f"Pipeline Name: {job_name}" in content
            assert f'setup_logging("INFO", "{job_name}_pipeline.py")' in content
            assert f'setup_context("{job_name}_context.json")' in content
            assert f'@monitor_execution("{job_name}")' in content
    
    def test_create_pipeline_file_content_structure(self, tmp_path):
        """Test pipeline file has correct structure and components."""
        job_path = tmp_path / "structure_test"
        job_path.mkdir()
        
        _create_pipeline_file(job_path, "structure_test")
        
        pipeline_file = job_path / "structure_test_pipeline.py"
        content = pipeline_file.read_text(encoding='utf-8')
        
        # Check key components exist in order
        assert content.index('"""') < content.index('from datapy')
        assert content.index('def pre_run():') < content.index('def run_pipeline(')
        assert content.index('def run_pipeline(') < content.index('def post_run(')
        assert content.index('def post_run(') < content.index('@monitor_execution')
        assert content.index('@monitor_execution') < content.index('def main():')
        assert content.index('def main():') < content.index('if __name__')


class TestCreateContextFile:
    """Test cases for _create_context_file function."""
    
    def test_create_context_file_success(self, tmp_path):
        """Test successful context file creation."""
        job_path = tmp_path / "test_job"
        job_path.mkdir()
        
        _create_context_file(job_path, "test_job")
        
        context_file = job_path / "test_job_context.json"
        assert context_file.exists()
        
        content = context_file.read_text(encoding='utf-8')
        context_data = json.loads(content)
        
        assert "_comment" in context_data
        assert "test_job" in context_data["_comment"]
        assert context_data["pipeline"]["name"] == "test_job"
        assert "data" in context_data
        assert context_data["data"]["input_path"] == "./input"
        assert context_data["data"]["output_path"] == "./output"
    
    def test_create_context_file_json_format(self, tmp_path):
        """Test context file is valid JSON with proper formatting."""
        job_path = tmp_path / "format_test"
        job_path.mkdir()
        
        _create_context_file(job_path, "format_test")
        
        context_file = job_path / "format_test_context.json"
        content = context_file.read_text(encoding='utf-8')
        
        # Should be parseable JSON
        context_data = json.loads(content)
        assert isinstance(context_data, dict)
        
        # Check formatting (indented)
        assert "  " in content  # Should have indentation
    
    def test_create_context_file_with_different_job_names(self, tmp_path):
        """Test context file creation with various job names."""
        job_names = ["customer_etl", "sales_report", "data_process"]
        
        for job_name in job_names:
            job_path = tmp_path / job_name
            job_path.mkdir()
            
            _create_context_file(job_path, job_name)
            
            context_file = job_path / f"{job_name}_context.json"
            assert context_file.exists()
            
            context_data = json.loads(context_file.read_text(encoding='utf-8'))
            assert context_data["pipeline"]["name"] == job_name


class TestCreateJobCommand:
    """Test cases for create-job CLI command."""
    
    def test_create_job_command_success(self, tmp_path, monkeypatch):
        """Test successful job creation via CLI."""
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        
        result = runner.invoke(create_job_command, ['test_job'])
        
        assert result.exit_code == SUCCESS
        assert "Successfully created job 'test_job'" in result.output
        
        # Check files were created
        job_dir = tmp_path / "test_job"
        assert job_dir.exists()
        assert (job_dir / "test_job_pipeline.py").exists()
        assert (job_dir / "test_job_context.json").exists()
    
    def test_create_job_command_with_output_dir(self, tmp_path):
        """Test job creation with custom output directory."""
        output_dir = tmp_path / "jobs"
        output_dir.mkdir()
        
        runner = CliRunner()
        result = runner.invoke(create_job_command, [
            'custom_job',
            '--output-dir', str(output_dir)
        ])
        
        assert result.exit_code == SUCCESS
        
        job_dir = output_dir / "custom_job"
        assert job_dir.exists()
        assert (job_dir / "custom_job_pipeline.py").exists()
        assert (job_dir / "custom_job_context.json").exists()
    
    def test_create_job_command_already_exists_no_force(self, tmp_path, monkeypatch):
        """Test job creation fails when directory exists without force."""
        monkeypatch.chdir(tmp_path)
        existing_dir = tmp_path / "existing_job"
        existing_dir.mkdir()
        
        runner = CliRunner()
        result = runner.invoke(create_job_command, ['existing_job'])
        
        assert result.exit_code == VALIDATION_ERROR
        assert "already exists" in result.output
    
    def test_create_job_command_already_exists_with_force(self, tmp_path, monkeypatch):
        """Test job creation succeeds when directory exists with force flag."""
        monkeypatch.chdir(tmp_path)
        existing_dir = tmp_path / "existing_job"
        existing_dir.mkdir()
        
        runner = CliRunner()
        result = runner.invoke(create_job_command, ['existing_job', '--force'])
        
        assert result.exit_code == SUCCESS
        assert "Successfully created job" in result.output
    
    def test_create_job_command_invalid_job_name(self):
        """Test job creation fails with invalid job name."""
        runner = CliRunner()
        
        # Test with hyphen
        result = runner.invoke(create_job_command, ['invalid-name'])
        assert result.exit_code == VALIDATION_ERROR
        
        # Test with space
        result = runner.invoke(create_job_command, ['invalid name'])
        assert result.exit_code == VALIDATION_ERROR
        
        # Test starting with number
        result = runner.invoke(create_job_command, ['123invalid'])
        assert result.exit_code == VALIDATION_ERROR
    
    def test_create_job_command_output_messages(self, tmp_path, monkeypatch):
        """Test CLI output messages are correct."""
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        
        result = runner.invoke(create_job_command, ['message_test'])
        
        assert "Creating job: message_test" in result.output
        assert "Successfully created job 'message_test'" in result.output
        assert "Job structure:" in result.output
        assert "message_test_pipeline.py" in result.output
        assert "message_test_context.json" in result.output
        assert "Next steps:" in result.output
    
    def test_create_job_command_help(self):
        """Test create-job help output."""
        runner = CliRunner()
        result = runner.invoke(create_job_command, ['--help'])
        
        assert result.exit_code == 0
        assert "create-job" in result.output
        assert "JOB_NAME" in result.output
        assert "--force" in result.output
        assert "--output-dir" in result.output


class TestTemplates:
    """Test cases for template content validation."""
    
    def test_pipeline_template_has_required_components(self):
        """Test pipeline template contains all required components."""
        assert "{job_name}" in PIPELINE_TEMPLATE
        assert "from datapy.mod_manager.sdk import" in PIPELINE_TEMPLATE
        assert "from datapy.utils.script_monitor import monitor_execution" in PIPELINE_TEMPLATE
        assert "def pre_run():" in PIPELINE_TEMPLATE
        assert "def run_pipeline(logger):" in PIPELINE_TEMPLATE
        assert "def post_run(logger, result):" in PIPELINE_TEMPLATE
        assert "@monitor_execution" in PIPELINE_TEMPLATE
        assert "def main():" in PIPELINE_TEMPLATE
        assert 'if __name__ == "__main__":' in PIPELINE_TEMPLATE
    
    def test_pipeline_template_formatting(self):
        """Test pipeline template can be formatted with job name."""
        formatted = PIPELINE_TEMPLATE.format(job_name="test_job")
        
        assert "Pipeline Name: test_job" in formatted
        assert 'setup_logging("INFO", "test_job_pipeline.py")' in formatted
        assert 'setup_context("test_job_context.json")' in formatted
        assert '@monitor_execution("test_job")' in formatted
    
    def test_context_template_structure(self):
        """Test context template has correct structure."""
        assert "_comment" in CONTEXT_TEMPLATE
        assert "{job_name}" in CONTEXT_TEMPLATE["_comment"]
        assert "pipeline" in CONTEXT_TEMPLATE
        assert "name" in CONTEXT_TEMPLATE["pipeline"]
        assert "data" in CONTEXT_TEMPLATE
        assert "input_path" in CONTEXT_TEMPLATE["data"]
        assert "output_path" in CONTEXT_TEMPLATE["data"]
    
    def test_context_template_values(self):
        """Test context template has correct default values."""
        assert CONTEXT_TEMPLATE["data"]["input_path"] == "./input"
        assert CONTEXT_TEMPLATE["data"]["output_path"] == "./output"
        assert CONTEXT_TEMPLATE["pipeline"]["name"] == "{job_name}"


class TestIntegrationScenarios:
    """Integration test cases for complete job creation workflows."""
    
    def test_complete_job_creation_workflow(self, tmp_path, monkeypatch):
        """Test complete job creation from start to finish."""
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        
        # Create job
        result = runner.invoke(create_job_command, ['integration_test'])
        assert result.exit_code == SUCCESS
        
        # Verify directory structure
        job_dir = tmp_path / "integration_test"
        assert job_dir.exists()
        assert job_dir.is_dir()
        
        # Verify pipeline file
        pipeline_file = job_dir / "integration_test_pipeline.py"
        assert pipeline_file.exists()
        pipeline_content = pipeline_file.read_text(encoding='utf-8')
        assert "Pipeline Name: integration_test" in pipeline_content
        assert "integration_test_pipeline.py" in pipeline_content
        assert "integration_test_context.json" in pipeline_content
        
        # Verify context file
        context_file = job_dir / "integration_test_context.json"
        assert context_file.exists()
        context_data = json.loads(context_file.read_text(encoding='utf-8'))
        assert context_data["pipeline"]["name"] == "integration_test"
        assert context_data["data"]["input_path"] == "./input"
        assert context_data["data"]["output_path"] == "./output"
        
        # Verify files are valid Python and JSON
        compile(pipeline_content, str(pipeline_file), 'exec')  # Should not raise
        json.loads(context_file.read_text(encoding='utf-8'))  # Should not raise
    
    def test_multiple_jobs_creation(self, tmp_path, monkeypatch):
        """Test creating multiple jobs in same directory."""
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        
        job_names = ["job1", "job2", "job3"]
        
        for job_name in job_names:
            result = runner.invoke(create_job_command, [job_name])
            assert result.exit_code == SUCCESS
        
        # Verify all jobs exist
        for job_name in job_names:
            job_dir = tmp_path / job_name
            assert job_dir.exists()
            assert (job_dir / f"{job_name}_pipeline.py").exists()
            assert (job_dir / f"{job_name}_context.json").exists()
    
    def test_job_creation_with_nested_output_directory(self, tmp_path):
        """Test creating job in nested output directory."""
        nested_dir = tmp_path / "projects" / "etl" / "jobs"
        nested_dir.mkdir(parents=True)
        
        runner = CliRunner()
        result = runner.invoke(create_job_command, [
            'nested_job',
            '--output-dir', str(nested_dir)
        ])
        
        assert result.exit_code == SUCCESS
        
        job_dir = nested_dir / "nested_job"
        assert job_dir.exists()
        assert (job_dir / "nested_job_pipeline.py").exists()
        assert (job_dir / "nested_job_context.json").exists()


class TestErrorHandling:
    """Test cases for error handling scenarios."""
    
    def test_create_job_with_permission_error(self, tmp_path):
        """Test handling of permission errors during job creation."""
        runner = CliRunner()
        
        with patch('pathlib.Path.mkdir', side_effect=PermissionError("Access denied")):
            result = runner.invoke(create_job_command, [
                'permission_test',
                '--output-dir', str(tmp_path)
            ])
            
            assert result.exit_code == RUNTIME_ERROR
            assert "Failed to create job directory" in result.output
    
    def test_create_pipeline_file_write_error(self, tmp_path):
        """Test handling of file write errors."""
        job_path = tmp_path / "write_error_test"
        job_path.mkdir()
        
        with patch('pathlib.Path.write_text', side_effect=IOError("Write failed")):
            with pytest.raises(SystemExit) as exc_info:
                _create_pipeline_file(job_path, "write_error_test")
            assert exc_info.value.code == RUNTIME_ERROR
    
    def test_create_context_file_json_error(self, tmp_path):
        """Test handling of JSON serialization errors."""
        job_path = tmp_path / "json_error_test"
        job_path.mkdir()
        
        with patch('json.dumps', side_effect=TypeError("Cannot serialize")):
            with pytest.raises(SystemExit) as exc_info:
                _create_context_file(job_path, "json_error_test")
            assert exc_info.value.code == RUNTIME_ERROR