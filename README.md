# DataPy ETL Framework

A Python framework for creating reusable ETL components with unified parameter management, logging, and result handling for workflows that can be invoked from both CLI and Python scripts.

## Table of Contents

- [Introduction](#introduction)
- [Getting Started](#getting-started)
- [Core Concepts](#core-concepts)
- [Usage Guide](#usage-guide)
- [Configuration](#configuration)
- [Development](#development)
- [Reference](#reference)
- [Contributing](#contributing)

---

## Introduction

### Overview

DataPy provides a modular architecture for building ETL pipelines through reusable components called "mods". Each mod is a self-contained unit with standardized interfaces for parameters, execution, and results. The framework supports both command-line and programmatic execution with comprehensive logging and error handling.

### Features

- **Registry-based Mod Management**: Centralized discovery and execution of ETL components
- **Dual Execution Models**: Both CLI and Python SDK interfaces
- **Parameter Resolution Chain**: Project defaults, mod defaults, and job-specific parameters
- **Context Variable Substitution**: Dynamic parameter replacement using JSON context files
- **Structured Tab-Delimited Logging**: Comprehensive execution tracking and debugging
- **Standardized Result Format**: Consistent success/warning/error handling across all components
- **Extensible Architecture**: Easy creation of custom mods with metadata validation

---

## Getting Started

### Installation

#### Prerequisites

- Python 3.12 or higher
- pip package manager

#### Install from Source

```bash
git clone <repository-url>
cd datapy
pip install -e .
```

#### Install Dependencies

```bash
pip install -r requirements.txt
```

### Quick Start

#### 1. Register a Mod

```bash
datapy register-mod datapy.mods.sources.csv_reader
```

#### 2. Create a Job Configuration

Create `my_job.yaml`:

```yaml
globals:
  log_level: "INFO"

mods:
  extract_customers:
    _type: csv_reader
    file_path: "data/customers.csv"
    encoding: "utf-8"
```

#### 3. Execute via CLI

```bash
datapy run-mod extract_customers --params my_job.yaml
```

#### 4. Execute via Python SDK

```python
from datapy.mod_manager.sdk import run_mod

result = run_mod("csv_reader", {
    "file_path": "data/customers.csv"
}, "extract_customers")

print(f"Status: {result['status']}")
print(f"Rows processed: {result['metrics']['rows_read']}")
```

---

## Core Concepts

### Architecture

#### Core Components

- **Mod Manager**: Central orchestration engine that handles mod discovery, parameter resolution, and execution coordination
- **Registry System**: Centralized catalog of available mods with metadata, parameter schemas, and execution information
- **Parameter Resolution**: Multi-layer parameter system supporting registry defaults, project configuration, job parameters, and context variable substitution
- **Result System**: Standardized output format providing execution status, performance metrics, data artifacts, and structured error reporting

### Generic Output Format

All DataPy mods return a standardized result format regardless of execution method (CLI or SDK). This ensures consistent error handling and result processing across the framework.

#### Success Result Schema

```json
{
  "status": "success",
  "execution_time": 1.234,
  "exit_code": 0,
  "metrics": {
    "rows_processed": 1000,
    "file_size_bytes": 52341,
    "processing_rate": 0.95
  },
  "artifacts": {
    "data": "<DataFrame>",
    "output_path": "/path/to/output.csv"
  },
  "globals": {
    "row_count": 1000,
    "last_processed": "2024-08-28"
  },
  "warnings": [],
  "errors": [],
  "logs": {
    "run_id": "csv_reader_20240828_143917_abc123",
    "mod_type": "csv_reader",
    "mod_name": "extract_customers"
  }
}
```

#### Warning Result Schema

```json
{
  "status": "warning",
  "execution_time": 2.156,
  "exit_code": 10,
  "metrics": {
    "rows_processed": 950,
    "rows_skipped": 50
  },
  "artifacts": {
    "data": "<DataFrame>"
  },
  "globals": {
    "row_count": 950
  },
  "warnings": [
    {
      "message": "Found 50 rows with missing values",
      "warning_code": 10,
      "timestamp": 1640995200.123
    }
  ],
  "errors": [],
  "logs": {
    "run_id": "csv_filter_20240828_143917_def456",
    "mod_type": "csv_filter", 
    "mod_name": "filter_customers"
  }
}
```

#### Error Result Schema

```json
{
  "status": "error",
  "execution_time": 0.045,
  "exit_code": 30,
  "metrics": {},
  "artifacts": {},
  "globals": {},
  "warnings": [],
  "errors": [
    {
      "message": "File not found: /missing/data.csv",
      "error_code": 30,
      "timestamp": 1640995200.456
    }
  ],
  "logs": {
    "run_id": "csv_reader_20240828_143917_ghi789",
    "mod_type": "csv_reader",
    "mod_name": "extract_missing"
  }
}
```

#### Field Descriptions

- **status**: Result status (`"success"`, `"warning"`, `"error"`)
- **execution_time**: Total execution time in seconds (float)
- **exit_code**: Process exit code (0=success, 10=warning, 20=validation error, 30=runtime error)
- **metrics**: Mod-specific performance and processing metrics
- **artifacts**: Output data and objects (DataFrames, file paths, processed results)
- **globals**: Shared variables for cross-mod communication
- **warnings**: Non-fatal issues with detailed messages and timestamps
- **errors**: Fatal errors with error codes and timestamps
- **logs**: Execution metadata including unique run ID and mod identification

### Logging

The framework uses structured tab-delimited logging to stderr for easy parsing and analysis.

#### Log Format

```
TIMESTAMP \t LEVEL \t LOGGER \t MOD_TYPE \t MOD_NAME \t MESSAGE \t EXTRA_FIELDS
```

#### Example Log Output

```
2024-08-28T14:39:17.011215Z	INFO	datapy.mod_manager.sdk	csv_reader	extract_customers	Starting mod execution	{"param_count": 3}
2024-08-28T14:39:17.245891Z	INFO	datapy.mods.sources.csv_reader	csv_reader	extract_customers	CSV read successful	{"rows": 1000, "columns": 5, "file_size": 52341}
2024-08-28T14:39:17.246123Z	WARNING	datapy.mods.sources.csv_reader	csv_reader	extract_customers	Found empty rows in data	{"empty_row_count": 5}
```

#### Log Fields

- **TIMESTAMP**: ISO 8601 timestamp with Z suffix
- **LEVEL**: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- **LOGGER**: Python logger name (typically module path)
- **MOD_TYPE**: Type of mod being executed (e.g., "csv_reader")
- **MOD_NAME**: Instance name of the mod (e.g., "extract_customers")
- **MESSAGE**: Human-readable log message
- **EXTRA_FIELDS**: JSON object with additional context (file paths, metrics, etc.)

---

## Usage Guide

### How to Use This Framework

DataPy can be used in several ways depending on your needs:

### 1. Command Line Interface (CLI)
**Best for**: Shell scripts, scheduled jobs, CI/CD pipelines

```bash
# Execute individual mods from YAML configuration
datapy run-mod extract_customers --params job_config.yaml --context prod_context.json

# List available mods
datapy list-registry

# Register custom mods
datapy register-mod my_package.mods.custom_processor
```

### 2. Python SDK
**Best for**: Complex workflows, custom applications, interactive notebooks

```python
from datapy.mod_manager.sdk import run_mod, set_context, set_log_level

# Configure framework
set_log_level("INFO")
set_context("environments/production.json")

# Execute mods programmatically
result = run_mod("csv_reader", {
    "file_path": "data/customers.csv",
    "encoding": "utf-8"
}, "extract_customers")

# Chain mods together
if result['status'] == 'success':
    customer_data = result['artifacts']['data']
    
    filter_result = run_mod("csv_filter", {
        "data": customer_data,
        "filter_conditions": {"age": {"gte": 25}}
    }, "filter_adults")
```

### 3. Standalone Python Scripts
**Best for**: Project-specific pipelines, custom data processing

```python
#!/usr/bin/env python3
# Complete ETL pipeline script
from datapy.mod_manager.sdk import run_mod, set_context

def main():
    # Setup
    set_context("config/production.json")
    
    # Execute pipeline
    extract_result = run_mod("csv_reader", {"file_path": "${data.input_path}/customers.csv"})
    filter_result = run_mod("csv_filter", {"data": extract_result['artifacts']['data']})
    write_result = run_mod("csv_writer", {"data": filter_result['artifacts']['filtered_data']})
    
    print(f"Pipeline completed: {write_result['status']}")

if __name__ == "__main__":
    main()
```

### 4. YAML-Driven Workflows
**Best for**: Configuration-driven pipelines, environment promotion

```yaml
# job_config.yaml
mods:
  extract_customers:
    _type: csv_reader
    file_path: "${env.data_path}/customers.csv"
    encoding: "utf-8"
  
  filter_adults:
    _type: csv_filter
    filter_conditions:
      age: {gte: 25}
    keep_columns: ["name", "age", "city"]
```

### SDK Usage Patterns

#### Basic Usage

```python
from datapy.mod_manager.sdk import run_mod, set_log_level

# Configure logging
set_log_level("INFO")

# Execute mod with auto-generated name
result = run_mod("csv_reader", {
    "file_path": "customers.csv"
})

# Execute mod with explicit name
result = run_mod("csv_reader", {
    "file_path": "customers.csv",
    "encoding": "utf-8"
}, "customer_extractor")

# Access results
if result['status'] == 'success':
    data = result['artifacts']['data']  # DataFrame
    row_count = result['globals']['row_count']
    print(f"Processed {row_count} rows successfully")
```

#### Context Management

```python
from datapy.mod_manager.sdk import set_context

# Set context for variable substitution
set_context("environments/production.json")

# Parameters with ${} variables will be substituted
result = run_mod("csv_reader", {
    "file_path": "${data.input_path}/customers.csv"
})
```

#### Log Level Management

```bash
# Set via CLI
datapy run-mod extract_data --params config.yaml --log-level DEBUG

# Set via SDK  
from datapy.mod_manager.sdk import set_log_level
set_log_level("DEBUG")
```

---

## Configuration

### Project Configuration

Create `project_defaults.yaml` in your project root:

```yaml
project_name: "My ETL Project"
project_version: "1.0.0"

globals:
  log_level: "INFO"
  base_path: "/data"
  default_encoding: "utf-8"

mod_defaults:
  csv_reader:
    delimiter: ","
    header_row: 0
  
  data_cleaner:
    remove_duplicates: true
    fill_missing: false
```

### Context Files

Create JSON files for environment-specific variables:

```json
{
  "env": {
    "name": "production",
    "data_path": "/prod/data"
  },
  "database": {
    "host": "prod-db.example.com",
    "port": 5432
  }
}
```

Use in YAML with `${}` syntax:

```yaml
mods:
  extract_data:
    _type: csv_reader
    file_path: "${env.data_path}/customers.csv"
    connection_string: "postgresql://${database.host}:${database.port}/mydb"
```

### Parameter Management

#### Parameter Resolution Chain
1. Registry-defined mod defaults
2. Project-level configuration
3. Job-specific parameters
4. Context variable substitution

#### Best Practices
1. **Use project defaults** for common settings across mods
2. **Leverage context files** for environment-specific values
3. **Keep job YAML files focused** on job-specific parameters
4. **Document parameter dependencies** between related mods

### Project Organization

```
my_etl_project/
├── project_defaults.yaml      # Project configuration
├── jobs/                      # Job definitions
│   ├── daily_extract.yaml
│   └── monthly_report.yaml
├── contexts/                  # Environment contexts
│   ├── development.json
│   └── production.json
├── scripts/                   # Custom pipeline scripts
│   └── complex_workflow.py
└── data/                      # Data files
    ├── input/
    └── output/
```

---

## Development

### Creating Custom Mods

#### 1. Mod Structure

Every mod must implement three components:

```python
# my_custom_mod.py
from datapy.mod_manager.result import ModResult
from datapy.mod_manager.base import ModMetadata, ConfigSchema

# Required metadata
METADATA = ModMetadata(
    type="my_custom_processor",
    version="1.0.0",
    description="Custom data processing mod",
    category="transformer",
    input_ports=["data"],
    output_ports=["processed_data"],
    globals=["record_count", "process_time"],
    packages=["pandas>=1.5.0"]
)

# Parameter schema
CONFIG_SCHEMA = ConfigSchema(
    required={
        "input_column": {
            "type": "str",
            "description": "Column name to process"
        }
    },
    optional={
        "method": {
            "type": "str", 
            "default": "standard",
            "description": "Processing method",
            "enum": ["standard", "advanced"]
        }
    }
)

# Execution function
def run(params):
    """Execute the mod with given parameters."""
    # Extract mod context
    mod_name = params.get("_mod_name", "my_custom_processor")
    result = ModResult("my_custom_processor", mod_name)
    
    try:
        # Get parameters
        input_column = params["input_column"]
        method = params.get("method", "standard")
        
        # Your processing logic here
        processed_data = your_processing_function(input_column, method)
        
        # Add metrics
        result.add_metric("records_processed", len(processed_data))
        result.add_metric("processing_method", method)
        
        # Add artifacts
        result.add_artifact("processed_data", processed_data)
        
        # Add globals for downstream mods
        result.add_global("record_count", len(processed_data))
        
        return result.success()
        
    except Exception as e:
        result.add_error(f"Processing failed: {str(e)}")
        return result.error()
```

#### 2. Mod Categories

- **source**: Data extraction mods (databases, files, APIs)
- **transformer**: Data processing and transformation mods  
- **sink**: Data output mods (files, databases, services)
- **solo**: Standalone utility mods

#### 3. Parameter Types

Supported parameter types in CONFIG_SCHEMA:

- `str`: String values
- `int`: Integer values  
- `float`: Decimal values
- `bool`: Boolean true/false
- `list`: Array of values
- `dict`: Nested object structures

#### 4. Result Guidelines

Always use ModResult for consistent output:

```python
# Success with data
result.add_artifact("data", dataframe)
result.add_metric("rows_processed", row_count)
return result.success()

# Warning with issues
result.add_warning("Found duplicate records")
return result.warning()

# Error with failure
result.add_error("Invalid file format")
return result.error()
```

#### 5. Registration

```bash
# Register your mod
datapy register-mod my_package.mods.my_custom_processor

# Verify registration
datapy mod-info my_custom_processor
```

### Best Practices

#### Mod Development

1. **Always validate inputs** in your mod's run() function
2. **Use meaningful parameter names** and descriptions
3. **Add comprehensive error handling** for expected failure modes
4. **Provide useful metrics** for monitoring and debugging
5. **Document expected input/output formats** in mod metadata
6. **Test with various parameter combinations** before registration

#### Advanced Examples

##### Multi-Mod Workflows

```yaml
# complex_workflow.yaml
globals:
  log_level: "INFO"
  project_name: "Data Pipeline"

mods:
  extract_customers:
    _type: csv_reader
    file_path: "raw/customers.csv"
  
  extract_orders:
    _type: csv_reader  
    file_path: "raw/orders.csv"
    
  process_data:
    _type: data_transformer
    join_keys: ["customer_id"]
    output_format: "parquet"
```

##### Python Script Integration

```python
# pipeline.py
import yaml
from datapy.mod_manager.sdk import run_mod, set_context

def main():
    # Load configuration
    with open('workflow_config.yaml') as f:
        config = yaml.safe_load(f)
    
    # Set environment context
    set_context("config/production.json")
    
    # Execute mods programmatically
    customers = run_mod("csv_reader", config['mods']['extract_customers'])
    
    if customers['status'] == 'success':
        # Process the extracted data
        customer_data = customers['artifacts']['data']
        # Your custom processing logic
        
if __name__ == "__main__":
    main()
```

---

## Reference

### Command Line Interface

#### Mod Execution

```bash
# Execute a mod from YAML configuration
datapy run-mod <mod_name> --params <yaml_file>

# With context variable substitution
datapy run-mod extract_data --params job.yaml --context prod_context.json
```

#### Registry Management

```bash
# List available mods
datapy list-registry

# Filter by category
datapy list-registry --category sources

# Get detailed mod information
datapy mod-info csv_reader

# Register new mod
datapy register-mod my_package.mods.custom_processor

# Validate all registered mods
datapy validate-registry

# Remove mod from registry
datapy delete-mod old_processor
```

### Error Handling

The framework provides standardized error codes:

- **0**: Success
- **10**: Success with warnings
- **20**: Validation error (invalid parameters, missing files)
- **30**: Runtime error (execution failure)

#### CLI Error Handling

```bash
# Exit on error (default)
datapy run-mod extract_data --params config.yaml

# Continue on error
datapy run-mod extract_data --params config.yaml --no-exit-on-error
```

#### SDK Error Handling

```python
result = run_mod("csv_reader", {"file_path": "missing.csv"})

if result['status'] == 'error':
    for error in result['errors']:
        print(f"Error: {error['message']}")
        print(f"Code: {error['error_code']}")
```

### Troubleshooting

#### Common Issues

**Mod not found in registry**
```bash
# Check available mods
datapy list-registry

# Register the mod
datapy register-mod your_module.path.mod_name
```

**Parameter validation errors**
```bash
# Check mod parameter requirements
datapy mod-info mod_name

# Validate your YAML syntax
python -c "import yaml; yaml.safe_load(open('config.yaml'))"
```

**Context substitution failures**
- Verify JSON context file syntax
- Check variable path exists: `${level1.level2.key}`
- Ensure context file is accessible

**Import errors**
- Verify mod module is in Python path
- Check all required dependencies are installed
- Validate mod implements required components (METADATA, CONFIG_SCHEMA, run)

#### Debug Mode

```bash
# Enable debug logging
datapy run-mod extract_data --params config.yaml --log-level DEBUG

# Validate registry
datapy validate-registry
```

---

## Contributing

### Development Setup

```bash
# Clone repository
git clone <repository-url>
cd datapy

# Install in development mode
pip install -e .

# Install development dependencies  
pip install -r requirements.txt
pip install pytest pytest-cov
```

### Testing

```bash
# Run framework tests
python -m pytest tests/

# Run integration tests
python testScripts/tester.py
```


## Known Limitations & Future Enhancements

### Resource Management
DataPy currently does not implement resource management controls. Future versions may include:

- **Timeout Handling**: Automatic termination of long-running mods
- **Memory Monitoring**: Memory usage limits and peak memory tracking  
- **Resource Cleanup**: Automatic cleanup of temporary files and resources on failure

For production environments requiring strict resource controls, consider implementing these at the infrastructure level (e.g., containerized deployments with resource limits).