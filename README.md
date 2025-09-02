# DataPy ETL Framework

A Python framework for creating reusable ETL components with unified parameter management, logging, and result handling for workflows that can be invoked from both CLI and Python scripts.

## Overview

DataPy provides a modular architecture for building ETL pipelines through reusable components called "mods". Each mod is a self-contained unit with standardized interfaces for parameters, execution, and results. The framework supports both command-line and programmatic execution with comprehensive logging and error handling.

## Features

- **Registry-based Mod Management**: Centralized discovery and execution of ETL components
- **Dual Execution Models**: Both CLI and Python SDK interfaces
- **Parameter Resolution Chain**: Project defaults, mod defaults, and job-specific parameters
- **Context Variable Substitution**: Dynamic parameter replacement using JSON context files
- **Structured JSON Logging**: Comprehensive execution tracking and debugging
- **Standardized Result Format**: Consistent success/warning/error handling across all components
- **Extensible Architecture**: Easy creation of custom mods with metadata validation

## Installation

### Prerequisites

- Python 3.12 or higher
- pip package manager

### Install from Source

```bash
git clone <repository-url>
cd datapy
pip install -e .
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

## Quick Start

### 1. Register a Mod

```bash
datapy register-mod datapy.mods.sources.csv_reader
```

### 2. Create a Job Configuration

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

### 3. Execute via CLI

```bash
datapy run-mod extract_customers --params my_job.yaml
```

### 4. Execute via Python SDK

```python
from datapy.mod_manager.sdk import run_mod

result = run_mod("csv_reader", {
    "file_path": "data/customers.csv"
}, "extract_customers")

print(f"Status: {result['status']}")
print(f"Rows processed: {result['metrics']['rows_read']}")
```

## Architecture

### Core Components

#### Mod Manager
The central orchestration engine that handles mod discovery, parameter resolution, and execution coordination.

#### Registry System
Centralized catalog of available mods with metadata, parameter schemas, and execution information.

#### Parameter Resolution
Multi-layer parameter system supporting:
- Registry-defined mod defaults
- Project-level configuration
- Job-specific parameters
- Context variable substitution

#### Result System
Standardized output format providing:
- Execution status (success/warning/error)
- Performance metrics
- Data artifacts
- Global variables for mod chaining
- Structured error reporting

## Command Line Interface

### Mod Execution

```bash
# Execute a mod from YAML configuration
datapy run-mod <mod_name> --params <yaml_file>

# With context variable substitution
datapy run-mod extract_data --params job.yaml --context prod_context.json
```

### Registry Management

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

## Python SDK

### Basic Usage

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

### Context Management

```python
from datapy.mod_manager.sdk import set_context

# Set context for variable substitution
set_context("environments/production.json")

# Parameters with ${} variables will be substituted
result = run_mod("csv_reader", {
    "file_path": "${data.input_path}/customers.csv"
})
```

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

## Creating Custom Mods

### 1. Mod Structure

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

### 2. Mod Categories

- **source**: Data extraction mods (databases, files, APIs)
- **transformer**: Data processing and transformation mods  
- **sink**: Data output mods (files, databases, services)
- **solo**: Standalone utility mods

### 3. Parameter Types

Supported parameter types in CONFIG_SCHEMA:

- `str`: String values
- `int`: Integer values  
- `float`: Decimal values
- `bool`: Boolean true/false
- `list`: Array of values
- `dict`: Nested object structures

### 4. Result Guidelines

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

### 5. Registration

```bash
# Register your mod
datapy register-mod my_package.mods.my_custom_processor

# Verify registration
datapy mod-info my_custom_processor
```

## Advanced Usage

### Multi-Mod Workflows

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

### Python Script Integration

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

Execute with:

```bash
python pipeline.py
```

## Error Handling

The framework provides standardized error codes:

- **0**: Success
- **10**: Success with warnings
- **20**: Validation error (invalid parameters, missing files)
- **30**: Runtime error (execution failure)

### CLI Error Handling

```bash
# Exit on error (default)
datapy run-mod extract_data --params config.yaml

# Continue on error
datapy run-mod extract_data --params config.yaml --no-exit-on-error
```

### SDK Error Handling

```python
result = run_mod("csv_reader", {"file_path": "missing.csv"})

if result['status'] == 'error':
    for error in result['errors']:
        print(f"Error: {error['message']}")
        print(f"Code: {error['error_code']}")
```

## Logging

The framework uses structured JSON logging to stderr:

```json
{
  "timestamp": "2024-08-27T22:39:17.011215Z",
  "level": "INFO", 
  "logger": "datapy.mod_manager.sdk",
  "message": "Starting mod execution: csv_reader_20240827_143917_123",
  "mod_type": "csv_reader",
  "mod_name": "csv_reader_20240827_143917_123"
}
```

### Log Levels

```bash
# Set via CLI
datapy run-mod extract_data --params config.yaml --log-level DEBUG

# Set via SDK  
from datapy.mod_manager.sdk import set_log_level
set_log_level("DEBUG")
```

## Best Practices

### Mod Development

1. **Always validate inputs** in your mod's run() function
2. **Use meaningful parameter names** and descriptions
3. **Add comprehensive error handling** for expected failure modes
4. **Provide useful metrics** for monitoring and debugging
5. **Document expected input/output formats** in mod metadata
6. **Test with various parameter combinations** before registration

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

### Parameter Management

1. **Use project defaults** for common settings across mods
2. **Leverage context files** for environment-specific values
3. **Keep job YAML files focused** on job-specific parameters
4. **Document parameter dependencies** between related mods

## Troubleshooting

### Common Issues

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

### Debug Mode

```bash
# Enable debug logging
datapy run-mod extract_data --params config.yaml --log-level DEBUG

# Validate registry
datapy validate-registry
```

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