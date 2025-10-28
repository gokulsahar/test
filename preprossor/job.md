# TASK: Convert Talend Job to DataPy Framework Python Pipeline

## INPUT FILES PROVIDED
- `.items` file: Contains job structure and component configurations
- `.properties` file: Contains job metadata and property definitions

## CONVERSION REQUIREMENTS

### 1. UNDERSTAND THE JOB
- Read and analyze both `.items` and `.properties` files thoroughly
- Extract the business logic, data flow, and transformations
- Identify all components, connections, and their configurations
- Document the overall job purpose and data processing steps

### 2. REVIEW EXISTING FRAMEWORK CODE
- Check `/mods` folder for existing reusable components (connectors, framework features)
- Check `/utils` folder for existing helper functions (JSON readers, simple utilities)
- Review `/projects` folder to understand existing pipeline structures and patterns
- Follow the same coding patterns and conventions used in existing jobs

### 3. IMPLEMENTATION GUIDELINES

**Core Requirements:**
- Create ONE main Python pipeline file following DataPy framework conventions
- Use DuckDB (https://duckdb.org/docs/stable/clients/python/relational_api) for ALL data transformations
- Reuse existing mods and utils wherever possible
- Create new utils ONLY for reusable simple functions
- Create new mods ONLY when absolutely necessary for framework-level components requiring context substitution, logging, or built-in features
- Create a context file (`.json`) for all configuration values
- Ensure zero SonarQube issues (clean, production-ready code)

**Code Quality:**
- Follow existing project structure from `/projects` folder
- Use proper error handling and logging
- Add docstrings and comments for clarity
- Follow PEP 8 style guidelines

### 4. DELIVERABLES
Generate:
1. `{job_name}_pipeline.py` - Main pipeline execution file
2. `{job_name}_context.json` - Configuration file with all parameters
2.1. create btoh these files inside a new folder under projects folder with the name `{job_name}`
3. New util files (if needed) - Simple reusable functions
4. New mod files (if absolutely necessary) - Framework components with metadata

### 5. VALIDATION CHECKLIST
Before completing, verify:
- [ ] Business logic from Talend job is accurately replicated
- [ ] All transformations use DuckDB relational API
- [ ] Existing mods/utils are reused where applicable
- [ ] Context file contains all configurable values
- [ ] Code is clean with no SonarQube violations
- [ ] Pipeline follows existing project patterns
- [ ] Error handling and logging are implemented
- [ ] Code is modular (≤200 LOC per file)

## NOTES
- Prioritize code reuse over creating new components
- When in doubt about creating a mod vs util, default to util
- Study existing jobs in `/projects` to match coding style
- Use DuckDB for efficiency and performance