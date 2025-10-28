Talend Job to DataPy Framework Conversion - Devin Instructions
🎯 Your Mission
You are tasked with converting a preprocessed Talend ETL job to the datapy framework. This is NOT a 1-to-1 component translation. Your goal is to:

Understand the business logic and requirements from the Talend job
Rebuild the solution in Python using datapy framework patterns
Preserve all transformations and functionality without losing any logic
Leverage existing mods and utilities from the datapy framework


📥 Input You'll Receive
You'll be given a preprocessed Talend job in JSON format with this structure:
json{
  "job_metadata": {...},           // Job name, version, description
  "components": [...],             // All Talend components with configs
  "connections": [...],            // Data flows between components
  "context_variables_used": [...], // Flagged: ${VAR} or context.VAR
  "routines_used": [...],          // Flagged: Java routines to convert
  "subjobs": [...],
  "notes": [...]
}
Key Points:

All SQL queries, expressions, mappings are preserved in components
Context variables are flagged but NOT merged
Routines are flagged but NOT converted (you'll do this)
Schema definitions are complete with types, keys, lengths


⚠️ CRITICAL: DUCKDB MANDATORY REQUIREMENT ⚠️
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔴 ABSOLUTE RULE: ALL DATA TRANSFORMATIONS MUST USE DUCKDB 🔴

- ✅ USE: DuckDB Relational API for ALL transformations
- ✅ USE: DuckDB SQL for queries and operations
- ✅ USE: DuckDB for filtering, joining, aggregating, mapping
- ✅ USE: DuckDB for data manipulation and processing

- ❌ DO NOT USE: Pandas for any transformations
- ❌ DO NOT USE: Polars for any transformations  
- ❌ DO NOT USE: Any other dataframe library for transformations
- ❌ DO NOT USE: Python loops for row-by-row processing

WHY DUCKDB ONLY:
- Performance: DuckDB is optimized for analytical queries
- Memory efficiency: Handles large datasets better
- SQL compatibility: Easy to understand and maintain
- Framework standard: All datapy mods use DuckDB

ALLOWED EXCEPTIONS:
- Reading initial data from sources (CSV, DB, etc.) may use appropriate libraries
- But immediately convert to DuckDB for ALL processing
- Only convert back to other formats for final output if required

EXAMPLE TRANSFORMATION FLOW:
```python
# ✅ CORRECT: Using DuckDB
import duckdb

# Read data (can use source library)
df = pd.read_csv("input.csv")

# Convert to DuckDB immediately
conn = duckdb.connect()
conn.register('data', df)

# ALL transformations via DuckDB
result = conn.execute("""
    SELECT 
        customer_id,
        SUM(amount) as total_amount,
        COUNT(*) as order_count
    FROM data
    WHERE status = 'active'
    GROUP BY customer_id
    HAVING SUM(amount) > 1000
""").df()

# ❌ WRONG: Using Pandas transformations
df_filtered = df[df['status'] == 'active']  # NO!
df_grouped = df_filtered.groupby('customer_id').agg(...)  # NO!
```

THIS RULE APPLIES TO:
- All filter operations
- All join operations  
- All aggregation operations
- All column calculations
- All data mapping
- All sorting/ordering
- All deduplication
- All set operations (union, intersect, etc.)

📌 REFERENCE EXISTING JOBS FOR DUCKDB PATTERNS:
The existing jobs in the datapy framework already use DuckDB for transformations.
During your discovery phase, pay close attention to how they implement DuckDB:

- Study how DuckDB connections are created and managed
- Note how data is registered with DuckDB (conn.register())
- Observe how SQL queries are structured
- Learn how DuckDB relational API is used
- See how results are retrieved and passed between mods
- Follow the same DuckDB patterns you see in existing jobs

When implementing your conversion:
- Use the exact same DuckDB patterns from existing jobs
- Refer back to existing code for examples
- Maintain consistency with the established DuckDB approach
- If unsure, check how similar transformations are done in existing jobs

NO EXCEPTIONS. NO COMPROMISES. DUCKDB ONLY.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📚 DUCKDB RELATIONAL API QUICK REFERENCE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Full documentation: https://duckdb.org/docs/stable/clients/python/relational_api

⚡ KEY CONCEPTS:
- Relations are LAZY EVALUATED - no data is retrieved until output methods are called
- Relations are symbolic representations of SQL queries
- You can chain operations to build complex transformations

🔧 CREATING RELATIONS:

```python
import duckdb

# Connect to DuckDB
conn = duckdb.connect()

# From DataFrame/file
rel = conn.from_df(df)                          # From pandas DataFrame
rel = conn.read_csv("file.csv")                 # From CSV
rel = conn.read_parquet("file.parquet")         # From Parquet
rel = conn.read_json("file.json")               # From JSON

# From SQL query
rel = conn.sql("SELECT * FROM table WHERE x > 5")
rel = conn.query("SELECT * FROM range(1, 100)")

# From existing table
rel = conn.table("table_name")

# From values
rel = conn.values([[1, 'a'], [2, 'b']])
```

🔄 TRANSFORMATION METHODS (Chained Operations):

```python
# FILTER - Apply conditions
rel = rel.filter("age >= 25 AND city IN ('NYC', 'LA')")
rel = rel.filter("status = 'active'")

# SELECT/PROJECT - Choose columns
rel = rel.select("customer_id, name, total_amount")
rel = rel.project("col1, col2, col3")

# AGGREGATE - Group and aggregate
rel = rel.aggregate("SUM(amount) as total, COUNT(*) as cnt", group_expr="customer_id")
rel = rel.aggregate("MAX(value)", group_expr="category")

# JOIN - Combine relations
rel1 = conn.sql("SELECT * FROM customers")
rel2 = conn.sql("SELECT * FROM orders")
result = rel1.join(rel2, condition="customer_id", how="inner")  # USING clause
result = rel1.join(rel2, condition="rel1.id = rel2.customer_id", how="left")  # ON clause

# ORDER - Sort results
rel = rel.order("created_date DESC, name ASC")
rel = rel.sort("value")

# LIMIT - Restrict rows
rel = rel.limit(100)
rel = rel.limit(50, offset=100)

# DISTINCT - Remove duplicates
rel = rel.distinct()

# UNION - Combine results
rel = rel1.union(rel2)  # Note: This is UNION ALL

# SET OPERATIONS
rel = rel1.intersect(rel2)
rel = rel1.except_(rel2)

# CROSS PRODUCT
rel = rel1.cross(rel2)
```

📊 COMMON AGGREGATION FUNCTIONS:

```python
# Count
rel.count("*")
rel.count("customer_id", groups="region")

# Sum/Average
rel.sum("amount")
rel.avg("value", groups="category")

# Min/Max
rel.min("price")
rel.max("quantity", groups="product_id")

# Statistical
rel.median("age")
rel.stddev("score")
rel.variance("measurement")
```

📤 OUTPUT METHODS (Trigger Execution):

```python
# Show preview (first rows)
rel.show()

# Convert to DataFrame
df = rel.df()              # Pandas DataFrame
df = rel.to_df()           # Same as above

# Convert to Arrow
arrow_table = rel.arrow()

# Create table in DuckDB
rel.to_table("table_name")

# Insert into existing table
rel.insert_into("existing_table")

# Get shape
rows, cols = rel.shape

# Get column info
columns = rel.columns      # List of column names
types = rel.dtypes         # List of column types
```

🔍 INSPECTION METHODS:

```python
# Get SQL query representation
sql_query = rel.sql_query()

# Explain query plan
rel.explain()

# Describe statistics
rel.describe()

# Get relation description
rel.description
```

💡 COMMON PATTERNS FOR ETL:

```python
import duckdb

# Pattern 1: Read -> Transform -> Write
conn = duckdb.connect()
result = (conn.read_csv("input.csv")
    .filter("status = 'active'")
    .select("customer_id, name, SUM(amount) as total")
    .aggregate("SUM(amount) as total", group_expr="customer_id, name")
    .order("total DESC")
)
result.to_table("output_table")

# Pattern 2: Multiple sources with join
customers = conn.read_csv("customers.csv")
orders = conn.read_csv("orders.csv")

result = (customers
    .set_alias("c")
    .join(orders.set_alias("o"), 
          condition="c.customer_id = o.customer_id", 
          how="left")
    .aggregate("c.name, COUNT(*) as order_count", 
               group_expr="c.customer_id, c.name")
)

# Pattern 3: Filter and enrich
result = (conn.read_parquet("data.parquet")
    .filter("date >= '2024-01-01'")
    .select("*, amount * 1.1 as amount_with_tax")
    .filter("amount_with_tax > 100")
)

# Pattern 4: Using SQL with relations
rel = conn.sql("""
    SELECT 
        customer_id,
        SUM(amount) as total_amount,
        COUNT(*) as order_count
    FROM read_csv('orders.csv')
    WHERE status = 'completed'
    GROUP BY customer_id
    HAVING SUM(amount) > 1000
""")
```

⚠️ IMPORTANT NOTES:

1. **Lazy Evaluation**: Relations don't execute until you call an output method
2. **Method Chaining**: Most transformation methods return a new relation
3. **Aliases**: Use `.set_alias()` before joins to reference in conditions
4. **SQL Mixing**: You can mix relational API with SQL queries
5. **Memory Efficient**: DuckDB handles large datasets efficiently

🚨 REMEMBER: In your conversion, ALWAYS use DuckDB's relational API or SQL 
for transformations. Never use Pandas operations like df.groupby(), 
df.merge(), df[df['col'] > 5], etc.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


🔍 STEP 0: Discover the DataPy Framework (MANDATORY FIRST STEP)
BEFORE doing ANYTHING else, you MUST explore and understand the datapy repository:
1. Understand the Framework Structure
bash# Explore the repository structure
tree -L 3 datapy/

# Key directories to understand:
# datapy/mods/         - All available mods (sources, transformers, sinks)
# datapy/utils/        - Utility functions and helpers
# datapy/mod_manager/  - Core framework (SDK, logging, context)
# projects/            - Example jobs and patterns
2. Discover Available Mods
bash# Check the mod registry - THIS IS YOUR SOURCE OF TRUTH
cat datapy/mod_registry.json

# Or use CLI to list all mods
datapy list-registry

# Explore by category
datapy list-registry --category sources
datapy list-registry --category transformers
datapy list-registry --category sinks

# Get detailed info about specific mods
datapy mod-info <mod_name>

# Browse the actual mod implementations
ls -la datapy/mods/sources/
ls -la datapy/mods/transformers/
ls -la datapy/mods/sinks/

# Read mod source code to understand capabilities
cat datapy/mods/sources/*.py
cat datapy/mods/transformers/*.py
cat datapy/mods/sinks/*.py
Document what you find:

What source mods exist? (file readers, DB readers, API readers?)
What transformer mods exist? (filters, joins, aggregators, mappers?)
What sink mods exist? (file writers, DB writers?)
What parameters does each mod accept?
What outputs does each mod provide?

3. Discover Existing Utilities
bash# Explore utility functions
ls -la datapy/utils/

# Read utility modules to understand available helpers
cat datapy/utils/expression_evaluator.py
cat datapy/utils/script_monitor.py
# ... check all files in utils/

# Look for reusable functions that might help with:
# - Expressions and calculations
# - Data validation
# - File operations
# - Monitoring and logging
Document what you find:

What utility functions are available?
What helper classes exist?
How can these be reused?

4. Study Existing Job Patterns
bash# Find all example jobs
find projects/ -name "*pipeline.py" -type f

# Study each example job
cat projects/demo_project/filter_job/pipeline.py
cat projects/demo_jobs/polars_pipeline.py
# ... read ALL example jobs you find

# Look for patterns:
# - How are mods chained?
# - How is context used?
# - How are errors handled?
# - How are results passed between mods?
# ⚠️ CRITICAL: Study how DuckDB is used for ALL transformations - this is the pattern to follow!
5. Read Framework Documentation
bash# Read the main README
cat README.md

# Check for any design docs
find . -name "*.md" -type f | grep -i design
find . -name "*.md" -type f | grep -i spec

# Look for mod specifications
cat datapy/mods_spec.txt  # if exists

# Read any developer guides
find . -name "*guide*.md" -type f
6. Understand the SDK
bash# Study the SDK interface
cat datapy/mod_manager/sdk.py

# Key functions to understand:
# - run_mod() - How to execute mods
# - setup_logging() - Logging configuration
# - setup_context() - Context management
# - get_context_value() - Reading context variables

📝 Create Your Discovery Document
Before proceeding to conversion, create a document summarizing:
markdown# DataPy Framework Discovery Summary

## Available Mods
### Sources (Data Input)
- mod_name: description, key parameters
- mod_name: description, key parameters
...

### Transformers (Data Processing)
- mod_name: description, key parameters
...

### Sinks (Data Output)
- mod_name: description, key parameters
...

## Available Utilities
- utility_name: purpose, key functions
...

## Job Structure Pattern (from examples)
- How pipelines are structured
- How context is used
- How error handling works
- How mods are chained
...

## DuckDB Usage Patterns (STUDY THESE CAREFULLY!)
- How DuckDB connections are created and managed
- How data is registered with DuckDB (conn.register())
- How SQL queries are structured in mods
- How DuckDB relational API is used
- How results are retrieved (conn.execute().df() or similar)
- How DuckDB is integrated with mod inputs/outputs
- Common DuckDB transformation patterns in existing code
...

## Context Variable Patterns
- How context files are structured
- How variables are referenced
...

## Common Patterns Observed
- Pattern 1: description
- Pattern 2: description
...
Share this document with me before proceeding!

🏗️ DataPy Framework Basics (What to Look For)
Pipeline Structure Pattern
When studying example jobs, look for this pattern:
pythonfrom datapy.mod_manager.sdk import run_mod, setup_logging, setup_context
from datapy.utils.script_monitor import monitor_execution

def pre_run():
    """Setup - logging and context initialization"""
    # Study how this is done in existing jobs
    pass

def run_pipeline(logger):
    """Main execution - chaining mods together"""
    # Study how mods are chained in existing jobs
    pass

@monitor_execution("job_name")
def main():
    """Entry point"""
    # Study the overall structure in existing jobs
    pass
Mod Execution Pattern
When studying mod usage, look for:
python# How mods are called
result = run_mod("mod_name", {
    "parameter": "value",
    "another_param": "value"
})

# How results are accessed
if result["status"] == "success":
    data = result["artifacts"]["data"]  # Output data
    metrics = result["metrics"]          # Performance info
else:
    errors = result["errors"]            # Error details
Context Usage Pattern
Look for how context variables are used:
json// context.json structure
{
  "section": {
    "key": "value"
  }
}
python# In pipeline - using ${section.key} syntax
run_mod("mod_name", {
    "parameter": "${section.key}"
})

💡 When and How to Propose New Mods/Utils
It's OK to Create New Components!
You are ENCOURAGED to propose new mods or utils when:

Existing components genuinely can't handle the requirement
The Talend job needs specific functionality not available
Creating something new would be cleaner than hacky workarounds

But you MUST ask first!
How to Propose a New Mod
Template for Proposing New Mod:
markdown## New Mod Proposal: <mod_name>

### Why It's Needed
- Talend job component: <component_name>
- Functionality required: <description>
- Why existing mods can't handle it: <explanation>

### What I've Checked
- [ ] Explored all mods in datapy/mods/<category>/
- [ ] Checked mod_registry.json
- [ ] Tried combining existing mods: <what I tried>
- [ ] Checked if utils could handle it: <what I checked>

### Proposed Implementation
**Category:** source | transformer | sink | solo
**Mod Name:** <mod_name>
**Input Parameters:**
- param1: type - description
- param2: type - description

**Output Artifacts:**
- artifact1: type - description

**Similar Mod Pattern:**
- Will follow the structure of: <existing_mod_name>
- Located at: datapy/mods/<category>/<mod_name>.py

### Can I Proceed?
Waiting for your approval to create this mod.
How to Propose New Utils
Template for Proposing New Utility:
markdown## New Utility Proposal: <utility_name>

### Why It's Needed
- Talend routines to convert: <list>
- OR custom logic needed: <description>
- Reusable across: <where it will be used>

### What I've Checked
- [ ] Explored all files in datapy/utils/
- [ ] Checked expression_evaluator.py for similar functions
- [ ] Looked for existing helpers: <what I found>

### Proposed Implementation
**File:** datapy/utils/<filename>.py

**Functions to add:**
```python
def function_name(params) -> return_type:
    """
    Description of what it does.
    Converts Talend routine: 
    """
    # Implementation
```

**Usage Example:**
```python
from datapy.utils. import function_name

result = function_name(data)
```

### Can I Proceed?
Waiting for your approval to create this utility file.
Decision Framework for Creating New Components
Ask yourself:

Have I thoroughly explored existing options?

Yes → Proceed to propose
No → Explore more first


Can this be done by combining existing components?

No → Proceed to propose
Yes → Try combining first


Is this genuinely new functionality?

Yes → Proceed to propose
Maybe → Ask me, I'll help decide


Have I followed the proposal template?

Yes → Send proposal
No → Fill out template completely



After Proposal:

I will review and respond
If approved: Proceed with implementation following existing patterns
If modifications needed: I'll suggest changes
If not needed: I'll explain alternatives


🎨 Conversion Strategy
❌ DON'T Do This (Bad Approach)
python# DON'T: Mechanically translate Talend component names
# Without understanding what mods actually exist
✅ DO This (Good Approach)

Complete STEP 0 first - Discover the framework thoroughly
Understand the Talend job business requirement:

What data is being extracted?
What transformations are applied?
What business rules are enforced?
What is the final output?


Map Talend logic to discovered datapy mods:

Look at your discovery document
Match Talend operations to available mods
Think about how to chain mods logically


Follow patterns from example jobs:

Use the same structure you saw in projects/
Follow the same error handling patterns
Use context variables the same way




🔧 Critical Instructions
1. Learn from Existing Jobs (MANDATORY)
Study ALL jobs in the projects/ folder:
bash# Find all pipeline files
find projects/ -name "*pipeline.py"

# Read each one and understand:
# - Structure
# - Patterns
# - Mod usage
# - Error handling
# - Context usage
# - Logging approach
Take notes on:

Common patterns you see across jobs
How complex transformations are handled
How data is passed between mods
How errors are propagated

2. Creating New Mods or Utils - Ask First, But It's Allowed!
IMPORTANT: You CAN create new mods or utility functions if needed, but you MUST ask me first and get approval.
The Process:

First, try to use existing mods/utils

Explore what's available thoroughly
Think creatively about combining existing mods
Check if utilities can handle it


If you genuinely need something new:

Stop and ask me first
Explain what functionality you need
Explain why existing mods/utils can't handle it
Propose the new mod/util design


Wait for my approval before creating

Example Request:

"I've analyzed the Talend job and found that it needs to read from Oracle database.
What I checked:

Explored all mods in datapy/mods/sources/
Checked mod_registry.json
Found CSV, file, and other readers but no Oracle reader

What I need:

A new source mod: oracle_reader
Should accept: host, port, username, password, query
Should return: DataFrame with query results

Proposed approach:

Create datapy/mods/sources/oracle_reader.py
Follow the pattern I saw in csv_reader.py
Use cx_Oracle library for connection

Can I proceed with creating this mod?"

When Creating is Appropriate:

✅ Talend job needs Oracle/SQL Server/specific database not in existing mods
✅ Talend routine has complex logic that doesn't fit in existing utils
✅ Talend component has unique functionality not covered by existing mods
✅ Performance/memory requirements need specialized implementation

When You Should NOT Create:

❌ Just because you didn't look thoroughly enough
❌ Without understanding what existing mods can do
❌ Without trying to combine existing mods first
❌ Without asking me first

3. Use Existing Utils - Create New Ones with My Approval
For existing utilities:

Check your discovery document for available utils
Reuse functions from datapy/utils/
Study how example jobs use utilities

For new utilities (Talend routines or custom logic):
YOU CAN CREATE NEW UTILS - Just Ask Me First!
The Process:

Identify what needs a utility function:

Talend routine conversion
Complex reusable logic
Helper functions for your pipeline


Check if it already exists:

Search through datapy/utils/
Look for similar functionality
Check if you can extend existing utils


Ask me before creating:

Example Request:

"The Talend job uses these routines that need conversion:
Routines to convert:

StringUtils.cleanEmail(email) - Trims, lowercases, validates email
DateUtils.formatDate(date, format) - Formats dates with pattern
ValidationUtils.checkSSN(ssn) - Validates social security numbers

What I checked:

Explored all files in datapy/utils/
expression_evaluator.py has some functions but not these
No existing email/date/validation utilities found

Proposed approach:

Create datapy/utils/talend_routines.py
Implement the 3 functions above in Python
Follow the coding style I saw in existing utils

Can I proceed with creating this utility file?"


Wait for approval, then create following existing patterns

When Creating Utils is Appropriate:

✅ Converting Talend routines to Python
✅ Complex reusable business logic
✅ Helper functions used multiple times in pipeline
✅ Domain-specific validations or transformations

4. Handle Context Variables
Study how context is used in example jobs, then:

Create context file based on the pattern you discovered
Map Talend context variables to datapy context structure
Use ${} syntax as seen in examples

5. Handle Routines
Talend routines are flagged in the input JSON:
json{
  "routines_used": [
    {
      "routine_name": "StringUtils.cleanEmail",
      "contexts": ["StringUtils.cleanEmail(row1.email)"]
    }
  ]
}
Convert to Python utilities:

ASK ME before creating new utility files
Study existing utility patterns in datapy/utils/
Create function following the same patterns
Document the conversion from Java to Python

6. Preserve ALL Transformations
CRITICAL: Do not lose ANY business logic from Talend job.
For each Talend component, ensure:

All SQL queries are converted
All mappings are preserved
All filter conditions are applied
All calculated fields are recreated
All joins preserve keys and types
All aggregations match

7. Error Handling
Follow the pattern you discovered in example jobs:
pythonresult = run_mod("mod_name", {...})

if result["status"] != "success":
    # Handle error as seen in examples
    pass

# Continue with data
data = result["artifacts"]["data"]
8. Testing & Validation
Follow logging patterns from example jobs:
pythonlogger.info(f"Step completed: {result['metrics']}")

📋 Step-by-Step Conversion Process
Step 0: Discovery (MANDATORY - Do This First!)

✅ Explore datapy repository structure
✅ List all available mods from registry
✅ Read mod source code to understand capabilities
✅ Explore all utility functions
✅ Study ALL example jobs in projects/
✅ Read framework documentation
✅ Create discovery summary document
✅ Share discovery document with me

DO NOT PROCEED until Step 0 is complete!
Step 1: Analyze Talend Input

Read the preprocessed JSON thoroughly
Identify the business requirement:

What is the job trying to accomplish?
What are the data sources and targets?
What transformations are critical?


Map Talend components to discovered datapy mods:

Use your discovery document
Match functionality, not component names
Consider combining multiple Talend components



Step 2: Design the Pipeline
Create a plan (comment in code):
pythondef run_pipeline(logger):
    """
    Business logic: [Describe what the job does]
    
    Talend Flow:
    1. [Talend component] → [what it does]
    2. [Talend component] → [what it does]
    ...
    
    DataPy Flow:
    1. [datapy mod] → [matching functionality]
    2. [datapy mod] → [matching functionality]
    ...
    """
    # Implementation here
Step 3: Implement Pipeline
Follow the structure pattern from example jobs:
python# Chain mods as you saw in examples
result1 = run_mod("discovered_mod_name", {...})
result2 = run_mod("another_mod_name", {"data": result1["artifacts"]["data"], ...})
# ... continue
Step 4: Create Context File
Follow the pattern you discovered:
json{
  "pipeline": {"name": "job_name"},
  "section": {
    "key": "value"
  }
}
Step 5: Convert Routines (If Needed)

Ask me first
Follow utility patterns from datapy/utils/
Implement Python equivalents
Import and use in pipeline

Step 6: Test & Validate
Run the pipeline and verify:

Data flows correctly
Transformations are applied
Outputs are created
Metrics look correct


⚠️ Common Mistakes to Avoid
❌ Mistake 1: Skipping Discovery
BAD: Starting to code without understanding the framework
✅ Correct: Complete Discovery First
GOOD: Spend time exploring, understanding, documenting
❌ Mistake 2: Creating Without Asking
python# BAD: Creating new mod without asking
# Just starts coding datapy/mods/sources/oracle_reader.py
✅ Correct: Propose First, Create After Approval
GOOD: 
1. Check if it exists
2. Try alternatives
3. Propose with template
4. Wait for approval
5. Then create
❌ Mistake 3: Not Following Patterns
python# BAD: Inventing your own structure
def my_custom_structure():
    # Different from all example jobs
✅ Correct: Follow Example Patterns
python# GOOD: Using the same structure as example jobs
def pre_run():
    # Same pattern as projects/demo_*/
❌ Mistake 4: Assuming Without Exploring
python# BAD: Assuming a mod doesn't exist without checking
# "I need an Oracle reader, let me create one"
# (Maybe there's a generic SQL reader that works!)
✅ Correct: Explore Thoroughly First
GOOD:
1. Check mod_registry.json
2. Read all mods in sources/
3. Look for generic database readers
4. Try existing mods first
5. Only then propose new one

📝 Communication Protocol
When Starting
First message to me should be:

"I've completed my discovery of the datapy framework. Here's what I found:
Available Mods:

[list of mods from registry]

Available Utilities:

[list of utils found]

Patterns Observed:

[key patterns from example jobs]

Ready to proceed with conversion?"

During Conversion
Ask me about:

Ambiguous business logic in Talend job
Which discovered mod is best for specific transformations
Whether to create new utility files
Complex routine conversions
Any design decisions

Before Creating New Files
ALWAYS notify me:

"Based on the Talend routines, I need to create:

datapy/utils/custom_routines.py with functions: [list]

Following the pattern I saw in [existing util file].
Is this correct?"


🎯 Success Criteria
Your conversion is successful when:
✅ Discovery Complete

All mods documented
All utils documented
All patterns understood
Discovery document created and shared

✅ Functionality Complete

All data sources are read using discovered mods
All transformations applied using available mods
All business rules enforced
All outputs created

✅ Quality Standards

Uses only discovered/registered mods
Follows patterns from example jobs
Proper error handling (same as examples)
Context variables used correctly (same pattern as examples)
Routines converted to utils (with my approval)

✅ Code Quality

Structure matches example jobs
Same logging approach as examples
Comments explaining business logic
No hardcoded values (uses context)


🚀 Ready to Start?
Your Checklist:

 Explore datapy repository structure
 Read datapy/mod_registry.json
 Browse and read all mods in datapy/mods/
 Browse and read all utils in datapy/utils/
 Read ALL example jobs in projects/
 Read README.md and any other docs
 Create discovery summary document
 Share discovery summary with me
 Wait for my confirmation
 THEN start conversion

Remember:

Discovery first, code later
Follow patterns, don't invent
Use what exists, don't create new mods
Ask questions when unsure

Good luck! 🎉
🧩 Understanding DataPy Mods
What are Mods?
Mods are reusable ETL components in the datapy framework. Each mod is a self-contained unit that performs a specific data operation (read, transform, write, etc.).
How to Discover Available Mods
FIRST, check the mod registry to see what's available:
bash# View all registered mods
cat datapy/mod_registry.json

# Or use the CLI
datapy list-registry

# Get details about a specific mod
datapy mod-info <mod_name>
SECOND, explore the mods directory:
bash# Browse available mods
ls -R datapy/mods/

# Check categories:
datapy/mods/sources/      # Data input mods (read files, DBs, APIs)
datapy/mods/transformers/ # Data processing mods (filter, join, aggregate)
datapy/mods/sinks/        # Data output mods (write files, DBs)
datapy/mods/solo/         # Utility mods
Mod Categories

Sources: Extract data from files, databases, APIs
Transformers: Filter, join, aggregate, map, calculate, validate data
Sinks: Write data to files, databases, services
Solo: Standalone utility operations

How Mods Work
Each mod has:

Input parameters: Configuration (file paths, filter conditions, etc.)
Outputs: Results with status, metrics, artifacts (data), errors

pythonresult = run_mod("mod_name", {
    "param1": "value1",
    "param2": "value2"
})

# Access results
status = result["status"]          # success|warning|error
data = result["artifacts"]["data"] # Output data (if applicable)
metrics = result["metrics"]        # Performance metrics
errors = result["errors"]          # Error details (if any)

🎨 Conversion Strategy
❌ DON'T Do This (Bad Approach)
python# DON'T: 1-to-1 Talend component conversion
# Just mechanically translating component names
✅ DO This (Good Approach)

Understand the business requirement:

What data is being extracted?
What transformations are applied?
What business rules are enforced?
What is the final output?


Discover available mods:

bash   # Check what mods exist
   cat datapy/mod_registry.json
   datapy list-registry
   
   # Explore categories
   ls datapy/mods/sources/
   ls datapy/mods/transformers/
   ls datapy/mods/sinks/

Design the Python solution:

Chain appropriate datapy mods
Combine multiple Talend components into single mod calls when logical
Use Pythonic patterns (list comprehensions, pandas operations, etc.)


Example mapping concept:

   Talend Pattern: Input → Multiple Transformations → Filter → Output
   
   DataPy Approach:
   - Find appropriate source mod (check datapy/mods/sources/)
   - Find transformation mods (check datapy/mods/transformers/)
   - Chain them logically
   - Find appropriate sink mod (check datapy/mods/sinks/)

🔧 Critical Instructions
1. Learn from Existing Jobs
BEFORE writing ANY code:
bash# Look at the existing jobs in the project
cd projects/
ls -la

# Study their structure:
# - How they use mods
# - How they chain operations
# - How they handle errors
# - How they define context
Key files to review:

projects/demo_project/filter_job/pipeline.py
projects/demo_jobs/polars_pipeline.py
Any job in projects/ folder

2. Use Existing Mods - DO NOT Create New Ones
CRITICAL: You are NOT allowed to create new mods. Only use mods that already exist in:

datapy/mods/sources/ (source mods)
datapy/mods/transformers/ (transformer mods)
datapy/mods/sinks/ (sink mods)

Check the mod registry:
bashcat datapy/mod_registry.json
3. Use Existing Utils - Create Utils for Routines ONLY
For existing utilities:

Check datapy/utils/ folder
Reuse functions from:

expression_evaluator.py - For expressions
script_monitor.py - For monitoring
Any other util modules



For Talend routines:

Create NEW utility functions in datapy/utils/
ASK ME FIRST before creating new utility files
Convert Java routine logic to Python functions
Example:

python  # datapy/utils/custom_routines.py
  def clean_email(email: str) -> str:
      """Convert Talend routine: StringUtils.cleanEmail"""
      if not email:
          return ""
      return email.strip().lower()
4. Handle Context Variables
Talend context variables are flagged in the input JSON:
json{
  "context_variables_used": [
    {
      "name": "DB_HOST",
      "referenced_in": [...]
    }
  ]
}
Map them to datapy context:
Create your_job_context.json:
json{
  "pipeline": {
    "name": "your_job"
  },
  "database": {
    "host": "localhost",
    "port": 5432,
    "name": "mydb",
    "user": "admin"
  },
  "data": {
    "input_path": "./input",
    "output_path": "./output"
  }
}
Use in pipeline with ${} syntax:
pythonrun_mod("csv_reader", {
    "file_path": "${data.input_path}/customers.csv"
})
5. Handle Routines
Talend routines are flagged in the input JSON:
json{
  "routines_used": [
    {
      "routine_name": "StringUtils.cleanEmail",
      "used_in_components": [...],
      "contexts": ["StringUtils.cleanEmail(row1.email)"]
    }
  ]
}
Convert to Python utilities:

ASK ME before creating new utility files
Create function in datapy/utils/your_routines.py
Use in pipeline:

python   from datapy.utils.your_routines import clean_email
   
   # Use in custom expressions
   run_mod("data_expression", {
       "data": input_data,
       "expressions": {
           "email_clean": "clean_email(email)"
       }
   })
6. Preserve ALL Transformations
CRITICAL: Do not lose ANY business logic from Talend job.
For each Talend component, ensure:

All SQL queries are converted
All mappings are preserved
All filter conditions are applied
All calculated fields are recreated
All joins preserve keys and types
All aggregations match

7. Error Handling
Follow datapy patterns:
pythonresult = run_mod("csv_reader", {...})

if result["status"] != "success":
    logger.error(f"Read failed: {result.get('errors', [])}")
    return result  # Propagate error

# Continue with data
data = result["artifacts"]["data"]
8. Testing & Validation
After conversion:
python# Add validation logging
logger.info(f"Rows processed: {result['metrics']['rows_read']}")
logger.info(f"Rows filtered: {result['metrics']['rows_filtered']}")
logger.info(f"Output written: {result['metrics']['rows_written']}")

📋 Step-by-Step Conversion Process
Step 1: Analyze Input (Do First!)

Read the preprocessed JSON thoroughly
Identify the business requirement:

What is the job trying to accomplish?
What are the data sources and targets?
What transformations are critical?


Map components to datapy mods:

   Talend Component → DataPy Mod(s)
   tFileInputDelimited → file_input or csv_reader
   tMap → data_expression + data_mapper
   tFilterRow → data_filter
   tJoin → data_join
   tAggregateRow → data_aggregator
   tFileOutputDelimited → file_output or csv_writer
Step 2: Study Existing Jobs
bash# Look at real examples
cat projects/demo_project/filter_job/pipeline.py
cat projects/demo_jobs/polars_pipeline.py
Learn:

How mods are chained
How context is used
How errors are handled
How results are passed
⚠️ CRITICAL: How DuckDB is used for transformations (connections, queries, relational API)

Step 3: Design the Pipeline
Create a plan (comment in code):
pythondef run_pipeline(logger):
    """
    Business logic: Load customers from CSV, filter adults in major cities,
    enrich with account balance, join with orders, write to output
    
    Steps:
    1. Read customers.csv
    2. Filter: age >= 25, city in [NYC, LA, CHI]
    3. Calculate: risk_score = debt / income
    4. Join with orders on customer_id
    5. Aggregate: total_orders per customer
    6. Write to output.csv
    """
    # Implementation here
Step 4: Implement Pipeline
python# Chain mods together
customers = run_mod("csv_reader", {...})
filtered = run_mod("data_filter", {"data": customers["artifacts"]["data"], ...})
enriched = run_mod("data_expression", {"data": filtered["artifacts"]["filtered_data"], ...})
# ... continue
Step 5: Create Context File
json{
  "pipeline": {"name": "customer_etl"},
  "data": {
    "input_path": "./input",
    "output_path": "./output"
  },
  "filters": {
    "min_age": 25,
    "cities": ["New York", "Los Angeles", "Chicago"]
  }
}
Step 6: Convert Routines

Ask me first before creating new utility files
Create datapy/utils/job_routines.py
Implement Python equivalents of Talend routines
Import and use in pipeline


⚠️ Common Mistakes to Avoid
❌ Mistake 1: 1-to-1 Component Mapping
python# BAD: Directly translating each Talend component
run_mod("talend_tmap", ...)  # This doesn't exist!
✅ Correct: Business Logic Translation
python# GOOD: Understanding what tMap does and using appropriate mods
run_mod("data_expression", {...})  # Add calculated fields
run_mod("data_mapper", {...})      # Select/rename columns
❌ Mistake 2: Creating New Mods
python# BAD: Creating custom mods
run_mod("my_custom_processor", ...)  # Don't create new mods!
✅ Correct: Use Existing Mods
python# GOOD: Using available mods creatively
run_mod("data_filter", {"data": data, "filter_conditions": {...}})
❌ Mistake 3: Ignoring Context Variables
python# BAD: Hardcoding values
run_mod("csv_reader", {"file_path": "/prod/data/customers.csv"})
✅ Correct: Using Context
python# GOOD: Using context variables
run_mod("csv_reader", {"file_path": "${data.input_path}/customers.csv"})

📝 Communication Protocol
When You Need Clarification
ASK ME about:

Ambiguous business logic in Talend job
Which mods to use for specific transformations
Whether to create new utility files
Complex routine conversions
Any design decisions

Example:

"I see the Talend job uses tMap with 15 expressions. Should I:
A) Use data_expression with all 15 expressions
B) Split into data_expression + data_mapper
C) Something else?"

Before Creating New Files
ALWAYS notify me:

"I need to create a utility file datapy/utils/talend_routines.py to convert:

StringUtils.cleanEmail
DateUtils.formatDate
CustomValidation.checkSSN

Is this approach correct?"

Progress Updates
Keep me informed:

"Step 1 complete: Analyzed Talend job, identified 3 data sources, 5 transformations"
"Step 2 complete: Mapped to datapy mods: file_input, data_filter, data_join"
"Step 3: Implementing pipeline..."


🎯 Success Criteria
Your conversion is successful when:
✅ Functionality Complete

All data sources are read correctly
All transformations are applied
All business rules are enforced
All outputs match Talend job outputs

✅ Quality Standards

Uses existing datapy mods only
Follows existing job patterns from projects/
⚠️ ALL transformations use DuckDB (no Pandas/Polars/other libraries)
Proper error handling
Context variables used correctly
Routines converted to utils (with my approval)

✅ Code Quality

Clean, readable Python code
Proper logging and monitoring
Comments explaining business logic
No hardcoded values

✅ Testing

Pipeline runs without errors
Data validation logs show correct metrics
Output files are created correctly


📚 Quick Reference
Check Available Mods
bashcat datapy/mod_registry.json | grep "\"type\":"
Study Existing Jobs
bashls -R projects/
cat projects/demo_project/filter_job/pipeline.py
Mod Result Structure
pythonresult = run_mod("csv_reader", {...})
# result = {
#     "status": "success|warning|error",
#     "metrics": {...},
#     "artifacts": {"data": dataframe},
#     "errors": []
# }
Context Variables
json// context.json
{"data": {"input_path": "./input"}}

// Usage in pipeline
"file_path": "${data.input_path}/file.csv"

🚀 Ready to Start?

Receive the preprocessed Talend job JSON
Study existing jobs in projects/ folder
Analyze the business requirements
Ask clarifying questions
Design the datapy pipeline
Implement step by step
Test and validate

Remember: Focus on WHAT the job does, not HOW Talend does it!
Good luck! 🎉