# How to Run Tests Yourself

This guide shows you how to test the REST Data Source on your own machine.

## Quick Start (5 minutes)

### Step 1: Navigate to the Project Directory

```bash
cd /Users/yipman/Downloads/github/pyspark-data-sources
```

### Step 2: Create a Test Virtual Environment

```bash
# Create a clean virtual environment
python3 -m venv .venv

# Activate it
source .venv/bin/activate
```

### Step 3: Install Dependencies

```bash
# Install PySpark
pip install pyspark

# Install the REST data source wheel
pip install dist/pyspark_data_sources-0.1.10-py3-none-any.whl
```

### Step 4: Run the Tests

```bash
# Run the comprehensive test suite
python3 test_rest_datasource.py
```

That's it! You should see all tests pass.

---

## Detailed Instructions

### Option 1: Run All Tests (Recommended)

This runs a comprehensive test suite covering all functionality:

```bash
# From the project directory
cd /Users/yipman/Downloads/github/pyspark-data-sources

# Setup and run
source test_venv/bin/activate
python3 test_rest_datasource.py
```

**Expected Output:**
```
======================================================================
REST DATA SOURCE - COMPREHENSIVE TEST SUITE
======================================================================

✅ PASS: Import
✅ PASS: Registration
✅ PASS: Partitions Fix
✅ PASS: GET Request
✅ PASS: POST Request
✅ PASS: Multiple Partitions
✅ PASS: Various Options

======================================================================
TOTAL: 7/7 tests passed
======================================================================

🎉 ALL TESTS PASSED! 🎉
```

### Option 2: Run Individual Tests

You can also test specific functionality:

#### Test 1: Quick Import Test

```bash
source test_venv/bin/activate
python3 -c "
from pyspark_datasources import RestDataSource
print('✅ Import successful!')
print(f'Data source name: {RestDataSource.name()}')
"
```

#### Test 2: Basic Spark Test

```python
# Save as quick_test.py
from pyspark.sql import SparkSession
from pyspark_datasources import RestDataSource

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
spark.dataSource.register(RestDataSource)

# Create test data
data = [(1,), (2,), (3,)]
df = spark.createDataFrame(data, ["id"])
df.createOrReplaceTempView("test_input")

# Test REST data source
result = spark.read.format("rest") \
    .option("url", "https://jsonplaceholder.typicode.com/posts") \
    .option("input", "test_input") \
    .option("method", "GET") \
    .option("partitions", "2") \
    .load()

print(f"✅ Test passed! Schema: {result.schema}")
spark.stop()
```

Run it:
```bash
source test_venv/bin/activate
python3 quick_test.py
```

### Option 3: Run Examples

Test with real-world examples:

```bash
source test_venv/bin/activate
python3 examples/rest_example.py
```

---

## Troubleshooting

### Issue: "No module named 'pyspark'"

**Solution:**
```bash
source test_venv/bin/activate
pip install pyspark
```

### Issue: "No module named 'pyspark_datasources'"

**Solution:**
```bash
source test_venv/bin/activate
pip install dist/pyspark_data_sources-0.1.10-py3-none-any.whl
```

### Issue: Virtual environment not activated

**Symptoms:** Commands don't find the installed packages

**Solution:**
```bash
# Make sure you're in the project directory
cd /Users/yipman/Downloads/github/pyspark-data-sources

# Activate the virtual environment
source test_venv/bin/activate

# You should see (test_venv) in your prompt
```

### Issue: "command not found: python3"

**Solution:** Use `python` instead:
```bash
python test_rest_datasource.py
```

---

## Step-by-Step Test Walkthrough

Let me walk you through the entire process from scratch:

### 1. Open Terminal

Open your terminal application.

### 2. Go to Project Directory

```bash
cd /Users/yipman/Downloads/github/pyspark-data-sources
```

### 3. Check What You Have

```bash
# List the files
ls -la

# You should see:
# - dist/pyspark_data_sources-0.1.10-py3-none-any.whl
# - test_rest_datasource.py
# - test_venv/ (directory)
```

### 4. Activate Virtual Environment

```bash
source test_venv/bin/activate
```

Your prompt should now show `(test_venv)` at the beginning.

### 5. Verify Installation

```bash
# Check PySpark is installed
pip list | grep pyspark

# Should show:
# pyspark                    4.0.1
# pyspark-data-sources       0.1.10
```

### 6. Run Tests

```bash
python3 test_rest_datasource.py
```

Watch the output - you should see tests running and all passing!

### 7. When Done

```bash
# Deactivate virtual environment
deactivate
```

---

## What the Tests Do

The test suite (`test_rest_datasource.py`) performs these checks:

1. **Import Test** - Can we import the RestDataSource?
2. **Registration Test** - Can we register it with Spark?
3. **Partition Bug Test** - Is the critical bug fixed? (This was the main issue)
4. **GET Request Test** - Can we make GET requests?
5. **POST Request Test** - Can we make POST requests?
6. **Multiple Partitions Test** - Does it work with 1, 3, 5 partitions?
7. **Options Test** - Do various options work?

Each test is independent, so if one fails, the others still run.

---

## Running Tests in Databricks

For detailed Databricks setup instructions, see **[DATABRICKS_SETUP.md](DATABRICKS_SETUP.md)**.

### Quick Databricks Setup

1. **Upload the wheel**:
   - Workspace → Upload `pyspark_data_sources-0.1.10-py3-none-any.whl`

2. **Install on cluster**:
   - Compute → Your Cluster → Libraries → Install New → Workspace
   - Select the uploaded wheel

3. **Create notebook and test**:
   ```python
   from pyspark_datasources import RestDataSource

   spark.dataSource.register(RestDataSource)

   # Create test input
   data = [(1,), (2,), (3,)]
   df = spark.createDataFrame(data, ["id"])
   df.createOrReplaceTempView("test_input")

   # Test REST API call
   result_df = spark.read.format("rest") \
       .option("url", "https://jsonplaceholder.typicode.com/posts") \
       .option("input", "test_input") \
       .option("method", "GET") \
       .option("partitions", "2") \
       .load()

   display(result_df)
   print(f"✅ Test passed!")
   ```

**Full Guide**: See [DATABRICKS_SETUP.md](DATABRICKS_SETUP.md) for:
- Step-by-step installation
- Complete test notebooks
- Real-world examples
- Secrets management
- Troubleshooting

---

## Expected Results

### Success Indicators

✅ All 7 tests pass
✅ No TypeErrors
✅ Schema is displayed
✅ Final message: "🎉 ALL TESTS PASSED! 🎉"

### What Success Looks Like

```
======================================================================
TEST SUMMARY
======================================================================
✅ PASS: Import
✅ PASS: Registration
✅ PASS: Partitions Fix
✅ PASS: GET Request
✅ PASS: POST Request
✅ PASS: Multiple Partitions
✅ PASS: Various Options

======================================================================
TOTAL: 7/7 tests passed
======================================================================

🎉 ALL TESTS PASSED! 🎉

The REST Data Source is working correctly!
The partition bug fix is verified and functional.
```

---

## Quick Reference Commands

```bash
# Full test (from project directory)
cd /Users/yipman/Downloads/github/pyspark-data-sources
source test_venv/bin/activate
python3 test_rest_datasource.py

# Quick import test
source test_venv/bin/activate
python3 -c "from pyspark_datasources import RestDataSource; print('✅ OK')"

# Run examples
source test_venv/bin/activate
python3 examples/rest_example.py

# Clean restart (if needed)
deactivate
rm -rf test_venv
python3 -m venv test_venv
source test_venv/bin/activate
pip install pyspark
pip install dist/pyspark_data_sources-0.1.10-py3-none-any.whl
python3 test_rest_datasource.py
```

---

## Need Help?

If tests fail:

1. Check you're in the right directory: `pwd`
2. Check virtual environment is active: Look for `(test_venv)` in prompt
3. Check installations: `pip list | grep -E "pyspark|pyspark-data-sources"`
4. Try the clean restart commands above
5. Check the detailed output for error messages

---

## Summary

**Easiest way to test:**

```bash
cd /Users/yipman/Downloads/github/pyspark-data-sources
source test_venv/bin/activate
python3 test_rest_datasource.py
```

That's it! Three commands and you're testing the REST data source.

**Time required:** ~30 seconds (tests run quickly)

**What you'll see:** All tests passing with green checkmarks ✅
