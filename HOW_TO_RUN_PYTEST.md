# How to Run Pytest Tests for REST Data Source

This guide shows you how to run the pytest test suite for the REST data source.

## Quick Start

### Step 1: Set Up Environment

```bash
cd /Users/yipman/Downloads/github/pyspark-data-sources

# Create virtual environment (if not exists)
python3 -m venv .venv

# Activate it
source .venv/bin/activate
```

### Step 2: Install Dependencies

```bash
# Install pytest
pip install pytest

# Install PySpark
pip install pyspark

# Install the REST data source wheel
pip install dist/pyspark_data_sources-0.1.10-py3-none-any.whl
```

### Step 3: Run Pytest Tests

```bash
# Run all REST data source tests
pytest tests/test_rest.py -v

# Or run specific test
pytest tests/test_rest.py::test_partition_bug_fix -v

# Or run all tests in the tests directory
pytest tests/ -v
```

## Expected Output

When tests pass, you'll see:

```
tests/test_rest.py::test_rest_datasource_registration PASSED
tests/test_rest.py::test_partition_bug_fix PASSED              ← Bug fix verified!
tests/test_rest.py::test_basic_get_request PASSED
tests/test_rest.py::test_post_request PASSED
tests/test_rest.py::test_missing_required_options PASSED
tests/test_rest.py::test_parallel_execution PASSED
tests/test_rest.py::test_include_inputs_option PASSED
tests/test_rest.py::test_custom_headers PASSED
tests/test_rest.py::test_schema_inference PASSED
tests/test_rest.py::test_timeout_options PASSED
tests/test_rest.py::test_querystring_type PASSED

===================== 11 passed in X.XXs =====================
```

## What Tests Are Included

The pytest suite (`tests/test_rest.py`) includes:

1. **test_rest_datasource_registration** - Verifies registration works
2. **test_partition_bug_fix** - **Verifies the critical bug fix** ⭐
3. **test_basic_get_request** - Tests GET requests
4. **test_post_request** - Tests POST requests with JSON
5. **test_missing_required_options** - Tests error handling
6. **test_parallel_execution** - Tests multiple partitions
7. **test_include_inputs_option** - Tests includeInputsInOutput option
8. **test_custom_headers** - Tests custom HTTP headers
9. **test_schema_inference** - Tests schema inference
10. **test_timeout_options** - Tests timeout configuration
11. **test_querystring_type** - Tests query string handling

## Running Specific Tests

### Run Only the Bug Fix Test

```bash
pytest tests/test_rest.py::test_partition_bug_fix -v
```

### Run Only GET/POST Tests

```bash
pytest tests/test_rest.py -k "get_request or post_request" -v
```

### Run with More Detail

```bash
# Show print statements
pytest tests/test_rest.py -v -s

# Show local variables on failure
pytest tests/test_rest.py -v -l

# Stop on first failure
pytest tests/test_rest.py -v -x
```

## Pytest vs Standalone Test Script

We have two test approaches:

### 1. Pytest (tests/test_rest.py)
- **Use for**: Unit testing during development
- **Pros**: Industry standard, fixtures, detailed output
- **Cons**: Requires pytest installation

```bash
source .venv/bin/activate
pytest tests/test_rest.py -v
```

### 2. Standalone Script (test_rest_datasource.py)
- **Use for**: Quick validation, CI/CD, demonstrations
- **Pros**: No pytest needed, comprehensive output
- **Cons**: Less detailed test isolation

```bash
source .venv/bin/activate
python3 test_rest_datasource.py
```

## One-Line Setup and Test

```bash
cd /Users/yipman/Downloads/github/pyspark-data-sources && \
python3 -m venv .venv && \
source .venv/bin/activate && \
pip install pytest pyspark dist/pyspark_data_sources-0.1.10-py3-none-any.whl && \
pytest tests/test_rest.py -v
```

## Troubleshooting

### Issue: "No module named 'pytest'"

```bash
source .venv/bin/activate
pip install pytest
```

### Issue: "No module named 'pyspark'"

```bash
source .venv/bin/activate
pip install pyspark
```

### Issue: "No module named 'pyspark_datasources'"

```bash
source .venv/bin/activate
pip install dist/pyspark_data_sources-0.1.10-py3-none-any.whl
```

### Issue: Tests fail with connection errors

This is expected if you're offline. The tests make real HTTP requests to `jsonplaceholder.typicode.com`.

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Test REST Data Source

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2

      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install pytest pyspark
          pip install dist/pyspark_data_sources-0.1.10-py3-none-any.whl

      - name: Run tests
        run: pytest tests/test_rest.py -v
```

## Test Coverage

To check test coverage:

```bash
pip install pytest-cov
pytest tests/test_rest.py --cov=pyspark_datasources.rest --cov-report=html
```

Then open `htmlcov/index.html` in your browser.

## Summary

**Quickest way to run pytest tests:**

```bash
# From project directory
source .venv/bin/activate
pytest tests/test_rest.py -v
```

**What you're testing:**
- ✅ Partition bug fix (the critical fix)
- ✅ GET and POST requests
- ✅ All configuration options
- ✅ Error handling
- ✅ Schema inference

**Time required:** ~10-30 seconds

**Expected result:** All 11 tests pass
