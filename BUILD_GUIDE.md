# Build Guide for pyspark-data-sources

This guide explains how to build the `pyspark-data-sources` package as a wheel distribution.

## Prerequisites

- Python 3.9-3.14
- pip (Python package installer)
- Virtual environment (recommended)

## Build Methods

### Method 1: Using Poetry (Recommended)

Poetry is the primary build tool for this project.

#### Step 1: Create a Virtual Environment

```bash
# Create virtual environment
python3 -m venv .venv

# Activate it
source .venv/bin/activate  # On Linux/macOS
# or
.venv\Scripts\activate  # On Windows
```

#### Step 2: Install Poetry

```bash
pip install poetry
```

#### Step 3: Build the Package

```bash
poetry build
```

This will create two files in the `dist/` directory:
- `pyspark_data_sources-0.1.10-py3-none-any.whl` (wheel package)
- `pyspark_data_sources-0.1.10.tar.gz` (source distribution)

#### Step 4: Verify the Build

```bash
ls -lh dist/
```

You should see:
```
pyspark_data_sources-0.1.10-py3-none-any.whl
pyspark_data_sources-0.1.10.tar.gz
```

### Method 2: Using Python Build Module

If you prefer not to use Poetry:

#### Step 1: Create and Activate Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate  # Linux/macOS
```

#### Step 2: Install Build Tools

```bash
pip install build
```

#### Step 3: Build the Package

```bash
python -m build
```

This will also create the wheel and source distribution in `dist/`.

## Installing the Built Wheel

After building, you can install the wheel locally:

```bash
pip install dist/pyspark_data_sources-0.1.10-py3-none-any.whl
```

Or install with optional dependencies:

```bash
# Install with specific extras
pip install dist/pyspark_data_sources-0.1.10-py3-none-any.whl[faker]

# Install with all extras
pip install dist/pyspark_data_sources-0.1.10-py3-none-any.whl[all]
```

## Testing the Installation

After installation, test that the REST data source is available:

```python
from pyspark.sql import SparkSession
from pyspark_datasources import RestDataSource

spark = SparkSession.builder.appName("test").getOrCreate()
spark.dataSource.register(RestDataSource)

print("✅ RestDataSource successfully imported and registered!")
```

## Inspecting the Wheel Contents

To see what's inside the wheel:

```bash
# Using Python's zipfile module
python -m zipfile -l dist/pyspark_data_sources-0.1.10-py3-none-any.whl

# Or using unzip (if available)
unzip -l dist/pyspark_data_sources-0.1.10-py3-none-any.whl
```

The wheel should contain:
```
pyspark_datasources/
  __init__.py
  arrow.py
  fake.py
  github.py
  googlesheets.py
  huggingface.py
  kaggle.py
  opensky.py
  rest.py          ← Our new REST data source
  robinhood.py
  salesforce.py
  simplejson.py
  stock.py
  jsonplaceholder.py
  ...
```

## Development Installation

For development purposes (changes reflect immediately):

```bash
# Using Poetry
poetry install

# Or using pip
pip install -e .
```

## Publishing to PyPI

To publish to PyPI (requires credentials):

```bash
# Using Poetry
poetry publish

# Or using twine
pip install twine
python -m twine upload dist/*
```

## Building Specific Versions

To build a specific version, update `pyproject.toml`:

```toml
[tool.poetry]
version = "0.1.11"  # Change this
```

Then rebuild:

```bash
poetry build
```

## Troubleshooting

### Issue: "Current Python version is not allowed"

**Problem**: Your Python version doesn't match the constraint in `pyproject.toml`

**Solution**: Update the Python version constraint in `pyproject.toml`:

```toml
[tool.poetry.dependencies]
python = ">=3.9,<3.15"  # Adjust as needed
```

### Issue: "externally-managed-environment"

**Problem**: System Python is protected (common on macOS/Linux)

**Solution**: Always use a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install poetry
```

### Issue: Poetry not found

**Problem**: Poetry is not installed or not in PATH

**Solution**: Install Poetry:

```bash
# Using pip
pip install poetry

# Or using pipx (recommended)
pipx install poetry

# Or using the official installer
curl -sSL https://install.python-poetry.org | python3 -
```

### Issue: Missing Dependencies

**Problem**: Build fails due to missing dependencies

**Solution**: Install all dependencies first:

```bash
poetry install
```

### Issue: Permission Denied

**Problem**: Cannot write to dist/ directory

**Solution**: Check permissions or use a different directory:

```bash
# Remove old dist
rm -rf dist/

# Rebuild
poetry build
```

## Clean Build

To start fresh:

```bash
# Remove built artifacts
rm -rf dist/ build/ *.egg-info

# Remove virtual environment
rm -rf .venv

# Recreate environment and rebuild
python3 -m venv .venv
source .venv/bin/activate
pip install poetry
poetry build
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Build and Test

on: [push, pull_request]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2

      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.11'

      - name: Install Poetry
        run: pip install poetry

      - name: Build package
        run: poetry build

      - name: Upload artifacts
        uses: actions/upload-artifact@v2
        with:
          name: distributions
          path: dist/
```

## Verifying the REST Data Source

After building and installing, verify the REST data source:

```bash
# Install the wheel
pip install dist/pyspark_data_sources-0.1.10-py3-none-any.whl

# Run a quick test
python3 << 'EOF'
from pyspark_datasources.rest import RestDataSource
print(f"✅ RestDataSource name: {RestDataSource.name()}")
print(f"✅ RestDataSource class: {RestDataSource}")
print("✅ REST data source is ready to use!")
EOF
```

## Build Artifacts

After a successful build, you'll have:

1. **Wheel file** (`.whl`): Universal Python wheel, can be installed with pip
   - Format: `pyspark_data_sources-{version}-py3-none-any.whl`
   - Size: ~45-50 KB (with REST data source)

2. **Source distribution** (`.tar.gz`): Source code archive
   - Format: `pyspark_data_sources-{version}.tar.gz`
   - Size: ~38-40 KB

## File Checksums

To verify integrity:

```bash
# Generate checksums
cd dist/
sha256sum *.whl *.tar.gz > SHA256SUMS

# Verify later
sha256sum -c SHA256SUMS
```

## Next Steps

1. **Test locally**: Install and test the wheel
2. **Run unit tests**: Ensure all tests pass
3. **Test examples**: Run example scripts
4. **Update version**: Bump version for release
5. **Publish**: Upload to PyPI (if releasing)

## Additional Resources

- [Poetry Documentation](https://python-poetry.org/docs/)
- [Python Packaging Guide](https://packaging.python.org/)
- [PEP 517 - Build System](https://www.python.org/dev/peps/pep-0517/)
- [Wheel Format](https://www.python.org/dev/peps/pep-0427/)

## Summary

**Quick Build Command:**
```bash
python3 -m venv .venv && \
source .venv/bin/activate && \
pip install poetry && \
poetry build
```

**Output:**
- `dist/pyspark_data_sources-0.1.10-py3-none-any.whl` ✅
- `dist/pyspark_data_sources-0.1.10.tar.gz` ✅

**Includes REST Data Source:** ✅
- `pyspark_datasources/rest.py` (18.5 KB)
- Full REST API calling capabilities
- All authentication methods
- Examples and documentation
