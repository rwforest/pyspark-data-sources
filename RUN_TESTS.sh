#!/bin/bash
#
# Simple script to run REST Data Source tests
# Usage: ./RUN_TESTS.sh
#

set -e  # Exit on error

echo "=============================================="
echo "REST Data Source - Test Runner"
echo "=============================================="
echo ""

# Navigate to script directory
cd "$(dirname "$0")"

# Check if test_venv exists
if [ ! -d "test_venv" ]; then
    echo "❌ Virtual environment not found!"
    echo ""
    echo "Please run setup first:"
    echo "  python3 -m venv test_venv"
    echo "  source test_venv/bin/activate"
    echo "  pip install pyspark"
    echo "  pip install dist/pyspark_data_sources-0.1.10-py3-none-any.whl"
    echo ""
    exit 1
fi

# Activate virtual environment
echo "✓ Activating virtual environment..."
source test_venv/bin/activate

# Check if PySpark is installed
if ! python3 -c "import pyspark" 2>/dev/null; then
    echo "❌ PySpark not installed!"
    echo ""
    echo "Installing PySpark..."
    pip install pyspark
fi

# Check if pyspark-data-sources is installed
if ! python3 -c "import pyspark_datasources" 2>/dev/null; then
    echo "❌ pyspark-data-sources not installed!"
    echo ""
    echo "Installing from wheel..."
    pip install dist/pyspark_data_sources-0.1.10-py3-none-any.whl
fi

echo "✓ All dependencies installed"
echo ""
echo "=============================================="
echo "Running Tests..."
echo "=============================================="
echo ""

# Run the tests
python3 test_rest_datasource.py

echo ""
echo "=============================================="
echo "Test run complete!"
echo "=============================================="
