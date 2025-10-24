# Documentation Index - REST Data Source

Complete guide to the REST Data Source for PySpark. Start here to find what you need!

## 🚀 Getting Started

**New to REST Data Source?** Start with these:

1. **[QUICK_START_WHEEL.md](QUICK_START_WHEEL.md)** - How to install and use the wheel
2. **[REST_DATASOURCE_README.md](REST_DATASOURCE_README.md)** - Quick reference guide
3. **[examples/rest_example.py](examples/rest_example.py)** - Working code examples

**Time needed**: 5-10 minutes to get started

---

## 📦 Installation & Setup

### Local Setup

- **[BUILD_GUIDE.md](BUILD_GUIDE.md)** - How to build the wheel from source
- **[WHEEL_BUILD_SUMMARY.md](WHEEL_BUILD_SUMMARY.md)** - What's in the built wheel

### Databricks Setup

- **[DATABRICKS_SETUP.md](DATABRICKS_SETUP.md)** ⭐ - Complete Databricks installation guide
  - Upload wheel to Databricks
  - Install on clusters
  - Test notebooks
  - Real-world examples
  - Secrets management

**Choose**: Local (build yourself) or Databricks (use the wheel)

---

## 🧪 Testing

### Quick Testing

- **[TESTING_QUICK_START.txt](TESTING_QUICK_START.txt)** - 3 commands to test
- **[RUN_TESTS.sh](RUN_TESTS.sh)** - Automated test script

### Comprehensive Testing

- **[HOW_TO_TEST.md](HOW_TO_TEST.md)** - Detailed testing guide (local)
- **[HOW_TO_RUN_PYTEST.md](HOW_TO_RUN_PYTEST.md)** - Using pytest framework
- **[TESTING_SUMMARY.md](TESTING_SUMMARY.md)** - Overview of all test approaches

### Test Files

- **[test_rest_datasource.py](test_rest_datasource.py)** - Standalone test script (7 tests)
- **[tests/test_rest.py](tests/test_rest.py)** - Pytest suite (11 tests)
- **[TEST_RESULTS.md](TEST_RESULTS.md)** - Previous test results

**Choose**: Quick (use RUN_TESTS.sh) or Comprehensive (read HOW_TO_TEST.md)

---

## 📖 Complete Documentation

### User Guides

- **[docs/rest-datasource.md](docs/rest-datasource.md)** ⭐ - **COMPLETE USER GUIDE**
  - All configuration options
  - Examples for every feature
  - Best practices
  - Use cases
  - Troubleshooting

### API Reference

- **[pyspark_datasources/rest.py](pyspark_datasources/rest.py)** - Source code with docstrings
- **RestDataSource class** - Main data source class
- **RestReader class** - Handles API calls

### Migration Information

- **[MIGRATION_SUMMARY.md](MIGRATION_SUMMARY.md)** - Scala to Python migration details
- What was migrated
- Key differences
- Improvements over Scala version

---

## 🐛 Bug Fix Documentation

### Partition Bug Fix

- **[BUGFIX_PARTITIONS.md](BUGFIX_PARTITIONS.md)** - Detailed bug fix explanation
  - What the bug was
  - How it was fixed
  - How to verify the fix

**Issue**: `TypeError: 'int' object is not callable`
**Status**: ✅ Fixed and verified

---

## 💡 Examples

### Code Examples

- **[examples/rest_example.py](examples/rest_example.py)** - 10 working examples
  1. Basic GET request
  2. POST with JSON
  3. Basic authentication
  4. Bearer token
  5. Custom headers
  6. Parallel processing
  7. CSV input
  8. OAuth1
  9. Inline URLs
  10. Without input in output

### Databricks Examples

- **[DATABRICKS_SETUP.md](DATABRICKS_SETUP.md)** - Includes:
  - Customer data enrichment
  - Address validation
  - Batch API lookups
  - Using secrets for API keys

---

## 🎯 Quick Lookup

### I want to...

| Goal | Document | Section |
|------|----------|---------|
| **Install the wheel** | [QUICK_START_WHEEL.md](QUICK_START_WHEEL.md) | Installation |
| **Test it locally** | [HOW_TO_TEST.md](HOW_TO_TEST.md) | Quick Start |
| **Use in Databricks** | [DATABRICKS_SETUP.md](DATABRICKS_SETUP.md) | Complete guide |
| **Run tests** | [TESTING_QUICK_START.txt](TESTING_QUICK_START.txt) | Option 1 or 2 |
| **See examples** | [examples/rest_example.py](examples/rest_example.py) | All examples |
| **Understand options** | [docs/rest-datasource.md](docs/rest-datasource.md) | Configuration |
| **Troubleshoot** | [docs/rest-datasource.md](docs/rest-datasource.md) | Troubleshooting |
| **Build from source** | [BUILD_GUIDE.md](BUILD_GUIDE.md) | Full guide |
| **Verify bug fix** | [BUGFIX_PARTITIONS.md](BUGFIX_PARTITIONS.md) | Complete details |
| **Use with secrets** | [DATABRICKS_SETUP.md](DATABRICKS_SETUP.md) | Secrets section |

---

## 📚 Documentation by Role

### For Data Engineers

1. Start: [DATABRICKS_SETUP.md](DATABRICKS_SETUP.md)
2. Learn: [docs/rest-datasource.md](docs/rest-datasource.md)
3. Examples: See "Real-World Examples" in Databricks guide
4. Best Practices: [docs/rest-datasource.md](docs/rest-datasource.md) - Best Practices section

### For Developers

1. Start: [BUILD_GUIDE.md](BUILD_GUIDE.md)
2. Test: [HOW_TO_RUN_PYTEST.md](HOW_TO_RUN_PYTEST.md)
3. Code: [pyspark_datasources/rest.py](pyspark_datasources/rest.py)
4. Contribute: [MIGRATION_SUMMARY.md](MIGRATION_SUMMARY.md)

### For DevOps/MLOps

1. Setup: [DATABRICKS_SETUP.md](DATABRICKS_SETUP.md) - CLI method
2. Test: [test_rest_datasource.py](test_rest_datasource.py)
3. CI/CD: [HOW_TO_RUN_PYTEST.md](HOW_TO_RUN_PYTEST.md) - CI/CD section
4. Monitor: [docs/rest-datasource.md](docs/rest-datasource.md) - Error Handling

### For QA/Testers

1. Tests: [TESTING_SUMMARY.md](TESTING_SUMMARY.md)
2. Run: [TESTING_QUICK_START.txt](TESTING_QUICK_START.txt)
3. Results: [TEST_RESULTS.md](TEST_RESULTS.md)
4. Verify: [BUGFIX_PARTITIONS.md](BUGFIX_PARTITIONS.md)

---

## 🎓 Learning Path

### Beginner (Never used before)

1. Read: [REST_DATASOURCE_README.md](REST_DATASOURCE_README.md) (5 min)
2. Install: Follow [QUICK_START_WHEEL.md](QUICK_START_WHEEL.md) (5 min)
3. Try: Run one example from [examples/rest_example.py](examples/rest_example.py) (10 min)

**Total time**: ~20 minutes

### Intermediate (Some experience)

1. Review: [docs/rest-datasource.md](docs/rest-datasource.md) - Configuration (15 min)
2. Setup: [DATABRICKS_SETUP.md](DATABRICKS_SETUP.md) (15 min)
3. Test: Run real API calls with your data (30 min)

**Total time**: ~1 hour

### Advanced (Production deployment)

1. Study: [docs/rest-datasource.md](docs/rest-datasource.md) - Complete guide (30 min)
2. Understand: [MIGRATION_SUMMARY.md](MIGRATION_SUMMARY.md) (15 min)
3. Deploy: Use [DATABRICKS_SETUP.md](DATABRICKS_SETUP.md) - Production section (45 min)
4. Monitor: Implement error tracking and monitoring (varies)

**Total time**: ~2 hours + monitoring setup

---

## 🔍 Search by Topic

### Authentication

- Basic Auth: [docs/rest-datasource.md](docs/rest-datasource.md) - Example 3
- Bearer Token: [docs/rest-datasource.md](docs/rest-datasource.md) - Example 4
- OAuth1: [docs/rest-datasource.md](docs/rest-datasource.md) - Example 5
- Secrets (Databricks): [DATABRICKS_SETUP.md](DATABRICKS_SETUP.md) - Using Secrets

### HTTP Methods

- GET: [examples/rest_example.py](examples/rest_example.py) - Example 1
- POST: [examples/rest_example.py](examples/rest_example.py) - Example 2
- PUT/DELETE: [docs/rest-datasource.md](docs/rest-datasource.md) - HTTP Methods

### Performance

- Parallelism: [docs/rest-datasource.md](docs/rest-datasource.md) - Best Practices
- Rate Limiting: [docs/rest-datasource.md](docs/rest-datasource.md) - Performance Tips
- Large Datasets: [DATABRICKS_SETUP.md](DATABRICKS_SETUP.md) - Optimize section

### Error Handling

- Corrupt Records: [docs/rest-datasource.md](docs/rest-datasource.md) - Handling Errors
- Timeouts: [docs/rest-datasource.md](docs/rest-datasource.md) - Troubleshooting
- Retry Logic: [docs/rest-datasource.md](docs/rest-datasource.md) - Future Enhancements

---

## 📋 Complete File List

### Core Files

```
pyspark_datasources/
├── rest.py                     # Main implementation (581 lines)
└── __init__.py                 # Exports RestDataSource

dist/
└── pyspark_data_sources-0.1.10-py3-none-any.whl  # Built wheel (45 KB)
```

### Documentation (20 files)

```
docs/
└── rest-datasource.md          # Complete user guide

examples/
└── rest_example.py             # 10 working examples

tests/
└── test_rest.py                # Pytest suite (11 tests)

Root Directory:
├── DATABRICKS_SETUP.md         # ⭐ Databricks guide
├── BUILD_GUIDE.md              # Build instructions
├── BUGFIX_PARTITIONS.md        # Bug fix details
├── DOCUMENTATION_INDEX.md      # This file
├── HOW_TO_TEST.md              # Local testing guide
├── HOW_TO_RUN_PYTEST.md        # Pytest guide
├── MIGRATION_SUMMARY.md        # Migration details
├── QUICK_START_WHEEL.md        # Quick start guide
├── REST_DATASOURCE_README.md   # Quick reference
├── RUN_TESTS.sh                # Test runner script
├── TEST_RESULTS.md             # Test results
├── TESTING_QUICK_START.txt     # 3-command testing
├── TESTING_SUMMARY.md          # Test overview
├── WHEEL_BUILD_SUMMARY.md      # Build summary
├── pyproject.toml              # Build configuration
└── test_rest_datasource.py     # Standalone tests (7 tests)
```

---

## ⚡ Quick Commands

```bash
# Test locally
cd /Users/yipman/Downloads/github/pyspark-data-sources
./RUN_TESTS.sh

# Test with pytest
source .venv/bin/activate
pytest tests/test_rest.py -v

# Build wheel
poetry build

# View documentation
cat TESTING_QUICK_START.txt
```

---

## 📞 Getting Help

1. **Quick questions**: Check [REST_DATASOURCE_README.md](REST_DATASOURCE_README.md)
2. **How-to guides**: See [HOW_TO_TEST.md](HOW_TO_TEST.md) or [DATABRICKS_SETUP.md](DATABRICKS_SETUP.md)
3. **Detailed info**: Read [docs/rest-datasource.md](docs/rest-datasource.md)
4. **Examples**: Run [examples/rest_example.py](examples/rest_example.py)
5. **Troubleshooting**: Check relevant guide's troubleshooting section

---

## ✅ Status

- **Migration**: ✅ Complete (Scala → Python)
- **Bug Fix**: ✅ Fixed and verified (partition conflict)
- **Build**: ✅ Wheel created and tested
- **Documentation**: ✅ Complete (20 documents)
- **Testing**: ✅ All tests passing (7/7 standalone, 11/11 pytest)
- **Databricks**: ✅ Setup guide complete

**Package**: `pyspark_data_sources-0.1.10-py3-none-any.whl`
**Status**: ✅ Production Ready

---

## 🎯 Next Steps

1. **For first-time users**: Read [QUICK_START_WHEEL.md](QUICK_START_WHEEL.md)
2. **For Databricks users**: Follow [DATABRICKS_SETUP.md](DATABRICKS_SETUP.md)
3. **For testing**: Run `./RUN_TESTS.sh`
4. **For development**: Check [BUILD_GUIDE.md](BUILD_GUIDE.md)

**Everything you need is documented. Pick your starting point above!**
