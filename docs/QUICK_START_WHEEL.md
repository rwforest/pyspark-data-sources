# Quick Start: Using the Built Wheel

This guide shows you how to use the `pyspark-data-sources` wheel package that includes the new REST data source.

## Installation

### From the Built Wheel

```bash
# Install the wheel
pip install dist/pyspark_data_sources-0.1.10-py3-none-any.whl
```

### With Optional Dependencies

```bash
# Install with all extras
pip install "dist/pyspark_data_sources-0.1.10-py3-none-any.whl[all]"

# Or just specific ones
pip install "dist/pyspark_data_sources-0.1.10-py3-none-any.whl[faker]"
```

## Using the REST Data Source

### Example 1: Basic REST API Call

```python
from pyspark.sql import SparkSession
from pyspark_datasources import RestDataSource

# Create Spark session
spark = SparkSession.builder \
    .appName("REST Example") \
    .master("local[*]") \
    .getOrCreate()

# Register the REST data source
spark.dataSource.register(RestDataSource)

# Create input parameters
data = [
    ("Nevada", "nn"),
    ("Northern California", "pr"),
    ("Virgin Islands region", "pr")
]
input_df = spark.createDataFrame(data, ["region", "source"])
input_df.createOrReplaceTempView("search_params")

# Call the REST API in parallel
result_df = spark.read.format("rest") \
    .option("url", "https://soda.demo.socrata.com/resource/6yvf-kk3n.json") \
    .option("input", "search_params") \
    .option("method", "GET") \
    .option("partitions", "3") \
    .load()

# Show results
result_df.show(truncate=False)
```

### Example 2: POST with JSON

```python
from pyspark_datasources import RestDataSource

spark.dataSource.register(RestDataSource)

# Create data to POST
posts = [
    ("My First Post", "This is the content", 1),
    ("Another Post", "More content here", 1),
]
posts_df = spark.createDataFrame(posts, ["title", "body", "userId"])
posts_df.createOrReplaceTempView("new_posts")

# POST to API
result_df = spark.read.format("rest") \
    .option("url", "https://jsonplaceholder.typicode.com/posts") \
    .option("input", "new_posts") \
    .option("method", "POST") \
    .option("postInputFormat", "json") \
    .load()

result_df.show()
```

### Example 3: With Authentication

```python
from pyspark_datasources import RestDataSource

spark.dataSource.register(RestDataSource)

# Input data
queries = [("search term 1",), ("search term 2",)]
queries_df = spark.createDataFrame(queries, ["q"])
queries_df.createOrReplaceTempView("queries")

# Call with Bearer token
result_df = spark.read.format("rest") \
    .option("url", "https://api.github.com/search/repositories") \
    .option("input", "queries") \
    .option("method", "GET") \
    .option("authType", "Bearer") \
    .option("oauthToken", "your_github_token") \
    .load()

result_df.show()
```

## Complete Working Example

Save this as `test_rest_datasource.py`:

```python
#!/usr/bin/env python3
"""
Test script for REST Data Source
Requires: PySpark 4.0+
"""

from pyspark.sql import SparkSession
from pyspark_datasources import RestDataSource

def main():
    # Create Spark session
    print("Creating Spark session...")
    spark = SparkSession.builder \
        .appName("REST DataSource Test") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    # Register REST data source
    print("Registering REST data source...")
    spark.dataSource.register(RestDataSource)

    # Test 1: Basic GET request
    print("\n" + "="*60)
    print("Test 1: Basic GET Request to JSONPlaceholder")
    print("="*60)

    input_data = [(1,), (2,), (3,)]
    input_df = spark.createDataFrame(input_data, ["id"])
    input_df.createOrReplaceTempView("test_input")

    print("\nInput data:")
    input_df.show()

    print("\nCalling REST API...")
    result_df = spark.read.format("rest") \
        .option("url", "https://jsonplaceholder.typicode.com/posts") \
        .option("input", "test_input") \
        .option("method", "GET") \
        .option("partitions", "2") \
        .load()

    print("\nResults:")
    result_df.printSchema()
    result_df.show(5, truncate=False)

    print(f"\nTotal results: {result_df.count()}")

    # Test 2: POST request
    print("\n" + "="*60)
    print("Test 2: POST Request to JSONPlaceholder")
    print("="*60)

    post_data = [("Test Post", "This is a test", 1)]
    post_df = spark.createDataFrame(post_data, ["title", "body", "userId"])
    post_df.createOrReplaceTempView("post_input")

    print("\nData to POST:")
    post_df.show()

    print("\nPosting to API...")
    post_result_df = spark.read.format("rest") \
        .option("url", "https://jsonplaceholder.typicode.com/posts") \
        .option("input", "post_input") \
        .option("method", "POST") \
        .option("postInputFormat", "json") \
        .load()

    print("\nPOST Results:")
    post_result_df.show(truncate=False)

    # Success!
    print("\n" + "="*60)
    print("✅ All tests passed! REST Data Source is working.")
    print("="*60)

    spark.stop()

if __name__ == "__main__":
    main()
```

Run it:

```bash
python3 test_rest_datasource.py
```

## Available Data Sources

After installing the wheel, you have access to:

```python
from pyspark_datasources import (
    ArrowDataSource,
    FakeDataSource,
    GithubDataSource,
    GoogleSheetsDataSource,
    HuggingFaceDatasets,
    KaggleDataSource,
    OpenSkyDataSource,
    RestDataSource,        # ← New!
    RobinhoodDataSource,
    SalesforceDataSource,
    SimpleJsonDataSource,
    StockDataSource,
    JSONPlaceholderDataSource
)
```

## Common Use Cases

### 1. Validate Addresses in Bulk

```python
# Load addresses from database or file
addresses_df = spark.read.parquet("addresses.parquet")
addresses_df.createOrReplaceTempView("addresses")

# Validate with external API
validated = spark.read.format("rest") \
    .option("url", "https://api.validator.com/address") \
    .option("input", "addresses") \
    .option("method", "POST") \
    .load()
```

### 2. Enrich Customer Data

```python
# Load customer IDs
customers_df = spark.read.table("customers")
customers_df.select("customer_id").createOrReplaceTempView("ids")

# Enrich with external API
enriched = spark.read.format("rest") \
    .option("url", "https://api.enrichment.com/lookup") \
    .option("input", "ids") \
    .option("partitions", "10") \
    .load()
```

### 3. Batch Sentiment Analysis

```python
# Load texts to analyze
texts_df = spark.read.csv("reviews.csv", header=True)
texts_df.select("review_id", "text").createOrReplaceTempView("texts")

# Analyze sentiment
sentiments = spark.read.format("rest") \
    .option("url", "https://api.sentiment.com/analyze") \
    .option("input", "texts") \
    .option("method", "POST") \
    .option("userId", "api_key") \
    .option("userPassword", "api_secret") \
    .load()
```

## Configuration Reference

### Most Common Options

```python
spark.read.format("rest") \
    .option("url", "https://api.example.com")        # Required: API endpoint
    .option("input", "table_name")                    # Required: Input table
    .option("method", "GET")                          # GET, POST, PUT, DELETE
    .option("partitions", "10")                       # Parallel partitions
    .option("userId", "username")                     # Basic auth username
    .option("userPassword", "password")               # Basic auth password
    .option("authType", "Bearer")                     # Auth type
    .option("oauthToken", "token")                    # Bearer/OAuth token
    .option("connectionTimeout", "5000")              # Connection timeout (ms)
    .option("readTimeout", "10000")                   # Read timeout (ms)
    .option("includeInputsInOutput", "Y")             # Include inputs in output
    .load()
```

See [docs/rest-datasource.md](docs/rest-datasource.md) for complete reference.

## Verifying Installation

```python
# Check that REST data source is available
from pyspark_datasources import RestDataSource

print(f"Name: {RestDataSource.name()}")
print(f"Class: {RestDataSource}")
print("✅ REST Data Source is installed and ready!")
```

## Performance Tips

1. **Adjust Partitions**: Use 10-20 partitions for 100+ API calls
2. **Increase Timeouts**: For slow APIs, increase `readTimeout`
3. **Monitor Rate Limits**: Reduce partitions if hitting rate limits
4. **Cache Results**: Use `.cache()` if you'll use results multiple times

```python
# Good for 1000 API calls
result_df = spark.read.format("rest") \
    .option("url", "https://api.example.com") \
    .option("input", "large_table") \
    .option("partitions", "20") \
    .option("readTimeout", "30000") \
    .load() \
    .cache()  # Cache for reuse
```

## Troubleshooting

### Import Error

```bash
# Make sure package is installed
pip list | grep pyspark-data-sources

# Reinstall if needed
pip install --force-reinstall dist/pyspark_data_sources-0.1.10-py3-none-any.whl
```

### Spark Not Found

```bash
# Install PySpark
pip install pyspark>=4.0.0
```

### API Timeout

```python
# Increase timeouts
.option("connectionTimeout", "10000")
.option("readTimeout", "60000")
```

## More Examples

See the `examples/` directory for more:

```bash
# Run comprehensive examples
python3 examples/rest_example.py
```

## Documentation

- **User Guide**: [docs/rest-datasource.md](docs/rest-datasource.md)
- **Build Guide**: [BUILD_GUIDE.md](BUILD_GUIDE.md)
- **Migration Details**: [MIGRATION_SUMMARY.md](MIGRATION_SUMMARY.md)
- **Quick Reference**: [REST_DATASOURCE_README.md](REST_DATASOURCE_README.md)

## Getting Help

1. Check the documentation in `docs/`
2. Run example scripts in `examples/`
3. Review error messages in `_corrupt_records` column
4. Open an issue on GitHub

## Next Steps

1. ✅ Install the wheel
2. ✅ Run the test script
3. ✅ Try with your own API
4. ✅ Read the full documentation
5. ✅ Share feedback!

---

**Package**: `pyspark_data_sources-0.1.10-py3-none-any.whl`
**Includes**: REST Data Source with full functionality
**Requirements**: PySpark 4.0+, Python 3.9-3.14
**Status**: ✅ Ready for use
