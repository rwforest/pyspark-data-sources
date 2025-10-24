# REST Data Source

The REST Data Source enables you to call REST APIs in parallel for multiple sets of input parameters and collate the results in a Spark DataFrame. This is a Python implementation of the original [spark-datasource-rest](https://github.com/Data-Science-Extensions/Data-Science-Extensions/tree/master/spark-datasource-rest) (Scala) library.

## Overview

REST-based services (e.g., address validation services, Google Search API, Watson Natural Language Processing API) typically accept one set of input parameters at a time and return corresponding results. For many data science problems, the same API needs to be called multiple times with different input parameters. This data source supports calling target service APIs in a distributed, parallel manner.

### Key Features

- **Parallel Execution**: Make multiple API calls in parallel using Spark's distributed computing
- **Schema Inference**: Automatically infer the DataFrame schema from API responses
- **Multiple HTTP Methods**: Support for GET, POST, PUT, and DELETE requests
- **Authentication**: Built-in support for Basic Auth, OAuth1, and Bearer tokens
- **Flexible Input**: Read input parameters from any Spark DataFrame or temporary table
- **Result Combination**: Automatically combine input parameters with API responses
- **Error Handling**: Capture failed API calls in `_corrupt_records` column

## Installation

The REST data source is included in the base `pyspark-data-sources` package:

```bash
pip install pyspark-data-sources
```

The required dependencies (`requests` and `requests-oauthlib`) are automatically installed.

## Quick Start

```python
from pyspark.sql import SparkSession
from pyspark_datasources import RestDataSource

# Create Spark session
spark = SparkSession.builder.appName("rest-demo").getOrCreate()

# Register the data source
spark.dataSource.register(RestDataSource)

# Create input parameters
input_data = [("param1", "value1"), ("param2", "value2")]
input_df = spark.createDataFrame(input_data, ["key", "value"])
input_df.createOrReplaceTempView("api_params")

# Call REST API for each input row
result_df = spark.read.format("rest") \
    .option("url", "https://api.example.com/endpoint") \
    .option("input", "api_params") \
    .option("method", "GET") \
    .load()

result_df.show()
```

## Configuration Options

### Required Options

| Option | Description |
|--------|-------------|
| `url` | The target REST API URL. Can include common query parameters. |
| `input` | Name of a temporary Spark table containing input parameters. Column names should match API parameter names. |

### Optional Options

#### HTTP Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `method` | `POST` | HTTP method: GET, POST, PUT, DELETE |
| `connectionTimeout` | `1000` | Connection timeout in milliseconds |
| `readTimeout` | `5000` | Read timeout in milliseconds |
| `partitions` | `2` | Number of partitions for parallel execution |

#### Authentication

| Option | Default | Description |
|--------|---------|-------------|
| `authType` | `Basic` | Authentication type: Basic, OAuth1, Bearer |
| `userId` | - | Username for Basic authentication |
| `userPassword` | - | Password for Basic authentication |
| `oauthConsumerKey` | - | OAuth1 consumer key |
| `oauthConsumerSecret` | - | OAuth1 consumer secret |
| `oauthToken` | - | OAuth1 token (also used as Bearer token when authType='Bearer') |
| `oauthTokenSecret` | - | OAuth1 token secret |

#### Request Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `postInputFormat` | `json` | Format for POST data: json, form |
| `queryType` | `querystring` | How to append GET parameters: querystring, inline |
| `headers` | - | Additional HTTP headers in JSON format |
| `cookie` | - | Cookie to send with request (format: 'name=value') |

#### Output Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `includeInputsInOutput` | `Y` | Include input parameters in output DataFrame (Y/N) |
| `schemaSamplePcnt` | `30` | Percentage of input records to use for schema inference (minimum 3) |
| `callStrictlyOnce` | `N` | If 'Y', calls API only once per input and caches results (useful for paid APIs) |

## Examples

### Example 1: Basic GET Request

Call a REST API with query parameters:

```python
from pyspark_datasources import RestDataSource

spark.dataSource.register(RestDataSource)

# Create input parameters
input_data = [
    ("Nevada", "nn"),
    ("Northern California", "pr"),
    ("Virgin Islands region", "pr")
]
input_df = spark.createDataFrame(input_data, ["region", "source"])
input_df.createOrReplaceTempView("search_params")

# Call API with GET method
df = spark.read.format("rest") \
    .option("url", "https://soda.demo.socrata.com/resource/6yvf-kk3n.json") \
    .option("input", "search_params") \
    .option("method", "GET") \
    .option("partitions", "3") \
    .load()

df.show()
```

### Example 2: POST Request with JSON

Send JSON data via POST:

```python
# Input data for POST requests
input_data = [
    ("Test Post 1", "This is the body", 1),
    ("Test Post 2", "Another body", 1),
]
input_df = spark.createDataFrame(input_data, ["title", "body", "userId"])
input_df.createOrReplaceTempView("posts_to_create")

# POST with JSON body
df = spark.read.format("rest") \
    .option("url", "https://jsonplaceholder.typicode.com/posts") \
    .option("input", "posts_to_create") \
    .option("method", "POST") \
    .option("postInputFormat", "json") \
    .load()

df.show()
```

### Example 3: Basic Authentication

Use username and password authentication:

```python
# Create input
input_data = [("query1",), ("query2",)]
input_df = spark.createDataFrame(input_data, ["q"])
input_df.createOrReplaceTempView("queries")

# Call with Basic Auth
df = spark.read.format("rest") \
    .option("url", "https://api.example.com/search") \
    .option("input", "queries") \
    .option("method", "GET") \
    .option("userId", "myusername") \
    .option("userPassword", "mypassword") \
    .load()
```

### Example 4: Bearer Token Authentication

Use a Bearer token for authentication:

```python
df = spark.read.format("rest") \
    .option("url", "https://api.github.com/user/repos") \
    .option("input", "params_table") \
    .option("method", "GET") \
    .option("authType", "Bearer") \
    .option("oauthToken", "ghp_your_github_token_here") \
    .load()
```

### Example 5: OAuth1 Authentication

Use OAuth1 for services like Twitter:

```python
df = spark.read.format("rest") \
    .option("url", "https://api.twitter.com/1.1/search/tweets.json") \
    .option("input", "search_terms") \
    .option("method", "GET") \
    .option("authType", "OAuth1") \
    .option("oauthConsumerKey", "your_consumer_key") \
    .option("oauthConsumerSecret", "your_consumer_secret") \
    .option("oauthToken", "your_token") \
    .option("oauthTokenSecret", "your_token_secret") \
    .load()
```

### Example 6: Custom Headers

Add custom HTTP headers:

```python
custom_headers = '{"X-API-Key": "your-key", "X-Custom-Header": "value"}'

df = spark.read.format("rest") \
    .option("url", "https://api.example.com/data") \
    .option("input", "params") \
    .option("method", "GET") \
    .option("headers", custom_headers) \
    .load()
```

### Example 7: Reading Input from CSV

Use a CSV file as the source of input parameters:

```python
# Read CSV file
input_df = spark.read.option("header", "true").csv("path/to/input.csv")
input_df.createOrReplaceTempView("csv_params")

# Use CSV data for API calls
df = spark.read.format("rest") \
    .option("url", "https://api.example.com/lookup") \
    .option("input", "csv_params") \
    .option("method", "GET") \
    .load()
```

### Example 8: Parallel Processing of Many Records

Process a large number of API calls efficiently:

```python
# Create many input parameters (e.g., 1000 user IDs)
input_data = [(i,) for i in range(1, 1001)]
input_df = spark.createDataFrame(input_data, ["userId"])
input_df.createOrReplaceTempView("user_ids")

# Process with 20 partitions for high parallelism
df = spark.read.format("rest") \
    .option("url", "https://api.example.com/users") \
    .option("input", "user_ids") \
    .option("method", "GET") \
    .option("partitions", "20") \
    .load()

print(f"Processed {df.count()} API calls")
```

### Example 9: Inline URL Parameters

Use inline URL parameters instead of query strings:

```python
# URL template with placeholders
# Input: {"id": "123", "type": "user"}
# Result: https://api.example.com/user/123

df = spark.read.format("rest") \
    .option("url", "https://api.example.com/{type}/{id}") \
    .option("input", "params") \
    .option("method", "GET") \
    .option("queryType", "inline") \
    .load()
```

### Example 10: Without Input in Output

Get only API responses without input parameters:

```python
df = spark.read.format("rest") \
    .option("url", "https://api.example.com/data") \
    .option("input", "params") \
    .option("method", "GET") \
    .option("includeInputsInOutput", "N") \
    .load()

# DataFrame will only contain 'output' column
```

## Output Structure

The DataFrame returned by the REST Data Source has the following structure:

### With Input Parameters (default)

```
root
 |-- param1: string (nullable = true)
 |-- param2: string (nullable = true)
 |-- output: struct/array (nullable = true)
 |    |-- field1: string (nullable = true)
 |    |-- field2: long (nullable = true)
 |    |-- ...
 |-- _corrupt_records: string (nullable = true)
```

- **Input columns**: Original parameter columns from the input table
- **output**: API response (structure depends on the API)
- **_corrupt_records**: Contains error messages for failed API calls

### Without Input Parameters

```
root
 |-- output: struct/array (nullable = true)
 |    |-- field1: string (nullable = true)
 |    |-- field2: long (nullable = true)
```

## Processing Results

### Extracting Data from Responses

```python
# If output is a struct
df.select("param1", "output.field1", "output.field2").show()

# If output is an array, use explode
from pyspark.sql.functions import explode

df.select("param1", explode("output").alias("item")).show()

# Using SQL with inline for arrays
df.createOrReplaceTempView("results")
spark.sql("SELECT param1, inline(output) FROM results").show()
```

### Handling Errors

```python
# Filter successful calls
successful = df.filter(df._corrupt_records.isNull())

# Filter failed calls
failed = df.filter(df._corrupt_records.isNotNull())

# Show errors
failed.select("param1", "_corrupt_records").show(truncate=False)
```

## Best Practices

### 1. Optimize Parallelism

```python
# For 1000 API calls, use 20-50 partitions
.option("partitions", "20")
```

Adjust based on:
- Number of API calls
- API rate limits
- Cluster resources

### 2. Handle Rate Limits

```python
# Reduce parallelism for rate-limited APIs
.option("partitions", "2")

# Increase timeouts
.option("readTimeout", "30000")
```

### 3. Use callStrictlyOnce for Paid APIs

```python
# Cache results to avoid duplicate calls
.option("callStrictlyOnce", "Y")
```

**Warning**: This caches all results in memory, so use carefully with large datasets.

### 4. Sample Schema Inference

```python
# For large input sets, use a smaller sample
.option("schemaSamplePcnt", "10")

# For diverse responses, use a larger sample
.option("schemaSamplePcnt", "50")
```

### 5. Error Handling

Always check for corrupt records:

```python
# Count failures
num_failures = df.filter(df._corrupt_records.isNotNull()).count()
print(f"Failed API calls: {num_failures}")

# Log failures
df.filter(df._corrupt_records.isNotNull()) \
  .select("*") \
  .write.mode("append") \
  .parquet("path/to/failures")
```

## Use Cases

### Address Validation

Validate addresses for a list of customers:

```python
customers_df = spark.read.table("customers")
customers_df.select("address", "city", "state", "zip") \
    .createOrReplaceTempView("addresses")

validated = spark.read.format("rest") \
    .option("url", "https://api.address-validator.com/validate") \
    .option("input", "addresses") \
    .option("method", "POST") \
    .load()
```

### Sentiment Analysis

Analyze sentiment for thousands of tweets:

```python
tweets_df = spark.read.table("tweets")
tweets_df.select("tweet_id", "text") \
    .createOrReplaceTempView("tweets_to_analyze")

sentiments = spark.read.format("rest") \
    .option("url", "https://api.watson.com/sentiment") \
    .option("input", "tweets_to_analyze") \
    .option("method", "POST") \
    .option("userId", "apikey") \
    .option("userPassword", "your_watson_key") \
    .load()
```

### Geocoding

Convert addresses to coordinates:

```python
locations = spark.read.format("rest") \
    .option("url", "https://maps.googleapis.com/maps/api/geocode/json") \
    .option("input", "addresses") \
    .option("method", "GET") \
    .load()
```

### API Data Enrichment

Enrich user data with external API lookups:

```python
users_df.select("user_id", "email") \
    .createOrReplaceTempView("users_to_enrich")

enriched = spark.read.format("rest") \
    .option("url", "https://api.clearbit.com/v2/people/find") \
    .option("input", "users_to_enrich") \
    .option("method", "GET") \
    .load()
```

## Comparison with Scala Version

This Python implementation provides the same core functionality as the original Scala version:

| Feature | Scala Version | Python Version |
|---------|---------------|----------------|
| Parallel API calls | ✅ | ✅ |
| Schema inference | ✅ | ✅ |
| GET/POST/PUT/DELETE | ✅ | ✅ |
| Basic Auth | ✅ | ✅ |
| OAuth1 | ✅ | ✅ |
| Bearer tokens | ❌ | ✅ |
| Custom headers | Partial | ✅ |
| Cookie support | ✅ | ✅ |
| Spark 2.x support | ✅ | ❌ (Spark 4.0+ only) |

## Troubleshooting

### SSL/HTTPS Issues

The data source uses `verify=True` by default. For development with self-signed certificates, you may need to:

```python
# Modify the rest.py file to add:
# verify=False  # Only for development!
```

### Timeout Errors

Increase timeouts for slow APIs:

```python
.option("connectionTimeout", "5000")
.option("readTimeout", "30000")
```

### Schema Inference Issues

If schema inference fails:

1. Increase sample percentage:
   ```python
   .option("schemaSamplePcnt", "50")
   ```

2. Ensure API returns consistent JSON structure

3. Check `_corrupt_records` for parsing errors

### Authentication Failures

Common issues:

- **Basic Auth**: Verify userId and userPassword are correct
- **Bearer Token**: Ensure token is valid and not expired
- **OAuth1**: All four parameters (consumerKey, consumerSecret, token, tokenSecret) are required

## Performance Tips

1. **Partition Sizing**: Aim for 100-1000 API calls per partition
2. **Batch Requests**: If the API supports batch requests, create custom logic to batch multiple inputs
3. **Caching**: Use `.cache()` on the result DataFrame if you'll use it multiple times
4. **Error Retry**: Implement retry logic for transient failures by filtering and re-running failed records

## Related Data Sources

- `github`: Specialized REST source for GitHub API
- `stock`: Specialized REST source for stock data
- `jsonplaceholder`: Example REST source for testing

## Contributing

To extend the REST data source with additional features:

1. Fork the repository
2. Modify `pyspark_datasources/rest.py`
3. Add tests
4. Submit a pull request

## License

Apache License 2.0 - same as the original Scala implementation.

## References

- [Original Scala Implementation](https://github.com/Data-Science-Extensions/Data-Science-Extensions/tree/master/spark-datasource-rest)
- [PySpark Data Source API](https://spark.apache.org/docs/4.0.0/api/python/tutorial/sql/python_data_source.html)
- [Requests Library](https://requests.readthedocs.io/)
- [OAuth1 Authentication](https://requests-oauthlib.readthedocs.io/)
