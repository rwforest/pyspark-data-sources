# REST Data Source - Streaming API Documentation

## Overview

The REST Data Source provides both **batch** and **streaming** capabilities for calling REST APIs in Apache Spark. It supports generic streaming for any REST API with configurable options for different API patterns.

**Key Features:**
- Generic streaming support for any REST API
- URL placeholder replacement for input parameters
- Configurable offset tracking (timestamp, cursor, incremental)
- Support for both object-based and array-based API responses
- Built-in rate limiting and authentication
- Helper functions for common response formats

**Real-World API Examples in This Guide:**
- **Clever API** (Object-based) - Educational platform API for schools, teachers, and students
  - API Docs: https://dev.clever.com/docs/api-overview
- **OpenSky Network API** (Array-based) - Real-time flight tracking data
  - API Docs: https://openskynetwork.github.io/opensky-api/rest.html

---

## Table of Contents

1. [Batch Mode](#batch-mode)
2. [Streaming Mode](#streaming-mode)
3. [Configuration Options](#configuration-options)
4. [Helper Functions](#helper-functions)
5. [Examples](#examples)
6. [Advanced Topics](#advanced-topics)

---

## Batch Mode

Batch mode allows you to call REST APIs one or more times and collect the results in a DataFrame. This is ideal for:
- One-time data fetches
- Calling the same API with different parameters
- Parallel API calls for bulk operations

### Helper Functions Overview

The REST data source provides two helper functions for batch operations:

1. **`rest_api_call(input_df, url, method, **options)`**
   - Best for: Small to medium datasets (< 10,000 rows)
   - Pros: Simplest to use, automatic serialization
   - Cons: Collects input to driver memory

2. **`rest_api_call_csv(input_df, url, method, **options)`**
   - Best for: Large datasets, Databricks environments
   - Pros: No driver memory collection, works at any scale
   - Cons: Creates temporary CSV files

**Recommendation:** Use `rest_api_call_csv()` for production workloads and Databricks.

### Complete Example: Clever API

This example demonstrates fetching hierarchical data from the Clever API (educational platform).
**API Documentation:** https://dev.clever.com/docs/api-overview

#### Step 1: Get District Information

```python
from pyspark.sql import SparkSession
from pyspark_datasources import rest_api_call_csv
import ast

# Initialize Spark
spark = SparkSession.builder \
    .appName("CleverAPI") \
    .master("local[*]") \
    .getOrCreate()

# Your Clever API token
CLEVER_TOKEN = "your_token_here"

# Create dummy input (required even for parameterless GET requests)
dummy_input = spark.createDataFrame([{"placeholder": "dummy"}])

# Fetch districts
districts_df = rest_api_call_csv(
    dummy_input,
    url="https://api.clever.com/v3.0/districts",
    method="GET",
    authType="Bearer",
    oauthToken=CLEVER_TOKEN,
    queryType="inline",
    partitions="1"
)

# Parse the response
district_response = districts_df.select("output").first()["output"]
district_data = ast.literal_eval(district_response)

# Extract district information
for district in district_data["data"]:
    district_info = district.get("data", {})
    print(f"District: {district_info.get('name')}")
    print(f"ID: {district_info.get('id')}")
```

**Key Points:**
- Use `ast.literal_eval()` to parse responses (not `json.loads()`)
- `queryType="inline"` prevents input parameters from being appended to URL
- `partitions="1"` for sequential API calls

#### Step 2: Get Schools with Pagination

```python
# Fetch schools (with limit parameter)
schools_df = rest_api_call_csv(
    dummy_input,
    url="https://api.clever.com/v3.0/schools?limit=10",
    method="GET",
    authType="Bearer",
    oauthToken=CLEVER_TOKEN,
    queryType="inline",
    partitions="1"
)

# Parse schools
school_response = schools_df.select("output").first()["output"]
school_data = ast.literal_eval(school_response)

# Extract school information
schools = []
for school in school_data["data"]:
    school_info = school.get("data", {})
    school_id = school_info.get("id")
    school_name = school_info.get("name")
    schools.append({"id": school_id, "name": school_name})
    print(f"School: {school_name} (ID: {school_id})")
```

#### Step 3: Parallel API Calls with URL Placeholders

```python
# Create input DataFrame with ALL school IDs
# The REST data source will make one API call per row in parallel
schools_input = spark.createDataFrame([
    {"school_id": "5940d254203e37907e0000f0", "school_name": "Slothgreat"},
    {"school_id": "5940d254203e37907e0000f1", "school_name": "Vultureroad"},
    {"school_id": "5940d254203e37907e0000f2", "school_name": "Swoopcharm"}
])

# Get teachers for ALL schools in parallel using URL placeholders
# One API call per school, executed in parallel across partitions
teachers_response = rest_api_call_csv(
    schools_input,
    url="https://api.clever.com/v3.0/schools/{school_id}/users?role=teacher&limit=10000",
    method="GET",
    authType="Bearer",
    oauthToken=CLEVER_TOKEN,
    queryType="inline"
)

print(f"✓ Fetched teachers for {teachers_response.count()} schools")
```

**Key Points:**
- Pass a multi-row DataFrame as input (not a loop!)
- Use `{school_id}` placeholder in URL - automatically replaced per row
- REST data source makes API calls in parallel (one per partition)
- Result has one row per input school with the API response

#### Step 4: Using Nested Data with Helper Functions

```python
from pyspark_datasources import flatten_json_response

# Flatten nested JSON response
flattened = flatten_json_response(
    schools_df,
    json_path="data",
    flatten_nested_key="data",
    fully_flatten=True,
    separator="_"
)

flattened.show()
```

### Batch Helper Functions Reference

#### `rest_api_call_csv(input_df, url, method, **options)`

Calls a REST API using CSV file approach (recommended for production).

**Parameters:**
- `input_df` (DataFrame): Input DataFrame with parameters for API calls
- `url` (str): REST API endpoint URL with `{field}` placeholders
- `method` (str): HTTP method - GET, POST, PUT, DELETE
- `**options`: Additional REST data source options

**Returns:**
- DataFrame with columns: `[input_columns..., output]`
- `output` column contains the API response as a string

**Common Options:**
```python
rest_api_call_csv(
    input_df,
    url="https://api.example.com/endpoint?param={param}",
    method="GET",
    authType="Bearer",           # Authentication type
    oauthToken="your_token",     # Bearer token
    queryType="inline",          # Don't append input to URL
    partitions="2",              # Parallel execution
    headers='{"X-Custom": "val"}'  # Custom headers (JSON string)
)
```

**⚠️ Important Default Values:**
- `method`: Defaults to `"POST"` (explicitly set to `"GET"` for read operations)
- `queryType`: Defaults to `"querystring"` (use `"inline"` to prevent URL pollution)
- `partitions`: Defaults to `"2"` (adjust based on API rate limits)

#### `rest_api_call(input_df, url, method, **options)`

Simplified version for smaller datasets (< 10,000 rows).

**Parameters:** Same as `rest_api_call_csv()`

**Differences:**
- Collects input DataFrame to driver
- Uses in-memory serialization
- Simpler implementation
- Not recommended for Databricks

**Example:**
```python
from pyspark_datasources import rest_api_call

# Small dataset example
result = rest_api_call(
    input_df,
    url="https://api.example.com/search?q={query}",
    method="GET",
    queryType="inline"
)
```

### URL Placeholder Syntax

URLs support `{field_name}` placeholders that are replaced with values from the input DataFrame:

**Input DataFrame:**
```python
input_df = spark.createDataFrame([
    {"city": "NYC", "limit": 50, "category": "tech"},
    {"city": "SF", "limit": 100, "category": "finance"}
])
```

**URL Template:**
```
https://api.example.com/search?city={city}&limit={limit}&category={category}
```

**Actual Requests:**
```
https://api.example.com/search?city=NYC&limit=50&category=tech
https://api.example.com/search?city=SF&limit=100&category=finance
```

**Features:**
- Automatic URL encoding
- Supports any number of placeholders
- Case-sensitive field names

### Response Parsing

API responses are returned as strings in the `output` column:

```python
# Parse single response
response_str = df.select("output").first()["output"]
data = ast.literal_eval(response_str)

# Parse multiple responses
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType

@udf(returnType=StructType([...]))
def parse_response(response_str):
    import ast
    return ast.literal_eval(response_str)

df_parsed = df.withColumn("parsed", parse_response("output"))
```

### Authentication Examples

#### Bearer Token (OAuth 2.0)
```python
rest_api_call_csv(
    input_df,
    url="https://api.example.com/data",
    method="GET",
    authType="Bearer",
    oauthToken="your_bearer_token_here"
)
```

#### Basic Authentication
```python
rest_api_call_csv(
    input_df,
    url="https://api.example.com/data",
    method="GET",
    authType="Basic",
    userId="username",
    userPassword="password"
)
```

#### OAuth 1.0
```python
rest_api_call_csv(
    input_df,
    url="https://api.twitter.com/1.1/search/tweets.json",
    method="GET",
    authType="OAuth1",
    oauthConsumerKey="consumer_key",
    oauthConsumerSecret="consumer_secret",
    oauthToken="access_token",
    oauthTokenSecret="token_secret"
)
```

#### Custom Headers
```python
import json

headers = {
    "X-API-Key": "your_api_key",
    "X-Custom-Header": "value"
}

rest_api_call_csv(
    input_df,
    url="https://api.example.com/data",
    method="GET",
    headers=json.dumps(headers)
)
```

### POST Request Example

```python
# Input data for POST body
post_data = spark.createDataFrame([
    {"username": "user1", "email": "user1@example.com"},
    {"username": "user2", "email": "user2@example.com"}
])

# POST request
result = rest_api_call_csv(
    post_data,
    url="https://api.example.com/users",
    method="POST",
    postInputFormat="json",
    authType="Bearer",
    oauthToken="your_token"
)
```

### Batch Processing Best Practices

1. **Use `rest_api_call_csv()` for production**
   ```python
   # ✅ Production
   rest_api_call_csv(input_df, url=..., method="GET")

   # ❌ Development only
   rest_api_call(input_df, url=..., method="GET")
   ```

2. **Always specify `method` explicitly**
   ```python
   # ✅ Explicit
   rest_api_call_csv(input_df, url=..., method="GET")

   # ❌ Uses POST by default
   rest_api_call_csv(input_df, url=...)
   ```

3. **Use `queryType="inline"` for GET requests**
   ```python
   # ✅ Prevents URL pollution
   rest_api_call_csv(input_df, url=..., method="GET", queryType="inline")

   # ❌ Appends all input columns to URL
   rest_api_call_csv(input_df, url=..., method="GET")
   ```

4. **Control parallelism with `partitions`**
   ```python
   # Rate-limited API (sequential)
   rest_api_call_csv(input_df, url=..., partitions="1")

   # High-throughput API (parallel)
   rest_api_call_csv(input_df, url=..., partitions="10")
   ```

---

## Streaming Mode

Streaming mode continuously polls a REST API at regular intervals, tracking progress using offsets. This is ideal for real-time data ingestion from APIs that provide incremental updates.

**Real-World Example:** OpenSky Network API streams live flight tracking data with timestamp-based offsets.

### Basic Streaming Setup

```python
from pyspark_datasources import RestDataSource
import json

# Register data source
spark.dataSource.register(RestDataSource)

# Create input data with parameters
input_json = json.dumps([{"region": "US", "category": "tech"}])

# Create streaming DataFrame
stream_df = spark.readStream.format("rest") \
    .option("url", "https://api.example.com/data?region={region}&category={category}") \
    .option("method", "GET") \
    .option("streaming", "true") \
    .option("inputData", input_json) \
    .option("streamingInterval", "10") \
    .option("offsetType", "timestamp") \
    .option("offsetField", "updated_at") \
    .option("initialOffset", "0") \
    .load()

# Start streaming query
query = stream_df.writeStream.format("console").start()
```

### Response Modes

The REST streaming reader supports two response modes:

#### Mode 1: Extract Individual Records (Default)

Use when the API returns an array of objects:

```json
{
  "data": [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"}
  ]
}
```

**Configuration:**
```python
.option("dataField", "data")  # Field containing array of records
```

**Behavior:**
- Extracts individual records from the specified array field
- Each record becomes a separate row
- Records must be dictionaries/objects

#### Mode 2: Whole Response as JSON String (Array-Based APIs)

Use when the API returns positional arrays (like OpenSky Network):

```json
{
  "time": 1234567890,
  "states": [
    ["val1", "val2", "val3"],
    ["val4", "val5", "val6"]
  ]
}
```

**Configuration:**
```python
.option("dataField", "")  # Empty string = return whole response
```

**Behavior:**
- Returns the entire API response as a JSON string in the `output` field
- Use with `parse_array_response_streaming()` helper to parse arrays
- Ideal for APIs with positional/array-based data

---

## Configuration Options

### Core Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `url` | string | *required* | REST API endpoint URL. Supports placeholders like `{field_name}` |
| `method` | string | `"GET"` | HTTP method: GET, POST, PUT, DELETE |
| `streaming` | string | `"false"` | Set to `"true"` to enable streaming mode |
| `inputData` | string | *required* | JSON string containing input parameters |

### Streaming-Specific Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `streamingInterval` | string | `"10"` | Interval between API calls in seconds |
| `offsetType` | string | `"timestamp"` | Type of offset tracking: `timestamp`, `cursor`, `incremental`, `max_field` |
| `offsetField` | string | `"timestamp"` | Field name for offset tracking |
| `initialOffset` | string | `"0"` | Starting offset value |
| `dataField` | string | `"data"` | ⚠️ Field containing array of records. **Use `""` (empty string) for array-based APIs!** |

**⚠️ Critical: `dataField` Default Value**
- **Default:** `"data"` - Expects API to return `{"data": [{"obj1"}, {"obj2"}]}`
- **For array-based APIs** (like OpenSky): Set to `""` (empty string)
- **Wrong value causes:** Empty results / schema mismatch errors
- **Example:**
  ```python
  # ✅ For array-based APIs (OpenSky, etc.)
  .option("dataField", "")

  # ✅ For object-based APIs
  .option("dataField", "results")  # if API returns {"results": [...]}

  # ❌ Default won't work for array APIs
  # (omitting dataField uses "data" by default)
  ```

### URL Placeholder Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `queryType` | string | `"querystring"` | How to append parameters: `querystring`, `inline` |
| `partitions` | string | `"2"` | Number of partitions for parallel execution |

### Authentication Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `userId` | string | - | Username for Basic authentication |
| `userPassword` | string | - | Password for Basic authentication |
| `authType` | string | `"Basic"` | Authentication type: `Basic`, `OAuth1`, `Bearer` |
| `oauthConsumerKey` | string | - | OAuth1 consumer key |
| `oauthConsumerSecret` | string | - | OAuth1 consumer secret |
| `oauthToken` | string | - | OAuth1 token / Bearer token |
| `oauthTokenSecret` | string | - | OAuth1 token secret |
| `headers` | string | - | Custom HTTP headers as JSON string |
| `cookie` | string | - | Cookie in format `name=value` |

### Advanced Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `connectionTimeout` | string | `"1000"` | Connection timeout in milliseconds |
| `readTimeout` | string | `"5000"` | Read timeout in milliseconds |
| `batchSize` | string | `"1000"` | Maximum records per batch |

---

## Helper Functions

### `parse_array_response_streaming(stream_df, array_path, column_names, timestamp_field)`

Parses streaming API responses that return positional arrays instead of objects.

**Use Case:** APIs like OpenSky Network that return data as `[[val1, val2, ...], ...]`

**Parameters:**
- `stream_df` (DataFrame): Streaming DataFrame with `output` column containing JSON responses
- `array_path` (str): Path to the array field in the response (e.g., `"states"`, `"data"`)
- `column_names` (list): List of column names for the array elements
- `timestamp_field` (str): Name of timestamp field in response

**Returns:**
- Streaming DataFrame with parsed rows and named columns

**Example:**
```python
from pyspark_datasources import parse_array_response_streaming

# Column names for positional array
column_names = ["id", "name", "value", "timestamp"]

# Parse array response
flights = parse_array_response_streaming(
    stream_df,
    array_path="data",
    column_names=column_names,
    timestamp_field="time"
)

# Start streaming
query = flights.writeStream.format("console").start()
```

### `parse_array_response(response_df, array_path, column_names, timestamp_field)`

Batch version of `parse_array_response_streaming()` for one-time API calls.

**Example:**
```python
from pyspark_datasources import parse_array_response

flights = parse_array_response(
    response_df,
    array_path="states",
    column_names=column_names,
    timestamp_field="time"
)
```

---

## Examples

### Example 1: Simple Streaming with Timestamp Offset

```python
from pyspark_datasources import RestDataSource
import json

spark.dataSource.register(RestDataSource)

# Input parameters
input_json = json.dumps([{"category": "news", "limit": 100}])

# Stream with timestamp-based offset
stream_df = spark.readStream.format("rest") \
    .option("url", "https://api.example.com/articles?category={category}&limit={limit}&since={timestamp}") \
    .option("method", "GET") \
    .option("streaming", "true") \
    .option("inputData", input_json) \
    .option("queryType", "querystring") \
    .option("streamingInterval", "30") \
    .option("offsetType", "timestamp") \
    .option("offsetField", "timestamp") \
    .option("initialOffset", "0") \
    .option("dataField", "articles") \
    .load()

query = stream_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
```

### Example 2: Array-Based API (OpenSky Network)

Real-time flight tracking using the OpenSky Network REST API.
**API Documentation:** https://openskynetwork.github.io/opensky-api/rest.html

```python
from pyspark_datasources import RestDataSource, parse_array_response_streaming
import json

spark.dataSource.register(RestDataSource)

# Define region bounding box for North America
input_json = json.dumps([{
    "region": "NORTH_AMERICA",
    "lamin": 7.0,
    "lamax": 72.0,
    "lomin": -168.0,
    "lomax": -60.0
}])

# Stream flight data
stream_df = spark.readStream.format("rest") \
    .option("url", "https://opensky-network.org/api/states/all?lamin={lamin}&lamax={lamax}&lomin={lomin}&lomax={lomax}") \
    .option("method", "GET") \
    .option("streaming", "true") \
    .option("inputData", input_json) \
    .option("queryType", "querystring") \
    .option("streamingInterval", "10") \
    .option("offsetType", "timestamp") \
    .option("offsetField", "time") \
    .option("initialOffset", "0") \
    .option("dataField", "") \
    .load()

# Parse array response
column_names = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "geo_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "baro_altitude",
    "squawk", "spi", "category"
]

flights = parse_array_response_streaming(
    stream_df,
    array_path="states",
    column_names=column_names,
    timestamp_field="time"
)

# Display streaming results
display(flights.select(
    "region", "time", "icao24", "callsign", "origin_country",
    "longitude", "latitude", "velocity"
), streamName="flight_tracker")
```

### Example 3: Cursor-Based Pagination

```python
# API returns: {"data": [...], "next_cursor": "abc123"}
stream_df = spark.readStream.format("rest") \
    .option("url", "https://api.example.com/items?cursor={cursor}") \
    .option("method", "GET") \
    .option("streaming", "true") \
    .option("inputData", '[{"cursor": ""}]') \
    .option("streamingInterval", "5") \
    .option("offsetType", "cursor") \
    .option("offsetField", "next_cursor") \
    .option("initialOffset", "") \
    .option("dataField", "data") \
    .load()
```

### Example 4: Authenticated API with Custom Headers

```python
stream_df = spark.readStream.format("rest") \
    .option("url", "https://api.example.com/data") \
    .option("method", "GET") \
    .option("streaming", "true") \
    .option("inputData", '[{"api_key": "secret123"}]') \
    .option("authType", "Bearer") \
    .option("oauthToken", "your-bearer-token") \
    .option("headers", '{"X-Custom-Header": "value"}') \
    .option("streamingInterval", "10") \
    .option("offsetType", "timestamp") \
    .option("offsetField", "updated_at") \
    .option("dataField", "results") \
    .load()
```

---

## Advanced Topics

### Offset Tracking Strategies

#### 1. Timestamp-Based (`offsetType="timestamp"`)

**Use when:** API supports filtering by timestamp/date

**How it works:**
- Extracts timestamp from response using `offsetField`
- Next request uses this timestamp as the "since" parameter
- Automatically tracks progress over time

**Example:**
```python
.option("offsetType", "timestamp")
.option("offsetField", "last_updated")
.option("initialOffset", "0")
```

**API Response:**
```json
{
  "last_updated": 1234567890,
  "data": [...]
}
```

#### 2. Cursor-Based (`offsetType="cursor"`)

**Use when:** API uses pagination cursors

**How it works:**
- Extracts cursor from response (e.g., `next_cursor`, `links.next`)
- Passes cursor to next request
- Continues until no cursor is returned

**Example:**
```python
.option("offsetType", "cursor")
.option("offsetField", "pagination.next_cursor")
.option("initialOffset", "")
```

**API Response:**
```json
{
  "data": [...],
  "pagination": {
    "next_cursor": "eyJpZCI6MTIzfQ=="
  }
}
```

#### 3. Incremental (`offsetType="incremental"`)

**Use when:** API supports ID-based pagination

**How it works:**
- Finds maximum ID in current batch
- Next request uses `max_id + 1` as starting point
- Ideal for sequential IDs

**Example:**
```python
.option("offsetType", "incremental")
.option("offsetField", "id")
.option("initialOffset", "0")
```

#### 4. Max Field (`offsetType="max_field"`)

**Use when:** Need to track maximum value of any field

**How it works:**
- Finds maximum value of specified field in records
- Uses this value for next request
- Similar to incremental but doesn't add 1

**Example:**
```python
.option("offsetType", "max_field")
.option("offsetField", "sequence_number")
```

### URL Placeholder Replacement

The streaming reader automatically replaces placeholders in URLs:

**Input Data:**
```json
[{"city": "NYC", "limit": 50, "category": "tech"}]
```

**URL Template:**
```
https://api.example.com/search?city={city}&limit={limit}&category={category}
```

**Actual Request:**
```
https://api.example.com/search?city=NYC&limit=50&category=tech
```

**Features:**
- Automatic URL encoding
- Supports any number of placeholders
- Works with nested objects (use dot notation in future)

### Rate Limiting

Built-in rate limiting ensures minimum interval between requests:

```python
.option("streamingInterval", "10")  # Minimum 10 seconds between calls
```

**Behavior:**
- Enforced before each API call
- Independent of Spark's trigger interval
- Prevents API rate limit violations

### Error Handling

The streaming reader includes robust error handling:

**On API Error:**
- Prints error message
- Returns empty batch with same offset (retry)
- Continues streaming

**On Network Error:**
- Catches `RequestException`
- Implements exponential backoff (if configured)
- Logs error details

**Example Output:**
```
API request failed: 429 Client Error: Too Many Requests
```

### Schema Inference

The streaming reader infers schema from input data:

**Input:**
```json
[{"region": "US", "limit": 100}]
```

**Inferred Schema:**
```
StructType([
    StructField("region", StringType(), True),
    StructField("limit", StringType(), True),
    StructField("output", StringType(), True)
])
```

**Notes:**
- All input fields are StringType
- `output` field always included
- Use explicit casting after parsing if needed

---

## Best Practices

### 1. Choose the Right Response Mode

**Use `dataField=""` when:**
- API returns positional arrays
- Need custom parsing logic
- Response structure is complex

**Use `dataField="fieldname"` when:**
- API returns array of objects
- Records are self-contained dictionaries
- Standard JSON structure

### 2. Set Appropriate Intervals

```python
# Public APIs with rate limits
.option("streamingInterval", "60")  # 1 minute

# Internal APIs with no limits
.option("streamingInterval", "5")   # 5 seconds

# Real-time critical data
.option("streamingInterval", "1")   # 1 second (minimum)
```

### 3. Use Helper Functions

Instead of manual parsing:
```python
# ❌ Manual parsing (verbose)
parsed_df = stream_df.withColumn("parsed", from_json(...))
exploded = parsed_df.select(..., explode(...))
# ... many more steps

# ✅ Use helper function
flights = parse_array_response_streaming(stream_df, ...)
```

### 4. Monitor Stream Health

```python
query = stream_df.writeStream.format("console").start()

# Check status
query.status

# Check progress
query.lastProgress

# Check if active
query.isActive
```

### 5. Handle Checkpoints

```python
query = flights.writeStream \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .format("delta") \
    .start("/path/to/output")
```

---

## Troubleshooting

### Issue: Empty Batches

**Symptoms:** Stream runs but returns no data

**Possible Causes:**
1. Incorrect `dataField` configuration
2. Wrong schema for `parse_array_response_streaming`
3. API returning empty results

**Solutions:**
```python
# Check what's in output field
stream_df.writeStream.format("console").option("truncate", False).start()

# Verify dataField setting
.option("dataField", "")  # For array-based APIs

# Check API response manually
curl "https://api.example.com/..." | jq
```

### Issue: Schema Mismatch Errors

**Error:** `DATA_SOURCE_RETURN_SCHEMA_MISMATCH`

**Solution:** Ensure `dataField=""` is set:
```python
.option("dataField", "")  # Returns whole response as string
```

### Issue: URL Placeholders Not Replaced

**Symptoms:** URL contains `{field_name}` instead of actual value

**Solution:** Check `inputData` format:
```python
# ✅ Correct
input_json = json.dumps([{"field": "value"}])

# ❌ Wrong
input_json = '{"field": "value"}'  # Missing array wrapper
```

### Issue: Rate Limit Errors

**Error:** `429 Too Many Requests`

**Solution:** Increase streaming interval:
```python
.option("streamingInterval", "60")  # Slow down requests
```

---

## API Reference Summary

### RestDataSource Class

```python
class RestDataSource(DataSource):
    @classmethod
    def name(cls) -> str: ...

    def schema(self) -> str: ...

    def reader(self, schema: StructType) -> DataSourceReader: ...

    def streamReader(self, schema: StructType) -> SimpleDataSourceStreamReader: ...
```

### RestStreamReader Class

```python
class RestStreamReader(SimpleDataSourceStreamReader):
    def __init__(self, schema: StructType, options: Dict[str, str]): ...

    def initialOffset(self) -> Dict[str, str]: ...

    def read(self, start: Dict[str, str]) -> Tuple[List[Tuple], Dict[str, str]]: ...

    def readBetweenOffsets(self, start: Dict[str, str], end: Dict[str, str]) -> Iterator[Tuple]: ...

    def commit(self, end: Dict[str, str]) -> None: ...
```

### Helper Functions

```python
def rest_api_call_csv(
    input_df: DataFrame,
    url: str,
    method: str = "POST",
    **options
) -> DataFrame: ...

def parse_array_response(
    response_df: DataFrame,
    array_path: str = "states",
    column_names: list = None,
    timestamp_field: str = "time"
) -> DataFrame: ...

def parse_array_response_streaming(
    stream_df: DataFrame,
    array_path: str = "states",
    column_names: list = None,
    timestamp_field: str = "time",
    output_column: str = "output"
) -> DataFrame: ...
```

---

## API References

This documentation uses real-world APIs to demonstrate the flexibility of the REST data source:

### Clever API (Object-based)
- **Documentation:** https://dev.clever.com/docs/api-overview
- **Response Format:** Object-based with nested `data` arrays
- **Authentication:** Bearer token
- **Example Response:**
  ```json
  {
    "data": [
      {"data": {"id": "123", "name": "School Name", ...}},
      {"data": {"id": "456", "name": "Another School", ...}}
    ]
  }
  ```
- **Used in Examples:** Batch mode - fetching schools, teachers, and students

### OpenSky Network API (Array-based)
- **Documentation:** https://openskynetwork.github.io/opensky-api/rest.html
- **Response Format:** Array-based with positional values
- **Authentication:** None (public API)
- **Example Response:**
  ```json
  {
    "time": 1234567890,
    "states": [
      ["abc123", "UAL123", "United States", ...],
      ["def456", "DAL456", "United States", ...]
    ]
  }
  ```
- **Used in Examples:** Streaming mode - real-time flight tracking

### Key Differences

| Feature | Clever API | OpenSky API |
|---------|-----------|-------------|
| Response Type | Object-based | Array-based |
| Data Field | `"data"` | `""` (empty - use whole response) |
| Authentication | Required (Bearer) | Not required |
| Best Mode | Batch (hierarchical data) | Streaming (real-time updates) |
| Helper Function | `flatten_json_response` | `parse_array_response_streaming` |

---

## Version History

- **v1.1.0** - Added generic streaming support with URL placeholder replacement
- **v1.0.0** - Initial release with batch mode

## License

Apache License 2.0

## Contributing

Contributions are welcome! Please see the main repository for contribution guidelines.
