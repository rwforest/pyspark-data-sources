# REST Data Source Streaming Support

Generic streaming support for calling any REST API as a Spark Structured Streaming source.

## Overview

The REST data source now supports **streaming mode**, allowing you to continuously poll any REST API and process new data as it becomes available. Unlike API-specific implementations (like OpenSky), this streaming support is **completely generic** and works with any REST API through configuration alone.

## Key Features

✅ **API-Agnostic**: Works with any REST API that supports polling/streaming patterns
✅ **Flexible Offset Tracking**: Timestamp, cursor, incremental ID, or custom fields
✅ **Configurable Rate Limiting**: Control polling interval
✅ **Full Authentication Support**: Basic, OAuth1, Bearer tokens
✅ **Generic JSON Parsing**: No hardcoded response structures
✅ **Dynamic Schema**: Automatically infers schema from responses
✅ **No Breaking Changes**: Existing batch mode unchanged

## Quick Start

### Basic Streaming Example

```python
from pyspark.sql import SparkSession
from pyspark_datasources import RestDataSource

spark = SparkSession.builder \
    .appName("RESTStreaming") \
    .getOrCreate()

# Register REST data source
spark.dataSource.register(RestDataSource)

# Stream data from any API
df = spark.readStream.format("rest") \
    .option("url", "https://api.example.com/data?since={timestamp}") \
    .option("method", "GET") \
    .option("authType", "Bearer") \
    .option("oauthToken", "your_token") \
    .option("streaming", "true") \
    .option("streamingInterval", "30") \
    .option("offsetField", "timestamp") \
    .option("offsetType", "timestamp") \
    .load()

# Write to console or any sink
query = df.writeStream \
    .format("console") \
    .start()

query.awaitTermination()
```

## Configuration Options

### Required Options

| Option | Description | Example |
|--------|-------------|---------|
| `url` | API endpoint with offset placeholder | `https://api.example.com/data?since={timestamp}` |
| `streaming` | Enable streaming mode | `"true"` |

### Streaming-Specific Options

| Option | Default | Description |
|--------|---------|-------------|
| `streamingInterval` | `"10"` | Seconds between API polls |
| `offsetField` | `"timestamp"` | Field name for tracking progress |
| `offsetType` | `"timestamp"` | Offset strategy (see below) |
| `initialOffset` | `"0"` | Starting offset value |
| `batchSize` | `"1000"` | Max records per batch |
| `dataField` | `"data"` | Field containing array of records |

### Offset Type Strategies

#### 1. `timestamp` - Timestamp-based (Most Common)

API returns a timestamp of the latest record:

```python
# API Response: {"data": [...], "latest_timestamp": 1234567890}
# Next Request: GET /api/data?since=1234567890

.option("url", "https://api.example.com/data?since={latest_timestamp}")
.option("offsetField", "latest_timestamp")
.option("offsetType", "timestamp")
```

#### 2. `cursor` - Cursor/Token-based Pagination

API returns a cursor/token for the next page:

```python
# API Response: {"data": [...], "next_cursor": "abc123"}
# Next Request: GET /api/data?cursor=abc123

.option("url", "https://api.example.com/data?cursor={next_cursor}")
.option("offsetField", "next_cursor")
.option("offsetType", "cursor")
.option("initialOffset", "")
```

#### 3. `incremental` - Incremental ID

Extract max ID from records, request records with ID > max:

```python
# API Response: {"data": [{"id": 100}, {"id": 101}]}
# Next Request: GET /api/data?start_id=102

.option("url", "https://api.example.com/data?start_id={id}")
.option("offsetField", "id")
.option("offsetType", "incremental")
```

#### 4. `max_field` - Max Value of Any Field

Find max value of a field in records:

```python
# API Response: {"data": [{"updated_at": "2025-01-01"}, ...]}
# Next Request: GET /api/data?updated_after=2025-01-01

.option("url", "https://api.example.com/data?updated_after={updated_at}")
.option("offsetField", "updated_at")
.option("offsetType", "max_field")
```

### Authentication Options (All Supported)

All existing REST authentication options work in streaming mode:

```python
# Bearer Token
.option("authType", "Bearer")
.option("oauthToken", "your_token")

# Basic Auth
.option("authType", "Basic")
.option("userId", "username")
.option("userPassword", "password")

# OAuth1
.option("authType", "OAuth1")
.option("oauthConsumerKey", "key")
.option("oauthConsumerSecret", "secret")
.option("oauthToken", "token")
.option("oauthTokenSecret", "token_secret")
```

## Real-World Examples

### Example 1: Twitter-like API

```python
tweets_df = spark.readStream.format("rest") \
    .option("url", "https://api.twitter-like.com/tweets?since={timestamp}") \
    .option("method", "GET") \
    .option("authType", "Bearer") \
    .option("oauthToken", "your_token") \
    .option("streaming", "true") \
    .option("streamingInterval", "30") \
    .option("offsetField", "timestamp") \
    .option("offsetType", "timestamp") \
    .option("initialOffset", "0") \
    .load()

# Process tweets
query = tweets_df.writeStream \
    .format("parquet") \
    .option("path", "/data/tweets") \
    .option("checkpointLocation", "/data/checkpoint") \
    .start()

query.awaitTermination()
```

### Example 2: Clever API (Education Data)

```python
# Stream new students from Clever API
students_df = spark.readStream.format("rest") \
    .option("url", "https://api.clever.com/v3.0/users?role=student&starting_after={last_id}") \
    .option("method", "GET") \
    .option("authType", "Bearer") \
    .option("oauthToken", "your_clever_token") \
    .option("streaming", "true") \
    .option("streamingInterval", "60") \
    .option("offsetField", "last_id") \
    .option("offsetType", "cursor") \
    .option("dataField", "data") \
    .load()

# Write to Delta Lake
query = students_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/data/clever_checkpoint") \
    .start("/data/students")

query.awaitTermination()
```

### Example 3: GitHub API (Events)

```python
# Stream GitHub events
events_df = spark.readStream.format("rest") \
    .option("url", "https://api.github.com/events?per_page=100") \
    .option("method", "GET") \
    .option("authType", "Bearer") \
    .option("oauthToken", "ghp_...") \
    .option("streaming", "true") \
    .option("streamingInterval", "60") \
    .option("offsetField", "id") \
    .option("offsetType", "incremental") \
    .load()

# Filter and process
filtered = events_df.filter("output.type = 'PushEvent'")

query = filtered.writeStream \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
```

### Example 4: REST API with Custom Headers

```python
import json

custom_headers = json.dumps({
    "X-API-Key": "your_key",
    "X-Custom-Header": "value"
})

data_df = spark.readStream.format("rest") \
    .option("url", "https://api.example.com/stream?after={cursor}") \
    .option("method", "GET") \
    .option("headers", custom_headers) \
    .option("streaming", "true") \
    .option("streamingInterval", "15") \
    .option("offsetField", "cursor") \
    .option("offsetType", "cursor") \
    .load()

query = data_df.writeStream.format("console").start()
query.awaitTermination()
```

## Response Format Support

The streaming reader supports various API response formats:

### 1. Direct Array

```json
[
  {"id": 1, "data": "..."},
  {"id": 2, "data": "..."}
]
```

Configuration: Default (no `dataField` needed)

### 2. Nested in Field

```json
{
  "data": [
    {"id": 1, "data": "..."},
    {"id": 2, "data": "..."}
  ],
  "timestamp": 1234567890
}
```

Configuration: `.option("dataField", "data")`

### 3. With Pagination Metadata

```json
{
  "items": [...],
  "next_cursor": "abc123",
  "has_more": true
}
```

Configuration:
```python
.option("dataField", "items")
.option("offsetField", "next_cursor")
```

## Comparison: Generic vs API-Specific

| Feature | OpenSky (Specific) | REST Streaming (Generic) |
|---------|-------------------|--------------------------|
| API Endpoint | ❌ Hardcoded | ✅ Configurable |
| Parameters | ❌ Fixed (bounding box) | ✅ User-defined (any params) |
| Rate Limiting | ❌ Hardcoded (5s) | ✅ Configurable |
| Authentication | ❌ OAuth2 only | ✅ Multiple types |
| Response Parsing | ❌ Aircraft-specific | ✅ Generic JSON |
| Schema | ❌ Fixed (18 columns) | ✅ Dynamic (inferred) |
| Offset Tracking | ❌ Simple timestamp | ✅ Flexible (4 strategies) |
| **Reusability** | ❌ OpenSky only | ✅ **Any REST API** |

## Error Handling

The streaming reader handles errors gracefully:

- **API Errors**: Returns empty batch, retries with same offset
- **Rate Limiting**: Automatically enforced via `streamingInterval`
- **Network Failures**: Retries with same offset
- **Invalid Responses**: Logs error, continues with next poll

## Performance Considerations

### Rate Limiting

Always respect API rate limits:

```python
# For APIs with rate limits (e.g., 100 requests/hour)
# Set streamingInterval = 3600 / 100 = 36 seconds
.option("streamingInterval", "36")
```

### Batch Size

Control memory usage with batch size:

```python
# Limit records per micro-batch
.option("batchSize", "500")
```

### Checkpointing

Always use checkpoints for fault tolerance:

```python
query = df.writeStream \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .start()
```

## Troubleshooting

### Error: "input is required for REST data source"

**Solution**: Add `.option("streaming", "true")` to enable streaming mode. Streaming mode doesn't require input data.

### Stream not fetching new data

**Checks**:
1. Verify `offsetField` matches your API response
2. Check API actually returns new offset value
3. Confirm URL contains `{offset_field_name}` placeholder
4. Test API manually with curl/Postman

### Schema issues

The streaming reader returns data in the `output` column. Parse it with:

```python
from pyspark.sql.functions import from_json

# Define your schema
schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType())
])

# Parse output column
parsed = df.select(from_json("output", schema).alias("data"))
expanded = parsed.select("data.*")
```

## Testing

Run the test examples:

```bash
source .venv/bin/activate
python test_rest_streaming.py
```

This shows configuration examples for various streaming patterns.

## Migration from Batch to Streaming

**Batch mode (existing)**:
```python
df = spark.read.format("rest") \
    .option("url", "...") \
    .option("inputData", "...") \
    .load()
```

**Streaming mode (new)**:
```python
df = spark.readStream.format("rest") \
    .option("url", "...") \
    .option("streaming", "true") \
    .option("streamingInterval", "30") \
    .option("offsetField", "timestamp") \
    .load()
```

**Key differences**:
- Use `readStream` instead of `read`
- Add `.option("streaming", "true")`
- No `inputData` needed (polls API automatically)
- Add offset configuration

## Architecture

```
┌─────────────────────────────────────────┐
│  Spark Structured Streaming Engine     │
└─────────────┬───────────────────────────┘
              │
              │ calls every N seconds
              ▼
┌─────────────────────────────────────────┐
│  RestStreamReader                       │
│  - Tracks offset (timestamp/cursor/ID)  │
│  - Enforces rate limiting               │
│  - Handles authentication               │
└─────────────┬───────────────────────────┘
              │
              │ HTTP GET/POST
              ▼
┌─────────────────────────────────────────┐
│  Your REST API                          │
│  - Returns data + next offset           │
└─────────────────────────────────────────┘
```

## Contributing

The streaming implementation is in `pyspark_datasources/rest_streaming.py`. To add new offset strategies or features:

1. Extend `_extract_next_offset()` for new offset types
2. Add configuration options to `__init__()`
3. Update documentation
4. Add test examples

## See Also

- [REST_DATASOURCE_README.md](REST_DATASOURCE_README.md) - Batch mode documentation
- [REST_STREAMING_DESIGN.md](REST_STREAMING_DESIGN.md) - Design decisions
- [CLEVER_API_GUIDE.md](CLEVER_API_GUIDE.md) - Real-world example with Clever API
- [test_rest_streaming.py](test_rest_streaming.py) - Test examples
