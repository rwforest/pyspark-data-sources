# REST Data Source - Streaming Support

## Overview

**YES - REST streaming is fully supported!** The REST data source includes a `RestStreamReader` that enables continuous polling of REST APIs in Spark Structured Streaming mode.

## Key Difference: Batch vs Streaming

| Aspect | Batch (`rest_api_call`) | Streaming (`readStream.format("rest")`) |
|--------|------------------------|----------------------------------------|
| **Execution** | One-time fetch | Continuous polling |
| **Use Case** | Periodic snapshots | Real-time monitoring |
| **API Calls** | Single call or parallel calls | Repeated calls at intervals |
| **Offset Tracking** | Not needed | Automatic (timestamp, cursor, ID) |
| **Processing** | DataFrame operations | Streaming queries with sinks |

## How It Works

### Batch Mode (rest_api_call)
```python
# One-time fetch
df = rest_api_call(input_df, url="https://api.example.com/data", ...)
df.show()  # Process immediately
```

### Streaming Mode (readStream)
```python
# Continuous polling
stream = spark.readStream.format("rest") \\
    .option("url", "https://api.example.com/events?since={timestamp}") \\
    .option("streaming", "true") \\
    .option("streamingInterval", "10") \\
    .load()

# Write to sink (files, console, Delta, etc.)
query = stream.writeStream.format("parquet").start()
query.awaitTermination()  # Runs forever
```

## Streaming Options

### Core Options

| Option | Description | Default |
|--------|-------------|---------|
| `streaming` | Enable streaming mode | `"false"` |
| `streamingInterval` | Seconds between API polls | `"10"` |
| `offsetField` | Field name for tracking progress | `"timestamp"` |
| `offsetType` | How to track progress (see below) | `"timestamp"` |
| `initialOffset` | Starting offset value | `"0"` |
| `batchSize` | Max records per batch | `"1000"` |
| `dataField` | Field containing array of records | `"data"` |

### Offset Types

The `offsetType` determines how the system tracks "where it left off":

1. **`"timestamp"`** - Use a timestamp field
   ```python
   # API returns: {"latest_timestamp": 1234567890, "events": [...]}
   .option("offsetType", "timestamp")
   .option("offsetField", "latest_timestamp")
   ```

2. **`"cursor"`** - Use API-provided cursor/token
   ```python
   # API returns: {"next_cursor": "abc123", "data": [...]}
   .option("offsetType", "cursor")
   .option("offsetField", "next_cursor")
   ```

3. **`"incremental"`** - Calculate max ID from records
   ```python
   # API returns: [{"id": 1}, {"id": 2}, ...]
   .option("offsetType", "incremental")
   .option("offsetField", "id")
   # Automatically uses max(id) + 1 for next request
   ```

4. **`"max_field"`** - Find max value of a field in records
   ```python
   # API returns: [{"created_at": "2024-01-01"}, ...]
   .option("offsetType", "max_field")
   .option("offsetField", "created_at")
   ```

## Example Patterns

### Pattern 1: Timestamp-Based Polling
```python
# Good for: Event logs, audit trails, change feeds
# API endpoint: /events?since={timestamp}

stream = spark.readStream.format("rest") \\
    .option("url", "https://api.example.com/events?since={timestamp}") \\
    .option("method", "GET") \\
    .option("streaming", "true") \\
    .option("inputData", '[{"dummy": "value"}]') \\
    .option("streamingInterval", "30") \\
    .option("offsetType", "timestamp") \\
    .option("offsetField", "timestamp") \\
    .option("initialOffset", "0") \\
    .option("dataField", "events") \\
    .load()
```

### Pattern 2: Cursor-Based Pagination
```python
# Good for: APIs with cursor/token pagination
# API endpoint: /data?cursor={next_cursor}

stream = spark.readStream.format("rest") \\
    .option("url", "https://api.example.com/data?cursor={next_cursor}") \\
    .option("method", "GET") \\
    .option("streaming", "true") \\
    .option("inputData", '[{"dummy": "value"}]') \\
    .option("streamingInterval", "60") \\
    .option("offsetType", "cursor") \\
    .option("offsetField", "next_cursor") \\
    .option("initialOffset", "start") \\
    .option("dataField", "results") \\
    .load()
```

### Pattern 3: Incremental ID
```python
# Good for: APIs that return sequential IDs
# API endpoint: /items?start_id={id}

stream = spark.readStream.format("rest") \\
    .option("url", "https://api.example.com/items?start_id={id}") \\
    .option("method", "GET") \\
    .option("streaming", "true") \\
    .option("inputData", '[{"dummy": "value"}]') \\
    .option("streamingInterval", "15") \\
    .option("offsetType", "incremental") \\
    .option("offsetField", "id") \\
    .option("initialOffset", "0") \\
    .load()
```

## Comparison with Custom Streaming Data Sources

### Custom Streaming (e.g., OpenSky)
```python
# Pros:
- API-specific optimizations
- Custom authentication handling
- Built-in rate limiting
- Error handling tuned for specific API

# Cons:
- Requires ~400-500 lines of code per API
- Needs registration before use
- Less flexible

# Example:
df = spark.readStream.format("opensky") \\
    .option("region", "NORTH_AMERICA") \\
    .load()
```

### Generic REST Streaming
```python
# Pros:
- Works with ANY REST API
- No custom code needed
- Configurable behavior via options
- Reuses existing authentication

# Cons:
- Less optimized for specific APIs
- Requires understanding offset tracking
- Manual configuration needed

# Example:
df = spark.readStream.format("rest") \\
    .option("url", "https://api.example.com/data?since={timestamp}") \\
    .option("streaming", "true") \\
    .load()
```

## When to Use Each Approach

### Use `rest_api_call()` (Batch) When:
- Need one-time or periodic snapshots
- Running scheduled ETL jobs
- Processing historical data
- Want simple, straightforward code
- Need to combine multiple API calls

### Use `readStream.format("rest")` (Streaming) When:
- Need real-time continuous monitoring
- Processing live event streams
- Want automatic offset management
- Building streaming pipelines
- Need exactly-once processing guarantees

### Use Custom Streaming Data Source When:
- Specific API needs special handling
- Performance optimization is critical
- API has complex authentication
- Want to package reusable component
- Building production system

## Processing Streamed Data

```python
# Read stream
stream_df = spark.readStream.format("rest") \\
    .option("url", "https://api.example.com/events") \\
    .option("streaming", "true") \\
    .load()

# Parse JSON output
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

schema = StructType([
    StructField("id", StringType()),
    StructField("event_type", StringType()),
    StructField("timestamp", LongType())
])

parsed_df = stream_df.select(
    from_json(col("output").cast("string"), schema).alias("event")
).select("event.*")

# Apply transformations
filtered_df = parsed_df.filter(col("event_type") == "important")

# Write to multiple sinks
query1 = filtered_df.writeStream \\
    .outputMode("append") \\
    .format("parquet") \\
    .option("path", "output/events") \\
    .option("checkpointLocation", "checkpoint/events") \\
    .start()

query2 = filtered_df.writeStream \\
    .outputMode("append") \\
    .format("console") \\
    .start()

# Wait for termination
query1.awaitTermination()
```

## Common Sinks

```python
# File Sink (Parquet)
.format("parquet") \\
.option("path", "output/data") \\
.option("checkpointLocation", "checkpoint/data")

# Delta Lake
.format("delta") \\
.option("path", "delta/events") \\
.option("checkpointLocation", "checkpoint/events")

# Console (for debugging)
.format("console") \\
.option("truncate", "false")

# Memory (for testing)
.format("memory") \\
.queryName("events_table")

# Kafka
.format("kafka") \\
.option("kafka.bootstrap.servers", "localhost:9092") \\
.option("topic", "events")
```

## Best Practices

1. **Start with Batch Mode**
   - Test API calls with `rest_api_call()` first
   - Understand the API response format
   - Then convert to streaming

2. **Choose Right Offset Type**
   - Timestamp: Best for time-series data
   - Cursor: Best for APIs with pagination
   - Incremental: Best for sequential IDs
   - Max Field: Best for custom fields

3. **Set Appropriate Intervals**
   - Respect API rate limits
   - Balance between latency and load
   - Typical: 10-60 seconds

4. **Monitor Your Queries**
   ```python
   query.status  # Current status
   query.lastProgress  # Latest batch info
   query.awaitTermination(timeout=None)
   ```

5. **Handle Failures**
   - Use checkpointing for fault tolerance
   - Implement proper error handling
   - Set up monitoring and alerts

## Limitations

1. **Schema Inference**: Currently returns single `output` field (needs manual parsing)
2. **Input Required**: Must provide `inputData` even for streaming
3. **API-Specific**: Some APIs may not fit standard patterns
4. **Rate Limits**: Must manually configure intervals
5. **Authentication**: May need custom headers for complex auth

## See Also

- [REST Data Source Batch Guide](REST_DATASOURCE_README.md)
- [Clever API Example](CLEVER_API_GUIDE.md) - Batch processing
- [OpenSky Data Source](../pyspark_datasources/opensky.py) - Custom streaming example
- [REST Streaming Implementation](../pyspark_datasources/rest_streaming.py) - Source code

## Conclusion

**REST streaming is fully supported and production-ready!** It provides a generic, configurable way to stream data from any REST API without writing custom code. For simple use cases, use `rest_api_call()` for batch processing. For real-time monitoring, use `readStream.format("rest")` with appropriate offset tracking.
