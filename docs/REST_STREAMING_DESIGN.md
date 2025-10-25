# REST Data Source Streaming Support - Design Document

## Overview

Add generic streaming support to the REST data source while maintaining API-agnostic design principles.

## Key Differences from OpenSky Implementation

| Aspect | OpenSky (Specific) | REST (Generic) |
|--------|-------------------|----------------|
| API Endpoint | Hardcoded OpenSky URL | User-configurable URL |
| Request Pattern | Always GET with bounding box | Configurable HTTP method + params |
| Response Parsing | OpenSky-specific state vectors | Generic JSON parsing |
| Rate Limiting | Hardcoded 5s intervals | User-configurable intervals |
| Authentication | OAuth2 for OpenSky | Existing REST auth (Basic, OAuth1, Bearer) |
| Offset Tracking | Simple timestamp | Configurable offset field |
| Data Schema | Hardcoded aircraft schema | Dynamic schema inference |

## Design Principles

1. **API-Agnostic**: Works with any REST API that supports streaming/polling patterns
2. **Configurable**: All behavior controlled by options, no hardcoded logic
3. **Reusable**: Leverages existing REST data source authentication and request logic
4. **Flexible Offset Tracking**: Support various offset mechanisms (timestamp, cursor, page number)

## Streaming Options

### New Options for Streaming

```python
# Enable streaming mode
.option("streaming", "true")

# Polling interval (seconds between API calls)
.option("streamingInterval", "10")  # Default: 10 seconds

# Offset tracking field in API response
.option("offsetField", "timestamp")  # e.g., "timestamp", "cursor", "next_page"

# Offset type
.option("offsetType", "timestamp")  # Options: timestamp, cursor, incremental

# Initial offset value (optional)
.option("initialOffset", "0")  # Start from this offset

# Maximum records per batch (optional)
.option("batchSize", "100")  # Limit records per streaming batch
```

### Existing Options (Reused)

All existing REST options work in streaming mode:
- `url`, `method`, `authType`, `oauthToken`
- `userId`, `userPassword`
- `headers`, `cookie`
- `postInputFormat`, `queryType`
- etc.

## Implementation Strategy

### 1. Add StreamReader Class

```python
class RestStreamReader(SimpleDataSourceStreamReader):
    """Generic streaming reader for REST APIs"""

    def __init__(self, schema: StructType, options: Dict[str, str]):
        self.schema = schema
        self.options = options
        self.interval = float(options.get("streamingInterval", "10"))
        self.offset_field = options.get("offsetField", "timestamp")
        self.offset_type = options.get("offsetType", "timestamp")
        self.last_request_time = 0

        # Reuse existing REST authentication/request logic
        self.rest_reader = RestReader(options, schema)

    def initialOffset(self) -> Dict[str, Any]:
        """Return initial offset"""
        initial = self.options.get("initialOffset", "0")
        return {self.offset_field: initial}

    def read(self, start: Dict[str, Any]) -> Tuple[List[Tuple], Dict[str, Any]]:
        """Fetch data from API and extract next offset"""
        # Rate limiting
        self._handle_rate_limit()

        # Add offset to request (as query param or body)
        request_params = self._prepare_request_with_offset(start)

        # Make API call (reuse existing REST logic)
        response = self._call_api(request_params)

        # Parse response
        records = self._parse_response(response)

        # Extract next offset from response
        next_offset = self._extract_offset(response, records)

        return (records, next_offset)

    def readBetweenOffsets(self, start: Dict, end: Dict) -> Iterator[Tuple]:
        """Read data between offsets (for reprocessing)"""
        data, _ = self.read(start)
        return iter(data)
```

### 2. Extend RestDataSource

```python
class RestDataSource(DataSource):
    def simpleStreamReader(self, schema: StructType):
        """Return stream reader if streaming is enabled"""
        if self.options.get("streaming", "false").lower() == "true":
            return RestStreamReader(schema, self.options)
        return None  # Not a streaming source
```

## Offset Tracking Strategies

### 1. Timestamp-based (Most Common)

API returns timestamp of latest record, use it in next request:

```python
# API response: {"data": [...], "latest_timestamp": 1234567890}
# Next request: GET /api/data?since=1234567890
```

**Configuration:**
```python
.option("streaming", "true")
.option("offsetField", "latest_timestamp")
.option("offsetType", "timestamp")
.option("url", "https://api.example.com/data?since={latest_timestamp}")
```

### 2. Cursor-based

API returns cursor/token for next page:

```python
# API response: {"data": [...], "next_cursor": "abc123"}
# Next request: GET /api/data?cursor=abc123
```

**Configuration:**
```python
.option("streaming", "true")
.option("offsetField", "next_cursor")
.option("offsetType", "cursor")
.option("url", "https://api.example.com/data?cursor={next_cursor}")
```

### 3. Incremental ID

API supports starting from specific ID:

```python
# API response: {"data": [{id: 100}, {id: 101}]}
# Next request: GET /api/data?start_id=102
```

**Configuration:**
```python
.option("streaming", "true")
.option("offsetField", "max_id")
.option("offsetType", "incremental")
.option("url", "https://api.example.com/data?start_id={max_id}")
```

## Usage Examples

### Example 1: Streaming Twitter-like API

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TwitterStream").getOrCreate()

# Register REST data source
from pyspark_datasources import RestDataSource
spark.dataSource.register(RestDataSource)

# Stream tweets
tweets_df = spark.readStream.format("rest") \
    .option("url", "https://api.example.com/tweets?since={timestamp}") \
    .option("method", "GET") \
    .option("authType", "Bearer") \
    .option("oauthToken", "your_token") \
    .option("streaming", "true") \
    .option("streamingInterval", "30") \
    .option("offsetField", "timestamp") \
    .option("offsetType", "timestamp") \
    .load()

# Write to console
query = tweets_df.writeStream \
    .format("console") \
    .start()

query.awaitTermination()
```

### Example 2: Streaming API with Cursor

```python
# Stream data using cursor pagination
data_df = spark.readStream.format("rest") \
    .option("url", "https://api.example.com/data?cursor={next_cursor}") \
    .option("method", "GET") \
    .option("authType", "Bearer") \
    .option("oauthToken", "your_token") \
    .option("streaming", "true") \
    .option("streamingInterval", "10") \
    .option("offsetField", "next_cursor") \
    .option("offsetType", "cursor") \
    .option("initialOffset", "") \
    .load()

query = data_df.writeStream \
    .format("parquet") \
    .option("path", "/data/stream") \
    .option("checkpointLocation", "/data/checkpoint") \
    .start()
```

### Example 3: Clever API Streaming (New Users)

```python
# Stream new users from Clever API
users_df = spark.readStream.format("rest") \
    .option("url", "https://api.clever.com/v3.0/users?role=student&starting_after={last_id}") \
    .option("method", "GET") \
    .option("authType", "Bearer") \
    .option("oauthToken", "d5c461921595f99bbb7c893c4ef932c9b457c06b") \
    .option("streaming", "true") \
    .option("streamingInterval", "60") \
    .option("offsetField", "last_id") \
    .option("offsetType", "cursor") \
    .load()

query = users_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/clever_checkpoint") \
    .start("/tmp/clever_users")
```

## Implementation Checklist

- [ ] Create `RestStreamReader` class extending `SimpleDataSourceStreamReader`
- [ ] Add `simpleStreamReader()` method to `RestDataSource`
- [ ] Implement offset extraction logic (configurable via options)
- [ ] Implement rate limiting (configurable interval)
- [ ] Reuse existing REST authentication and request logic
- [ ] Add offset placeholder replacement in URL (e.g., `{offset}` → actual value)
- [ ] Support offset in request body for POST requests
- [ ] Handle edge cases (empty responses, API errors)
- [ ] Add tests for streaming functionality
- [ ] Update documentation

## Key Design Decisions

### ✅ DO (Generic Approach)

- Make everything configurable via options
- Reuse existing REST data source authentication
- Support flexible offset mechanisms
- Let users define how offset appears in requests
- Generic JSON response parsing
- Configurable rate limiting

### ❌ DON'T (Avoid Specific Logic)

- Hardcode API endpoints
- Assume specific response structure
- Implement API-specific authentication
- Hardcode rate limits
- Parse API-specific data formats
- Assume specific offset field names

## Migration Path

Existing REST data source usage (batch mode) remains unchanged:
```python
# Batch mode (existing)
df = spark.read.format("rest") \
    .option("url", "...") \
    .option("inputData", "...") \
    .load()
```

New streaming mode is opt-in via `streaming=true`:
```python
# Streaming mode (new)
df = spark.readStream.format("rest") \
    .option("url", "...") \
    .option("streaming", "true") \
    .load()
```

## Benefits

1. **No Breaking Changes**: Existing batch functionality unchanged
2. **Consistent API**: Same options work for both batch and streaming
3. **Maximum Flexibility**: Works with any REST API that supports polling
4. **Reusable Code**: Leverages existing authentication and request logic
5. **Simple Configuration**: Users just add `streaming=true` + offset config
