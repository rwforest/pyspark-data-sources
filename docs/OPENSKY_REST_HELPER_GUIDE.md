# OpenSky Network - Using REST Helper

This guide shows how to fetch live flight data from OpenSky Network using the `rest_api_call` helper for batch processing.

## Three Approaches to OpenSky Data

| Feature | REST Helper (Batch) | REST Streaming | Custom Streaming (`opensky.py`) |
|---------|-------------------|----------------|--------------------------------|
| **Code Complexity** | ~10 lines for fetch | ~15 lines config | ~480 lines |
| **Mode** | Batch (on-demand) | Streaming (polling) | Streaming (continuous) |
| **Use Case** | Periodic snapshots | Real-time with generic setup | Real-time with optimization |
| **Setup** | Import helper | Enable streaming option | Register data source |
| **Rate Limiting** | Manual control | Configurable intervals | Automatic |
| **Offset Tracking** | Not needed | Automatic | Built-in |

**This guide focuses on the REST Helper (Batch) approach - the simplest way to get started.**

For streaming approaches, see:
- [REST Streaming Guide](REST_STREAMING_GUIDE.md) - Generic REST streaming with configurable offset tracking
- [OpenSky Custom Streaming](../pyspark_datasources/opensky.py) - Production-grade implementation

## Files

### 1. **[opensky_simple.ipynb](../opensky_simple.ipynb)** - Jupyter Notebook
Interactive notebook for exploring flight data:
- Fetch live flights worldwide or by region
- Parse and analyze data
- Save to CSV
- Great for learning and demos

### 2. **[opensky_example.py](../opensky_example.py)** - Standalone Script
Run directly without Jupyter:
```bash
python opensky_example.py
```

## Quick Start

### Basic Usage

```python
from pyspark.sql import SparkSession
from pyspark_datasources import rest_api_call
import ast

# Initialize Spark
spark = SparkSession.builder \
    .appName("OpenSky") \
    .master("local[*]") \
    .getOrCreate()

# Create dummy input
dummy_input = spark.createDataFrame([{"placeholder": "dummy"}])

# Fetch ALL flights worldwide
flights_response = rest_api_call(
    dummy_input,
    url="https://opensky-network.org/api/states/all",
    method="GET",
    queryType="inline",
    partitions="1"
)

# Parse response
output = flights_response.select("output").first()["output"]
response_data = ast.literal_eval(output)
```

### Fetch Specific Region

```python
# San Francisco Bay Area
flights_response = rest_api_call(
    dummy_input,
    url="https://opensky-network.org/api/states/all?lamin=36.5&lamax=38.5&lomin=-123&lomax=-121",
    method="GET",
    queryType="inline",
    partitions="1"
)
```

### Fetch Multiple Regions in Parallel

```python
# Define multiple regions
regions = spark.createDataFrame([
    {"region": "San Francisco", "lamin": 36.5, "lamax": 38.5, "lomin": -123.0, "lomax": -121.0},
    {"region": "New York", "lamin": 40.0, "lamax": 41.5, "lomin": -75.0, "lomax": -73.0},
    {"region": "London", "lamin": 51.0, "lamax": 52.0, "lomin": -1.0, "lomax": 1.0},
])

# Fetch all regions in parallel
flights_response = rest_api_call(
    regions,
    url="https://opensky-network.org/api/states/all?lamin={lamin}&lamax={lamax}&lomin={lomin}&lomax={lomax}",
    method="GET",
    queryType="inline"
)
```

## Understanding OpenSky Response Format

OpenSky returns data in an unusual **array-based format** (not typical JSON objects):

```json
{
  "time": 1234567890,
  "states": [
    [
      "icao24",           // [0] Unique aircraft identifier
      "callsign",         // [1] Flight number
      "origin_country",   // [2] Country
      time_position,      // [3] Position update time
      last_contact,       // [4] Last contact time
      longitude,          // [5] Longitude
      latitude,           // [6] Latitude
      geo_altitude,       // [7] Altitude (meters)
      on_ground,          // [8] On ground flag
      velocity,           // [9] Velocity (m/s)
      true_track,         // [10] Track angle
      vertical_rate,      // [11] Vertical rate
      sensors,            // [12] Sensor IDs
      baro_altitude,      // [13] Barometric altitude
      squawk,             // [14] Squawk code
      spi,                // [15] SPI flag
      category            // [16] Aircraft category
    ]
  ]
}
```

### Parsing the Response

```python
# Parse the JSON response
response_data = ast.literal_eval(output)
timestamp = response_data.get("time")
states = response_data.get("states", [])

# Convert to structured data
flights = []
for state in states:
    if state and len(state) >= 17 and state[0]:  # Valid state
        flights.append({
            "icao24": state[0],
            "callsign": state[1].strip() if state[1] else "N/A",
            "country": state[2],
            "longitude": float(state[5]) if state[5] is not None else None,
            "latitude": float(state[6]) if state[6] is not None else None,
            "altitude_m": float(state[7]) if state[7] is not None else None,
            "on_ground": bool(state[8]) if state[8] is not None else None,
            "velocity_ms": float(state[9]) if state[9] is not None else None,
        })

# Create DataFrame
flights_df = spark.createDataFrame(flights)
```

**Important:** Use explicit type conversion (`float()`, `bool()`) to avoid schema mismatch errors.

## API Rate Limits

- **Anonymous**: 100 calls per day
- **Authenticated**: 4000 calls per day (research accounts)
- **Data Contributors**: 8000 calls per day
- **Minimum interval**: 5 seconds between requests

## Bounding Box Coordinates

Common regions:

| Region | lamin | lamax | lomin | lomax |
|--------|-------|-------|-------|-------|
| **San Francisco Bay** | 36.5 | 38.5 | -123.0 | -121.0 |
| **New York Area** | 40.0 | 41.5 | -75.0 | -73.0 |
| **London Area** | 51.0 | 52.0 | -1.0 | 1.0 |
| **Tokyo Area** | 35.0 | 36.0 | 139.0 | 140.5 |
| **Paris Area** | 48.5 | 49.5 | 1.5 | 3.5 |
| **Global** | -90.0 | 90.0 | -180.0 | 180.0 |

## Example Output

```
================================================================================
LIVE FLIGHTS
================================================================================
+----------+--------+-------------+------+--------+---------+---------+-----------+
|altitude_m|callsign|country      |icao24|latitude|longitude|on_ground|velocity_ms|
+----------+--------+-------------+------+--------+---------+---------+-----------+
|11269.98  |TVF8392 |France       |39de4e|45.3756 |2.9826   |false    |235.29     |
|2049.78   |AXB2546 |India        |80162d|19.0021 |72.7521  |false    |121.99     |
|11582.4   |AXB1208 |India        |801638|26.5939 |84.6082  |false    |200.2      |
+----------+--------+-------------+------+--------+---------+---------+-----------+
```

## When to Use Each Approach

### Use REST Helper (Batch) - This Guide ✅
**Best for:** Learning, development, periodic snapshots, ETL jobs

✅ **Pros:**
- Simplest approach (~10 lines)
- No configuration complexity
- Direct control over calls
- Easy debugging
- Perfect for scheduled jobs

❌ **Cons:**
- No automatic polling
- Manual rate limit handling
- No built-in offset tracking

**Example:**
```python
# Simple one-time fetch
flights_df = rest_api_call(dummy_input, url="https://opensky-network.org/api/states/all", ...)
flights_df.show()
```

### Use REST Streaming - See [REST_STREAMING_GUIDE.md](REST_STREAMING_GUIDE.md)
**Best for:** Generic real-time monitoring, any REST API

✅ **Pros:**
- Works with any REST API
- Automatic polling
- Configurable offset tracking
- No custom code needed

❌ **Cons:**
- Requires understanding offset strategies
- More configuration options
- Not optimized for specific APIs

**Example:**
```python
# Continuous polling with timestamp tracking
stream = spark.readStream.format("rest") \
    .option("url", "https://opensky-network.org/api/states/all?time={timestamp}") \
    .option("streaming", "true") \
    .option("streamingInterval", "10") \
    .load()
```

### Use Custom Streaming (`opensky.py`) - Production Grade
**Best for:** Production flight tracking systems

✅ **Pros:**
- Highly optimized for OpenSky API
- Built-in rate limiting and retries
- Region-specific optimizations
- OAuth2 authentication support
- Production-tested

❌ **Cons:**
- Most complex (~480 lines)
- OpenSky-specific only
- Requires data source registration

**Example:**
```python
# Production streaming with automatic management
df = spark.readStream.format("opensky") \
    .option("region", "NORTH_AMERICA") \
    .option("client_id", "...") \
    .load()
```

## Example: Periodic Snapshots (Manual Polling)

If you need periodic snapshots but not full streaming, you can poll manually:

```python
import time

for i in range(10):  # Fetch 10 snapshots
    print(f"\n--- Snapshot #{i+1} ---")

    flights_response = rest_api_call(
        dummy_input,
        url="https://opensky-network.org/api/states/all",
        method="GET",
        queryType="inline",
        partitions="1"
    )

    # Process data...
    # Parse, transform, save, etc.

    time.sleep(10)  # Wait 10 seconds (min 5 seconds required)
```

**Tip:** For true streaming with automatic management, use the streaming approaches above instead of manual polling.

## See Also

### Related Guides
- **[REST Streaming Guide](REST_STREAMING_GUIDE.md)** - Generic streaming for any REST API
- **[Clever API Guide](CLEVER_API_GUIDE.md)** - Example using `rest_api_call` with `flatten_json_response`
- **[REST Data Source Documentation](rest-datasource.md)** - Full REST data source reference

### OpenSky Resources
- **[OpenSky Custom Streaming](../pyspark_datasources/opensky.py)** - Production-grade implementation
- **[OpenSky Network API](https://opensky-network.org/apidoc/)** - Official API documentation
- **[OpenSky Network Terms of Use](https://opensky-network.org/about/terms-of-use)** - Usage guidelines

### Key Differences Summary

| Need | Use This |
|------|----------|
| One-time fetch or periodic snapshots | **REST Helper** (this guide) |
| Real-time monitoring of any REST API | **REST Streaming** ([guide](REST_STREAMING_GUIDE.md)) |
| Production OpenSky flight tracking | **Custom Streaming** ([code](../pyspark_datasources/opensky.py)) |
| Nested JSON responses (like Clever) | **REST Helper** + `flatten_json_response` ([Clever guide](CLEVER_API_GUIDE.md)) |
| Array-based responses (like OpenSky) | **REST Helper** + manual parsing (this guide) |

## Citation

When using OpenSky data in research, please cite:

> Matthias Schäfer, Martin Strohmeier, Vincent Lenders, Ivan Martinovic and Matthias Wilhelm.
> "Bringing Up OpenSky: A Large-scale ADS-B Sensor Network for Research".
> In Proceedings of the 13th IEEE/ACM International Symposium on Information Processing
> in Sensor Networks (IPSN), pages 83-94, April 2014.
