"""
Test REST Data Source Streaming Support

This example demonstrates how to use the generic REST streaming functionality
with various API patterns.
"""

from pyspark.sql import SparkSession
from pyspark_datasources import RestDataSource


def test_timestamp_based_streaming():
    """
    Example: Timestamp-based streaming
    API returns latest_timestamp, use it for next request
    """
    print("=" * 70)
    print("Test 1: Timestamp-Based Streaming")
    print("=" * 70)

    spark = SparkSession.builder \
        .appName("RESTStreamingTimestamp") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Register REST data source
    spark.dataSource.register(RestDataSource)

    # Example API that returns: {"data": [...], "timestamp": 1234567890}
    # Next request: GET /api/data?since=1234567890
    df = spark.readStream.format("rest") \
        .option("url", "https://api.example.com/data?since={timestamp}") \
        .option("method", "GET") \
        .option("authType", "Bearer") \
        .option("oauthToken", "your_token") \
        .option("streaming", "true") \
        .option("streamingInterval", "30") \
        .option("offsetField", "timestamp") \
        .option("offsetType", "timestamp") \
        .option("initialOffset", "0") \
        .load()

    print(f"Schema: {df.schema}")
    print("Stream configured successfully!")
    print()

    spark.stop()


def test_cursor_based_streaming():
    """
    Example: Cursor-based streaming
    API returns next_cursor, use it for pagination
    """
    print("=" * 70)
    print("Test 2: Cursor-Based Streaming")
    print("=" * 70)

    spark = SparkSession.builder \
        .appName("RESTStreamingCursor") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Register REST data source
    spark.dataSource.register(RestDataSource)

    # Example API that returns: {"data": [...], "next_cursor": "abc123"}
    # Next request: GET /api/data?cursor=abc123
    df = spark.readStream.format("rest") \
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

    print(f"Schema: {df.schema}")
    print("Stream configured successfully!")
    print()

    spark.stop()


def test_incremental_id_streaming():
    """
    Example: Incremental ID-based streaming
    Extract max ID from records, request records with ID > max_id
    """
    print("=" * 70)
    print("Test 3: Incremental ID-Based Streaming")
    print("=" * 70)

    spark = SparkSession.builder \
        .appName("RESTStreamingIncremental") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Register REST data source
    spark.dataSource.register(RestDataSource)

    # Example API that returns: {"data": [{"id": 100}, {"id": 101}]}
    # Next request: GET /api/data?start_id=102
    df = spark.readStream.format("rest") \
        .option("url", "https://api.example.com/data?start_id={max_id}") \
        .option("method", "GET") \
        .option("authType", "Bearer") \
        .option("oauthToken", "your_token") \
        .option("streaming", "true") \
        .option("streamingInterval", "15") \
        .option("offsetField", "id") \
        .option("offsetType", "incremental") \
        .option("initialOffset", "0") \
        .option("dataField", "data") \
        .load()

    print(f"Schema: {df.schema}")
    print("Stream configured successfully!")
    print()

    spark.stop()


def test_clever_api_streaming():
    """
    Example: Clever API streaming (real API)
    Stream new users from Clever API
    """
    print("=" * 70)
    print("Test 4: Clever API Streaming (Real Example)")
    print("=" * 70)

    spark = SparkSession.builder \
        .appName("CleverAPIStreaming") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Register REST data source
    spark.dataSource.register(RestDataSource)

    # Clever API token
    CLEVER_TOKEN = "d5c461921595f99bbb7c893c4ef932c9b457c06b"

    # Stream students from Clever API
    # API returns: {"data": [...], "links": [{"rel": "next", "uri": "..."}]}
    df = spark.readStream.format("rest") \
        .option("url", "https://api.clever.com/v3.0/users?role=student&limit=10&starting_after={last_id}") \
        .option("method", "GET") \
        .option("authType", "Bearer") \
        .option("oauthToken", CLEVER_TOKEN) \
        .option("streaming", "true") \
        .option("streamingInterval", "60") \
        .option("offsetField", "last_id") \
        .option("offsetType", "cursor") \
        .option("initialOffset", "") \
        .option("dataField", "data") \
        .load()

    print(f"Schema: {df.schema}")
    print("Clever API stream configured successfully!")

    # For real streaming, you would write:
    # query = df.writeStream \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .start()
    # query.awaitTermination()

    print()

    spark.stop()


def print_comparison():
    """Print comparison between OpenSky and generic REST streaming"""
    print("=" * 70)
    print("Comparison: OpenSky (Specific) vs REST (Generic)")
    print("=" * 70)
    print()

    print("OpenSky Data Source (API-Specific):")
    print("  - Hardcoded OpenSky API endpoint")
    print("  - Fixed bounding box parameters")
    print("  - Hardcoded 5-second rate limit")
    print("  - OpenSky-specific authentication (OAuth2)")
    print("  - Parses aircraft state vectors")
    print("  - Fixed schema (18 columns for aircraft data)")
    print()

    print("REST Data Source (Generic):")
    print("  - ✓ Any REST API endpoint (configurable)")
    print("  - ✓ Any parameters (user-defined)")
    print("  - ✓ Configurable rate limiting (streamingInterval)")
    print("  - ✓ Multiple auth types (Basic, OAuth1, Bearer)")
    print("  - ✓ Generic JSON parsing (any structure)")
    print("  - ✓ Dynamic schema (inferred from responses)")
    print("  - ✓ Flexible offset tracking (timestamp, cursor, ID)")
    print()

    print("Usage Patterns Supported:")
    print("  1. Timestamp-based streaming (since=timestamp)")
    print("  2. Cursor-based pagination (cursor=token)")
    print("  3. Incremental ID tracking (start_id=N)")
    print("  4. Max field tracking (updated_after=date)")
    print()


if __name__ == "__main__":
    print()
    print_comparison()

    # Run example configurations (won't actually stream, just show setup)
    test_timestamp_based_streaming()
    test_cursor_based_streaming()
    test_incremental_id_streaming()
    test_clever_api_streaming()

    print("=" * 70)
    print("All streaming configuration examples completed!")
    print("=" * 70)
    print()
    print("To actually run a stream, uncomment the writeStream code")
    print("and use .start() + .awaitTermination()")
