"""
REST Streaming Example - Continuous API Polling

Demonstrates using the REST data source in streaming mode to continuously
poll REST APIs and process data in real-time.

Run with: python rest_streaming_example.py
"""

from pyspark.sql import SparkSession
from pyspark_datasources import RestDataSource
import time

def main():
    print("=" * 80)
    print("REST Streaming Example - Continuous API Polling")
    print("=" * 80)

    # Initialize Spark with streaming support
    spark = SparkSession.builder \
        .appName("RestStreaming") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Register the REST data source
    spark.dataSource.register(RestDataSource)

    print("\n✓ Spark initialized with streaming support")

    # Example 1: Stream data from a JSON placeholder API
    print("\n" + "=" * 80)
    print("Example 1: Streaming Posts from JSONPlaceholder API")
    print("=" * 80)
    print("\nThis example polls the API every 5 seconds for new posts...")

    # Create dummy input (required even for streaming)
    dummy_data = [{"placeholder": "dummy"}]
    import json
    input_json = json.dumps(dummy_data)

    # Read stream from REST API
    # The API returns: [{"userId": 1, "id": 1, "title": "...", "body": "..."}]
    stream_df = spark.readStream.format("rest") \
        .option("url", "https://jsonplaceholder.typicode.com/posts?_start={offset}&_limit=5") \
        .option("method", "GET") \
        .option("streaming", "true") \
        .option("inputData", input_json) \
        .option("streamingInterval", "5") \
        .option("offsetField", "offset") \
        .option("offsetType", "incremental") \
        .option("initialOffset", "0") \
        .option("batchSize", "5") \
        .load()

    # Write to console (streaming sink)
    query = stream_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "10") \
        .start()

    print("\n✓ Streaming query started!")
    print("⏱️  Fetching new data every 5 seconds...")
    print("📝 Press Ctrl+C to stop")
    print("\nWaiting for data (will run for 30 seconds)...\n")

    # Run for 30 seconds
    try:
        time.sleep(30)
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")

    # Stop the query
    query.stop()
    print("\n✓ Streaming query stopped")

    # Example 2: Stream with custom offset tracking
    print("\n" + "=" * 80)
    print("Example 2: Streaming with Timestamp-based Offset")
    print("=" * 80)
    print("\nThis example shows how to use timestamp-based polling...")
    print("(Not running - just showing the configuration)")

    example_config = '''
# For APIs that support "since" timestamps:
stream_df = spark.readStream.format("rest") \\
    .option("url", "https://api.example.com/events?since={timestamp}") \\
    .option("method", "GET") \\
    .option("streamingInterval", "10") \\
    .option("offsetField", "timestamp") \\
    .option("offsetType", "timestamp") \\
    .option("initialOffset", "0") \\
    .option("dataField", "events") \\
    .load()

# For APIs with cursor-based pagination:
stream_df = spark.readStream.format("rest") \\
    .option("url", "https://api.example.com/data?cursor={next_cursor}") \\
    .option("method", "GET") \\
    .option("streamingInterval", "15") \\
    .option("offsetField", "next_cursor") \\
    .option("offsetType", "cursor") \\
    .option("initialOffset", "start") \\
    .option("dataField", "results") \\
    .load()
'''
    print(example_config)

    # Example 3: Processing streamed data
    print("\n" + "=" * 80)
    print("Example 3: Processing Streamed Data")
    print("=" * 80)
    print("\nYou can apply transformations to streaming data just like batch:")

    processing_example = '''
# Read stream
stream_df = spark.readStream.format("rest") \\
    .option("url", "https://api.example.com/events") \\
    .load()

# Parse and transform
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

# Define schema for the output field
event_schema = StructType([
    StructField("id", StringType()),
    StructField("type", StringType()),
    StructField("timestamp", StringType())
])

# Parse JSON output and select fields
parsed_df = stream_df.select(
    from_json(col("output").cast("string"), event_schema).alias("event")
).select("event.*")

# Filter and aggregate
filtered_df = parsed_df.filter(col("type") == "important")

# Write to different sinks
query = filtered_df.writeStream \\
    .outputMode("append") \\
    .format("parquet") \\
    .option("path", "output/events") \\
    .option("checkpointLocation", "checkpoint/events") \\
    .start()
'''
    print(processing_example)

    print("\n" + "=" * 80)
    print("Streaming Options Reference")
    print("=" * 80)

    options_ref = '''
Key Options:
  url: API endpoint (use {offset_field} placeholder)
  streamingInterval: Seconds between API calls (default: 10)
  offsetField: Name of offset field (default: "timestamp")
  offsetType: How to track progress:
    - "timestamp": Use timestamp from response
    - "cursor": Use cursor from response
    - "incremental": Calculate max ID + 1
    - "max_field": Find max value in records
  initialOffset: Starting offset value (default: "0")
  batchSize: Max records per batch (default: 1000)
  dataField: Field containing array of records (default: "data")

Authentication (same as batch):
  authType: "Basic", "Bearer", "OAuth1"
  userId, userPassword: For Basic auth
  oauthToken: For Bearer auth
  oauthConsumerKey, etc.: For OAuth1
'''
    print(options_ref)

    print("\n" + "=" * 80)
    print("COMPLETE!")
    print("=" * 80)
    print("\nKey Takeaways:")
    print("  1. REST streaming polls APIs at regular intervals")
    print("  2. Use offset tracking to avoid duplicate data")
    print("  3. Supports various offset strategies (timestamp, cursor, incremental)")
    print("  4. Apply same transformations as batch data")
    print("  5. Write to any Spark sink (console, files, Delta, Kafka, etc.)")
    print("=" * 80)

    spark.stop()

if __name__ == "__main__":
    main()
