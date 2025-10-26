"""
OpenSky Network Streaming Example
Continuously polls OpenSky API and saves flight data to files.

Run with: python opensky_streaming.py
Stop with: Ctrl+C
"""

from pyspark.sql import SparkSession
from pyspark_datasources import RestDataSource
import json
import time

def main():
    print("=" * 80)
    print("OpenSky Network - Streaming Flight Tracker")
    print("=" * 80)
    print("\nThis script will continuously poll the OpenSky API every 10 seconds")
    print("and save flight data to the output folder.\n")
    print("Press Ctrl+C to stop.\n")

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("OpenSkyStreaming") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Register the REST data source
    spark.dataSource.register(RestDataSource)
    print("✓ Spark initialized with REST data source")

    # Configure streaming
    input_json = json.dumps([{"placeholder": "dummy"}])

    stream_df = spark.readStream.format("rest") \
        .option("url", "https://opensky-network.org/api/states/all") \
        .option("method", "GET") \
        .option("streaming", "true") \
        .option("inputData", input_json) \
        .option("streamingInterval", "10") \
        .option("offsetType", "timestamp") \
        .option("offsetField", "time") \
        .option("initialOffset", "0") \
        .option("queryType", "inline") \
        .option("partitions", "1") \
        .load()

    print("✓ Streaming configured (polling every 10 seconds)")

    # Write to JSON files
    output_path = "output/opensky_stream"
    checkpoint_path = "checkpoint/opensky_stream"

    print(f"\n📁 Output directory: {output_path}")
    print(f"🔖 Checkpoint directory: {checkpoint_path}\n")

    query = stream_df.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .start()

    print("✓ Streaming query started!")
    print("⏱️  Collecting flight data...\n")

    # Run until interrupted
    try:
        # Option 1: Run for a specific duration (e.g., 5 minutes)
        # query.awaitTermination(timeout=300)

        # Option 2: Run indefinitely until Ctrl+C
        while query.isActive:
            time.sleep(5)

            # Print status every 30 seconds
            if query.lastProgress:
                num_rows = query.lastProgress.get("numInputRows", 0)
                batch_id = query.lastProgress.get("batchId", 0)
                print(f"Batch {batch_id}: Received {num_rows} records")

    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    finally:
        print("\nStopping streaming query...")
        query.stop()
        print("✓ Query stopped")

        # Print summary
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Output saved to: {output_path}")
        print("\nTo process the collected data:")
        print("  1. Read: df = spark.read.json('output/opensky_stream')")
        print("  2. Parse: Use parse_array_response on the 'output' field")
        print("  3. Analyze: Group by country, filter by altitude, etc.")
        print("=" * 80)

        spark.stop()
        print("\n✓ Spark stopped")

if __name__ == "__main__":
    main()
