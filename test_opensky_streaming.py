#!/usr/bin/env python3
"""
Test script for OpenSky streaming with generic REST data source.
This tests the streaming logic locally before deploying to Databricks.
"""

from pyspark.sql import SparkSession
from pyspark_datasources import RestDataSource, parse_array_response_streaming
import json
import time

def test_opensky_streaming():
    """Test OpenSky streaming with generic REST data source"""

    print("=" * 70)
    print("Testing OpenSky Streaming with Generic REST Data Source")
    print("=" * 70)

    # Create Spark session
    print("\n1. Creating Spark session...")
    spark = SparkSession.builder \
        .appName("OpenSkyStreamingTest") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("   ✓ Spark session created")

    # Define region
    region = "NORTH_AMERICA"
    regions = {
        "NORTH_AMERICA": {"lamin": 7.0, "lamax": 72.0, "lomin": -168.0, "lomax": -60.0},
    }
    bbox = regions[region]

    print(f"\n2. Setting up streaming for region: {region}")
    print(f"   Bounding box: {bbox}")

    # Create input DataFrame
    input_df = spark.createDataFrame([{"region": region, **bbox}])
    # Create JSON directly without pandas
    input_data = [{"region": region, **bbox}]
    input_json = json.dumps(input_data)
    print(f"   Input data: {input_json}")

    # Register REST data source
    print("\n3. Registering REST data source...")
    spark.dataSource.register(RestDataSource)
    print("   ✓ REST data source registered")

    # Configure streaming
    url = "https://opensky-network.org/api/states/all?lamin={lamin}&lamax={lamax}&lomin={lomin}&lomax={lomax}"

    print("\n4. Creating streaming DataFrame...")
    print(f"   URL template: {url}")
    print("   Options:")
    print("     - Streaming interval: 10 seconds")
    print("     - Offset type: timestamp")
    print("     - Offset field: time")

    try:
        stream_df = spark.readStream.format("rest") \
            .option("url", url) \
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

        print(f"   ✓ Stream DataFrame created")
        print(f"   Schema: {stream_df.schema}")

    except Exception as e:
        print(f"   ✗ Error creating stream DataFrame: {e}")
        import traceback
        traceback.print_exc()
        return False

    # Parse the output field using helper function
    print("\n5. Parsing array response using parse_array_response_streaming...")

    # Column names for OpenSky flight arrays
    column_names = [
        "icao24", "callsign", "origin_country", "time_position", "last_contact",
        "longitude", "latitude", "geo_altitude", "on_ground", "velocity",
        "true_track", "vertical_rate", "sensors", "baro_altitude",
        "squawk", "spi", "category"
    ]

    try:
        # Use helper function to parse array response
        flights = parse_array_response_streaming(
            stream_df,
            array_path="states",
            column_names=column_names,
            timestamp_field="time"
        )

        # Select columns to display
        flights_display = flights.select(
            "region", "time", "icao24", "callsign", "origin_country",
            "longitude", "latitude", "geo_altitude", "velocity"
        )

        print(f"   ✓ Parsing DataFrame created")
        print(f"   Schema: {flights_display.schema}")

    except Exception as e:
        print(f"   ✗ Error setting up parsing: {e}")
        import traceback
        traceback.print_exc()
        return False

    # Start streaming query
    print("\n6. Starting streaming query...")
    print("   Writing to console for 60 seconds...")

    try:
        query = flights_display.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 20) \
            .start()

        print("   ✓ Streaming query started")
        print(f"   Query ID: {query.id}")
        print(f"   Status: {query.status}")

        # Run for 60 seconds
        print("\n7. Monitoring stream (60 seconds)...")
        print("   Press Ctrl+C to stop early\n")

        start_time = time.time()
        iteration = 0

        while time.time() - start_time < 60:
            time.sleep(5)
            iteration += 1

            status = query.status
            progress = query.lastProgress

            print(f"   [{iteration * 5}s] Status: {status['isDataAvailable']}, "
                  f"Message: {status.get('message', 'N/A')}")

            if progress:
                print(f"        Progress: {progress.get('numInputRows', 0)} input rows, "
                      f"{progress.get('processedRowsPerSecond', 0):.2f} rows/sec")

            if not query.isActive:
                print("   ⚠ Query stopped unexpectedly")
                break

        print("\n8. Stopping streaming query...")
        query.stop()
        print("   ✓ Query stopped")

        # Check if we got any data
        if query.lastProgress:
            total_rows = sum(p.get('numInputRows', 0) for p in [query.lastProgress])
            print(f"\n   Total rows processed: {total_rows}")
            if total_rows > 0:
                print("   ✓ SUCCESS - Streaming is working!")
                return True
            else:
                print("   ⚠ WARNING - No data received (may be rate limited or API issue)")
                return False
        else:
            print("   ⚠ WARNING - No progress information available")
            return False

    except KeyboardInterrupt:
        print("\n   Interrupted by user")
        if 'query' in locals():
            query.stop()
        return False

    except Exception as e:
        print(f"\n   ✗ Error during streaming: {e}")
        import traceback
        traceback.print_exc()
        if 'query' in locals():
            query.stop()
        return False

    finally:
        print("\n9. Cleaning up...")
        spark.stop()
        print("   ✓ Spark session stopped")


if __name__ == "__main__":
    print("\nOpenSky Streaming Test")
    print("This will test the streaming logic locally\n")

    success = test_opensky_streaming()

    print("\n" + "=" * 70)
    if success:
        print("TEST RESULT: ✓ PASSED")
        print("The streaming configuration should work on Databricks!")
    else:
        print("TEST RESULT: ⚠ NEEDS ATTENTION")
        print("Check the output above for issues")
    print("=" * 70)

    exit(0 if success else 1)
