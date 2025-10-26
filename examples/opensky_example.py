"""
OpenSky Network - Simple Flight Tracker Script

Fetches real-time flight data using REST data source helper.
Run with: python opensky_example.py
"""

from pyspark.sql import SparkSession
from pyspark_datasources import rest_api_call, parse_array_response

def main():
    print("=" * 80)
    print("OpenSky Network - Live Flight Tracker")
    print("=" * 80)

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("OpenSkyExample") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Create dummy input
    dummy_input = spark.createDataFrame([{"placeholder": "dummy"}])

    print("\n✓ Spark initialized")
    print("\nFetching live flights from OpenSky Network...")
    print("⏱️  This may take 10-30 seconds...")

    # Fetch flights (you can add bounding box parameters for specific regions)
    # Example for San Francisco: ?lamin=36.5&lamax=38.5&lomin=-123&lomax=-121
    flights_response = rest_api_call(
        dummy_input,
        url="https://opensky-network.org/api/states/all",
        method="GET",
        queryType="inline",
        partitions="1"
    )

    print("✓ Response received!")

    # Parse the response using parse_array_response helper
    # Note: OpenSky uses ARRAYS instead of objects - different from Clever API!
    # OpenSky: {"time": 123, "states": [["val1", "val2", ...], ...]}  ← arrays
    # Clever:  {"data": [{"key1": "val1", "key2": "val2"}]}           ← objects
    # We use parse_array_response() instead of flatten_json_response()

    # Define column names for the 17-element arrays
    column_names = [
        "icao24", "callsign", "origin_country", "time_position", "last_contact",
        "longitude", "latitude", "geo_altitude", "on_ground", "velocity",
        "true_track", "vertical_rate", "sensors", "baro_altitude",
        "squawk", "spi", "category"
    ]

    print("\nParsing array-based response to DataFrame...")
    flights_df = parse_array_response(
        flights_response,
        array_path="states",        # Field containing the array of arrays
        column_names=column_names,  # Names for the 17 array elements
        timestamp_field="time"      # Timestamp field from response
    )

    print(f"✓ Parsed {flights_df.count()} flights")

    # Limit to 100 for display
    flights_df = flights_df.limit(100)

    print("\n" + "=" * 80)
    print(f"DISPLAYING FIRST {flights_df.count()} FLIGHTS")
    print("=" * 80)

    # Show flights
    flights_df.show(20, truncate=False)

    # Statistics
    print("\n" + "=" * 80)
    print("STATISTICS")
    print("=" * 80)

    total = flights_df.count()
    countries = flights_df.select('origin_country').distinct().count()
    airborne = flights_df.filter(flights_df.on_ground == "False").count()
    on_ground = flights_df.filter(flights_df.on_ground == "True").count()

    print(f"\nTotal flights (sample): {total}")
    print(f"Countries: {countries}")
    print(f"Airborne: {airborne}")
    print(f"On ground: {on_ground}")

    print("\nTop 10 countries:")
    flights_df.groupBy("origin_country").count().orderBy("count", ascending=False).show(10, truncate=False)

    # Save data
    output_path = "output/opensky_snapshot.csv"
    flights_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"\n✓ Saved to {output_path}")

    print("\n" + "=" * 80)
    print("COMPLETE!")
    print("=" * 80)

    spark.stop()

if __name__ == "__main__":
    main()
