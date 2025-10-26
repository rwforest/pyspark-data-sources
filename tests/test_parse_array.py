"""Test parse_array_response with OpenSky data"""

from pyspark.sql import SparkSession
from pyspark_datasources import rest_api_call, parse_array_response

spark = SparkSession.builder \
    .appName("TestParseArray") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

dummy_input = spark.createDataFrame([{"placeholder": "dummy"}])

print("Fetching OpenSky data...")
flights_response = rest_api_call(
    dummy_input,
    url="https://opensky-network.org/api/states/all?lamin=36.5&lamax=38.5&lomin=-123&lomax=-121",
    method="GET",
    queryType="inline",
    partitions="1"
)

print("✓ Response received!")

# Define column names for OpenSky's 17-element arrays
column_names = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "geo_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "baro_altitude",
    "squawk", "spi", "category"
]

print("\nParsing arrays to DataFrame...")
flights_df = parse_array_response(
    flights_response,
    array_path="states",
    column_names=column_names,
    timestamp_field="time"
)

print(f"✓ Parsed {flights_df.count()} flights")
print(f"Columns: {flights_df.columns}")

print("\nSample data:")
flights_df.show(10, truncate=False)

print("\nSchema:")
flights_df.printSchema()

spark.stop()
