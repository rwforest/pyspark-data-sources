"""
Test script for streaming with parsed flight data
Tests the logic from the new notebook cell without running for 5 minutes
"""

from pyspark.sql import SparkSession
from pyspark_datasources import RestDataSource
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType
import json
import time

print("=" * 80)
print("Testing Streaming with Parsed Flight Data")
print("=" * 80)

# Initialize Spark
spark = SparkSession.builder \
    .appName("OpenSkyStreamingTest") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Register data source
spark.dataSource.register(RestDataSource)

# Define regions (from opensky.py)
regions = {
    "EUROPE": {"lamin": 35.0, "lamax": 72.0, "lomin": -25.0, "lomax": 45.0},
    "NORTH_AMERICA": {"lamin": 7.0, "lamax": 72.0, "lomin": -168.0, "lomax": -60.0},
}

# Use North America (smaller region for testing)
region = "NORTH_AMERICA"
bbox = regions[region]
url = f"https://opensky-network.org/api/states/all?lamin={bbox['lamin']}&lamax={bbox['lamax']}&lomin={bbox['lomin']}&lomax={bbox['lomax']}"

print(f"\n📍 Region: {region}")
print(f"   Bounds: {bbox}")
print(f"🌐 URL: {url}\n")

# Configure streaming
input_json = json.dumps([{"placeholder": "dummy"}])
stream_df = spark.readStream.format("rest") \
    .option("url", url) \
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

# Define schema for JSON response
response_schema = StructType([
    StructField("time", LongType(), True),
    StructField("states", ArrayType(ArrayType(StringType())), True)
])

# Column names
column_names = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "geo_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "baro_altitude",
    "squawk", "spi", "category"
]

# Parse JSON and explode to individual flights
parsed_stream = stream_df \
    .select(
        col("placeholder"),
        from_json(col("output"), response_schema).alias("data")
    ) \
    .select(
        col("placeholder"),
        col("data.time").alias("api_time"),
        explode(col("data.states")).alias("flight_array")
    )

# Extract fields
for i, col_name in enumerate(column_names):
    parsed_stream = parsed_stream.withColumn(col_name, col("flight_array")[i])

# Select final columns
flights_stream = parsed_stream.select(
    "api_time",
    "icao24",
    "callsign",
    "origin_country",
    "longitude",
    "latitude",
    "geo_altitude",
    "velocity",
    "on_ground"
)

print("✓ Stream configured with parsing")
print("⏱️  Will poll every 10 seconds")
print("⏰ Running for 30 seconds (test mode)\n")

# Start streaming
query = flights_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", "10") \
    .start()

print("✓ Streaming started!\n")

# Run for 30 seconds (test mode)
try:
    time.sleep(30)
except KeyboardInterrupt:
    print("\n⚠️  Interrupted")

# Stop
print("\n⏹️  Stopping...")
query.stop()

print("\n" + "=" * 80)
print("✓ Test completed!")
print("=" * 80)
print("Expected output: Individual flight rows (not just 1 response row)")
print("Each batch should show multiple flights with ICAO, callsign, country, etc.")
print("=" * 80)

spark.stop()
