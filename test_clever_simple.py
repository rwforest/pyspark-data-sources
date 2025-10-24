"""
Simple test of Clever API with REST data source (direct format approach)
"""
from pyspark.sql import SparkSession
from pyspark_datasources.rest import RestDataSource

# Token for Chess4Life (Dev) Sandbox
CLEVER_TOKEN = "d5c461921595f99bbb7c893c4ef932c9b457c06b"

# Initialize Spark
spark = SparkSession.builder \
    .appName("CleverAPISimple") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Register the REST data source
spark.dataSource.register(RestDataSource)

print("Testing Clever API - Getting Districts")
print("=" * 70)

# Create a minimal input - just need one row to make one API call
input_data = '[{"_": "dummy"}]'  # Minimal JSON input

try:
    # Try to get districts
    result_df = spark.read.format("rest") \
        .option("url", "https://api.clever.com/v3.0/districts") \
        .option("method", "GET") \
        .option("authType", "Bearer") \
        .option("oauthToken", CLEVER_TOKEN) \
        .option("inputData", input_data) \
        .option("queryType", "inline") \
        .option("partitions", "1") \
        .load()

    print("Success! Got districts:")
    result_df.show(truncate=False)

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

spark.stop()
