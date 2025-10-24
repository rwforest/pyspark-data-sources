#!/usr/bin/env python3
"""
Diagnostic test for POST request empty output issue
"""

from pyspark.sql import SparkSession
from pyspark_datasources import RestDataSource

# Create Spark session
spark = SparkSession.builder \
    .appName("POST Issue Diagnostic") \
    .master("local[2]") \
    .getOrCreate()

# Register the REST data source
spark.dataSource.register(RestDataSource)

print("=" * 80)
print("TEST 1: POST Request - Creating Posts")
print("=" * 80)

# Create test data
post_data = [
    ("My First Post", "This is the content of my first post", 1),
    ("Another Post", "More content here", 1)
]

post_df = spark.createDataFrame(post_data, ["title", "body", "userId"])
post_df.createOrReplaceTempView("posts_to_create")

print("\nInput DataFrame:")
post_df.show(truncate=False)

# POST to API
result_df = spark.read.format("rest") \
    .option("url", "https://jsonplaceholder.typicode.com/posts") \
    .option("input", "posts_to_create") \
    .option("method", "POST") \
    .option("postInputFormat", "json") \
    .load()

print("\nResult DataFrame Schema:")
result_df.printSchema()

print("\nResult DataFrame Count:")
print(f"Rows: {result_df.count()}")

print("\nResult DataFrame Content:")
result_df.show(truncate=False)

# Try to see the actual output column
print("\nTrying to select just the output column:")
result_df.select("output").show(truncate=False)

# Check if output is actually populated
print("\nChecking output column type and sample:")
output_sample = result_df.select("output").first()
print(f"Output value: {output_sample}")
print(f"Output type: {type(output_sample[0]) if output_sample else 'None'}")

print("\n" + "=" * 80)
print("TEST 2: GET Request for Comparison")
print("=" * 80)

# Test with GET for comparison
get_input = [(1,), (2,)]
get_df = spark.createDataFrame(get_input, ["id"])
get_df.createOrReplaceTempView("get_test")

get_result = spark.read.format("rest") \
    .option("url", "https://jsonplaceholder.typicode.com/posts") \
    .option("input", "get_test") \
    .option("method", "GET") \
    .option("queryType", "inline") \
    .load()

print("\nGET Result Schema:")
get_result.printSchema()

print("\nGET Result Count:")
print(f"Rows: {get_result.count()}")

print("\nGET Result Content:")
get_result.show(truncate=False)

spark.stop()
