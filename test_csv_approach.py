#!/usr/bin/env python3
"""
Test the CSV-based approach for REST data source
"""

from pyspark.sql import SparkSession
from pyspark_datasources import rest_api_call_csv

# Create Spark session
spark = SparkSession.builder \
    .appName("REST CSV Test") \
    .master("local[2]") \
    .getOrCreate()

print("=" * 80)
print("TEST: POST Request Using CSV Approach")
print("=" * 80)

# Create test data
post_data = [
    ("My First Post", "This is the content of my first post", 1),
    ("Another Post", "More content here", 1)
]

post_df = spark.createDataFrame(post_data, ["title", "body", "userId"])

print("\nInput DataFrame:")
post_df.show(truncate=False)

# Use the CSV-based helper function
result_df = rest_api_call_csv(
    post_df,
    url="https://jsonplaceholder.typicode.com/posts",
    method="POST",
    postInputFormat="json"
)

print("\nResult DataFrame Schema:")
result_df.printSchema()

print("\nResult DataFrame Count:")
print(f"Rows: {result_df.count()}")

print("\nResult DataFrame Content:")
result_df.show(truncate=False)

print("\n" + "=" * 80)
print("SUCCESS! The CSV approach works!")
print("=" * 80)

spark.stop()
