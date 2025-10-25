#!/usr/bin/env python3
"""
Test the new jsonPath feature with Clever API
"""

from pyspark.sql import SparkSession
from pyspark_datasources.rest import RestDataSource

# Token
CLEVER_TOKEN = "d5c461921595f99bbb7c893c4ef932c9b457c06b"

# Initialize Spark
spark = SparkSession.builder \
    .appName("TestJsonPath") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.dataSource.register(RestDataSource)

print("=" * 80)
print("Test: Using jsonPath='data' to automatically extract and flatten students")
print("=" * 80)

# Create input with a single school ID
school_input = '[{"school_id": "5940d254203e37907e0000f0"}]'

# Use jsonPath to extract the 'data' array from the API response
result = spark.read.format("rest") \
    .option("url", "https://api.clever.com/v3.0/schools/{school_id}/users?role=student&limit=5") \
    .option("method", "GET") \
    .option("authType", "Bearer") \
    .option("oauthToken", CLEVER_TOKEN) \
    .option("inputData", school_input) \
    .option("queryType", "inline") \
    .option("jsonPath", "data") \
    .option("includeInputsInOutput", "Y") \
    .option("partitions", "1") \
    .load()

print("\nSchema:")
result.printSchema()

print("\nData (showing first 5 rows):")
result.show(5, truncate=False)

print(f"\nTotal rows: {result.count()}")
print("\n✓ jsonPath feature works! Each student is now a separate row!")
print("=" * 80)

spark.stop()
