#!/usr/bin/env python3
"""
Test script to see actual Clever API response structure
"""

from pyspark.sql import SparkSession
from pyspark_datasources import rest_api_call
import ast

# Initialize Spark
spark = SparkSession.builder \
    .appName("TestClever") \
    .master("local[*]") \
    .getOrCreate()

# Token from guide
CLEVER_TOKEN = ""

# Create dummy input
dummy_input = spark.createDataFrame([{"placeholder": "dummy"}])

print("=" * 80)
print("TEST 1: Fetch a few schools")
print("=" * 80)

schools_response = rest_api_call(
    dummy_input,
    url="https://api.clever.com/v3.0/schools?limit=3",
    method="GET",
    authType="Bearer",
    oauthToken=CLEVER_TOKEN,
    queryType="inline",
    partitions="1"
)

print("\nSchools DataFrame Schema:")
schools_response.printSchema()

print("\nSchools DataFrame:")
schools_response.show(truncate=False)

# Get the actual response
school_output = schools_response.select("output").first()["output"]
print("\nRaw output type:", type(school_output))
print("\nRaw output preview (first 500 chars):")
print(school_output[:500])

# Parse it
school_data = ast.literal_eval(school_output)
print("\nParsed data keys:", school_data.keys())
print("\nFirst school:")
if school_data.get("data"):
    print(school_data["data"][0])
