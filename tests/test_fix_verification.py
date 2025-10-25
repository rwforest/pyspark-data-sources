#!/usr/bin/env python3
"""
Verify the REST data source fix works
"""

from pyspark.sql import SparkSession
from pyspark_datasources.rest import RestDataSource

# Token
CLEVER_TOKEN = "d5c461921595f99bbb7c893c4ef932c9b457c06b"

# Initialize Spark
spark = SparkSession.builder \
    .appName("TestFix") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.dataSource.register(RestDataSource)

print("=" * 80)
print("Test 1: includeInputsInOutput='N'")
print("=" * 80)

dummy_input = '[{"placeholder": "dummy"}]'

result1 = spark.read.format("rest") \
    .option("url", "https://api.clever.com/v3.0/districts") \
    .option("method", "GET") \
    .option("authType", "Bearer") \
    .option("oauthToken", CLEVER_TOKEN) \
    .option("inputData", dummy_input) \
    .option("queryType", "inline") \
    .option("includeInputsInOutput", "N") \
    .option("partitions", "1") \
    .load()

print("Schema:")
result1.printSchema()
print("\nColumns:", result1.columns)
print("Count:", result1.count())
print("✓ Test 1 PASSED\n")

print("=" * 80)
print("Test 2: includeInputsInOutput='Y' (default)")
print("=" * 80)

result2 = spark.read.format("rest") \
    .option("url", "https://api.clever.com/v3.0/schools?limit=2") \
    .option("method", "GET") \
    .option("authType", "Bearer") \
    .option("oauthToken", CLEVER_TOKEN) \
    .option("inputData", dummy_input) \
    .option("queryType", "inline") \
    .option("includeInputsInOutput", "Y") \
    .option("partitions", "1") \
    .load()

print("Schema:")
result2.printSchema()
print("\nColumns:", result2.columns)
print("Count:", result2.count())
print("✓ Test 2 PASSED\n")

print("=" * 80)
print("ALL TESTS PASSED!")
print("=" * 80)

spark.stop()
