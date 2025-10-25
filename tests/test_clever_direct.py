#!/usr/bin/env python3
"""
Test direct REST data source call
"""

from pyspark.sql import SparkSession
from pyspark_datasources.rest import RestDataSource
import json

# Initialize Spark
spark = SparkSession.builder \
    .appName("TestCleverDirect") \
    .master("local[*]") \
    .getOrCreate()

# Register REST data source
spark.dataSource.register(RestDataSource)

# Token from guide
CLEVER_TOKEN = ""

# Create input
dummy_input = '[{"placeholder": "dummy"}]'

print("=" * 80)
print("Testing direct REST data source")
print("=" * 80)

try:
    schools_df = spark.read.format("rest") \
        .option("url", "https://api.clever.com/v3.0/schools?limit=3") \
        .option("method", "GET") \
        .option("authType", "Bearer") \
        .option("oauthToken", CLEVER_TOKEN) \
        .option("inputData", dummy_input) \
        .option("queryType", "inline") \
        .option("includeInputsInOutput", "N") \
        .option("partitions", "1") \
        .load()

    print("\nSchema:")
    schools_df.printSchema()

    print("\nData:")
    schools_df.show(truncate=False)

    print("\nCollecting data...")
    rows = schools_df.collect()
    print(f"Got {len(rows)} rows")

    if rows:
        print("\nFirst row:")
        print(rows[0])
        print("\nOutput column:")
        print(rows[0]["output"])

except Exception as e:
    print(f"\nERROR: {e}")
    import traceback
    traceback.print_exc()
