#!/usr/bin/env python3
"""
Debug Clever API issue - see actual error message
"""

from pyspark.sql import SparkSession
from pyspark_datasources.rest import RestDataSource
import json

# Initialize Spark
spark = SparkSession.builder \
    .appName("TestCleverDebug") \
    .master("local[*]") \
    .getOrCreate()

# Register REST data source
spark.dataSource.register(RestDataSource)

# Token
CLEVER_TOKEN = "d5c461921595f99bbb7c893c4ef932c9b457c06b"

# Create input
dummy_input = '[{"placeholder": "dummy"}]'

print("=" * 80)
print("Testing with includeInputsInOutput=Y to see _corrupt_record")
print("=" * 80)

try:
    schools_df = spark.read.format("rest") \
        .option("url", "https://api.clever.com/v3.0/schools?limit=1") \
        .option("method", "GET") \
        .option("authType", "Bearer") \
        .option("oauthToken", CLEVER_TOKEN) \
        .option("inputData", dummy_input) \
        .option("queryType", "inline") \
        .option("includeInputsInOutput", "Y") \
        .option("partitions", "1") \
        .load()

    print("\nSchema:")
    schools_df.printSchema()

    print("\nCollecting...")
    rows = schools_df.collect()
    print(f"Got {len(rows)} rows")

    if rows:
        print("\nFirst row:")
        print(rows[0])

        if "_corrupt_record" in rows[0].asDict():
            print("\nERROR MESSAGE:")
            print(rows[0]["_corrupt_record"])

except Exception as e:
    print(f"\nERROR: {e}")
    import traceback
    traceback.print_exc()
