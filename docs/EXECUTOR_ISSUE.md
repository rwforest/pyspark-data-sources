# REST Data Source - Executor Access Issue

## Problem Summary

The REST data source is encountering empty outputs due to a fundamental architectural issue with how it tries to access input data.

### Error Observed
```
RuntimeError: No active Spark session found
```

### Root Cause

The Python Data Source API methods (`partitions()` and `read()`) run in contexts where `SparkSession.getActiveSession()` returns `None`:

1. **In `partitions()` method**: Runs on driver but **before** the Spark context is fully initialized for the data source
2. **In `read()` method**: Runs on executor nodes which don't have access to the driver's Spark session

### Current Implementation Problem

The REST data source tries to:
1. Read input data from a temporary table (`SELECT * FROM {self.input_table}`)
2. This requires access to `SparkSession`
3. But `SparkSession` is not available in these methods

```python
# This FAILS:
def partitions(self) -> List[InputPartition]:
    spark = SparkSession.getActiveSession()  # Returns None!
    input_df = spark.sql(f"SELECT * FROM {self.input_table}")  # FAILS
```

## Why This Design Doesn't Work

The Python Data Source API is designed for reading **external data** (files, databases, APIs with fixed endpoints), not for reading from **other Spark DataFrames/tables**.

### Comparison:

**Working Pattern** (Other data sources):
```python
# JSONPlaceholder data source
df = spark.read.format("jsonplaceholder") \\
    .option("endpoint", "posts") \\  # Fixed external resource
    .load()
```

**Problematic Pattern** (REST data source):
```python
# REST data source - tries to read from Spark table
df = spark.read.format("rest") \\
    .option("url", "https://api.example.com") \\
    .option("input", "my_spark_table") \\  # Needs to read Spark table!
    .load()
```

## Proposed Solutions

### Solution 1: Use UDF Instead of Data Source (Recommended)

Convert the REST data source to a UDF-based approach:

```python
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import StringType
import requests

@udf(returnType=StringType())
def call_rest_api(url, method, **params):
    if method == "POST":
        response = requests.post(url, json=params)
    else:
        response = requests.get(url, params=params)
    return response.text

# Usage:
input_df = spark.createDataFrame([...])
result_df = input_df.withColumn(
    "api_response",
    call_rest_api(lit("https://api.example.com/posts"), lit("POST"), struct(*input_df.columns))
)
```

**Pros:**
- Works with Spark's execution model
- Can access input DataFrame directly
- Simpler implementation
- Better error handling

**Cons:**
- Different API than original Scala version
- Not a "data source" anymore

### Solution 2: Reimplement as Table Function

Use Spark's Table Function API (if available in PySpark 4.0):

```python
# This would allow:
df = spark.sql("""
    SELECT * FROM rest_call(
        url => 'https://api.example.com/posts',
        method => 'POST',
        input => SELECT * FROM my_input_table
    )
""")
```

**Status:** Need to check if Table Functions are available in PySpark 4.0

### Solution 3: Pre-serialize Input Data in Options

Pass the actual input data as a serialized option:

```python
# In user code:
input_data = input_df.collect()  # Collect on driver
serialized = json.dumps([row.asDict() for row in input_data])

result_df = spark.read.format("rest") \\
    .option("url", "https://api.example.com") \\
    .option("inputData", serialized) \\  # Pass actual data, not table name
    .load()
```

**Pros:**
- Keeps data source API
- Works with current constraints

**Cons:**
- Requires collecting data to driver first
- Options have size limits
- Not user-friendly
- Defeats purpose of distributed processing for large datasets

### Solution 4: Use Broadcast Variables (Complex)

Create a separate registration step that broadcasts the input data:

```python
# Step 1: Register input data
from pyspark_datasources import RestDataSource
RestDataSource.register_input("my_input", input_df)

# Step 2: Use data source
result_df = spark.read.format("rest") \\
    .option("url", "https://api.example.com") \\
    .option("inputRef", "my_input") \\  # Reference to broadcast data
    .load()
```

**Pros:**
- Keeps data source pattern
- Can handle larger datasets via broadcast

**Cons:**
- Complex implementation
- Memory intensive
- Requires global state management

## Recommendation

**For immediate fix**: Implement **Solution 3** (Pre-serialize Input Data)
- Quickest to implement
- Maintains compatibility with existing API
- Works for small to medium datasets
- Document the size limitations

**For long-term**: Provide **Solution 1** (UDF-based) as alternative
- More Spark-native approach
- Better for production use
- Can handle arbitrary dataset sizes
- Provide both options and let users choose

## Migration Path

1. **Phase 1** (Immediate): Fix current implementation with Solution 3
2. **Phase 2** (Next release): Add UDF-based alternative API
3. **Phase 3** (Future): Deprecate data source approach, recommend UDF

## Implementation Notes

The Scala version likely works because Scala's Data Source V2 API has different access patterns and may allow accessing the Spark session or catalog differently. The Python API is more restrictive.
