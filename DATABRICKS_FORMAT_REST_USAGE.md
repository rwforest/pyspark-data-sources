# Using .format("rest") Directly in Databricks

## TL;DR - Recommended Approach

**Use the helper function** - it's simpler and handles everything:

```python
from pyspark_datasources import rest_api_call

result_df = rest_api_call(
    input_df,
    url="https://api.example.com",
    method="POST"
)
```

---

## Why .format("rest") with "input" option fails

The error you're seeing:

```
RuntimeError: No active Spark session found. When using 'input' option
with table name, ensure the Spark session is active.
```

This happens because:
1. Databricks' Python Data Source API execution context doesn't provide `SparkSession.getActiveSession()`
2. The data source can't read from the temporary table
3. This is a **limitation of the Python Data Source API in Databricks**

## Solution: Use "inputData" option instead

If you must use `.format("rest")` directly, you need to manually serialize the input:

```python
import json
from pyspark_datasources import RestDataSource

# Register data source
spark.dataSource.register(RestDataSource)

# Create input data
post_data = [
    ("My First Post", "This is the content of my first post", 1),
    ("Another Post", "More content here", 1)
]
post_df = spark.createDataFrame(post_data, ["title", "body", "userId"])

# MANUAL APPROACH: Collect and serialize
input_data = [row.asDict() for row in post_df.collect()]
serialized_input = json.dumps(input_data)

# Use inputData option instead of input
result_df = spark.read.format("rest") \
    .option("url", "https://jsonplaceholder.typicode.com/posts") \
    .option("inputData", serialized_input) \
    .option("method", "POST") \
    .option("postInputFormat", "json") \
    .option("includeInputsInOutput", "N") \
    .load()

# Note: This only returns the "output" column
# You'll need to join it back to post_df if you want input columns
display(result_df)
```

### Issues with this approach:
1. ❌ **Verbose** - lots of manual code
2. ❌ **No input columns** - only returns "output" column
3. ❌ **Manual join required** - if you want input + output together
4. ❌ **Schema mismatch risk** - if `includeInputsInOutput='Y'`

## Comparison: Helper vs Direct

### Option 1: Helper Function (Recommended) ✅

```python
from pyspark_datasources import rest_api_call

result_df = rest_api_call(
    post_df,
    url="https://jsonplaceholder.typicode.com/posts",
    method="POST",
    postInputFormat="json"
)

# Returns: input columns + output column
display(result_df)
```

**Pros:**
- ✅ Simple one-liner
- ✅ Returns input + output columns
- ✅ Handles serialization automatically
- ✅ No schema issues
- ✅ Correct row counts

**Cons:**
- ⚠️ Collects to driver (< 10K rows recommended)

### Option 2: Direct format("rest") with inputData

```python
import json

# Manual serialization
input_data = [row.asDict() for row in post_df.collect()]
serialized = json.dumps(input_data)

# Direct call
result_df = spark.read.format("rest") \
    .option("url", "https://jsonplaceholder.typicode.com/posts") \
    .option("inputData", serialized) \
    .option("method", "POST") \
    .option("includeInputsInOutput", "N") \
    .load()

# Manual join to get input columns back
from pyspark.sql.functions import monotonically_increasing_id
input_with_id = post_df.withColumn("_id", monotonically_increasing_id())
result_with_id = result_df.withColumn("_id", monotonically_increasing_id())
final_df = input_with_id.join(result_with_id, "_id").drop("_id")

display(final_df)
```

**Pros:**
- ✅ Direct data source API usage

**Cons:**
- ❌ Verbose (lots of code)
- ❌ Manual serialization required
- ❌ Manual join required
- ❌ Join issues in distributed mode (same problem we fixed)
- ❌ More error-prone

### Option 3: Direct format("rest") with "input" table ❌ DOESN'T WORK

```python
# THIS FAILS IN DATABRICKS:
post_df.createOrReplaceTempView("posts_to_create")

result_df = spark.read.format("rest") \
    .option("url", "https://jsonplaceholder.typicode.com/posts") \
    .option("input", "posts_to_create") \
    .option("method", "POST") \
    .load()  # RuntimeError: No active Spark session found ❌
```

**Why it fails:**
- ❌ Databricks Python Data Source API doesn't provide session access
- ❌ Can't read from temp tables in that context

## Recommendation by Use Case

### For API Enrichment (< 10K rows) → Use Helper Function
```python
from pyspark_datasources import rest_api_call

enriched_df = rest_api_call(input_df, url=api_url, method="POST")
```

### For Larger Datasets (> 10K rows) → Use UDF
```python
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import StringType
import requests

@udf(returnType=StringType())
def call_api(params):
    response = requests.post(api_url, json=params)
    return response.text

result_df = input_df.withColumn(
    "api_response",
    call_api(struct(*input_df.columns))
)
```

### For Complex Workflows → Use Helper in Batches
```python
from pyspark_datasources import rest_api_call

# Process in batches of 5000 rows
batch_size = 5000
total_rows = input_df.count()
num_batches = (total_rows + batch_size - 1) // batch_size

results = []
for i in range(num_batches):
    batch_df = input_df.limit(batch_size).offset(i * batch_size)
    batch_result = rest_api_call(batch_df, url=api_url, method="POST")
    results.append(batch_result)

final_df = results[0]
for result in results[1:]:
    final_df = final_df.union(result)
```

## Summary

| Approach | Works in Databricks? | Row Count Correct? | Complexity | Recommended? |
|----------|---------------------|-------------------|------------|--------------|
| `rest_api_call()` | ✅ Yes | ✅ Yes | Low | ✅ **YES** |
| `.format("rest")` + `inputData` | ✅ Yes | ⚠️ Manual join needed | High | ⚠️ Maybe |
| `.format("rest")` + `input` table | ❌ No | N/A | N/A | ❌ **NO** |
| UDF approach | ✅ Yes | ✅ Yes | Medium | ✅ For large datasets |

## Your Error - Quick Fix

Change from:
```python
# This fails:
result_df = spark.read.format("rest") \
    .option("url", "https://jsonplaceholder.typicode.com/posts") \
    .option("input", "test_input") \
    .option("method", "GET") \
    .load()
```

To:
```python
# This works:
from pyspark_datasources import rest_api_call

# Get the DataFrame instead of using table name
input_df = spark.table("test_input")  # or your existing DataFrame

result_df = rest_api_call(
    input_df,
    url="https://jsonplaceholder.typicode.com/posts",
    method="GET"
)
```

## Why We Can't Fix the "input" option in Databricks

The Python Data Source API in PySpark 4.0 has architectural limitations:
1. The `partitions()` method runs in a restricted context
2. `SparkSession.getActiveSession()` returns `None` in Databricks
3. Can't access catalog/temp tables from that context
4. This is by design for security/isolation

The Scala version works because:
- Scala's Data Source V2 API has different access patterns
- Can access catalog directly
- Different execution model

**Bottom line**: Use `rest_api_call()` helper - it's the recommended approach!
