# REST Data Source - Fix Summary

## Issue Discovered

When running POST requests in Databricks, the REST data source returns empty outputs due to two fundamental issues:

### Issue 1: Spark Session Access ✅ FIXED

**Problem**: The Python Data Source API's `partitions()` and `read()` methods don't have access to `SparkSession.getActiveSession()`, causing failures when trying to read input tables.

**Solution Implemented**:
- Added `inputData` option to pass pre-serialized JSON data
- Created `rest_api_call()` helper function that collects data on driver and passes it to the data source
- Maintained backward compatibility with `input` table option (with better error messages)

### Issue 2: Schema Mismatch ❌ IN PROGRESS

**Problem**: The data source returns a hardcoded schema `"output STRING"` but actual data includes multiple columns (input columns + output column), causing:
```
PySparkRuntimeError: [DATA_SOURCE_RETURN_SCHEMA_MISMATCH]
Return schema mismatch in the result from 'read' method.
Expected: 1 columns, Found: 4 columns.
```

**Root Cause**: The `schema()` method in `RestDataSource` returns:
```python
def schema(self) -> str:
    return "output STRING"
```

But the actual data returned has:
- All input columns (when `includeInputsInOutput='Y'`)
- An `output` column with the API response
- Possibly a `_corrupt_record` column for errors

**Challenge**: The schema depends on:
1. Input data columns (unknown until runtime)
2. `includeInputsInOutput` option
3. Whether errors occurred (corrupt records column)
4. Structure of API responses (could be objects, arrays, primitives)

## Attempted Solutions

### Attempt 1: Dynamic Schema Inference
**Status**: Not possible with current PySpark Data Source API

The Python Data Source API's `schema()` method:
- Must return a schema string or DDL
- Is called BEFORE `partitions()` and `read()`
- Doesn't have access to options or input data at schema definition time

### Attempt 2: Use JSON Inference
**Status**: Partially working, needs refinement

Current approach returns `"output STRING"` and relies on Spark's JSON parsing, but this doesn't match the actual Row structure being returned.

## Current Workaround

The data IS being processed correctly (API calls are made, responses received), but Spark rejects the results due to schema mismatch.

**Temporary Fix Options**:

### Option A: Always return full schema (Recommended for now)
Change `includeInputsInOutput` default to `'N'` and always return just:
```python
def schema(self) -> str:
    return "output STRING, _corrupt_record STRING"
```

This simplifies the schema but loses input column context.

### Option B: Force includeInputsInOutput='N' in helper function
Modify `rest_api_call()` to always set `includeInputsInOutput='N'`, then join results back to input DataFrame:

```python
def rest_api_call(input_df, url, method, **options):
    # Force simplified output
    options['includeInputsInOutput'] = 'N'

    result_df = spark.read.format("rest")...

    # Join back to input if needed
    return input_df.join(result_df, ...)
```

### Option C: Abandon Data Source API, Use UDF
Convert to a UDF-based approach which doesn't have schema restrictions:

```python
@udf(returnType=StringType())
def call_rest_api(title, body, userId):
    response = requests.post(url, json={"title": title, "body": body, "userId": userId})
    return response.text

result_df = input_df.withColumn("api_response", call_rest_api(*input_df.columns))
```

## Recommendation

**Immediate**: Implement Option B (force `includeInputsInOutput='N'` in helper)

**Long-term**: Provide both options:
1. Keep REST data source for simple use cases
2. Add UDF-based approach for production use

The Python Data Source API has limitations that make it unsuitable for this dynamic schema use case.

## Files Modified So Far

1. `pyspark_datasources/rest.py`:
   - Added support for `inputData` option
   - Improved error messages
   - Fixed partition data distribution

2. `pyspark_datasources/rest_helper.py`: (NEW)
   - Helper function `rest_api_call()`
   - Handles data collection and serialization

3. `pyspark_datasources/__init__.py`:
   - Exported `rest_api_call` function

## Next Steps

1. Implement Option B in `rest_helper.py`
2. Test with POST requests
3. Update documentation
4. Consider adding UDF-based alternative for v2
