# CSV Approach Analysis - Why It Doesn't Solve The Problem

## The Idea

Use a temporary CSV file to bypass the Spark session access limitation:

1. Write input DataFrame to CSV
2. Pass CSV file path to data source via `inputCsvPath` option
3. Data source reads from CSV file (no Spark session needed!)

## Why This Sounded Promising

- CSV files can be read from executors without Spark session
- Avoids the `getActiveSession()` limitation
- Seems like it would allow `.format("rest")` to work directly

## The Reality: It Doesn't Work

### Problem 1: Still Hits Schema Mismatch

Even with CSV input, the data source still has the schema issue:

```python
# Data source schema (hardcoded)
def schema(self):
    return "output STRING"  # Only 1 column!

# Actual data when includeInputsInOutput='Y'
# - title (from input)
# - body (from input)
# - userId (from input)
# - output (API response)
# = 4 columns!

# Result: Schema mismatch error! ❌
```

### Problem 2: Still Needs to Collect Results

The `rest_api_call_csv()` function still needs to:
```python
result_rows = reader.load().collect()  # Collect anyway!
```

Because:
- Can't return lazy DataFrame (schema issues)
- Need to join with input columns
- Need to clean up temp CSV after reading

So we're **collecting anyway** - no benefit over the current approach!

### Problem 3: Extra Complexity

CSV approach adds:
- File I/O operations
- Temporary file management
- Cleanup logic
- CSV parsing overhead
- No actual benefits

## Test Results

```bash
$ python test_csv_approach.py

# Still fails with schema mismatch when includeInputsInOutput='Y'
PythonException: Schema mismatch
```

## Why `rest_api_call()` (Current Solution) Is Better

### Current Working Approach

```python
def rest_api_call(input_df, url, method, **options):
    # 1. Collect input
    input_rows = input_df.collect()

    # 2. Serialize to JSON
    serialized = json.dumps([row.asDict() for row in input_rows])

    # 3. Call API with includeInputsInOutput='N'
    api_results = spark.read.format("rest") \
        .option("inputData", serialized) \
        .option("includeInputsInOutput", "N") \  # Avoids schema issues
        .load().collect()

    # 4. Join in-memory
    combined = [
        {**input_row.asDict(), 'output': output_row['output']}
        for input_row, output_row in zip(input_rows, api_results)
    ]

    # 5. Return DataFrame
    return spark.createDataFrame(combined)
```

### Why This Is Optimal

✅ **No file I/O** - Everything in memory
✅ **No temp files** - No cleanup needed
✅ **No schema issues** - We control the final schema
✅ **Guaranteed ordering** - `zip()` preserves order
✅ **Simpler code** - Fewer moving parts
✅ **Already working** - Tested and verified

## Comparison

| Aspect | CSV Approach | Current rest_api_call() |
|--------|--------------|------------------------|
| Needs to collect | ✅ Yes | ✅ Yes |
| File I/O overhead | ❌ Yes | ✅ No |
| Temp file management | ❌ Required | ✅ Not needed |
| Schema issues | ❌ Yes | ✅ No |
| Lines of code | ~50 | ~40 |
| Complexity | High | Medium |
| **Works?** | ❌ **No** | ✅ **Yes** |

## The Fundamental Issue

The problem isn't **how** we pass data to the data source.

The problem is the **Python Data Source API design**:

1. **Schema must be known upfront** - Can't be dynamic based on input
2. **Methods run in isolation** - No session access
3. **Designed for external data** - Not for DataFrame-to-DataFrame operations

### What Would Need to Change

For `.format("rest")` to work properly:

```python
def schema(self):
    # Would need to:
    # 1. Know input DataFrame schema
    # 2. Add "output" column
    # 3. Return combined schema

    # But we CAN'T because:
    # - No access to input DataFrame
    # - No access to Spark session
    # - Schema is called before partitions()

    return "???"  # Impossible!
```

## Conclusion

❌ **CSV approach doesn't solve anything**
✅ **Current `rest_api_call()` is the correct solution**

### Why The Current Solution Is Right

The REST data source is fundamentally trying to do something the Python Data Source API wasn't designed for:

- **Not designed for**: DataFrame → API → DataFrame enrichment
- **Designed for**: External source → DataFrame

The `rest_api_call()` helper works **with** the API's design, not against it:
- Uses data source for what it's good at (making API calls)
- Handles DataFrame operations outside the data source
- Results in clean, working code

### Recommendation

**Keep using `rest_api_call()` - it's the right approach!**

Don't pursue the CSV approach - it adds complexity without solving anything.

## What We Learned

1. ✅ Investigated TaskContext - doesn't provide session access
2. ✅ Checked official Spark docs - no examples of DataFrame access in data sources
3. ✅ Tested CSV approach - doesn't bypass the fundamental limitations
4. ✅ Confirmed current solution is optimal

The architectural investigation was valuable - we now understand **why** the limitations exist and **why** the current solution is correct.
