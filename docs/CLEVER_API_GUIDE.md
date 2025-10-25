# Clever API Integration Guide

This guide shows how to use the PySpark REST Data Source to fetch data from the Clever API.

## Token Information

**Token:** ``
**District:** Chess4Life (Dev) Sandbox
**District ID:** `5940d0b58ec81e0001541ef3`

The token provides access to:
- 1 District (Chess4Life Dev Sandbox)
- 100 Schools
- Teachers data
- Students data

## Prerequisites

```bash
# Install the pyspark-data-sources package
pip install dist/pyspark_datasources-0.1.0-py3-none-any.whl

# Or build from source
poetry build
```

## Clever API Hierarchy

Clever API has a hierarchical structure:
1. **Districts** - Top level (no parameters needed)
2. **Schools** - Belong to districts
3. **Teachers/Students** - Belong to schools

## Basic Usage Pattern

### Step 1: Get Districts

```python
from pyspark.sql import SparkSession
from pyspark_datasources import rest_api_call
import json
import ast

# Initialize Spark
spark = SparkSession.builder \
    .appName("CleverAPI") \
    .master("local[*]") \
    .getOrCreate()

# Your Clever API token
CLEVER_TOKEN = ""

# Create dummy input DataFrame (required but not used for GET requests)
dummy_input = spark.createDataFrame([{"placeholder": "dummy"}])

# Get districts using rest_helper
districts_df = rest_api_call(
    dummy_input,
    url="https://api.clever.com/v3.0/districts",
    method="GET",
    authType="Bearer",
    oauthToken=CLEVER_TOKEN,
    queryType="inline",
    partitions="1"
)

# Parse the response from the output column
district_response = districts_df.select("output").first()["output"]
district_data = ast.literal_eval(district_response)

# Extract district information
for district in district_data["data"]:
    district_info = district.get("data", {})
    print(f"District: {district_info.get('name')}")
    print(f"ID: {district_info.get('id')}")
```

### Step 2: Get Schools

```python
# Get schools for the district (limit to 10 for demo)
schools_df = rest_api_call(
    dummy_input,
    url="https://api.clever.com/v3.0/schools?limit=10",
    method="GET",
    authType="Bearer",
    oauthToken=CLEVER_TOKEN,
    queryType="inline",
    partitions="1"
)

# Parse schools
school_response = schools_df.select("output").first()["output"]
school_data = ast.literal_eval(school_response)

# Extract school information
schools = []
for school in school_data["data"]:
    school_info = school.get("data", {})
    school_id = school_info.get("id")
    school_name = school_info.get("name")
    schools.append({"id": school_id, "name": school_name})
    print(f"School: {school_name} (ID: {school_id})")
```

### Step 3: Get Teachers for a School

```python
# Pick a school
school_id = schools[0]["id"]
school_name = schools[0]["name"]

# Create input DataFrame with school ID
school_input = spark.createDataFrame([{"school_id": school_id}])

# Get teachers for this school
teachers_df = rest_api_call(
    school_input,
    url=f"https://api.clever.com/v3.0/schools/{school_id}/users?role=teacher&limit=10",
    method="GET",
    authType="Bearer",
    oauthToken=CLEVER_TOKEN,
    queryType="inline",
    partitions="1"
)

# Parse teachers
teacher_response = teachers_df.select("output").first()["output"]
teacher_data = ast.literal_eval(teacher_response)

# Extract teacher information
for teacher in teacher_data["data"]:
    teacher_info = teacher.get("data", {})
    teacher_name = teacher_info.get("name", {})
    print(f"Teacher: {teacher_name.get('first')} {teacher_name.get('last')}")
```

### Step 4: Get Students for a School

```python
# Get students for the same school
students_df = rest_api_call(
    school_input,
    url=f"https://api.clever.com/v3.0/schools/{school_id}/users?role=student&limit=10",
    method="GET",
    authType="Bearer",
    oauthToken=CLEVER_TOKEN,
    queryType="inline",
    partitions="1"
)

# Parse students
student_response = students_df.select("output").first()["output"]
student_data = ast.literal_eval(student_response)

# Extract student information
for student in student_data["data"]:
    student_info = student.get("data", {})
    student_name = student_info.get("name", {})
    print(f"Student: {student_name.get('first')} {student_name.get('last')}")
```

## Key Clever API Endpoints

| Resource | Endpoint | Description | Parameters |
|----------|----------|-------------|------------|
| Districts | `GET /v3.0/districts` | Get all districts | None |
| All Schools | `GET /v3.0/schools` | Get all schools in district | `?limit=N` |
| School Details | `GET /v3.0/schools/{id}` | Get specific school | School ID in URL |
| School Users | `GET /v3.0/schools/{id}/users` | Get users for school | `?role=teacher` or `?role=student` |
| All Users | `GET /v3.0/users` | Get all users | `?role=teacher` or `?role=student` |

## Using the Helper Function

The `rest_api_call()` helper function simplifies REST API calls by:

- **Automatic serialization**: Converts DataFrames to JSON automatically
- **Cleaner syntax**: Less boilerplate code
- **No manual schema handling**: Combines input and output correctly
- **Better error handling**: Works around Python Data Source API limitations

### Two Helper Functions Available

1. **`rest_api_call(input_df, url, method, **options)`**
   - Best for small to medium datasets (< 10,000 rows)
   - Simplest to use
   - Automatically handles data serialization

2. **`rest_api_call_csv(input_df, url, method, **options)`**
   - Best for large datasets or Databricks environments
   - Doesn't collect data to driver
   - Uses temporary CSV files for input

### Complete Example Using rest_api_call()

```python
from pyspark.sql import SparkSession
from pyspark_datasources import rest_api_call
import ast

# Initialize Spark
spark = SparkSession.builder \
    .appName("CleverAPI") \
    .master("local[*]") \
    .getOrCreate()

# Your Clever API token
CLEVER_TOKEN = ""

# Example 1: Simple GET request (no parameters)
dummy_input = spark.createDataFrame([{"placeholder": "dummy"}])

districts_df = rest_api_call(
    dummy_input,
    url="https://api.clever.com/v3.0/districts",
    method="GET",
    authType="Bearer",
    oauthToken=CLEVER_TOKEN,
    queryType="inline",
    partitions="1"
)

# Parse and display results
district_response = districts_df.select("output").first()["output"]
district_data = ast.literal_eval(district_response)
print(f"District: {district_data['data'][0]['data']['name']}")

# Example 2: GET request with URL parameters
schools_df = rest_api_call(
    dummy_input,
    url="https://api.clever.com/v3.0/schools?limit=10",
    method="GET",
    authType="Bearer",
    oauthToken=CLEVER_TOKEN,
    queryType="inline",
    partitions="1"
)

# Example 3: Multiple schools in parallel
# Create input DataFrame with multiple school IDs
school_ids = ["5940d254203e37907e0000f0", "5940d254203e37907e0000f1"]
schools_input = spark.createDataFrame([{"school_id": sid} for sid in school_ids])

# Get teachers for multiple schools at once
# Note: You would need to use URL template substitution or build URLs dynamically
# For this example, we process one school at a time
for school_id in school_ids:
    school_input = spark.createDataFrame([{"school_id": school_id}])

    teachers_df = rest_api_call(
        school_input,
        url=f"https://api.clever.com/v3.0/schools/{school_id}/users?role=teacher&limit=5",
        method="GET",
        authType="Bearer",
        oauthToken=CLEVER_TOKEN,
        queryType="inline",
        partitions="1"
    )

    # Process results
    teacher_response = teachers_df.select("output").first()["output"]
    teacher_data = ast.literal_eval(teacher_response)
    print(f"School {school_id}: {len(teacher_data.get('data', []))} teachers")
```

### Complete Example Using rest_api_call_csv() (For Production)

```python
from pyspark.sql import SparkSession
from pyspark_datasources import rest_api_call_csv
import ast

# Initialize Spark
spark = SparkSession.builder \
    .appName("CleverAPI") \
    .master("local[*]") \
    .getOrCreate()

# Your Clever API token
CLEVER_TOKEN = ""

# Create input DataFrame (works with any size dataset)
dummy_input = spark.createDataFrame([{"placeholder": "dummy"}])

# Use CSV-based approach (better for production/Databricks)
districts_df = rest_api_call_csv(
    dummy_input,
    url="https://api.clever.com/v3.0/districts",
    method="GET",
    authType="Bearer",
    oauthToken=CLEVER_TOKEN,
    queryType="inline",
    partitions="1"
)

# Parse results (same as before)
district_response = districts_df.select("output").first()["output"]
district_data = ast.literal_eval(district_response)
print(f"District: {district_data['data'][0]['data']['name']}")
```

### Important Configuration Options

When using `rest_api_call()` or `rest_api_call_csv()`, pass these as keyword arguments:

#### Required Options

- `url`: The Clever API endpoint
- `method`: "GET" for all Clever API calls
- `authType`: "Bearer" for token authentication
- `oauthToken`: Your Clever API token

#### Recommended Options

- `queryType`: "inline" - prevents input parameters from being appended to URL
- `partitions`: "1" - use single partition for sequential API calls

Note: The helper functions automatically set `includeInputsInOutput` appropriately.

## Response Parsing

The REST data source returns responses as Python dict strings (displayed with single quotes). To parse them:

```python
import ast

# Get the output column
response_str = df.select("output").first()["output"]

# Parse using ast.literal_eval (handles Python dict format)
data = ast.literal_eval(response_str)

# Now you can access the data
for item in data["data"]:
    print(item)
```

## Alternative: Direct REST Data Source Usage

If you prefer not to use the helper functions, you can use the REST data source directly:

```python
from pyspark.sql import SparkSession
from pyspark_datasources.rest import RestDataSource

spark = SparkSession.builder.appName("CleverAPI").master("local[*]").getOrCreate()
spark.dataSource.register(RestDataSource)

# Create input as JSON string
dummy_input = '[{"placeholder": "dummy"}]'

# Use the REST data source directly
districts_df = spark.read.format("rest") \
    .option("url", "https://api.clever.com/v3.0/districts") \
    .option("method", "GET") \
    .option("authType", "Bearer") \
    .option("oauthToken", CLEVER_TOKEN) \
    .option("inputData", dummy_input) \
    .option("queryType", "inline") \
    .option("includeInputsInOutput", "N") \
    .option("partitions", "1") \
    .load()
```

However, the `rest_api_call()` helper is recommended for cleaner, more maintainable code.

## Complete Working Example

See [test_clever_workflow.py](test_clever_workflow.py) for a complete end-to-end example that:
1. Fetches district information
2. Gets schools for the district
3. Gets teachers for a specific school
4. Gets students for the same school

Run it with:
```bash
source .venv/bin/activate
python test_clever_workflow.py
```

## Testing Your Token

To verify your Clever API token has access:

```bash
source .venv/bin/activate
python test_clever_district.py
```

This will show:
- District information
- Number of schools available
- Sample teachers
- Sample students

## Clever API Documentation

- API Overview: https://dev.clever.com/docs/api-overview
- Data Model: https://dev.clever.com/docs/data-model
- Schools: https://dev.clever.com/docs/schools
- Users: https://dev.clever.com/docs/users

## Token Details (Chess4Life Dev Sandbox)

**Authentication:**
```python
.option("authType", "Bearer")
.option("oauthToken", "")
```

**Available Data:**
- District: Chess4Life (Dev) Sandbox (ID: `5940d0b58ec81e0001541ef3`)
- Schools: 100 schools including:
  - Slothgreat (ID: `5940d254203e37907e0000f0`)
  - Vultureroad (ID: `5940d254203e37907e0000f1`)
  - Swoopcharm (ID: `5940d254203e37907e0000f2`)
- Teachers: Available by school
- Students: Available by school

## Troubleshooting

### Error: "Unrecognized token string"
**Solution:** Check that your token is valid and correctly specified in the `oauthToken` option.

### Error: "Schema mismatch - Expected 1 columns, Found 2 columns"
**Solution:** Set `.option("includeInputsInOutput", "N")` to exclude input parameters from output.

### Error: JSON parsing fails
**Solution:** Use `ast.literal_eval()` instead of `json.loads()` because the output is formatted as a Python dict string, not JSON.

### Empty results
**Solution:**
- Verify the token has access to the requested resource
- Check that the endpoint URL is correct
- Ensure you're using `queryType="inline"` to prevent parameters from being appended to the URL

## Next Steps

1. **Pagination**: Clever API returns paginated results. Use the `links.next` field to fetch additional pages.
2. **Batch Processing**: Process multiple schools in parallel using multiple partitions.
3. **Data Transformation**: Use PySpark transformations to process and analyze the data.
4. **Storage**: Save results to Parquet, Delta Lake, or other formats for further analysis.

## Support

For issues with:
- **Clever API**: See https://dev.clever.com/docs
- **PySpark REST Data Source**: See [README.md](README.md) and [REST_DATASOURCE_README.md](REST_DATASOURCE_README.md)
