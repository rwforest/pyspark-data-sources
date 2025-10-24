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
from pyspark_datasources.rest import RestDataSource
import json
import ast

# Initialize Spark
spark = SparkSession.builder \
    .appName("CleverAPI") \
    .master("local[*]") \
    .getOrCreate()

# Register REST data source
spark.dataSource.register(RestDataSource)

# Your Clever API token
CLEVER_TOKEN = ""

# Create dummy input (required but not used for GET requests)
dummy_input = '[{"placeholder": "dummy"}]'

# Get districts
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

# Parse the response
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
schools_df = spark.read.format("rest") \
    .option("url", "https://api.clever.com/v3.0/schools?limit=10") \
    .option("method", "GET") \
    .option("authType", "Bearer") \
    .option("oauthToken", CLEVER_TOKEN) \
    .option("inputData", dummy_input) \
    .option("queryType", "inline") \
    .option("includeInputsInOutput", "N") \
    .option("partitions", "1") \
    .load()

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

# Create input with school ID
school_input = json.dumps([{"school_id": school_id}])

# Get teachers for this school
teachers_df = spark.read.format("rest") \
    .option("url", f"https://api.clever.com/v3.0/schools/{school_id}/users?role=teacher&limit=10") \
    .option("method", "GET") \
    .option("authType", "Bearer") \
    .option("oauthToken", CLEVER_TOKEN) \
    .option("inputData", school_input) \
    .option("queryType", "inline") \
    .option("includeInputsInOutput", "N") \
    .option("partitions", "1") \
    .load()

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
students_df = spark.read.format("rest") \
    .option("url", f"https://api.clever.com/v3.0/schools/{school_id}/users?role=student&limit=10") \
    .option("method", "GET") \
    .option("authType", "Bearer") \
    .option("oauthToken", CLEVER_TOKEN) \
    .option("inputData", school_input) \
    .option("queryType", "inline") \
    .option("includeInputsInOutput", "N") \
    .option("partitions", "1") \
    .load()

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

## Important Configuration Options

### Required Options

- `url`: The Clever API endpoint
- `method`: "GET" for all Clever API calls
- `authType`: "Bearer" for token authentication
- `oauthToken`: Your Clever API token
- `inputData`: JSON array (use dummy data for GET requests without parameters)

### Recommended Options

- `queryType`: "inline" - prevents input parameters from being appended to URL
- `includeInputsInOutput`: "N" - only return API response, not input data
- `partitions`: "1" - use single partition for sequential API calls

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
