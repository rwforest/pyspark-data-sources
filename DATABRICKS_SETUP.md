# Databricks Setup Guide for REST Data Source

This guide shows you how to install and test the REST Data Source in Databricks.

## Prerequisites

- Databricks workspace (AWS, Azure, or GCP)
- Databricks Runtime 15.4 LTS or higher (includes PySpark 4.0+)
- Access to create/manage clusters

## Quick Start (5 minutes)

### Step 1: Upload the Wheel File

1. **Download the wheel** to your local machine:
   - File: `pyspark_data_sources-0.1.10-py3-none-any.whl`
   - Location: `dist/` directory

2. **Upload to Databricks Workspace**:
   - Click on **Workspace** in the left sidebar
   - Navigate to your home directory (or create a folder)
   - Right-click → **Create** → **Folder** → Name it `wheels`
   - Click on the `wheels` folder
   - Click the **↓** (down arrow) dropdown → **Upload files**
   - Select `pyspark_data_sources-0.1.10-py3-none-any.whl`
   - Wait for upload to complete

### Step 2: Install on Cluster

#### Option A: Install on Existing Cluster

1. Go to **Compute** in the left sidebar
2. Click on your cluster name
3. Click the **Libraries** tab
4. Click **Install new**
5. Select **Library Source**: **Workspace**
6. Navigate to: `/Users/<your-email>/wheels/pyspark_data_sources-0.1.10-py3-none-any.whl`
7. Click **Install**
8. Wait for status to show **Installed** (green checkmark)

#### Option B: Create New Cluster with Library

1. Go to **Compute** → **Create Compute**
2. Configure your cluster:
   - **Cluster name**: REST-DataSource-Test
   - **Cluster mode**: Single Node (for testing)
   - **Databricks runtime**: 15.4 LTS or higher
   - **Node type**: Choose appropriate size
3. Expand **Libraries** section
4. Click **Add** → **Workspace**
5. Navigate to your uploaded wheel file
6. Click **Add**
7. Click **Create Cluster**

### Step 3: Create Test Notebook

1. Click **Workspace** in the left sidebar
2. Navigate to your home directory
3. Click the **↓** dropdown → **Create** → **Notebook**
4. Name it: `REST DataSource Test`
5. Select your cluster
6. Choose **Python** as language
7. Click **Create**

### Step 4: Test the Installation

Copy and paste this code into cells:

#### Cell 1: Import and Register

```python
from pyspark_datasources import RestDataSource

# Register the REST data source
spark.dataSource.register(RestDataSource)

print("✅ REST Data Source registered successfully!")
```

#### Cell 2: Create Test Input

```python
# Create sample input data
test_data = [
    (1, "test query 1"),
    (2, "test query 2"),
    (3, "test query 3")
]

# Create DataFrame
input_df = spark.createDataFrame(test_data, ["id", "query"])

# Create temporary view
input_df.createOrReplaceTempView("test_input")

# Display input
display(input_df)
```

#### Cell 3: Test GET Request

```python
# Call REST API with GET method
result_df = spark.read.format("rest") \
    .option("url", "https://jsonplaceholder.typicode.com/posts") \
    .option("input", "test_input") \
    .option("method", "GET") \
    .option("partitions", "3") \
    .load()

# Display schema
print("Schema:")
result_df.printSchema()

# Display results
print("\nResults:")
display(result_df)
```

#### Cell 4: Test POST Request (Using Helper Function - Recommended)

```python
# Import helper function
from pyspark_datasources import rest_api_call

# Create data for POST
post_data = [
    ("My First Post", "This is the content of my first post", 1),
    ("Another Post", "More content here", 1)
]

post_df = spark.createDataFrame(post_data, ["title", "body", "userId"])

# POST to API using helper function
post_result_df = rest_api_call(
    post_df,
    url="https://jsonplaceholder.typicode.com/posts",
    method="POST",
    postInputFormat="json"
)

print("POST Results:")
display(post_result_df)

print("✅ All tests passed!")
```

**Why use `rest_api_call()`?**

The helper function handles:
- Data collection and serialization automatically
- Schema management
- Joining results back to input data

This is the recommended approach for Databricks usage!

Run all cells and verify they execute without errors!

---

## Detailed Setup Instructions

### Method 1: Install via Databricks UI (Recommended)

#### A. Upload Wheel to DBFS

1. **Navigate to Data**:
   - Click **Data** in left sidebar
   - Click **Create Table**
   - Select **DBFS** tab

2. **Upload File**:
   - Choose **File Upload**
   - Select `pyspark_data_sources-0.1.10-py3-none-any.whl`
   - Note the path: `/FileStore/tables/pyspark_data_sources-0.1.10-py3-none-any.whl`

3. **Install on Cluster**:
   - Go to **Compute** → Select cluster → **Libraries**
   - Click **Install new**
   - Choose **DBFS**
   - Enter path: `dbfs:/FileStore/tables/pyspark_data_sources-0.1.10-py3-none-any.whl`
   - Click **Install**

#### B. Upload Wheel to Workspace

1. **Create Folder Structure**:
   ```
   /Users/<your-email>/
   └── libraries/
       └── wheels/
   ```

2. **Upload Wheel**:
   - Navigate to `/Users/<your-email>/libraries/wheels/`
   - Upload `pyspark_data_sources-0.1.10-py3-none-any.whl`

3. **Install on Cluster**:
   - Libraries → Install new → Workspace
   - Path: `/Users/<your-email>/libraries/wheels/pyspark_data_sources-0.1.10-py3-none-any.whl`

### Method 2: Install via Notebook (Temporary)

For quick testing without permanent installation:

```python
# Install in notebook (valid for current session only)
%pip install /Workspace/Users/<your-email>/wheels/pyspark_data_sources-0.1.10-py3-none-any.whl

# Restart Python to load the package
dbutils.library.restartPython()
```

Then in next cell:

```python
from pyspark_datasources import RestDataSource
spark.dataSource.register(RestDataSource)
```

### Method 3: Install via Databricks CLI

1. **Install Databricks CLI**:
   ```bash
   pip install databricks-cli
   ```

2. **Configure Authentication**:
   ```bash
   databricks configure --token
   ```
   Enter your Databricks workspace URL and personal access token.

3. **Upload Wheel**:
   ```bash
   databricks fs cp dist/pyspark_data_sources-0.1.10-py3-none-any.whl \
       dbfs:/FileStore/wheels/pyspark_data_sources-0.1.10-py3-none-any.whl
   ```

4. **Install via UI** or use cluster API.

---

## Complete Test Notebook

Here's a comprehensive test notebook you can use:

```python
# ============================================================
# Cell 1: Install and Import (if using %pip method)
# ============================================================
%pip install /Workspace/Users/<your-email>/wheels/pyspark_data_sources-0.1.10-py3-none-any.whl
dbutils.library.restartPython()

# ============================================================
# Cell 2: Import and Register
# ============================================================
from pyspark_datasources import RestDataSource

spark.dataSource.register(RestDataSource)
print("✅ REST Data Source registered!")

# ============================================================
# Cell 3: Test Basic GET Request
# ============================================================
print("TEST 1: Basic GET Request")
print("=" * 60)

# Create test data
data = [(1,), (2,), (3,)]
df = spark.createDataFrame(data, ["id"])
df.createOrReplaceTempView("get_test_input")

# Call REST API
result = spark.read.format("rest") \
    .option("url", "https://jsonplaceholder.typicode.com/posts") \
    .option("input", "get_test_input") \
    .option("method", "GET") \
    .option("partitions", "2") \
    .load()

print(f"Schema: {result.schema}")
display(result)
print("✅ GET request successful!\n")

# ============================================================
# Cell 4: Test POST Request
# ============================================================
print("TEST 2: POST Request")
print("=" * 60)

# Create data for POST
post_data = [
    ("Test Title 1", "Test Body 1", 1),
    ("Test Title 2", "Test Body 2", 1)
]
post_df = spark.createDataFrame(post_data, ["title", "body", "userId"])
post_df.createOrReplaceTempView("post_test_input")

# POST to API
post_result = spark.read.format("rest") \
    .option("url", "https://jsonplaceholder.typicode.com/posts") \
    .option("input", "post_test_input") \
    .option("method", "POST") \
    .option("postInputFormat", "json") \
    .load()

display(post_result)
print("✅ POST request successful!\n")

# ============================================================
# Cell 5: Test with Multiple Partitions
# ============================================================
print("TEST 3: Multiple Partitions")
print("=" * 60)

# Create larger dataset
large_data = [(i,) for i in range(1, 21)]
large_df = spark.createDataFrame(large_data, ["id"])
large_df.createOrReplaceTempView("large_test_input")

# Call with 5 partitions
parallel_result = spark.read.format("rest") \
    .option("url", "https://jsonplaceholder.typicode.com/posts") \
    .option("input", "large_test_input") \
    .option("method", "GET") \
    .option("partitions", "5") \
    .load()

print(f"Processed {parallel_result.count()} API calls with 5 partitions")
print("✅ Parallel execution successful!\n")

# ============================================================
# Cell 6: Test Various Options
# ============================================================
print("TEST 4: Various Configuration Options")
print("=" * 60)

# Test with custom timeouts
timeout_result = spark.read.format("rest") \
    .option("url", "https://jsonplaceholder.typicode.com/posts") \
    .option("input", "get_test_input") \
    .option("method", "GET") \
    .option("connectionTimeout", "5000") \
    .option("readTimeout", "10000") \
    .option("includeInputsInOutput", "N") \
    .load()

print("Columns:", timeout_result.columns)
print("✅ Options test successful!\n")

# ============================================================
# Cell 7: Summary
# ============================================================
print("=" * 60)
print("🎉 ALL TESTS PASSED!")
print("=" * 60)
print("\nThe REST Data Source is working correctly in Databricks!")
print("\nYou can now use it for:")
print("  • Calling REST APIs in parallel")
print("  • Data enrichment from external APIs")
print("  • Batch API operations")
print("  • Any REST-based data integration")
```

---

## Real-World Examples for Databricks

### Example 1: Enrich Customer Data

```python
# Load customer data
customers_df = spark.read.table("customers")

# Select fields for API enrichment
customers_df.select("customer_id", "email").createOrReplaceTempView("customers_to_enrich")

# Enrich via external API
enriched = spark.read.format("rest") \
    .option("url", "https://api.clearbit.com/v2/people/find") \
    .option("input", "customers_to_enrich") \
    .option("method", "GET") \
    .option("userId", dbutils.secrets.get("api-keys", "clearbit-key")) \
    .option("partitions", "10") \
    .load()

# Save enriched data
enriched.write.mode("overwrite").saveAsTable("enriched_customers")
```

### Example 2: Validate Addresses

```python
# Load addresses from Delta table
addresses = spark.read.table("addresses")
addresses.createOrReplaceTempView("addresses_to_validate")

# Validate via API
validated = spark.read.format("rest") \
    .option("url", "https://api.address-validator.com/validate") \
    .option("input", "addresses_to_validate") \
    .option("method", "POST") \
    .option("postInputFormat", "json") \
    .option("userId", dbutils.secrets.get("api-keys", "validator-key")) \
    .option("partitions", "20") \
    .load()

# Filter valid addresses
valid_addresses = validated.filter("output.is_valid = true")
```

### Example 3: Batch API Lookups

```python
# Get list of user IDs to lookup
user_ids = spark.sql("""
    SELECT DISTINCT user_id
    FROM events
    WHERE date = current_date()
""")
user_ids.createOrReplaceTempView("user_ids")

# Lookup user details from API
user_details = spark.read.format("rest") \
    .option("url", "https://api.example.com/users/{user_id}") \
    .option("input", "user_ids") \
    .option("method", "GET") \
    .option("queryType", "inline") \
    .option("partitions", "10") \
    .load()

# Join with events
events = spark.table("events")
enriched_events = events.join(user_details, "user_id")
```

---

## Using Secrets for API Keys

### Store API Keys Securely

```python
# Set up secret scope (run once)
# Note: Requires Databricks admin access
dbutils.secrets.createScope("api-keys")

# Add secrets via Databricks CLI:
# databricks secrets put --scope api-keys --key my-api-key
```

### Use in REST Data Source

```python
# Retrieve secret
api_key = dbutils.secrets.get("api-keys", "my-api-key")

# Use in REST data source
result = spark.read.format("rest") \
    .option("url", "https://api.example.com/data") \
    .option("input", "params") \
    .option("method", "GET") \
    .option("authType", "Bearer") \
    .option("oauthToken", api_key) \
    .load()
```

---

## Databricks-Specific Tips

### 1. Use dbutils for File Paths

```python
# Upload to DBFS
wheel_path = "dbfs:/FileStore/wheels/pyspark_data_sources-0.1.10-py3-none-any.whl"

# Install
%pip install $wheel_path
dbutils.library.restartPython()
```

### 2. Monitor API Usage

```python
# Track API calls
result_df = spark.read.format("rest") \
    .option("url", "https://api.example.com/data") \
    .option("input", "params") \
    .load()

# Count successful calls
total_calls = result_df.count()
failed_calls = result_df.filter("_corrupt_records IS NOT NULL").count()

print(f"Total API calls: {total_calls}")
print(f"Failed calls: {failed_calls}")
print(f"Success rate: {(total_calls - failed_calls) / total_calls * 100:.2f}%")
```

### 3. Optimize for Large Datasets

```python
# For large datasets, increase partitions
large_result = spark.read.format("rest") \
    .option("url", "https://api.example.com/data") \
    .option("input", "large_input_table") \
    .option("partitions", "50") \
    .option("readTimeout", "30000") \
    .load()

# Cache if using multiple times
large_result.cache()
```

### 4. Use Delta Lake for Results

```python
# Write results to Delta table
result_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("api_results")

# Optimize Delta table
spark.sql("OPTIMIZE api_results")
```

---

## Troubleshooting in Databricks

### Issue: Library Installation Fails

**Solution**:
1. Check Databricks Runtime version (must be 15.4 LTS+)
2. Verify wheel file is uploaded correctly
3. Try reinstalling:
   ```python
   %pip install --force-reinstall /path/to/wheel.whl
   dbutils.library.restartPython()
   ```

### Issue: Import Error

**Solution**:
```python
# After installing, always restart Python
dbutils.library.restartPython()

# Then import in next cell
from pyspark_datasources import RestDataSource
```

### Issue: API Rate Limiting

**Solution**:
```python
# Reduce parallelism
.option("partitions", "2")

# Increase timeouts
.option("connectionTimeout", "10000")
.option("readTimeout", "30000")
```

### Issue: Authentication Errors

**Solution**:
```python
# Use Databricks secrets
api_key = dbutils.secrets.get("api-keys", "my-key")

# Test authentication first
import requests
response = requests.get(
    "https://api.example.com/test",
    headers={"Authorization": f"Bearer {api_key}"}
)
print(response.status_code)
```

---

## Cluster Configuration Recommendations

### For Testing

```
Cluster Mode: Single Node
Runtime: 15.4 LTS
Node Type: Standard_D3_v2 (or equivalent)
Autoscaling: Disabled
```

### For Production

```
Cluster Mode: Standard
Runtime: 15.4 LTS
Workers: 2-10 (based on API call volume)
Node Type: Memory Optimized
Autoscaling: Enabled
Spot Instances: Not recommended (for reliability)
```

---

## Complete Databricks Workflow

```python
# ============================================================
# Step 1: Setup (Run once per cluster)
# ============================================================
%pip install /Workspace/Users/<your-email>/wheels/pyspark_data_sources-0.1.10-py3-none-any.whl
dbutils.library.restartPython()

# ============================================================
# Step 2: Import and Register (Run once per session)
# ============================================================
from pyspark_datasources import RestDataSource
spark.dataSource.register(RestDataSource)

# ============================================================
# Step 3: Load Input Data
# ============================================================
input_data = spark.read.table("my_input_table")
input_data.createOrReplaceTempView("api_input")

# ============================================================
# Step 4: Call REST API
# ============================================================
# Get API key from secrets
api_key = dbutils.secrets.get("api-keys", "my-api-key")

# Make API calls
results = spark.read.format("rest") \
    .option("url", "https://api.example.com/endpoint") \
    .option("input", "api_input") \
    .option("method", "POST") \
    .option("postInputFormat", "json") \
    .option("authType", "Bearer") \
    .option("oauthToken", api_key) \
    .option("partitions", "10") \
    .load()

# ============================================================
# Step 5: Process Results
# ============================================================
# Filter successful calls
successful = results.filter("_corrupt_records IS NULL")

# Save to Delta table
successful.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("api_results")

# ============================================================
# Step 6: Monitor and Report
# ============================================================
total = results.count()
failed = results.filter("_corrupt_records IS NOT NULL").count()
print(f"API Calls - Total: {total}, Success: {total-failed}, Failed: {failed}")
```

---

## Summary

**Easiest Setup**:
1. Upload wheel to Databricks workspace
2. Install on cluster via Libraries UI
3. Create notebook and import
4. Run test code

**Time Required**: ~5 minutes

**Best Practice**: Use Databricks Secrets for API keys

**Recommended Runtime**: 15.4 LTS or higher

For questions or issues, check the troubleshooting section or refer to other documentation files.
