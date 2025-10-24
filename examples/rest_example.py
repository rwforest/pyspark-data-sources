"""
Examples for using the REST Data Source

This demonstrates how to use the RestDataSource to call REST APIs
in parallel for multiple input parameter sets.
"""

from pyspark.sql import SparkSession
from pyspark_datasources import RestDataSource


def example_basic_get_request():
    """
    Example 1: Basic GET request with query parameters

    This example calls the SODA API (Socrata Open Data API) for different
    parameter combinations to retrieve earthquake data.
    """
    print("\n=== Example 1: Basic GET Request ===\n")

    spark = SparkSession.builder \
        .appName("REST DataSource - GET Example") \
        .getOrCreate()

    # Register the REST data source
    spark.dataSource.register(RestDataSource)

    # Create input data with different parameter sets
    # Each row will result in one API call
    input_data = [
        ("Nevada", "nn"),
        ("Northern California", "pr"),
        ("Virgin Islands region", "pr")
    ]

    input_df = spark.createDataFrame(input_data, ["region", "source"])
    input_df.createOrReplaceTempView("soda_input")

    print("Input parameters:")
    input_df.show()

    # Call the REST API for each input row
    result_df = spark.read.format("rest") \
        .option("url", "https://soda.demo.socrata.com/resource/6yvf-kk3n.json") \
        .option("input", "soda_input") \
        .option("method", "GET") \
        .option("readTimeout", "10000") \
        .option("connectionTimeout", "2000") \
        .option("partitions", "3") \
        .load()

    print("\nAPI Results:")
    result_df.printSchema()
    result_df.show(truncate=False)

    # You can now process the results with SQL
    result_df.createOrReplaceTempView("soda_results")

    # Example: Extract data from the output array
    spark.sql("""
        SELECT region, source, output
        FROM soda_results
    """).show(truncate=False)


def example_post_with_json():
    """
    Example 2: POST request with JSON body

    This example demonstrates POST requests to JSONPlaceholder API.
    """
    print("\n=== Example 2: POST Request with JSON ===\n")

    spark = SparkSession.builder \
        .appName("REST DataSource - POST Example") \
        .getOrCreate()

    spark.dataSource.register(RestDataSource)

    # Create input data for POST requests
    input_data = [
        ("Test Post 1", "This is the body of post 1", 1),
        ("Test Post 2", "This is the body of post 2", 1),
        ("Test Post 3", "This is the body of post 3", 2)
    ]

    input_df = spark.createDataFrame(input_data, ["title", "body", "userId"])
    input_df.createOrReplaceTempView("posts_to_create")

    print("Posts to create:")
    input_df.show()

    # POST to the API
    result_df = spark.read.format("rest") \
        .option("url", "https://jsonplaceholder.typicode.com/posts") \
        .option("input", "posts_to_create") \
        .option("method", "POST") \
        .option("postInputFormat", "json") \
        .load()

    print("\nCreated posts:")
    result_df.printSchema()
    result_df.show(truncate=False)


def example_with_authentication():
    """
    Example 3: Using Basic Authentication

    This example shows how to use Basic authentication with REST APIs.
    Note: Replace with your actual credentials and API endpoint.
    """
    print("\n=== Example 3: Basic Authentication ===\n")

    spark = SparkSession.builder \
        .appName("REST DataSource - Auth Example") \
        .getOrCreate()

    spark.dataSource.register(RestDataSource)

    # Create input parameters
    input_data = [
        ("query1",),
        ("query2",),
        ("query3",)
    ]

    input_df = spark.createDataFrame(input_data, ["q"])
    input_df.createOrReplaceTempView("search_queries")

    # Example with Basic Auth (replace with real credentials)
    result_df = spark.read.format("rest") \
        .option("url", "https://api.example.com/search") \
        .option("input", "search_queries") \
        .option("method", "GET") \
        .option("userId", "your_username") \
        .option("userPassword", "your_password") \
        .load()

    print("Results with authentication:")
    result_df.show()


def example_with_bearer_token():
    """
    Example 4: Using Bearer Token Authentication

    This example shows how to use Bearer token authentication.
    """
    print("\n=== Example 4: Bearer Token Authentication ===\n")

    spark = SparkSession.builder \
        .appName("REST DataSource - Bearer Token") \
        .getOrCreate()

    spark.dataSource.register(RestDataSource)

    # Create input parameters
    input_data = [("param1",), ("param2",)]
    input_df = spark.createDataFrame(input_data, ["search"])
    input_df.createOrReplaceTempView("api_params")

    # Use Bearer token
    result_df = spark.read.format("rest") \
        .option("url", "https://api.example.com/data") \
        .option("input", "api_params") \
        .option("method", "GET") \
        .option("authType", "Bearer") \
        .option("oauthToken", "your_bearer_token_here") \
        .load()

    print("Results with Bearer token:")
    result_df.show()


def example_with_custom_headers():
    """
    Example 5: Custom HTTP Headers

    This example shows how to add custom HTTP headers.
    """
    print("\n=== Example 5: Custom Headers ===\n")

    spark = SparkSession.builder \
        .appName("REST DataSource - Custom Headers") \
        .getOrCreate()

    spark.dataSource.register(RestDataSource)

    # Create input parameters
    input_data = [("value1",), ("value2",)]
    input_df = spark.createDataFrame(input_data, ["param"])
    input_df.createOrReplaceTempView("input_data")

    # Add custom headers
    custom_headers = '{"X-Custom-Header": "CustomValue", "X-API-Key": "your-api-key"}'

    result_df = spark.read.format("rest") \
        .option("url", "https://api.example.com/endpoint") \
        .option("input", "input_data") \
        .option("method", "GET") \
        .option("headers", custom_headers) \
        .load()

    print("Results with custom headers:")
    result_df.show()


def example_parallel_api_calls():
    """
    Example 6: Parallel API Calls

    This example demonstrates calling an API many times in parallel,
    which is the main use case for this data source.
    """
    print("\n=== Example 6: Many Parallel API Calls ===\n")

    spark = SparkSession.builder \
        .appName("REST DataSource - Parallel Calls") \
        .getOrCreate()

    spark.dataSource.register(RestDataSource)

    # Create a large number of input parameters
    # For example, validating addresses or looking up data for many IDs
    input_data = [(i,) for i in range(1, 51)]  # IDs 1-50

    input_df = spark.createDataFrame(input_data, ["id"])
    input_df.createOrReplaceTempView("user_ids")

    print(f"Making {input_df.count()} API calls in parallel...")

    # Call JSONPlaceholder API to get user data
    # With 10 partitions, these calls happen in parallel
    result_df = spark.read.format("rest") \
        .option("url", "https://jsonplaceholder.typicode.com/posts") \
        .option("input", "user_ids") \
        .option("method", "GET") \
        .option("partitions", "10") \
        .option("includeInputsInOutput", "Y") \
        .load()

    print("\nResults from parallel API calls:")
    result_df.printSchema()
    result_df.show(10)
    print(f"\nTotal results: {result_df.count()}")


def example_reading_from_csv():
    """
    Example 7: Reading Input Parameters from CSV

    This example shows how to read input parameters from a CSV file.
    """
    print("\n=== Example 7: Input from CSV File ===\n")

    spark = SparkSession.builder \
        .appName("REST DataSource - CSV Input") \
        .getOrCreate()

    spark.dataSource.register(RestDataSource)

    # First, create a sample CSV file
    csv_data = [
        ("Nevada", "nn"),
        ("Northern California", "pr"),
        ("Virgin Islands region", "pr")
    ]

    csv_df = spark.createDataFrame(csv_data, ["region", "source"])

    # Save as CSV
    csv_path = "/tmp/rest_input.csv"
    csv_df.write.mode("overwrite").option("header", "true").csv(csv_path)

    # Read the CSV file
    input_df = spark.read.option("header", "true").csv(csv_path)
    input_df.createOrReplaceTempView("csv_input")

    print("Input from CSV:")
    input_df.show()

    # Use the CSV data as input for REST calls
    result_df = spark.read.format("rest") \
        .option("url", "https://soda.demo.socrata.com/resource/6yvf-kk3n.json") \
        .option("input", "csv_input") \
        .option("method", "GET") \
        .load()

    print("\nResults:")
    result_df.show(truncate=False)


def example_without_input_in_output():
    """
    Example 8: Exclude Input Parameters from Output

    Sometimes you only want the API response, not the input parameters.
    """
    print("\n=== Example 8: Output Without Input Parameters ===\n")

    spark = SparkSession.builder \
        .appName("REST DataSource - No Input in Output") \
        .getOrCreate()

    spark.dataSource.register(RestDataSource)

    # Create input data
    input_data = [(1,), (2,), (3,)]
    input_df = spark.createDataFrame(input_data, ["id"])
    input_df.createOrReplaceTempView("post_ids")

    # Get results without input parameters
    result_df = spark.read.format("rest") \
        .option("url", "https://jsonplaceholder.typicode.com/posts") \
        .option("input", "post_ids") \
        .option("method", "GET") \
        .option("includeInputsInOutput", "N") \
        .load()

    print("Results (without input parameters):")
    result_df.printSchema()
    result_df.show(truncate=False)


if __name__ == "__main__":
    print("=" * 80)
    print("REST Data Source Examples")
    print("=" * 80)

    # Run the basic GET example
    example_basic_get_request()

    # Run the POST example
    example_post_with_json()

    # Run parallel API calls example
    example_parallel_api_calls()

    # Run CSV input example
    example_reading_from_csv()

    # Run without input in output example
    example_without_input_in_output()

    print("\n" + "=" * 80)
    print("Examples completed!")
    print("=" * 80)

    # Note: Authentication examples are commented out as they require
    # real credentials. Uncomment and modify with your credentials to test:
    # example_with_authentication()
    # example_with_bearer_token()
    # example_with_custom_headers()
