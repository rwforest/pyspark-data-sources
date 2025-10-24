"""
Helper functions for REST Data Source

This module provides convenience functions to work around limitations
in the Python Data Source API.
"""

import json
import os
import tempfile
from typing import Optional
from pyspark.sql import DataFrame, SparkSession


def rest_api_call(
    input_df: DataFrame,
    url: str,
    method: str = "POST",
    **options
) -> DataFrame:
    """
    Call a REST API for each row in the input DataFrame.

    This is a convenience wrapper that handles the data collection
    and serialization required by the REST data source.

    Args:
        input_df: Input DataFrame with parameters for API calls
        url: The REST API endpoint URL
        method: HTTP method (GET, POST, PUT, DELETE)
        **options: Additional REST data source options

    Returns:
        DataFrame with API responses

    Example:
        >>> from pyspark_datasources import rest_api_call
        >>>
        >>> input_data = [("param1", "value1"), ("param2", "value2")]
        >>> input_df = spark.createDataFrame(input_data, ["key", "value"])
        >>>
        >>> result_df = rest_api_call(
        ...     input_df,
        ...     url="https://api.example.com/endpoint",
        ...     method="POST",
        ...     postInputFormat="json"
        ... )

    Note:
        This function collects the input DataFrame to the driver,
        so it's best suited for small to medium datasets (< 10,000 rows).
        For larger datasets, consider using a UDF-based approach.
    """
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active Spark session found")

    # Collect input data on driver and create result in-memory
    # This avoids distributed join issues
    input_rows = input_df.collect()
    input_data = [row.asDict() for row in input_rows]

    # Serialize to JSON
    serialized_input = json.dumps(input_data)

    # Register REST data source
    from .rest import RestDataSource
    spark.dataSource.register(RestDataSource)

    # Force includeInputsInOutput='N' to avoid schema mismatch issues
    options['includeInputsInOutput'] = 'N'

    # Create read with serialized data
    reader = spark.read.format("rest") \
        .option("url", url) \
        .option("method", method) \
        .option("inputData", serialized_input)

    # Add additional options
    for key, value in options.items():
        reader = reader.option(key, str(value))

    # Get API responses (just output column)
    api_results_df = reader.load()
    api_results = api_results_df.collect()

    # Combine input and output in-memory (preserves order)
    combined_data = []
    for idx, (input_row, output_row) in enumerate(zip(input_rows, api_results)):
        combined = input_row.asDict()
        combined['output'] = output_row['output']
        combined_data.append(combined)

    # Create final DataFrame with all columns
    if combined_data:
        result = spark.createDataFrame(combined_data)
    else:
        # Empty result - create empty DataFrame with expected schema
        from pyspark.sql.types import StructType, StructField, StringType
        schema = input_df.schema.add(StructField("output", StringType(), True))
        result = spark.createDataFrame([], schema)

    return result


def rest_api_call_csv(
    input_df: DataFrame,
    url: str,
    method: str = "POST",
    **options
) -> DataFrame:
    """
    Call a REST API for each row in the input DataFrame using CSV file approach.

    This version writes the input to a temporary CSV file and uses the
    'inputCsvPath' option, which works in all Spark environments including
    Databricks without requiring data collection to the driver.

    Args:
        input_df: Input DataFrame with parameters for API calls
        url: The REST API endpoint URL
        method: HTTP method (GET, POST, PUT, DELETE)
        **options: Additional REST data source options

    Returns:
        DataFrame with API responses

    Example:
        >>> from pyspark_datasources import rest_api_call_csv
        >>>
        >>> input_data = [("param1", "value1"), ("param2", "value2")]
        >>> input_df = spark.createDataFrame(input_data, ["key", "value"])
        >>>
        >>> result_df = rest_api_call_csv(
        ...     input_df,
        ...     url="https://api.example.com/endpoint",
        ...     method="POST",
        ...     postInputFormat="json"
        ... )

    Note:
        This function works for any dataset size since it doesn't collect
        to the driver. The CSV file is created in a temporary location.
    """
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active Spark session found")

    # Create temporary CSV file
    temp_dir = tempfile.mkdtemp()

    # Write DataFrame to CSV (single file for simplicity)
    input_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir + "/data")

    # Find the actual CSV file (Spark creates part-*.csv files)
    import glob
    csv_files = glob.glob(os.path.join(temp_dir, "data", "part-*.csv"))
    if not csv_files:
        raise RuntimeError("Failed to create CSV file")

    actual_csv_path = csv_files[0]

    # Register REST data source
    from .rest import RestDataSource
    spark.dataSource.register(RestDataSource)

    # Use inputCsvPath option
    reader = spark.read.format("rest") \
        .option("url", url) \
        .option("method", method) \
        .option("inputCsvPath", actual_csv_path) \
        .option("includeInputsInOutput", "Y")

    # Add additional options
    for key, value in options.items():
        reader = reader.option(key, str(value))

    # Get results - materialize immediately with collect()
    # This ensures the file is read before we clean up
    result_rows = reader.load().collect()

    # Clean up temporary files AFTER reading
    import shutil
    try:
        shutil.rmtree(temp_dir)
    except:
        pass  # Best effort cleanup

    # Convert back to DataFrame
    if result_rows:
        result_df = spark.createDataFrame(result_rows)
    else:
        # Empty result
        from pyspark.sql.types import StructType, StructField, StringType
        schema = input_df.schema.add(StructField("output", StringType(), True))
        result_df = spark.createDataFrame([], schema)

    return result_df
