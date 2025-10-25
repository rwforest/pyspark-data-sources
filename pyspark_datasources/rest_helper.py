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


def flatten_json_response(
    response_df,
    json_path: str = "data",
    flatten_nested_key: str = "data",
    fully_flatten: bool = True,
    separator: str = "_"
):
    """
    Flatten nested JSON arrays from API responses into individual rows.

    This helper extracts nested arrays from API responses and creates
    one row per item. Useful for APIs that return arrays nested inside
    objects (e.g., {"data": [{...}, {...}]}).

    Args:
        response_df: DataFrame with 'output' column containing JSON responses
        json_path: Dot-separated path to extract array from JSON (default: 'data')
                   e.g., 'data', 'results', 'items.list'
        flatten_nested_key: If items have this nested key, flatten it (default: 'data')
                           Set to None to disable nested flattening
        fully_flatten: If True, recursively flatten all nested dicts into columns (default: True)
        separator: Separator for nested column names (default: '_')

    Returns:
        DataFrame with flattened rows (one row per array item)

    Example:
        >>> # API returns: {"data": [{"data": {"id": 1, "name": {"first": "A"}}}, ...]}
        >>> response = rest_api_call(input_df, url=api_url, ...)
        >>> flattened = flatten_json_response(response, json_path="data", fully_flatten=True)
        >>> flattened.show()  # One row per item with id, name_first columns
    """
    import ast

    def _flatten_dict(d, parent_key='', sep='_'):
        """Recursively flatten a nested dictionary."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict) and v:  # Only flatten non-empty dicts
                items.extend(_flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Convert lists to comma-separated strings
                items.append((new_key, ', '.join(str(x) for x in v) if v else ''))
            else:
                items.append((new_key, v))
        return dict(items)

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active Spark session found")

    # Collect and parse responses
    all_items = []
    for row in response_df.collect():
        row_dict = row.asDict()
        output = row_dict.get("output")

        if output:
            # Parse JSON response
            response_data = ast.literal_eval(output) if isinstance(output, str) else output

            # Extract data at json_path
            parts = json_path.split(".")
            current = response_data
            for part in parts:
                if isinstance(current, dict):
                    current = current.get(part, [])
                else:
                    current = []

            # Process each item in the extracted array
            if isinstance(current, list):
                for item in current:
                    item_dict = {k: v for k, v in row_dict.items() if k != "output"}

                    # Flatten nested key if specified and present
                    if flatten_nested_key and isinstance(item, dict) and flatten_nested_key in item:
                        nested_data = item.get(flatten_nested_key, {})
                        if isinstance(nested_data, dict):
                            if fully_flatten:
                                # Recursively flatten all nested dictionaries
                                flattened = _flatten_dict(nested_data, sep=separator)
                                item_dict.update(flattened)
                            else:
                                # Merge nested fields into the item (old behavior)
                                for key, value in nested_data.items():
                                    if isinstance(value, (dict, list)):
                                        item_dict[key] = str(value)
                                    else:
                                        item_dict[key] = value
                        else:
                            item_dict[flatten_nested_key] = nested_data
                    elif isinstance(item, dict):
                        # No nested key or not found - just flatten the dict directly
                        if fully_flatten:
                            # Recursively flatten all nested dictionaries
                            flattened = _flatten_dict(item, sep=separator)
                            item_dict.update(flattened)
                        else:
                            # Old behavior - convert complex types to strings
                            for key, value in item.items():
                                if isinstance(value, (dict, list)):
                                    item_dict[key] = str(value)
                                else:
                                    item_dict[key] = value
                    else:
                        # Not a dict - store as value
                        item_dict["value"] = item

                    all_items.append(item_dict)

    # Create DataFrame from flattened items
    if all_items:
        return spark.createDataFrame(all_items)
    else:
        # Return empty DataFrame with at least the input columns
        from pyspark.sql.types import StructType
        return spark.createDataFrame([], StructType([]))


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
