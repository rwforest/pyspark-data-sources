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


def parse_array_response(
    response_df,
    array_path: str = "states",
    column_names: list = None,
    timestamp_field: str = "time"
):
    """
    Parse API responses that return arrays instead of objects (e.g., OpenSky Network).

    Some APIs return data as positional arrays instead of named objects:
    {"time": 123, "states": [["val1", "val2", ...], ...]}

    This helper converts those arrays into a proper DataFrame with named columns.

    Args:
        response_df: DataFrame with 'output' column containing JSON responses
        array_path: Path to the array field in the response (default: 'states')
        column_names: List of column names for the array elements
        timestamp_field: Name of timestamp field in response (default: 'time')

    Returns:
        DataFrame with parsed rows and named columns

    Example:
        >>> # OpenSky returns: {"time": 123, "states": [["icao", "call", ...], ...]}
        >>> column_names = ["icao24", "callsign", "country", "longitude", "latitude", ...]
        >>> flights_df = parse_array_response(
        ...     response_df,
        ...     array_path="states",
        ...     column_names=column_names
        ... )
    """
    import ast
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active Spark session found")

    if column_names is None:
        raise ValueError("column_names must be provided for array parsing")

    all_items = []
    input_columns = set()

    for row in response_df.collect():
        row_dict = row.asDict()
        output = row_dict.get("output")

        # Track input columns
        input_columns.update(k for k in row_dict.keys() if k != "output")

        if output:
            # Parse JSON response
            response_data = ast.literal_eval(output) if isinstance(output, str) else output

            # Extract timestamp if present
            timestamp = response_data.get(timestamp_field) if timestamp_field else None

            # Extract array data
            array_data = response_data.get(array_path, [])

            # Convert each array to a Row
            for item_array in array_data:
                if item_array and len(item_array) > 0:
                    # Start with input columns (excluding 'output')
                    row_values = {k: v for k, v in row_dict.items() if k != "output"}

                    # Add timestamp if present
                    if timestamp:
                        row_values[timestamp_field] = int(timestamp)

                    # Map array values to column names
                    for i, col_name in enumerate(column_names):
                        if i < len(item_array):
                            value = item_array[i]
                            # Store all values as-is, let schema handle conversion
                            row_values[col_name] = value
                        else:
                            row_values[col_name] = None

                    all_items.append(row_values)

    # Create DataFrame with explicit schema to avoid inference issues
    if all_items:
        # Build schema
        schema_fields = []

        # Add input column fields
        for col in sorted(input_columns):
            schema_fields.append(StructField(col, StringType(), True))

        # Add timestamp field
        if timestamp_field:
            schema_fields.append(StructField(timestamp_field, LongType(), True))

        # Add data column fields (all as string to avoid type issues)
        for col_name in column_names:
            schema_fields.append(StructField(col_name, StringType(), True))

        schema = StructType(schema_fields)

        # Convert all values to strings for schema consistency
        rows_for_df = []
        for item in all_items:
            row_data = {}
            for field in schema.fields:
                value = item.get(field.name)
                if value is None:
                    row_data[field.name] = None
                elif field.dataType == LongType():
                    row_data[field.name] = int(value) if value is not None else None
                else:
                    row_data[field.name] = str(value) if value is not None else None
            rows_for_df.append(row_data)

        return spark.createDataFrame(rows_for_df, schema)
    else:
        return spark.createDataFrame([], StructType([]))


def parse_array_response_streaming(
    stream_df,
    array_path: str = "states",
    column_names: list = None,
    timestamp_field: str = "time",
    output_column: str = "output"
):
    """
    Parse streaming API responses that return arrays instead of objects.

    This is the streaming version of parse_array_response() that uses Spark SQL
    transformations instead of .collect(), making it compatible with streaming DataFrames.

    Args:
        stream_df: Streaming DataFrame with output column containing JSON responses
        array_path: Path to the array field in the response (default: 'states')
        column_names: List of column names for the array elements
        timestamp_field: Name of timestamp field in response (default: 'time')
        output_column: Name of the output column (default: 'output')

    Returns:
        Streaming DataFrame with parsed rows and named columns

    Example:
        >>> # OpenSky streaming: {"time": 123, "states": [["icao", "call", ...], ...]}
        >>> column_names = ["icao24", "callsign", "country", "longitude", "latitude", ...]
        >>> flights_df = parse_array_response_streaming(
        ...     stream_df,
        ...     array_path="states",
        ...     column_names=column_names
        ... )
        >>> query = flights_df.writeStream.format("console").start()
    """
    from pyspark.sql.functions import col, from_json, explode
    from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

    if column_names is None:
        raise ValueError("column_names must be provided for array parsing")

    # Define schema for the JSON response
    # Inner array: array of strings (positional data)
    inner_array_schema = ArrayType(StringType())

    # Outer structure: {timestamp_field: long, array_path: array<array<string>>}
    response_schema = StructType([
        StructField(timestamp_field, LongType(), True),
        StructField(array_path, ArrayType(inner_array_schema), True)
    ])

    # Parse JSON and explode arrays
    parsed_df = stream_df \
        .select(
            col("*"),
            from_json(col(output_column), response_schema).alias("parsed")
        ) \
        .select(
            col("*"),
            col(f"parsed.{timestamp_field}").alias(timestamp_field),
            explode(col(f"parsed.{array_path}")).alias("array_item")
        ) \
        .drop(output_column, "parsed")

    # Extract individual columns from the array using positional indexing
    select_exprs = [col(c) for c in parsed_df.columns if c not in [timestamp_field, "array_item"]]
    select_exprs.append(col(timestamp_field))

    for i, col_name in enumerate(column_names):
        select_exprs.append(col("array_item")[i].alias(col_name))

    result_df = parsed_df.select(*select_exprs)

    return result_df


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
