"""
REST Data Source for Apache Spark

This data source enables calling REST APIs in parallel for multiple sets of input parameters
and collating the results in a DataFrame. It's a Python implementation of the original
spark-datasource-rest (Scala) library.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from typing import Dict, List, Iterator, Optional, Tuple, Any
import requests
import json
from urllib.parse import urlencode, quote
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth1
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Row


class RestDataSource(DataSource):
    """
    A PySpark data source for calling REST APIs in parallel with multiple input parameters.

    This data source allows you to:
    - Call REST APIs multiple times with different parameter sets in parallel
    - Automatically infer the schema from API responses
    - Support various HTTP methods (GET, POST, PUT, DELETE)
    - Handle authentication (Basic, OAuth1, Bearer tokens)
    - Include input parameters in the output for easy tracking

    Name: `rest`

    Required Options
    ----------------
    url : str
        The target REST API URL (can include common query parameters)
    input : str
        Name of a temporary Spark table containing input parameters, where column names
        match the API parameter names

    Optional Options
    ----------------
    method : str, default 'POST'
        HTTP method to use (GET, POST, PUT, DELETE)
    userId : str
        Username for Basic authentication
    userPassword : str
        Password for Basic authentication
    authType : str, default 'Basic'
        Authentication type (Basic, OAuth1, Bearer)
    oauthConsumerKey : str
        OAuth1 consumer key
    oauthConsumerSecret : str
        OAuth1 consumer secret
    oauthToken : str
        OAuth1 token (also used as Bearer token when authType='Bearer')
    oauthTokenSecret : str
        OAuth1 token secret
    partitions : int, default 2
        Number of partitions for parallel execution
    connectionTimeout : int, default 1000
        Connection timeout in milliseconds
    readTimeout : int, default 5000
        Read timeout in milliseconds
    schemaSamplePcnt : int, default 30
        Percentage of input records to use for schema inference (minimum 3)
    callStrictlyOnce : str, default 'N'
        If 'Y', calls API only once per input and caches results (useful for paid APIs)
    includeInputsInOutput : str, default 'Y'
        If 'Y', includes input parameters in output DataFrame
    postInputFormat : str, default 'json'
        Format for POST data (json, form)
    queryType : str, default 'querystring'
        How to append GET parameters (querystring, inline)
    headers : str
        Additional HTTP headers in JSON format, e.g., '{"X-Custom": "value"}'
    cookie : str
        Cookie to send with request in format 'name=value'
    jsonPath : str
        Path to extract nested data from JSON response, e.g., 'data' or 'data.items'
        When specified, extracts array from this path and creates one row per item
        Automatically infers schema from nested data structure

    Examples
    --------
    Basic GET request with query parameters:

    >>> from pyspark_datasources import RestDataSource
    >>> spark.dataSource.register(RestDataSource)
    >>>
    >>> # Create input DataFrame with parameters
    >>> data = [("Nevada", "nn"), ("Northern California", "pr")]
    >>> input_df = spark.createDataFrame(data, ["region", "source"])
    >>> input_df.createOrReplaceTempView("input_params")
    >>>
    >>> # Call REST API for each row
    >>> df = spark.read.format("rest") \\
    ...     .option("url", "https://soda.demo.socrata.com/resource/6yvf-kk3n.json") \\
    ...     .option("input", "input_params") \\
    ...     .option("method", "GET") \\
    ...     .load()
    >>> df.show()

    POST request with JSON body:

    >>> # Input table for POST requests
    >>> data = [("user1", "email1@test.com"), ("user2", "email2@test.com")]
    >>> input_df = spark.createDataFrame(data, ["username", "email"])
    >>> input_df.createOrReplaceTempView("users_to_create")
    >>>
    >>> df = spark.read.format("rest") \\
    ...     .option("url", "https://api.example.com/users") \\
    ...     .option("input", "users_to_create") \\
    ...     .option("method", "POST") \\
    ...     .option("postInputFormat", "json") \\
    ...     .load()

    With Basic Authentication:

    >>> df = spark.read.format("rest") \\
    ...     .option("url", "https://api.example.com/data") \\
    ...     .option("input", "input_params") \\
    ...     .option("method", "GET") \\
    ...     .option("userId", "myuser") \\
    ...     .option("userPassword", "mypass") \\
    ...     .load()

    With OAuth1:

    >>> df = spark.read.format("rest") \\
    ...     .option("url", "https://api.twitter.com/1.1/search/tweets.json") \\
    ...     .option("input", "search_terms") \\
    ...     .option("method", "GET") \\
    ...     .option("authType", "OAuth1") \\
    ...     .option("oauthConsumerKey", "key") \\
    ...     .option("oauthConsumerSecret", "secret") \\
    ...     .option("oauthToken", "token") \\
    ...     .option("oauthTokenSecret", "token_secret") \\
    ...     .load()

    Output Structure
    ----------------
    The returned DataFrame contains:
    - All input parameter columns (if includeInputsInOutput='Y')
    - An 'output' column containing the API response (struct or array)
    - A '_corrupt_records' column if some API calls failed

    Notes
    -----
    - Each row in the input table results in one API call
    - API calls are parallelized based on the 'partitions' option
    - Schema is inferred from a sample of responses (controlled by schemaSamplePcnt)
    - For paid APIs or rate-limited services, use callStrictlyOnce='Y'
    """

    @classmethod
    def name(cls) -> str:
        return "rest"

    def __init__(self, options: Optional[Dict[str, str]] = None):
        self.options = options or {}
        self._validate_options()

    def _validate_options(self):
        """Validate required options"""
        if "url" not in self.options:
            raise ValueError("Option 'url' is required for REST data source")

        # For streaming mode, input data is not required (it polls API)
        is_streaming = self.options.get("streaming", "false").lower() == "true"
        if not is_streaming:
            if "input" not in self.options and "inputData" not in self.options and "inputCsvPath" not in self.options:
                raise ValueError("Either 'input' (table name), 'inputData' (serialized JSON), or 'inputCsvPath' (CSV file path) is required for REST data source")

    def schema(self) -> str:
        """
        Returns a dynamic schema that will be inferred from API responses.
        The actual schema inference happens in the reader.
        """
        # Check if we should include input columns in output
        include_inputs = self.options.get("includeInputsInOutput", "Y")

        if include_inputs == "N":
            # Only return output column
            return "output STRING"
        else:
            # Need to infer input schema from inputData or inputCsvPath
            import json

            if "inputData" in self.options:
                # Parse inputData to get schema
                try:
                    input_data = json.loads(self.options["inputData"])
                    if input_data and len(input_data) > 0:
                        # Get keys from first record
                        first_record = input_data[0]
                        input_columns = ", ".join([f"{key} STRING" for key in first_record.keys()])
                        return f"{input_columns}, output STRING"
                except:
                    pass
            elif "inputCsvPath" in self.options:
                # Read CSV header to get column names
                try:
                    import csv
                    with open(self.options["inputCsvPath"], 'r') as f:
                        reader = csv.DictReader(f)
                        if reader.fieldnames:
                            input_columns = ", ".join([f"{col} STRING" for col in reader.fieldnames])
                            return f"{input_columns}, output STRING"
                except:
                    pass

            # Fallback: just return output column
            return "output STRING"

    def reader(self, schema: StructType) -> DataSourceReader:
        return RestReader(self.options, schema)

    def simpleStreamReader(self, schema: StructType):
        """
        Return stream reader if streaming mode is enabled.

        This method is called by Spark when using readStream API.
        Returns None if not in streaming mode (will use batch reader instead).

        Args:
            schema: The schema for the data source

        Returns:
            RestStreamReader if streaming=true, None otherwise
        """
        is_streaming = self.options.get("streaming", "false").lower() == "true"
        if is_streaming:
            from .rest_streaming import RestStreamReader
            return RestStreamReader(schema, self.options)
        return None


import warnings

class RestReader(DataSourceReader):
    """Reader implementation for REST API calls"""

    def __init__(self, options: Dict[str, str], schema: StructType):
        self.options = options
        self.schema_type = schema

        # Required options
        self.url = options["url"]
        self.input_table = options.get("input", "")  # Table name (optional if inputData provided)
        self.input_data_json = options.get("inputData", "")  # Serialized JSON data (optional if input provided)
        self.input_csv_path = options.get("inputCsvPath", "")  # CSV file path (optional alternative)

        if self.input_table:
            warnings.warn(
                "The 'input' option is deprecated and may not work reliably in all environments. "
                "It is recommended to use the 'inputData' option instead.",
                DeprecationWarning
            )

        # Optional options with defaults
        self.method = options.get("method", "POST").upper()
        self.user_id = options.get("userId", "")
        self.user_password = options.get("userPassword", "")
        self.auth_type = options.get("authType", "Basic")
        self.num_partitions = int(options.get("partitions", "2"))
        self.connection_timeout = int(options.get("connectionTimeout", "1000")) / 1000  # Convert to seconds
        self.read_timeout = int(options.get("readTimeout", "5000")) / 1000  # Convert to seconds
        self.schema_sample_pcnt = int(options.get("schemaSamplePcnt", "30"))
        self.call_strictly_once = options.get("callStrictlyOnce", "N")
        self.include_inputs = options.get("includeInputsInOutput", "Y")
        self.post_input_format = options.get("postInputFormat", "json")
        self.query_type = options.get("queryType", "querystring")
        self.cookie = options.get("cookie", "")
        self.custom_headers = options.get("headers", "")
        self.json_path = options.get("jsonPath", "")  # Path to extract nested data

        # OAuth options
        self.oauth_consumer_key = options.get("oauthConsumerKey", "")
        self.oauth_consumer_secret = options.get("oauthConsumerSecret", "")
        self.oauth_token = options.get("oauthToken", "")
        self.oauth_token_secret = options.get("oauthTokenSecret", "")

        # Will be set during partitioning
        self.input_data: List[Dict[str, Any]] = []
        self.column_names: List[str] = []

    def partitions(self) -> List[InputPartition]:
        """
        Create partitions for parallel execution.
        This is called before read() to split the input data.
        """
        import json

        # Load input data - from JSON, CSV, or Spark table
        if self.input_data_json:
            # Use pre-serialized input data
            all_input_data = json.loads(self.input_data_json)
            if all_input_data:
                self.column_names = list(all_input_data[0].keys())
        elif self.input_csv_path:
            # Read from CSV file (works in all environments!)
            import csv
            all_input_data = []
            try:
                with open(self.input_csv_path, 'r') as csvfile:
                    reader = csv.DictReader(csvfile)
                    self.column_names = reader.fieldnames or []
                    for row in reader:
                        all_input_data.append(row)
            except Exception as e:
                raise RuntimeError(f"Failed to read CSV file '{self.input_csv_path}': {e}")
        elif self.input_table:
            # Load from Spark table (requires Spark session access)
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()
            if spark is None:
                raise RuntimeError(
                    "\n" + "="*70 + "\n"
                    "ERROR: The 'input' table option does NOT work in Databricks.\n\n"
                    "The Python Data Source API in Databricks/PySpark 4.0 does not provide\n"
                    "access to the Spark session in the partitions() method, so reading\n"
                    "from temporary tables is not possible.\n\n"
                    "SOLUTION: Use the rest_api_call() helper function instead:\n\n"
                    "   from pyspark_datasources import rest_api_call\n\n"
                    "   result_df = rest_api_call(\n"
                    "       input_df,  # Pass your DataFrame directly\n"
                    "       url='https://api.example.com/endpoint',\n"
                    "       method='POST'\n"
                    "   )\n\n"
                    "This helper function handles all the complexity for you!\n"
                    + "="*70
                )

            # Get input data from the temporary table
            input_df = spark.sql(f"SELECT * FROM {self.input_table}")
            self.column_names = input_df.columns

            # Convert to list of dictionaries
            all_input_data = [row.asDict() for row in input_df.collect()]
        else:
            raise ValueError("Either 'input' or 'inputData' must be provided")

        if not all_input_data:
            return []

        # Distribute data across partitions
        partitions = []
        for i in range(self.num_partitions):
            partition_data = [
                row for idx, row in enumerate(all_input_data)
                if idx % self.num_partitions == i
            ]
            # Store partition data in InputPartition
            partition = InputPartition(i)
            partition.data = partition_data
            partition.column_names = self.column_names
            partitions.append(partition)

        return partitions

    def read(self, partition: InputPartition) -> Iterator[Row]:
        """
        Read data by calling the REST API for each input parameter set.
        This is called once per partition in parallel.
        """
        try:
            # Get partition data that was distributed in partitions()
            partition_data = getattr(partition, 'data', [])
            self.column_names = getattr(partition, 'column_names', [])

            if not partition_data:
                return iter([])

            # Process each input record
            results = []
            for input_row in partition_data:
                try:
                    result = self._call_rest_api(input_row)

                    # If jsonPath is specified, extract nested data and explode into multiple rows
                    if self.json_path:
                        # Get the output data
                        output_data = result.asDict().get("output")
                        if output_data:
                            # Extract data at jsonPath
                            extracted_data = self._extract_json_path(output_data, self.json_path)

                            # If extracted data is a list, create one row per item
                            if isinstance(extracted_data, list):
                                for item in extracted_data:
                                    item_dict = input_row.copy() if self.include_inputs == "Y" else {}

                                    # If item is a dict with nested 'data', flatten it
                                    if isinstance(item, dict) and "data" in item:
                                        # Flatten the nested 'data' object
                                        flattened = item.get("data", {})
                                        if isinstance(flattened, dict):
                                            item_dict.update(flattened)
                                        else:
                                            item_dict["output"] = flattened
                                    elif isinstance(item, dict):
                                        # Merge dict fields directly
                                        item_dict.update(item)
                                    else:
                                        # Store as output
                                        item_dict["output"] = item

                                    results.append(Row(**item_dict))
                            elif extracted_data is not None:
                                # Single item, not a list
                                item_dict = input_row.copy() if self.include_inputs == "Y" else {}
                                if isinstance(extracted_data, dict):
                                    item_dict.update(extracted_data)
                                else:
                                    item_dict["output"] = extracted_data
                                results.append(Row(**item_dict))
                    else:
                        # No jsonPath - return as-is
                        results.append(result)

                except Exception as e:
                    # Include error information in corrupt records
                    # IMPORTANT: Always include "output" field to match schema
                    error_result = input_row.copy() if self.include_inputs == "Y" else {}
                    error_result["output"] = None  # Must include output field to match schema
                    error_result["_corrupt_record"] = str(e)
                    results.append(Row(**error_result))

            return iter(results)

        except Exception as e:
            print(f"Error in RestReader.read: {e}")
            import traceback
            traceback.print_exc()
            return iter([])

    def _call_rest_api(self, input_params: Dict[str, Any]) -> Row:
        """
        Make a REST API call with the given input parameters.

        Args:
            input_params: Dictionary of parameter names and values

        Returns:
            Row containing input params (if enabled) and API response
        """
        # Prepare authentication
        auth = self._prepare_auth()

        # Prepare headers
        headers = self._prepare_headers()

        # Prepare request based on HTTP method
        if self.method == "GET":
            response = self._make_get_request(input_params, auth, headers)
        elif self.method == "POST":
            response = self._make_post_request(input_params, auth, headers)
        elif self.method == "PUT":
            response = self._make_put_request(input_params, auth, headers)
        elif self.method == "DELETE":
            response = self._make_delete_request(input_params, auth, headers)
        else:
            raise ValueError(f"Unsupported HTTP method: {self.method}")

        # Parse response
        response_data = self._parse_response(response)

        # Prepare output
        return self._prepare_output(input_params, response_data)

    def _prepare_auth(self) -> Optional[Any]:
        """Prepare authentication based on authType"""
        if self.auth_type == "OAuth1" and self.oauth_consumer_key:
            return OAuth1(
                self.oauth_consumer_key,
                client_secret=self.oauth_consumer_secret,
                resource_owner_key=self.oauth_token,
                resource_owner_secret=self.oauth_token_secret
            )
        elif self.user_id and self.user_password:
            return HTTPBasicAuth(self.user_id, self.user_password)
        return None

    def _prepare_headers(self) -> Dict[str, str]:
        """Prepare HTTP headers"""
        headers = {}

        # Add custom headers
        if self.custom_headers:
            try:
                custom = json.loads(self.custom_headers)
                headers.update(custom)
            except json.JSONDecodeError:
                print(f"Warning: Could not parse custom headers: {self.custom_headers}")

        # Add Bearer token if specified
        if self.auth_type == "Bearer" and self.oauth_token:
            headers["Authorization"] = f"Bearer {self.oauth_token}"

        return headers

    def _make_get_request(
        self,
        params: Dict[str, Any],
        auth: Optional[Any],
        headers: Dict[str, str]
    ) -> requests.Response:
        """Make a GET request"""
        url = self._prepare_get_url(params)

        cookies = self._prepare_cookies()

        response = requests.get(
            url,
            auth=auth,
            headers=headers,
            cookies=cookies,
            timeout=(self.connection_timeout, self.read_timeout),
            verify=True  # Can be made configurable if needed
        )
        response.raise_for_status()
        return response

    def _make_post_request(
        self,
        params: Dict[str, Any],
        auth: Optional[Any],
        headers: Dict[str, str]
    ) -> requests.Response:
        """Make a POST request"""
        cookies = self._prepare_cookies()

        if self.post_input_format == "json":
            headers["Content-Type"] = "application/json"
            data = json.dumps(params)
            response = requests.post(
                self.url,
                data=data,
                auth=auth,
                headers=headers,
                cookies=cookies,
                timeout=(self.connection_timeout, self.read_timeout),
                verify=True
            )
        elif self.post_input_format == "form":
            response = requests.post(
                self.url,
                data=params,
                auth=auth,
                headers=headers,
                cookies=cookies,
                timeout=(self.connection_timeout, self.read_timeout),
                verify=True
            )
        else:
            raise ValueError(f"Unsupported postInputFormat: {self.post_input_format}")

        response.raise_for_status()
        return response

    def _make_put_request(
        self,
        params: Dict[str, Any],
        auth: Optional[Any],
        headers: Dict[str, str]
    ) -> requests.Response:
        """Make a PUT request"""
        headers["Content-Type"] = "application/json"
        data = json.dumps(params)
        cookies = self._prepare_cookies()

        response = requests.put(
            self.url,
            data=data,
            auth=auth,
            headers=headers,
            cookies=cookies,
            timeout=(self.connection_timeout, self.read_timeout),
            verify=True
        )
        response.raise_for_status()
        return response

    def _make_delete_request(
        self,
        params: Dict[str, Any],
        auth: Optional[Any],
        headers: Dict[str, str]
    ) -> requests.Response:
        """Make a DELETE request"""
        cookies = self._prepare_cookies()

        response = requests.delete(
            self.url,
            auth=auth,
            headers=headers,
            cookies=cookies,
            timeout=(self.connection_timeout, self.read_timeout),
            verify=True
        )
        response.raise_for_status()
        return response

    def _prepare_get_url(self, params: Dict[str, Any]) -> str:
        """Prepare URL for GET request based on queryType"""
        if self.query_type == "querystring":
            # Append parameters as query string
            query_string = urlencode(params)
            if "?" in self.url:
                return f"{self.url}&{query_string}"
            else:
                return f"{self.url}?{query_string}"
        elif self.query_type == "inline":
            # Replace placeholders in URL (e.g., /api/{key}/{value})
            url = self.url
            for key, value in params.items():
                url = url.replace(f"{{{key}}}", quote(str(value)))
            return url
        else:
            raise ValueError(f"Unsupported queryType: {self.query_type}")

    def _prepare_cookies(self) -> Dict[str, str]:
        """Prepare cookies from cookie option"""
        if self.cookie and "=" in self.cookie:
            name, value = self.cookie.split("=", 1)
            return {name: value}
        return {}

    def _parse_response(self, response: requests.Response) -> Any:
        """Parse response based on content type"""
        try:
            # Try to parse as JSON
            return response.json()
        except json.JSONDecodeError:
            # Return as text if not JSON
            return response.text

    def _prepare_output(self, input_params: Dict[str, Any], response_data: Any) -> Row:
        """
        Prepare output row combining input parameters and API response.

        Args:
            input_params: Original input parameters
            response_data: Parsed API response

        Returns:
            Row with combined data (or multiple rows if jsonPath specified)
        """
        if self.include_inputs == "Y":
            # Include input parameters in output
            output_dict = input_params.copy()
            output_dict["output"] = response_data
        else:
            # Only return the response
            output_dict = {"output": response_data}

        return Row(**output_dict)

    def _extract_json_path(self, data: Any, path: str) -> Any:
        """
        Extract data from nested JSON using dot notation path.

        Args:
            data: JSON data (dict or list)
            path: Dot-separated path like 'data' or 'data.items'

        Returns:
            Extracted data at the path
        """
        if not path:
            return data

        parts = path.split(".")
        current = data

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                current = current[int(part)]
            else:
                return None

            if current is None:
                return None

        return current
