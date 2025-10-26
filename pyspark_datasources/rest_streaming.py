"""
REST Data Source Streaming Support for Apache Spark

This module adds generic streaming support to the REST data source, allowing
any REST API that supports polling patterns to be used as a Spark Structured
Streaming source.

Key Design Principles:
- API-Agnostic: Works with any REST API
- Configurable: All behavior controlled by options
- Reusable: Leverages existing REST authentication and request logic

Licensed under the Apache License, Version 2.0
"""

from typing import Dict, List, Tuple, Any, Iterator, Optional
import time
import json
import requests
from pyspark.sql.datasource import SimpleDataSourceStreamReader
from pyspark.sql.types import StructType
from pyspark.sql import Row
from urllib.parse import urlencode, quote
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth1


class RestStreamReader(SimpleDataSourceStreamReader):
    """
    Generic streaming reader for REST APIs.

    This reader supports any REST API that provides:
    1. A way to track progress (timestamp, cursor, page number, etc.)
    2. A mechanism to request data from a specific point forward

    The streaming behavior is entirely configured through options, making it
    work with various API patterns without hardcoding specific logic.
    """

    def __init__(self, schema: StructType, options: Dict[str, str]):
        super().__init__()
        self.schema = schema
        self.options = options

        # Core REST options (reused from batch REST data source)
        self.url = options["url"]
        self.method = options.get("method", "GET").upper()
        self.auth_type = options.get("authType", "Basic")

        # Authentication options
        self.user_id = options.get("userId", "")
        self.user_password = options.get("userPassword", "")
        self.oauth_consumer_key = options.get("oauthConsumerKey", "")
        self.oauth_consumer_secret = options.get("oauthConsumerSecret", "")
        self.oauth_token = options.get("oauthToken", "")
        self.oauth_token_secret = options.get("oauthTokenSecret", "")
        self.custom_headers = options.get("headers", "")
        self.cookie = options.get("cookie", "")

        # Timeout options
        self.connection_timeout = int(options.get("connectionTimeout", "1000")) / 1000
        self.read_timeout = int(options.get("readTimeout", "5000")) / 1000

        # Streaming-specific options
        self.streaming_interval = float(options.get("streamingInterval", "10"))
        self.offset_field = options.get("offsetField", "timestamp")
        self.offset_type = options.get("offsetType", "timestamp")
        self.initial_offset_value = options.get("initialOffset", "0")
        self.batch_size = int(options.get("batchSize", "1000"))
        self.data_field = options.get("dataField", "data")  # Field containing array of records

        # Query and format options
        self.query_type = options.get("queryType", "querystring")
        self.post_input_format = options.get("postInputFormat", "json")

        # Parse input data to get input fields (for includeInputsInOutput)
        self.input_fields = {}
        if "inputData" in options:
            try:
                input_data = json.loads(options["inputData"])
                if input_data and len(input_data) > 0:
                    # Use first record as template for input fields
                    self.input_fields = input_data[0]
            except json.JSONDecodeError:
                pass

        # State tracking
        self.last_request_time = 0

    def initialOffset(self) -> Dict[str, str]:
        """
        Return the initial offset to start streaming from.

        Returns:
            Dict with offset field name and initial value
        """
        return {self.offset_field: str(self.initial_offset_value)}

    def _handle_rate_limit(self):
        """Ensure minimum interval between API requests"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time

        if time_since_last_request < self.streaming_interval:
            sleep_time = self.streaming_interval - time_since_last_request
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def _prepare_auth(self) -> Optional[Any]:
        """Prepare authentication (reused from REST data source)"""
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
        """Prepare HTTP headers (reused from REST data source)"""
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

    def _prepare_cookies(self) -> Dict[str, str]:
        """Prepare cookies from cookie option"""
        if self.cookie and "=" in self.cookie:
            parts = self.cookie.split("=", 1)
            return {parts[0]: parts[1]}
        return {}

    def _prepare_url_with_offset(self, offset_value: str) -> str:
        """
        Prepare URL with offset value replaced.

        Supports placeholders like:
        - https://api.example.com/data?since={timestamp}
        - https://api.example.com/data?cursor={next_cursor}
        - https://api.example.com/data?start_id={max_id}

        Args:
            offset_value: The current offset value to use

        Returns:
            URL with offset placeholder replaced
        """
        # Replace offset placeholder in URL
        url = self.url.replace(f"{{{self.offset_field}}}", quote(str(offset_value)))
        return url

    def _call_api(self, url: str) -> requests.Response:
        """
        Make API call with configured authentication.

        Args:
            url: The full URL to call

        Returns:
            Response object

        Raises:
            requests.RequestException: If API call fails
        """
        auth = self._prepare_auth()
        headers = self._prepare_headers()
        cookies = self._prepare_cookies()

        if self.method == "GET":
            response = requests.get(
                url,
                auth=auth,
                headers=headers,
                cookies=cookies,
                timeout=(self.connection_timeout, self.read_timeout),
                verify=True
            )
        elif self.method == "POST":
            response = requests.post(
                url,
                auth=auth,
                headers=headers,
                cookies=cookies,
                timeout=(self.connection_timeout, self.read_timeout),
                verify=True
            )
        else:
            raise ValueError(f"Unsupported HTTP method for streaming: {self.method}")

        response.raise_for_status()
        return response

    def _parse_response(self, response: requests.Response) -> Any:
        """Parse API response as JSON"""
        try:
            return response.json()
        except json.JSONDecodeError:
            return {"_raw": response.text}

    def _extract_records(self, response_data: Any) -> List[Dict]:
        """
        Extract records from API response.

        Supports:
        - Direct array: [{"id": 1}, {"id": 2}]
        - Nested in field: {"data": [{"id": 1}, {"id": 2}]}

        Args:
            response_data: Parsed JSON response

        Returns:
            List of record dictionaries
        """
        if isinstance(response_data, list):
            # Response is directly an array
            return response_data
        elif isinstance(response_data, dict):
            # Response is an object, look for data field
            if self.data_field in response_data:
                data = response_data[self.data_field]
                if isinstance(data, list):
                    return data
                else:
                    # Single record
                    return [data]
            else:
                # Treat entire response as single record
                return [response_data]
        else:
            return []

    def _extract_next_offset(self, response_data: Any, records: List[Dict]) -> str:
        """
        Extract next offset value from API response.

        Strategies based on offset_type:
        - "timestamp": Use offset_field from response (e.g., "latest_timestamp")
        - "cursor": Use offset_field from response (e.g., "next_cursor", "links.next")
        - "incremental": Calculate max ID from records
        - "max_field": Find max value of offset_field in records

        Args:
            response_data: Full parsed response
            records: Extracted records

        Returns:
            Next offset value as string
        """
        if self.offset_type == "incremental":
            # Calculate next ID from records (max + 1)
            if records and self.offset_field in records[-1]:
                try:
                    max_value = max(int(r.get(self.offset_field, 0)) for r in records if self.offset_field in r)
                    return str(max_value + 1)
                except (ValueError, TypeError):
                    pass

        elif self.offset_type == "max_field":
            # Find max value of a field in records
            if records and self.offset_field in records[-1]:
                try:
                    max_value = max(r.get(self.offset_field, "") for r in records if self.offset_field in r)
                    return str(max_value)
                except (ValueError, TypeError):
                    pass

        # Default: extract from top-level response (timestamp, cursor, etc.)
        if isinstance(response_data, dict) and self.offset_field in response_data:
            return str(response_data[self.offset_field])

        # Support nested fields (e.g., "links.next")
        if isinstance(response_data, dict) and "." in self.offset_field:
            parts = self.offset_field.split(".")
            current = response_data
            for part in parts:
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    break
            if current != response_data:
                return str(current)

        # If no new offset found, return last record's timestamp/id if available
        if records:
            last_record = records[-1]
            # Try common field names
            for field in [self.offset_field, "id", "timestamp", "created_at", "updated_at"]:
                if field in last_record:
                    return str(last_record[field])

        # Fallback: return current timestamp
        return str(int(time.time()))

    def _convert_records_to_rows(self, records: List[Dict]) -> List[Tuple]:
        """
        Convert record dictionaries to tuples for Spark rows.

        The schema includes input fields (from inputData) followed by the output field.
        For example, if inputData is [{"placeholder": "dummy"}], the schema is:
        "placeholder STRING, output STRING"

        So we need to return tuples with (input_field_values..., output_value)

        Args:
            records: List of record dictionaries

        Returns:
            List of tuples (one per record)
        """
        rows = []
        for record in records[:self.batch_size]:
            # Build row tuple: (input_field1, input_field2, ..., output)
            row_values = []

            # Add input field values in the order they appear in schema
            for field_name, field_value in self.input_fields.items():
                row_values.append(field_value)

            # Add output field (the API response record)
            row_values.append(record)

            rows.append(tuple(row_values))
        return rows

    def read(self, start: Dict[str, str]) -> Tuple[List[Tuple], Dict[str, str]]:
        """
        Read data from API starting from given offset.

        Args:
            start: Starting offset dictionary

        Returns:
            Tuple of (records, next_offset)
        """
        try:
            # Rate limiting
            self._handle_rate_limit()

            # Get current offset value
            offset_value = start.get(self.offset_field, self.initial_offset_value)

            # Prepare URL with offset
            url = self._prepare_url_with_offset(offset_value)

            # Call API
            response = self._call_api(url)

            # Parse response
            response_data = self._parse_response(response)

            # Extract records
            records = self._extract_records(response_data)

            # Convert to Spark rows
            rows = self._convert_records_to_rows(records)

            # Extract next offset
            next_offset_value = self._extract_next_offset(response_data, records)
            next_offset = {self.offset_field: next_offset_value}

            return (rows, next_offset)

        except requests.exceptions.RequestException as e:
            print(f"API request failed: {str(e)}")
            # Return empty batch with same offset (will retry)
            return ([], start)
        except Exception as e:
            print(f"Unexpected error in streaming read: {str(e)}")
            import traceback
            traceback.print_exc()
            # Return empty batch with same offset
            return ([], start)

    def readBetweenOffsets(
        self,
        start: Dict[str, str],
        end: Dict[str, str]
    ) -> Iterator[Tuple]:
        """
        Read data between two offsets (for reprocessing).

        For REST APIs, this typically means reading from start offset.
        The end offset is mainly for bounded replay scenarios.

        Args:
            start: Starting offset
            end: Ending offset (may be ignored for unbounded streams)

        Returns:
            Iterator of record tuples
        """
        # For REST APIs, just read from start
        # (we don't support arbitrary range queries in generic case)
        data, _ = self.read(start)
        return iter(data)

    def commit(self, end: Dict[str, str]) -> None:
        """
        Commit the offset (optional operation).

        For REST APIs, we don't need external offset management
        as Spark handles checkpointing automatically.

        Args:
            end: Offset to commit
        """
        pass  # No-op for REST streaming
