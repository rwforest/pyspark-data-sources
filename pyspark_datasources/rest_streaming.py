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
from pyspark.sql.datasource import DataSourceStreamReader, InputPartition
from pyspark.sql.types import StructType
from pyspark.sql import Row
from urllib.parse import urlencode, quote
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth1


class RestStreamReader(DataSourceStreamReader):
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
        self.data_field = options.get("dataField", "")  # Field containing array of records

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

    def initialOffset(self) -> str:
        """
        Return the initial offset to start streaming from.
        """
        return json.dumps({self.offset_field: str(self.initial_offset_value)})

    def latestOffset(self) -> str:
        """
        Return the latest offset in the source.
        This implementation makes a request to the API to get the latest data
        and extracts an offset from it. This might not be efficient for all APIs.
        """
        try:
            self._handle_rate_limit()
            # A bit of a hack: use the initial offset value to make a request
            # to get the latest data. For many APIs, a request without a specific
            # "since" parameter returns the latest data.
            url = self._prepare_url_with_offset(self.initial_offset_value)
            response = self._call_api(url)
            response_data = self._parse_response(response)
            records = self._extract_records(response_data)
            next_offset_value = self._extract_next_offset(response_data, records)
            return json.dumps({self.offset_field: next_offset_value})
        except Exception as e:
            print(f"Error getting latest offset: {e}")
            # Fallback to initial offset on error
            return self.initialOffset()

    def partitions(self, start: str, end: str) -> List[InputPartition]:
        """
        Returns a list of partitions.
        For now, we only support a single partition.
        """
        start_offset = json.loads(start)
        end_offset = json.loads(end)
        return [{"start": start_offset, "end": end_offset}]

    def read(self, partition: InputPartition) -> Iterator[Row]:
        """
        Read data from API for a given partition.
        """
        start = partition["start"]
        
        try:
            self._handle_rate_limit()
            offset_value = start.get(self.offset_field, self.initial_offset_value)
            url = self._prepare_url_with_offset(offset_value)
            response = self._call_api(url)
            response_data = self._parse_response(response)

            if self.data_field == "":
                row_values = []
                for field_name, field_value in self.input_fields.items():
                    row_values.append(field_value)
                row_values.append(json.dumps(response_data))
                yield Row(*row_values)
            else:
                records = self._extract_records(response_data)
                rows_as_tuples = self._convert_records_to_rows(records)
                for r in rows_as_tuples:
                    yield Row(*r)

        except requests.exceptions.RequestException as e:
            print(f"API request failed: {str(e)}")
        except Exception as e:
            print(f"Unexpected error in streaming read: {str(e)}")
            import traceback
            traceback.print_exc()

    def commit(self, end: str) -> None:
        """
        Commit the offset (optional operation).
        """
        pass  # No-op for REST streaming

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
        Prepare URL with offset value and input field values replaced.
        """
        url = self.url
        url = url.replace(f"{{{self.offset_field}}}", quote(str(offset_value)))
        for field_name, field_value in self.input_fields.items():
            placeholder = f"{{{field_name}}}"
            if placeholder in url:
                url = url.replace(placeholder, quote(str(field_value)))
        return url

    def _call_api(self, url: str) -> requests.Response:
        """
        Make API call with configured authentication.
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
        """
        if isinstance(response_data, list):
            return response_data
        elif isinstance(response_data, dict):
            if self.data_field in response_data:
                data = response_data[self.data_field]
                if isinstance(data, list):
                    return data
                else:
                    return [data]
            else:
                return [response_data]
        else:
            return []

    def _extract_next_offset(self, response_data: Any, records: List[Dict]) -> str:
        """
        Extract next offset value from API response.
        """
        if self.offset_type == "incremental":
            if records and self.offset_field in records[-1]:
                try:
                    max_value = max(int(r.get(self.offset_field, 0)) for r in records if self.offset_field in r)
                    return str(max_value + 1)
                except (ValueError, TypeError):
                    pass
        elif self.offset_type == "max_field":
            if records and self.offset_field in records[-1]:
                try:
                    max_value = max(r.get(self.offset_field, "") for r in records if self.offset_field in r)
                    return str(max_value)
                except (ValueError, TypeError):
                    pass

        if isinstance(response_data, dict) and self.offset_field in response_data:
            return str(response_data[self.offset_field])

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

        if records:
            last_record = records[-1]
            for field in [self.offset_field, "id", "timestamp", "created_at", "updated_at"]:
                if field in last_record:
                    return str(last_record[field])

        return str(int(time.time()))

    def _convert_records_to_rows(self, records: List[Dict]) -> List[Tuple]:
        """
        Convert record dictionaries to tuples for Spark rows.
        """
        rows = []
        for record in records[:self.batch_size]:
            row_values = []
            for field_name, field_value in self.input_fields.items():
                row_values.append(field_value)
            row_values.append(record)
            rows.append(tuple(row_values))
        return rows