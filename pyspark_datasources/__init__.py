from .arrow import ArrowDataSource
from .fake import FakeDataSource
from .github import GithubDataSource
from .googlesheets import GoogleSheetsDataSource
from .huggingface import HuggingFaceDatasets
from .kaggle import KaggleDataSource
from .opensky import OpenSkyDataSource
from .rest import RestDataSource
from .rest_helper import rest_api_call, rest_api_call_csv, flatten_json_response, parse_array_response, parse_array_response_streaming
from .robinhood import RobinhoodDataSource
from .salesforce import SalesforceDataSource
from .simplejson import SimpleJsonDataSource
from .stock import StockDataSource
from .jsonplaceholder import JSONPlaceholderDataSource

# Optional: Streaming support (requires PySpark 3.5.0+)
try:
    from .rest_streaming import RestStreamReader
except ImportError:
    # Streaming not available in this PySpark version
    RestStreamReader = None
