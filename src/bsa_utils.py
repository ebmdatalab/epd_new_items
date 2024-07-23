import os
import json
import grequests
import requests
import pandas as pd
import gzip
import io
import urllib.parse
from datetime import datetime
import logging
import time
import warnings

warnings.simplefilter("ignore", category=UserWarning)

logging.basicConfig(level=logging.WARNING)

class Config:
    """
    Configuration class to set up API endpoints and directory paths.
    """
    def __init__(self):
        self.base_endpoint = 'https://opendata.nhsbsa.net/api/3/action/'
        self.package_list_method = 'package_list'
        self.package_show_method = 'package_show?id='
        self.action_method = 'datastore_search_sql?'

        # Create data directory if it doesn't exist
        self.DATA_DIR = os.path.join("..", "data")
        self.CACHE_DIR = os.path.join(self.DATA_DIR, "cache")
        self.CACHE_MAPPING_FILE = os.path.join(self.CACHE_DIR, "cache_mapping.json")

        self.create_directories()

    def create_directories(self):
        os.makedirs(self.DATA_DIR, exist_ok=True)
        os.makedirs(self.CACHE_DIR, exist_ok=True)

CONFIG_OBJ = Config()

class CacheManager:
    """
    Manages caching of API responses to avoid redundant API calls.
    """
    def __init__(self, cache_dir, cache_mapping_file):
        self.cache_dir = cache_dir
        self.cache_mapping_file = cache_mapping_file

    def load_cache_mapping(self):
        if os.path.exists(self.cache_mapping_file):
            with open(self.cache_mapping_file, 'r') as f:
                return json.load(f)
        return {}

    def save_cache_mapping(self, cache_mapping):
        with open(self.cache_mapping_file, 'w') as f:
            json.dump(cache_mapping, f, indent=4)

    def save_to_cache(self, api_url, response_json):
        cache_mapping = self.load_cache_mapping()
        cache_file = os.path.join(self.cache_dir, f"cache_{len(cache_mapping) + 1}.json")
        with open(cache_file, 'w') as f:
            json.dump(response_json, f)
        cache_mapping[api_url] = cache_file
        self.save_cache_mapping(cache_mapping)

    def check_cache(self, api_url):
        cache_mapping = self.load_cache_mapping()
        if api_url in cache_mapping:
            cache_file = cache_mapping[api_url]
            if os.path.exists(cache_file):
                logging.info(f"Retrieving {api_url} from cache")
                with open(cache_file, 'r') as f:
                    return json.load(f)
        return None

CACHE_MANAGER_OBJ = CacheManager(CONFIG_OBJ.CACHE_DIR, CONFIG_OBJ.CACHE_MAPPING_FILE)

class ResourceNames:
    """
    Handles fetching and filtering resource names based on date ranges.
    """
    def __init__(self, resource, date_from, date_to):
        self.resource = resource
        self.resources_table = None
        self.resource_from = None
        self.resource_to = None

        self.get_resource_names()
        self.resource_from = self.set_date(date_from, date_type="from")
        self.resource_to = self.set_date(date_to, date_type="to")
        self.resource_name_list_filter()

    def get_resource_names(self):
        response = requests.get(f"{CONFIG_OBJ.base_endpoint}{CONFIG_OBJ.package_show_method}{self.resource}")
        response.raise_for_status()  # Ensure the request was successful
        metadata_response = response.json()
        self.resources_table = pd.json_normalize(metadata_response['result']['resources'])
        self.resources_table['date'] = pd.to_datetime(
            self.resources_table['bq_table_name'].str.extract(r'(\d{6})')[0], format='%Y%m', errors='coerce'
        )

    @staticmethod
    def validate_date(date_str):
        try:
            datetime.strptime(date_str, "%Y%m")
            return True
        except ValueError:
            return False

    @staticmethod
    def convert_YYMM_to_date(date_str):
        date = datetime.strptime(date_str, "%Y%m")
        return date.strftime("%Y-%m-%d")

    def get_nth_date(self, date_type, n, ascending=True):
        sorted_dates = self.resources_table['date'].sort_values(ascending=ascending).unique()
        if n < len(sorted_dates):
            return sorted_dates[n]
        max_val = len(sorted_dates) - 1
        raise ValueError(f"The value '{date_type}{n}' is out of range. Maximum allowable is '{date_type}{max_val}'.")

    def set_date(self, date, date_type):
        if date == "earliest":
            return self.resources_table['date'].min()
        if date == "latest":
            return self.resources_table['date'].max()
        if date.startswith("latest-"):
            try:
                n = int(date.split('-')[1])
                if n > 0:
                    return self.get_nth_date('latest-', n, ascending=False)
                raise ValueError("The value after 'latest-' must be a positive integer.")
            except ValueError as e:
                raise ValueError("Invalid format for 'latest-n'. Expected 'latest-1', 'latest-2', etc.") from e
        if date.startswith("earliest+"):
            try:
                n = int(date.split('+')[1])
                if n > 0:
                    return self.get_nth_date('earliest+', n, ascending=True)
                raise ValueError("The value after 'earliest+' must be a positive integer.")
            except ValueError as e:
                raise ValueError("Invalid format for 'earliest+n'. Expected 'earliest+1', 'earliest+2', etc.") from e
        if date == "" and date_type == "from":
            return self.resources_table['date'].min()
        if date == "" and date_type == "to":
            return self.resources_table['date'].max()
        if self.validate_date(date):
            return self.convert_YYMM_to_date(date)
        raise ValueError(
            "Unexpected date format. Expected one of the following: 'YYYYMM', 'earliest', 'latest', 'latest-n', or 'earliest+n' "
            "(e.g., 'latest-1', 'earliest+1')."
        )

    def resource_name_list_filter(self):
        filtered_df = self.resources_table[
            (self.resources_table['date'] >= self.resource_from) & 
            (self.resources_table['date'] <= self.resource_to)
        ]
        
        self.resource_name_list = filtered_df['bq_table_name'].tolist()
        self.date_list = filtered_df['date'].tolist()

    def return_date_list(self):
        return self.date_list
    
    def return_resources_from(self):
        return self.resource_from
    
    def return_resources_to(self):
        return self.resource_to

class APICall:
    """
    Represents a single API call with caching capabilities.
    """
    def __init__(self, resource_id, sql, cache=False):
        self.resource_id = resource_id
        self.sql = sql
        self.cache = cache
        self.api_url = None
        self.cache_data = None
        self.set_table_name()
        self.generate_url()
        self.collect_cache_data()

    def set_table_name(self):
        placeholder = "{FROM_TABLE}"
        if placeholder not in self.sql:
            raise ValueError(f"Placeholder {placeholder} not found in the SQL query.")
        self.sql = self.sql.replace(placeholder, f"FROM `{self.resource_id}`")

    def generate_url(self):
        self.api_url = (
            f"{CONFIG_OBJ.base_endpoint}{CONFIG_OBJ.action_method}"
            f"resource_id={self.resource_id}&"
            f"sql={urllib.parse.quote(self.sql)}"
        )
    
    def collect_cache_data(self):
        if self.cache:
            self.cache_data = CACHE_MANAGER_OBJ.check_cache(self.api_url)

class FetchData:
    """
    Orchestrates the fetching of data from the API, including handling
    of cache, API calls, and data processing.
    """
    def __init__(self, resource, sql, date_from, date_to, cache=False, max_attempts = 3):
        print (f"Fetching data please wait...")
        self.resource = resource
        self.sql = sql
        self.cache = cache
        self.max_attempts = max_attempts
        self.resource_names_obj = ResourceNames(resource, date_from, date_to)
        self.api_calls_list = []
        self.returned_json_list = []
        self.requests_map = []
        self.resource_list = []
        self.full_results_df = None
        self.generate_api_calls()
        self.generate_request_map()
        self.request_data()
        self.process_data()
        print (f"Data retrieved.")

    def generate_api_calls(self):
        for resource_id in self.resource_names_obj.resource_name_list:
            self.api_calls_list.append(APICall(resource_id, self.sql, self.cache))

    def generate_request_map(self):
        for api_call in self.api_calls_list:
            if api_call.cache_data:
                self.returned_json_list.append(api_call.cache_data)
            else:
                self.requests_map.append(api_call.api_url)
                self.resource_list.append(api_call.resource_id)

    def request_data(self):
        retry_counter = 1
        while self.requests_map and retry_counter <= self.max_attempts:
            if retry_counter > 1:
                logging.info("Retrying failed API requests")
                time.sleep(2 ** retry_counter)  # Exponential backoff
            
            rs = [grequests.get(url) for url in self.requests_map]
            for response in grequests.imap(rs, size=5):
                if response.status_code == 200:
                    self.returned_json_list.append(response.json())
                    CACHE_MANAGER_OBJ.save_to_cache(response.url, response.json())
                    self.requests_map.remove(response.url)
                    logging.info(f"Success for {response.url}")
                else:
                    logging.error(f"Error {response.status_code} for {response.url}. Will retry.")
                    #raise requests.HTTPError(response.status_code, response.url)

            retry_counter += 1

    def process_data(self):
        dataframes = []
        logging.info("Processing response data")
        for idx, response_json in enumerate(self.returned_json_list):
            if 'records_truncated' in response_json['result'] and response_json['result']['records_truncated'] == 'true':
                download_url = response_json['result']['gc_urls'][0]['url']
                logging.info(f"Downloading truncated data from URL: {download_url}")
                r = requests.get(download_url)
                with gzip.open(io.BytesIO(r.content), 'rt') as f:
                    tmp_df = pd.read_csv(f)
            else:
                tmp_df = pd.json_normalize(response_json['result']['result']['records'])
            dataframes.append(tmp_df)

        self.full_results_df = pd.concat(dataframes, ignore_index=True)
        logging.info("Data processing complete")
        
    def results(self):
        return self.full_results_df
    
    def return_resources_from(self):
        # Given Timestamp
        timestamp = pd.Timestamp(self.resource_names_obj.return_resources_from())

        # Convert to string in the format YYYY-MM
        formatted_string = timestamp.strftime('%Y-%m')

        return formatted_string
    
    def return_resources_to(self):
        # Given Timestamp
        timestamp = pd.Timestamp(self.resource_names_obj.return_resources_to())

        # Convert to string in the format YYYY-MM
        formatted_string = timestamp.strftime('%Y-%m')

        return formatted_string

def show_available_datasets():
    # Extract list of datasets
    datasets_response = requests.get(CONFIG_OBJ.base_endpoint +  CONFIG_OBJ.package_list_method).json()
    
    # Get as a list
    dataset_list=datasets_response['result']
    
    # Excluse FOIs from the results
    list_to_exclude=["foi"]
    filtered_list = [item for item in dataset_list if not any(item.startswith(prefix) for prefix in list_to_exclude)]
    
    # Print available datasets
    for item in filtered_list:
        print (item)