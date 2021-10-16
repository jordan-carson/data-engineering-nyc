"""
@Author     : Jordan Carson
@Content    : How to query NYC Open Data API via Python 
@Endpoint   : 

"""
# We have a few options to query the API
# Note that the API Call has a specified limit of 1000

# We have several options to pull data from the API
#  1. We can query via cURL
#  2. We can query via requests in Python
#  3. We can use sodapy.Socrate to create a client request to pull via the same API
#  4. We can use google bigquery as all of the data is stored in 

# Paging through the data
# Heads Up! The order of the results of a query are not implicitly ordered, so if you're paging, 
# make sure you provide an $order clause or at a minimum $order=:id. That will guarantee that the order 
# of your results will be stable as you page through the dataset.

# https://dev.socrata.com/docs/queries/
# We want to use "Pagination" to query through the API to pull back all the records.
import sys
import os
from pathlib import Path
CWD = str(Path.cwd().parent.parent)
sys.path.append(CWD)
from pandas.core import api
import requests
from requests.auth import HTTPBasicAuth
import json
import base64 
import traceback
from datetime import datetime, timedelta
import warnings
from infra.aws.secrets_manager import get_secret
from sodapy import Socrata
import pandas as pd

# Constant flags
API_VERIFY_SSL = False
# API_OUTPUT = '/Users/jordancarson/Projects/JPM/data-engineering-nyc/.data/json/data_pagination.json'
# API_WRITE_FILE = open(API_OUTPUT, 'w')

# API Constants
API_LIMIT = 50000 # we want to pull 50,000 records at each iteration
NYC_OPEN_DATA_API_ENDPOINT = 'https://data.cityofnewyork.us/resource/h9gi-nx95.json'

secretARN = json.load(open(os.path.join('/Users/jordancarson/Projects/JPM/data-engineering-nyc', 'infra', 'aws', 'secret.json')))[0]
SECRET_DATA = json.loads(get_secret(os.getenv('AWS_SECRET_NAME_NYC_OPEN_DATA') or secretARN.get('AWS_SECRET_NAME_NYC_OPEN_DATA')))
assert isinstance(SECRET_DATA, dict), 'SECRET_DATA was not loaded properly!'

NYC_OPEN_DATA_API_KEY = SECRET_DATA["NYC_OPEN_DATA_API_KEY"]
NYC_OPEN_DATA_API_SECRET =  SECRET_DATA['NYC_OPEN_DATA_API_SECRET']
NYC_OPEN_DATA_APP_TOKEN = SECRET_DATA['NYC_OPEN_DATA_APP_TOKEN']
NYC_OPEN_DATA_APP_SECRET = SECRET_DATA['NYC_OPEN_DATA_APP_SECRET']

BASE_CREDENTIALS_64 = base64.b64encode(str.encode(f'{NYC_OPEN_DATA_API_KEY}:{NYC_OPEN_DATA_API_SECRET}')).decode()
TOKEN_DATA = {
    'client_id': NYC_OPEN_DATA_API_KEY, 'client_secret': NYC_OPEN_DATA_API_SECRET
}
TOKEN_HEADERS = HEADERS = {'Authorization': f'Basic {BASE_CREDENTIALS_64}', }


def get_response():
    response = requests.get(NYC_OPEN_DATA_API_ENDPOINT, None)
    return response


def api_pagination_results(last_offset_value=1829000, orient = 'records'):
    """
    One method to pull data from the Open Source API is to 
    """
    ID = 'collision_id'
    finished = False
    offset = 0
    out_frames = list()
    while not finished:
        ENDPOINT = f'https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit={API_LIMIT}&$offset={offset}&$order={ID}'
        response = requests.get(ENDPOINT, auth=HTTPBasicAuth(NYC_OPEN_DATA_API_KEY, NYC_OPEN_DATA_API_SECRET))

        temp_df = pd.read_json(response.text, orient=orient)
        length = len(temp_df)
        out_frames.append(temp_df)
        print(length)
        
        offset += temp_df.shape[0] # len(temp_df)

        if length < API_LIMIT:
            finished = True
        
    # concatenate the list into one master dataframe
    df = pd.concat(out_frames, ignore_index=True)
    return df
    

def socrate_results():

    # Unauthenticated client only works with public data sets. Note 'None'
    # in place of application token, and no username or password:
    print("Running via Socrata...")
    client = Socrata("data.cityofnewyork.us", None)

    # Example authenticated client (needed for non-public datasets):
    # client = Socrata(data.cityofnewyork.us,
    #                  MyAppToken,
    #                  userame="user@example.com",
    #                  password="AFakePassword")

    # First 2000 results, returned as JSON from API / converted to Python list of
    # dictionaries by sodapy.
    results = client.get("h9gi-nx95", limit=2000, offset=0)

    # Convert to pandas DataFrame
    results_df = pd.DataFrame.from_records(results)
    return results_df


# print(socrate_results())