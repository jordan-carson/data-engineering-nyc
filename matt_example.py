# @decorators.timeit

import requests
import pandas as pd


# def api_pagination_results(orient = 'records'):
#     """
#     One method to pull data from the Open Source API is to 
#     """
#     ID = 'collision_id'
#     finished = False
#     offset = 0
#     out_frames = list()
#     while not finished:
#         ENDPOINT = f'https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit={API_LIMIT}&$offset={offset}&$order={ID}'
#         response = requests.get(ENDPOINT, None)

#         temp_df = pd.read_json(response.text, orient=orient)
#         length = len(temp_df)
#         out_frames.append(temp_df)
        
#         offset += temp_df.shape[0] # len(temp_df)

#         if length < API_LIMIT:
#             finished = True
        
#     # concatenate the list into one master dataframe, do not do this inside the loop!
#     df = pd.concat(out_frames, ignore_index=True)
#     del out_frames, length, offset
#     return df

ID = 'collision_id'
offset = 0
API_LIMIT = 1000
ENDPOINT = f'https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit={API_LIMIT}&$offset={offset}&$order={ID}'
response = requests.get(ENDPOINT, None)
breakpoint()
response.next