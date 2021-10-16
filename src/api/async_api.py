from re import S
import pandas as pd
import asyncio
from asyncio.events import get_event_loop
from warnings import resetwarnings
import aiohttp
from asyncio import ensure_future, gather

import time
start = time.time()

###### Example 1: Using aiohttp.ClientSession with asyncio.ensure_future - uses gather to return the results

async def request_controller(urls):
    async with aiohttp.ClientSession() as sess:
        tasks = [ensure_future(request_worker(sess, url)) for url in urls]
        res = await gather(*tasks)
    return res


async def request_worker(session, url):
    async with session.get(url) as response:
        return await response.json()

def create_urls(id='collision_id'):
    # we start with an offset of 0, we then increment the offset to be equal to the number of records returned. We are specifying the 
    # number of records returned via the API_LIMIT. 
    offset, limit = 0, 50000
    urls = list()
    for _ in range(0, 2_000_000, limit): #2_000_000 / 
        ENDPOINT = f'https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit={limit}&$offset={offset}&$order={id}'
        urls.append(ENDPOINT)
        offset += limit
    return urls


def get_async_data():
    urls = create_urls()
    assert isinstance(urls, list), 'Urls is not a list!'
    loop = asyncio.get_event_loop()
    text_results = loop.run_until_complete(request_controller(urls))
    print('Resuls of the first element in the text results: ')
    print(text_results[0])
    dfs = [pd.read_json(response, orient='records') for response in text_results]
    return pd.concat(dfs, ignore_index=True)



######### Example 2

async def get_response_text_asynchronous(url, session):
    try:
        response = await session.request(method='GET', url=url)
    except Exception as err:
        print(f'Exception has occurred: {err}')
    response_text = await response.text.encode("utf8")
    return response_text

async def run_all(url, session): # wrapper for running the asynchronous program
    try:
        text_response = await get_response_text_asynchronous(url, session)
    except Exception as err:
        print('Exception has occurred, ')
    return text_response

LIST_URLS = create_urls()

async def finalize_program():
    with asyncio.ClientSession() as session:
        test_response = await gather(*[run_all(url, session) for url in LIST_URLS])
        dfs = [pd.read_json(response, orient='records') for response in test_response]
    return pd.concat(dfs, ignore_index=True)
