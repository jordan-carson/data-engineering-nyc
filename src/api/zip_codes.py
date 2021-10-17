from bs4 import BeautifulSoup
import requests
import numpy as np
import pandas as pd

URL = 'https://www.nycbynatives.com/nyc_info/new_york_city_zip_codes.php'
# keep columns 0 and 1, remove 2, separate 3 and 4 as a separate dataframe then concatenate.

def _get_table_data_text(table):
    def __get_row_data_text(tr, tag='id'):
        return [td.get_text(strip=True) for td in tr.find_all(tag)]
    rows = list()
    trs = table.find_all('tr')
    headerrow = __get_row_data_text(trs[0], 'th')
    if headerrow:
        rows.append(headerrow)
        trs = trs[1:]
    for tr in trs:
        rows.append(__get_row_data_text(tr, 'td'))
    return rows


def _get_data():
    page = requests.get(URL)
    soup = BeautifulSoup(page.text, 'html.parser')
    table = soup.find('table')
    return _get_table_data_text(table)


def _corrected_output(output: list) -> list:
    if not isinstance(output, list):
        raise ValueError('Expected `{}` as argument of {}, got={}'.format(list.__name__, __name__, type(output)))

    return [r[:2] for r in output] + [r[3:] for r in output]
    
    
def get_zip_codes():
    lists = _corrected_output(_get_data())
    columns = ['zip_code', 'borough']
    return pd.DataFrame(lists, columns=columns)