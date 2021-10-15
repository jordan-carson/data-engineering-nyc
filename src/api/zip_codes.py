from bs4 import BeautifulSoup
import requests
import numpy as np
import pandas as pd
import urllib

URL = 'https://www.nycbynatives.com/nyc_info/new_york_city_zip_codes.php'
# keep columns 0 and 1, remove 2, separate 3 and 4 as a separate dataframe then concatenate.


def getTableDataText(table):
    def rowGetDataText(tr, tag='id'):
        return [td.get_text(strip=True) for td in tr.find_all(tag)]

    rows = list()
    trs = table.find_all('tr')

    headerrow = rowGetDataText(trs[0], 'th')
    if headerrow:
        rows.append(headerrow)
        trs = trs[1:]
    for tr in trs:
        rows.append(rowGetDataText(tr, 'td'))
    return rows


def get_data():
    page = requests.get(URL)
    soup = BeautifulSoup(page.text, 'html.parser')
    table = soup.find('table')
    # rows = table.find_all(lambda t: t.name == 'tr')
    return getTableDataText(table)


def corrected_output(output: list) -> list:
    if not isinstance(output, list):
        raise ValueError('Expected `{}` as argument of {}, got={}'.format(list.__name__, __name__, type(output)))

    return [r[:2] for r in output] + [r[3:] for r in output]
    

def pandas_output():
    df = pd.read_html(URL)[0]
    df.columns = ['zipcode1', 'borough1', 'NA', 'zipcode2', 'borough2']

    df.drop(['NA'], inplace=True, axis=1)

    print(df.head())


result = get_data()
print(corrected_output(result))
