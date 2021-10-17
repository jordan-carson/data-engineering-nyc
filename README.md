# JP Morgan Assignment

__Author__ - Jordan Carson\
__Date__   - Thursday October 14th, 2021
__Role__   - Data Engineer

This assignment is broken up into two stages - with each stage containing its own workbook. Part 1 will consist of downloading data via the NYC OpenData API, while the second stage consists of exploratory data analysis using the data gathered from Part 1. 

The links to the Notbook and Dashboard can be found below:

- [Data Download](https://github.com/jordan-carson/data-engineering-nyc/blob/main/src/part1_data_download.ipynb)
- [Exploratory Data Analysis](https://github.com/jordan-carson/data-engineering-nyc/blob/main/src/part2_eda.ipynb)
- [Tableau Dashboard Online](https://public.tableau.com/app/profile/jordan.carson/viz/NYCOpenData/NYCOpenData?publish=yes)
### NYC Open Data

The source data for the assignment is the [Motor Vehicle Collisions - Crash table](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95), or MVCC, provided by the **NYC OpenData** database. This table contains details on crash events from all NYC Boroughs, where each row represents a unique crash event.

The information from the MVCC originates from [Police Reports (MV-104AN)](https://www.nhtsa.gov/sites/nhtsa.dot.gov/files/documents/ny_overlay_mv-104an_rev05_2004.pdf) filled in by the NYPD. According to the data description, "police report (MV104-AN) is required to be filled out for collisions where someone is injured or killed, or where there is at least $1000 worth of damage". Additionally, this data is preliminary and subject to modifications when the forms are amended based on revised crash details. For the most accurate information, which is updated weekly and referred to as the [NYPD Motor Vehicle Collisions page](https://www1.nyc.gov/site/nypd/stats/traffic-data/traffic-data-collision.page) and updated monthly - [Vision Zero View](https://vzv.nyc/).

[To read more information about the CompStat program.](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95).

### Unit Testing and Mock API Requests

Within the tests directory we have included a unit test for the API Response, using `unittest` and `unittest.mock.patch`.

### File Structure

The file structure below after .gitignore

📦data-engineering-nyc
 ┣ 📂.data
 ┃ ┣ 📜output.csv
 ┃ ┗ 📜output.feather
 ┣ 📂infra
 ┃ ┗ 📂aws
 ┃ ┃ ┣ 📜secret.json
 ┃ ┃ ┗ 📜secrets_manager.py
 ┣ 📂src
 ┃ ┣ 📂api
 ┃ ┃ ┣ 📜async_api.py
 ┃ ┃ ┣ 📜get_data.py
 ┃ ┃ ┗ 📜zip_codes.py
 ┃ ┣ 📂common
 ┃ ┃ ┣ 📂data_blend
 ┃ ┃ ┃ ┣ 📜__init__.py
 ┃ ┃ ┃ ┗ 📜operations.py
 ┃ ┃ ┣ 📂db_utilities
 ┃ ┃ ┃ ┣ 📜database.py
 ┃ ┃ ┃ ┣ 📜db_access.py
 ┃ ┃ ┃ ┗ 📜db_utilities.py
 ┃ ┃ ┗ 📂utilities
 ┃ ┃ ┃ ┣ 📜__init__.py
 ┃ ┃ ┃ ┣ 📜decorators.py
 ┃ ┃ ┃ ┣ 📜df_utils.py
 ┃ ┃ ┃ ┣ 📜filesystem.py
 ┃ ┃ ┃ ┣ 📜logging_settings.py
 ┃ ┃ ┃ ┣ 📜parallel.py
 ┃ ┃ ┃ ┗ 📜time_utils.py
 ┃ ┣ 📂notebooks
 ┃ ┃ ┗ 📜main.ipynb
 ┃ ┣ 📜part1_data_download.ipynb
 ┃ ┗ 📜part2_eda.ipynb
 ┣ 📂tests
 ┃ ┗ 📜tests_api_response.py
 ┣ 📜.gitattributes
 ┣ 📜.gitignore
 ┣ 📜.pre-commit-config.yaml
 ┣ 📜.python-version
 ┣ 📜AUTHORS.rst
 ┣ 📜README.md
 ┣ 📜requirements.txt
 ┗ 📜requirements.yaml
### References

Useful Resources, additional sources of data, and API documentation.
1. https://towardsdatascience.com/fast-and-async-in-python-accelerate-your-requests-using-asyncio-62dafca83c33
2. https://www.nycbynatives.com/nyc_info/new_york_city_zip_codes.php
3. https://dev.socrata.com/docs/queries/
4. https://dev.socrata.com/docs/paging.html
5. https://datatable.readthedocs.io/en/latest/manual/comparison_with_pandas.html