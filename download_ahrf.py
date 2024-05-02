# Databricks notebook source

import requests

# COMMAND ----------

download_lst = [
    "https://data.hrsa.gov//DataDownload/AHRF/AHRF_SAS_2022-2023.zip",
    "https://data.hrsa.gov//DataDownload/AHRF/AHRF_2021-2022_SAS.zip",
    "https://data.hrsa.gov//DataDownload/AHRF/AHRF_2020-2021_SAS.zip",
    "https://data.hrsa.gov//DataDownload/AHRF/AHRF_2019-2020_SAS.zip",
    "https://data.hrsa.gov//DataDownload/AHRF/AHRF_SN_SAS_2022-2023.zip", # state/national
    "https://data.hrsa.gov//DataDownload/AHRF/AHRF_SN_2021-2022_SAS.zip",
    "https://data.hrsa.gov//DataDownload/AHRF/AHRF_SN_2020-2021_SAS.zip",
    "https://data.hrsa.gov//DataDownload/AHRF/AHRF_SN_2019-2020_SAS.zip"
    ]
volumepath = "/Volumes/mimi_ws_1/hrsa/src/zipfiles"

# COMMAND ----------

def download_file(url, filename, folder):
    # NOTE the stream=True parameter below
    with requests.get(f"{url}", stream=True) as r:
        r.raise_for_status()
        with open(f"{folder}/{filename}", 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)

# COMMAND ----------

for url in download_lst:
    filename = url.split("/")[-1]
    download_file(url, filename, volumepath)

# COMMAND ----------


