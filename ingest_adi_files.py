# Databricks notebook source
from pathlib import Path
import zipfile
from dateutil.parser import parse
import pandas as pd
volumepath = "/Volumes/mimi_ws_1/neighborhoodatlas/src"

# COMMAND ----------

for file_downloaded in Path(volumepath + "/zipfiles/").glob("*"):
    with zipfile.ZipFile(file_downloaded, "r") as zip_ref:
        for member in zip_ref.namelist():
            if "readme.txt" in member.lower():
                continue
            if not Path(f"{volumepath}/adi/{member}").exists():
                print(f"Extracting {member}...")
                zip_ref.extract(member, path=f"{volumepath}/adi")

# COMMAND ----------



# COMMAND ----------

def to_int(x):
    try: 
        return int(x)
    except ValueError:
        return None

for file in Path(volumepath + "/adi/").glob("US_*"):
    year = file.stem[3:7]
    dt = parse(f"{year}-12-31").date()
    dt_str = dt.strftime('%Y-%m-%d')
    pdf = pd.read_csv(file, dtype={"FIPS": str})
    pdf.columns = [x.lower() for x in pdf.columns]
    pdf["nat_gq"] = (pdf["adi_natrank"] == "GQ")
    pdf["nat_ph"] = (pdf["adi_natrank"] == "PH")
    pdf["nat_gqph"] = (pdf["adi_natrank"] == "GQ-PH")
    pdf["nat_qdi"] = (pdf["adi_natrank"] == "QDI")
    pdf["state_gq"] = (pdf["adi_staternk"] == "GQ")
    pdf["state_ph"] = (pdf["adi_staternk"] == "PH")
    pdf["state_gqph"] = (pdf["adi_staternk"] == "GQ-PH")
    pdf["state_qdi"] = (pdf["adi_staternk"] == "QDI")
    pdf["adi_staternk"] = pd.to_numeric(pdf["adi_staternk"], errors="coerce")
    pdf["adi_natrank"] = pd.to_numeric(pdf["adi_natrank"], errors="coerce")    
    pdf["_input_file_date"] = dt
    if "unnamed: 0" in pdf.columns:
        pdf = pdf.drop(columns=["unnamed: 0"])
    df = spark.createDataFrame(pdf)
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_input_file_date = '{dt_str}'")
        .saveAsTable(f"mimi_ws_1.neighborhoodatlas.adi_censusblock"))

# COMMAND ----------


