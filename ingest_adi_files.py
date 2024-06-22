# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

import zipfile
volumepath = "/Volumes/mimi_ws_1/neighborhoodatlas/src"

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.neighborhoodatlas.adi_censusblock;

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

def to_int(x):
    try: 
        return int(x)
    except ValueError:
        return None

for file in Path(volumepath + "/adi/").glob("US_*"):
    year = file.stem[3:7]
    dt = parse(f"{year}-12-31").date()
    dt_str = dt.strftime('%Y-%m-%d')
    print(dt_str, " ingesting...")
    pdf = pd.read_csv(file, dtype={"FIPS": str})
    pdf.columns = change_header(pdf.columns)
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
    pdf["mimi_src_file_date"] = dt
    pdf["mimi_src_file_name"] = file.name
    pdf["mimi_dlt_load_date"] = datetime.today().date()
    if "unnamed_0" in pdf.columns:
        pdf = pdf.drop(columns=["unnamed_0"])
    df = spark.createDataFrame(pdf)
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"mimi_src_file_date = '{dt_str}'")
        .saveAsTable("mimi_ws_1.neighborhoodatlas.adi_censusblock"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derive Census-tract version

# COMMAND ----------

df = spark.read.table("mimi_ws_1.neighborhoodatlas.adi_censusblock")

# COMMAND ----------

import pyspark.sql.functions as f
df_ct = (df.filter(f.col("adi_natrank").isNotNull())
            .filter(f.col("adi_staternk").isNotNull())
            .withColumn('fips_censustract', f.col('fips').substr(1, 11))
            .groupBy("mimi_src_file_date", "mimi_src_file_name", "fips_censustract")
            .agg(f.avg(f.col("adi_natrank")).alias("adi_natrank_avg"),
                 f.avg(f.col("adi_staternk")).alias("adi_staternk_avg"),
                 f.median(f.col("adi_natrank")).alias("adi_natrank_median"),
                 f.median(f.col("adi_staternk")).alias("adi_staternk_median"),
                f.std(f.col("adi_natrank")).alias("adi_natrank_std"),
                 f.std(f.col("adi_staternk")).alias("adi_staternk_std"))
            .withColumn('mimi_dlt_load_date', f.lit(datetime.today().date()))
            )

# COMMAND ----------

(df_ct.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("mimi_ws_1.neighborhoodatlas.adi_censustract"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## County version

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.neighborhoodatlas.adi_county

# COMMAND ----------

df = spark.read.table("mimi_ws_1.neighborhoodatlas.adi_censusblock")
df_ct = (df.filter(f.col("adi_natrank").isNotNull())
            .filter(f.col("adi_staternk").isNotNull())
            .withColumn('fips_county', f.col('fips').substr(1, 5))
            .groupBy("mimi_src_file_date", "mimi_src_file_name", "fips_county")
            .agg(f.avg(f.col("adi_natrank")).alias("adi_natrank_avg"),
                 f.avg(f.col("adi_staternk")).alias("adi_staternk_avg"),
                 f.median(f.col("adi_natrank")).alias("adi_natrank_median"),
                 f.median(f.col("adi_staternk")).alias("adi_staternk_median"),
                f.std(f.col("adi_natrank")).alias("adi_natrank_std"),
                 f.std(f.col("adi_staternk")).alias("adi_staternk_std"))
            .withColumn('mimi_dlt_load_date', f.lit(datetime.today().date()))
            )
(df_ct.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("mimi_ws_1.neighborhoodatlas.adi_county"))

# COMMAND ----------


