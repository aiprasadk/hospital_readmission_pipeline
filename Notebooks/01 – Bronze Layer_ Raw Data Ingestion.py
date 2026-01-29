# %% [markdown]
# ### Bronze Layer – Raw Data Ingestion
# 
# This notebook ingests raw healthcare CSV datasets from Databricks Volumes
# and stores them as Bronze Delta tables without any transformation.

# %%
# Volume paths
RAW_VOLUME_PATH = "/Volumes/healthcare/bronze/raw_volume"

DIABETIC_DATA_PATH = f"{RAW_VOLUME_PATH}/diabetic_data.csv"
IDS_MAPPING_PATH = f"{RAW_VOLUME_PATH}/IDS_mapping.csv"

# %%
# Read diabetic_data.csv
diabetic_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(DIABETIC_DATA_PATH)
)

diabetic_df.printSchema()
diabetic_df.show(5)

# %%
# Read IDs_mapping.csv
ids_mapping_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(IDS_MAPPING_PATH)
)

ids_mapping_df.printSchema()
ids_mapping_df.show(5)

# %%
# Bronze Table – Patient Encounters
(
    diabetic_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("healthcare.bronze.patient_encounters_raw")
)

# %%
# Bronze Table – Code Mapping
(
    ids_mapping_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("healthcare.bronze.code_mapping_raw")
)

# %%
%sql
SELECT COUNT(*) FROM healthcare.bronze.patient_encounters_raw;

# %%
%sql
SELECT COUNT(*) FROM healthcare.bronze.code_mapping_raw;

# %%
%sql
SELECT * 
FROM healthcare.bronze.patient_encounters_raw
LIMIT 5;

# %%
%sql
SELECT * 
FROM healthcare.bronze.code_mapping_raw
LIMIT 5;


