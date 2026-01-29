# %%
# 1. Imports
from pyspark.sql.functions import col, when, count, avg, trim

# %%
# 2. Load Silver Data
# Using your verified final table name
silver_df = spark.read.table("healthcare.silver.patient_encounters_cleaned_final")

# %%
# 3. ICD-9 Diagnosis Grouping Logic
def group_icd9(column_name):
    return (when(col(column_name).rlike('^428|^425|^402|^391|^429'), "Circulatory")
            .when(col(column_name).rlike('^490|^491|^492|^493|^494|^495|^496|^500|^501|^502|^503|^504|^505|^508|^511|^518|^460|^461|^462|^463|^464|^465|^466|^470|^471|^472|^473|^474|^475|^476|^477|^478|^480|^481|^482|^483|^484|^485|^486|^487|^488'), "Respiratory")
            .when(col(column_name).rlike('^530|^531|^532|^533|^534|^535|^536|^537|^538|^539|^540|^541|^542|^543|^550|^551|^552|^553|^555|^556|^557|^558|^560|^562|^564|^565|^566|^567|^568|^569|^570|^571|^572|^573|^574|^575|^576|^577|^578|^579'), "Digestive")
            .when(col(column_name).rlike('^250'), "Diabetes")
            .when(col(column_name).rlike('^800|^999'), "Injury") # Shortened for clarity
            .when(col(column_name).rlike('^710|^739'), "Musculoskeletal")
            .when(col(column_name).rlike('^580|^629'), "Genitourinary")
            .when(col(column_name).rlike('^140|^239'), "Neoplasms")
            .otherwise("Other"))

# Apply grouping and enforce Integer types for numeric features to prevent merge errors
gold_features = silver_df.withColumn("primary_diag_group", group_icd9("diag_1")) \
                         .withColumn("secondary_diag_group", group_icd9("diag_2")) \
                         .withColumn("tertiary_diag_group", group_icd9("diag_3")) \
                         .withColumn("time_in_hospital", col("time_in_hospital").cast("int")) \
                         .withColumn("num_lab_procedures", col("num_lab_procedures").cast("int")) \
                         .withColumn("num_procedures", col("num_procedures").cast("int")) \
                         .withColumn("num_medications", col("num_medications").cast("int"))

# %%
# 4. Final Feature Selection
final_ml_df = gold_features.select(
    "patient_nbr", "age", "gender", "race", 
    "time_in_hospital", "num_lab_procedures", "num_procedures", "num_medications",
    "number_outpatient", "number_emergency", "number_inpatient", 
    "number_diagnoses", "primary_diag_group", "secondary_diag_group", 
    "tertiary_diag_group", "label"
)

# %%
# 5. Write to Gold Delta Table with overwriteSchema
(
    final_ml_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true") # This fixes the [DELTA_FAILED_TO_MERGE_FIELDS] error
    .saveAsTable("healthcare.gold.readmission_features")
)

print("Gold Layer successfully saved with forced schema overwrite.")

# %%
display(spark.sql("SELECT * FROM healthcare.gold.readmission_features LIMIT 5"))

# %%
# Check the schema of your Gold table
spark.table("healthcare.gold.readmission_features").printSchema()

# Check a few rows to ensure the diagnosis grouping worked
display(spark.sql("SELECT primary_diag_group, count(*) FROM healthcare.gold.readmission_features GROUP BY 1"))


