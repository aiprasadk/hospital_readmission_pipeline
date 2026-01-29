# %%
# 1. Imports
from pyspark.sql.functions import col, when, trim, expr

# %%
# 2. Load Data
encounters_df = spark.read.table("healthcare.bronze.patient_encounters_raw")
mapping_df = spark.read.table("healthcare.bronze.code_mapping_raw")

# %%
# 3. STEP ONE: Clean strings but KEEP THEM AS STRINGS for now
# This replaces '?', empty strings, and whitespace with None (Null)
df_string_clean = encounters_df.select([
    when(trim(col(c)) == '?', None)
    .when(trim(col(c)) == '', None)
    .otherwise(trim(col(c))).alias(c) 
    for c in encounters_df.columns
])

# %%
# 4. Create the Target Label (Still as a string/int works here)
df_with_label = df_string_clean.withColumn(
    "label", 
    when(col("readmitted") == "<30", 1).otherwise(0)
)

# %%
# 5. STEP TWO: Use the "Safe Cast" only inside a select statement 
# We use try_cast() via expr to ensure that if a weird character persists, it becomes NULL, not an ERROR.
silver_final = df_with_label.select(
    "*",
    expr("try_cast(discharge_disposition_id AS INT)").alias("discharge_id_fixed"),
    expr("try_cast(admission_type_id AS INT)").alias("admission_id_fixed")
).drop("discharge_disposition_id", "admission_type_id") \
 .withColumnRenamed("discharge_id_fixed", "discharge_disposition_id") \
 .withColumnRenamed("admission_id_fixed", "admission_type_id")

# %%
# 6. STEP THREE: Final Filter for Expired Patients
# Now that it's an INT, we can safely filter
silver_final = silver_final.filter(
    ~col("discharge_disposition_id").isin([11, 13, 14, 19, 20, 21])
)

# %%
# 7. Write to Table (Using a fresh name to be safe)
(
    silver_final
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true") 
    .saveAsTable("healthcare.silver.patient_encounters_cleaned_final")
)

print("SUCCESS: Silver layer processed. The 'malformed' error has been defeated.")

# %%
# Validation check: Verify the casting and the new label
display(spark.sql("""
    SELECT 
        patient_nbr, 
        discharge_disposition_id, 
        admission_type_id, 
        readmitted, 
        label 
    FROM healthcare.silver.patient_encounters_cleaned_final 
    LIMIT 10
"""))

# %%
# Check 1: Ensure no 'Expired' patients remain (IDs 11, 19, 20, 21 etc.)
print("Checking for Expired/Hospice IDs (Should be 0):")
display(spark.sql("""
    SELECT discharge_disposition_id, count(*) 
    FROM healthcare.silver.patient_encounters_cleaned_final 
    WHERE discharge_disposition_id IN (11, 13, 14, 19, 20, 21)
    GROUP BY 1
"""))



# %%
# Check 2: Count of Readmitted (1) vs Not Readmitted (0)
print("Target Variable Distribution:")
display(spark.sql("""
    SELECT label, count(*) as patient_count 
    FROM healthcare.silver.patient_encounters_cleaned_final 
    GROUP BY label
"""))


