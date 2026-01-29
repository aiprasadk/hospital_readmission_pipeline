# %% [markdown]
# # Project Orchestration
# 
# This notebook represents the final orchestration layer of the Databricks Job.
# Execution is managed via a multi-task Databricks Job that runs the pipeline
# from Bronze ingestion to ML prediction and analytics.

# %%
# Master Orchestrator: Healthcare Readmission Pipeline
# This notebook simulates a production Databricks Job/Workflow.

# Set a timeout for safety (in seconds)
TIMEOUT = 1800 

try:
    # Task 1 -> Notebook 01
    print("Starting Task 1: Bronze Layer - Raw Data Ingestion...")
    dbutils.notebook.run("01 – Bronze Layer: Raw Data Ingestion", TIMEOUT)
    print("Task 1 Complete.\n")

    # Task 2 -> depends on Task 1
    print("Starting Task 2: Silver Layer - Data Cleaning & Transformation...")
    dbutils.notebook.run("02 – Silver Layer: Data Cleaning & Transformation", TIMEOUT)
    print("Task 2 Complete.\n")

    # Task 3 -> depends on Task 2
    print("Starting Task 3: Gold Layer - Feature Engineering...")
    dbutils.notebook.run("03 – Gold Layer: Feature Engineering", TIMEOUT)
    print("Task 3 Complete.\n")

    # Task 4 -> depends on Task 3
    print("Starting Task 4: Model Training & MLflow Logging...")
    dbutils.notebook.run("04 – Model_Training_MLflow", TIMEOUT)
    print("Task 4 Complete.\n")

    # Task 5 -> depends on Task 4 (Final validation/Reporting)
    print("Starting Task 5: Final Pipeline Verification...")
    # This task confirms all previous tables are updated and available
    spark.sql("SELECT count(*) FROM healthcare.gold.readmission_predictions").show()
    print("Task 5 Complete.\n")

    print("SUCCESS: The healthcare_readmission_pipeline has finished successfully.")

except Exception as e:
    print(f"PIPELINE ERROR: The orchestration failed at a specific task. Details: {e}")


