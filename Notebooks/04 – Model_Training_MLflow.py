# %%
import gc
# Clear specific large variables from memory
for var in ['model', 'pipeline', 'rf', 'predictions', 'train_df', 'test_df']:
    if var in locals():
        del locals()[var]

gc.collect()
print("Python memory cleared. Proceed to the next cell.")

# %%
# 1. Imports
import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import col
from pyspark.ml.functions import vector_to_array

# %%
# 2. Load and Cast Data
# Loading the Gold features table prepared in the previous notebook
gold_df = spark.read.table("healthcare.gold.readmission_features")

# List of numeric columns to be cast to Double for the VectorAssembler
numeric_cols = [
    "time_in_hospital", "num_lab_procedures", "num_procedures", 
    "num_medications", "number_diagnoses", "number_outpatient", 
    "number_emergency", "number_inpatient"
]

for c in numeric_cols:
    gold_df = gold_df.withColumn(c, col(c).cast("double"))

# %%
# 3. Explicitly Define String Indexers
# These stages convert string categories into numerical indices with your requested suffix (_idx)
age_indexer = StringIndexer(inputCol="age", outputCol="age_idx", handleInvalid="keep")
gender_indexer = StringIndexer(inputCol="gender", outputCol="gender_idx", handleInvalid="keep")
race_indexer = StringIndexer(inputCol="race", outputCol="race_idx", handleInvalid="keep")

# Start building the pipeline stages list
stages = [age_indexer, gender_indexer, race_indexer]

# 4. Define Feature Columns List
# This is your specific list used for the VectorAssembler
feature_cols = [
    "age_idx",
    "gender_idx",
    "race_idx",
    "time_in_hospital",
    "num_lab_procedures",
    "num_procedures",
    "num_medications",
    "number_outpatient",
    "number_emergency",
    "number_inpatient",
    "number_diagnoses"
]

# %%
# 5. Build Vector Assembler
# Combines all features into a single 'features' vector for the model
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
stages += [assembler]

# %%
# 6. Define the UC Volume path for staging (Required for Serverless)
uc_volume_path = "/Volumes/healthcare/bronze/raw_volume/ml_tmp"

# %%
# 7. MLflow Tracking & Model Training
experiment_path = f"/Users/{spark.sql('SELECT current_user()').collect()[0][0]}/Readmission_Prediction"
mlflow.set_experiment(experiment_path)

with mlflow.start_run(run_name="RF_Final_Integrated_Fix"):
    
    # Lightweight Classifier for Serverless (10 trees, depth 5)
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10, maxDepth=5)
    stages += [rf]
    
    # Create the Pipeline and Split Data
    pipeline = Pipeline(stages=stages)
    train_df, test_df = gold_df.randomSplit([0.8, 0.2], seed=42)
    
    # Train the Model
    model = pipeline.fit(train_df)
    
    # Transform test data to get predictions
    predictions = model.transform(test_df)
    
    # --- CRITICAL FIX: Extract Readmission Risk ---
    # probability[1] is the likelihood of the patient being readmitted (label 1)
    predictions = predictions.withColumn(
        "readmission_risk", 
        vector_to_array(col("probability"))[1]
    )
    
    # Evaluate Performance (AUC)
    evaluator = BinaryClassificationEvaluator(labelCol="label")
    auc = evaluator.evaluate(predictions)
    
    # Log metrics and model artifacts to MLflow
    mlflow.log_metric("auc", auc)
    mlflow.spark.log_model(
        spark_model=model, 
        artifact_path="readmission_rf_model",
        dfs_tmpdir=uc_volume_path
    )
    
    print(f"SUCCESS! Model Training Complete. AUC Score: {auc}")

# %%
# 8. FINAL WRITE: Save predictions with exactly 5 columns
(
    predictions.select(
        "patient_nbr", 
        "label", 
        "prediction", 
        "probability", 
        "readmission_risk"
    )
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true") 
    .saveAsTable("healthcare.gold.readmission_predictions")
)

# %%
%sql
DESCRIBE TABLE healthcare.gold.readmission_predictions;


