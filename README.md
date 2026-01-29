# 🏥 Hospital Readmission Risk Prediction
[![Databricks](https://img.shields.io/badge/Platform-Databricks-orange.svg)](https://www.databricks.com/)
[![MLflow](https://img.shields.io/badge/MLOps-MLflow-blue.svg)](https://mlflow.org/)
[![Spark](https://img.shields.io/badge/Engine-Spark_MLlib-red.svg)](https://spark.apache.org/mllib/)

## 🚀 Executive Summary
This project delivers an end-to-end clinical data engineering and machine learning pipeline built on **Databricks** using the **Medallion Architecture**. By automating the data lifecycle from raw ingestion to predictive analytics, the system forecasts 30-day readmission risks for diabetic patients, providing actionable insights for clinical decision support.

---

## 🎯 Problem Statement
Hospitals face significant challenges in identifying patients likely to be readmitted within 30 days of discharge. Standard rule-based systems often overlook the complex interplay between patient demographics, multi-categorical diagnoses, and hospitalization history.

**Objective:** Develop an AI-driven system to predict readmission probability using historical encounter data, leveraging the **Databricks Lakehouse** to unify data engineering and MLOps.

---

## 🏗️ Technical Architecture
The solution is governed by **Unity Catalog** and processed using **Databricks Serverless Compute** for maximum scalability and performance.



### 1. The Medallion Data Pipeline
* The solution leverages a managed Medallion architecture within Unity Catalog to ensure data lineage and governance.

<img width="1858" height="917" alt="Screenshot 2026-01-29 113141" src="https://github.com/user-attachments/assets/097ed0f2-c33c-438a-bf3e-a79b838095d5" />

* **🥉 Bronze (Raw):** Automated ingestion of patient encounters and ICD-9 clinical code mappings into Delta format.
* **🥈 Silver (Cleaned):** Implementation of schema enforcement and clinical validation. Specifically, the pipeline filters out deceased or hospice patients to ensure the model focuses only on actionable readmission cases.
* **🥇 Gold (Curated):** Advanced feature engineering where complex ICD-9 codes are mapped into 9 high-level clinical categories (e.g., Circulatory, Respiratory, Diabetes) to reduce feature sparsity.


### 2. Machine Learning & MLOps Lifecycle
* **Algorithm:** Random Forest Classifier optimized for high-dimensional clinical data.
* **Feature Pipeline:** Integrated `StringIndexer` for categorical encoding (Race, Gender, Age) and `VectorAssembler` for unified feature vectorization.
* **MLflow Integration:** Full experiment tracking, logging of **AUC (0.537)**, and model versioning directly within Unity Catalog Volumes.
* **Post-Processing:** Leveraged `vector_to_array` to convert Spark ML probability vectors into human-readable risk percentages for dashboarding.
  
<img width="1875" height="912" alt="Screenshot 2026-01-29 114326" src="https://github.com/user-attachments/assets/912fd3fa-8238-4478-a6f2-c6d0206f5b8a" />

---

## 📊 Clinical Insights & Dashboarding
The Gold-layer predictions feed an interactive **Databricks SQL Dashboard**:

| Visual | Clinical Insight |
| :--- | :--- |
| **⚠️ High-Risk List** | Real-time table of patients with high probability scores for targeted discharge planning. |
| **🎯 Model Accuracy** | The model accurately identifies **88.68%** of patients who do *not* require intervention, effectively reducing clinical alert fatigue. |
| **📈 Risk Drivers** | Scatter plots identify a strong correlation between **Length of Stay** and readmission risk, peaking for stays between 8–14 days. |


<img width="1870" height="914" alt="Screenshot 2026-01-29 113506" src="https://github.com/user-attachments/assets/7e7d3b48-262b-4bfe-a0cb-d99a10439866" />
<img width="1872" height="916" alt="Screenshot 2026-01-29 113718" src="https://github.com/user-attachments/assets/99e84f8b-c21f-4e3c-9188-c136e71ab4ca" />

---

## ⚙️ Orchestration & MLOps
Automation is handled via the **05 – Project_Orchestrator** notebook, simulating a production-grade Databricks Workflow:

1.  **Ingestion:** Task 1 (Bronze)
2.  **Cleaning:** Task 2 (Silver) - *Depends on Task 1*
3.  **Engineering:** Task 3 (Gold) - *Depends on Task 2*
4.  **ML Training:** Task 4 (MLflow) - *Depends on Task 3*
5.  **Validation:** Automated schema and count verification.

---

## 🛠️ Setup & Execution
1.  **Environment:** Ensure the `healthcare` catalog and associated schemas are initialized in Unity Catalog.
2.  **Execution:** Trigger the `05 – Project_Orchestrator` notebook to run the end-to-end pipeline.
3.  **Analytics:** Explore results in the **SQL Workspace** under the **Hospital Readmission Risk** dashboard.

---
**👤 Author** 
**Prasad Kulkarni** 
*Databricks 14-Day AI Challenge Participant*
