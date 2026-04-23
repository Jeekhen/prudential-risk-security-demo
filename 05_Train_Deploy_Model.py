# Databricks notebook source
# MAGIC %md
# MAGIC # Train & Deploy Fraud Detection Model
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Reads claims and provider data from Unity Catalog
# MAGIC 2. Engineers features for fraud detection
# MAGIC 3. Trains a LightGBM classifier tracked with MLflow
# MAGIC 4. Registers the model in Unity Catalog
# MAGIC 5. Deploys it to a Model Serving endpoint
# MAGIC 6. Tests the live endpoint with a sample payload
# MAGIC
# MAGIC **Requirements:** Databricks Runtime 14.3 ML+ with access to Unity Catalog.

# COMMAND ----------

CATALOG = dbutils.widgets.get("CATALOG")
SCHEMA = dbutils.widgets.get("SCHEMA")
TABLE_PREFIX = f"{CATALOG}.{SCHEMA}"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.demo_fraud_model"
ENDPOINT_NAME = "demo-fraud-scoring"

print(f"Reading from: {TABLE_PREFIX}.demo_claims_*")
print(f"Model will be registered as: {MODEL_NAME}")
print(f"Serving endpoint: {ENDPOINT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Data

# COMMAND ----------

claims_df = spark.table(f"{TABLE_PREFIX}.demo_claims_raw")
providers_df = spark.table(f"{TABLE_PREFIX}.demo_claims_providers")

print(f"Claims: {claims_df.count():,} rows")
print(f"Providers: {providers_df.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Feature Engineering

# COMMAND ----------

from pyspark.sql import functions as F

# Join claims with provider features
joined_df = claims_df.join(
    providers_df.select(
        "provider_id",
        F.col("provider_type").alias("prov_type"),
        "region",
        "flagged_claims_pct",
        "risk_tier",
    ),
    on="provider_id",
    how="left",
)

# Select feature columns and target
feature_cols_raw = [
    "claim_amount_hkd",
    "processing_days",
    "claim_type",
    "prov_type",
    "region",
    "submission_channel",
    "flagged_claims_pct",
    "risk_tier",
]
target_col = "is_flagged"

pdf = joined_df.select(*feature_cols_raw, target_col).toPandas()
print(f"Training dataset shape: {pdf.shape}")
pdf.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encode categorical features

# COMMAND ----------

import pandas as pd
from sklearn.preprocessing import LabelEncoder

categorical_cols = ["claim_type", "prov_type", "region", "submission_channel", "risk_tier"]
label_encoders = {}

for col in categorical_cols:
    le = LabelEncoder()
    pdf[col] = le.fit_transform(pdf[col].astype(str))
    label_encoders[col] = le

# Feature matrix and target
feature_cols = [
    "claim_amount_hkd",
    "processing_days",
    "claim_type",
    "prov_type",
    "region",
    "submission_channel",
    "flagged_claims_pct",
    "risk_tier",
]
X = pdf[feature_cols]
y = pdf[target_col].astype(int)

print(f"Features: {list(X.columns)}")
print(f"Target distribution:\n{y.value_counts().to_string()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Train Model with MLflow

# COMMAND ----------

import mlflow
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score
from lightgbm import LGBMClassifier

mlflow.set_registry_uri("databricks-uc")

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"Train: {X_train.shape[0]:,}  |  Test: {X_test.shape[0]:,}")

# COMMAND ----------

mlflow.autolog(log_models=False)

experiment_path = f"/Users/{spark.sql('SELECT current_user()').first()[0]}/demo_fraud_detection"
mlflow.set_experiment(experiment_path)
print(f"MLflow experiment: {experiment_path}")

with mlflow.start_run(run_name="fraud_lgbm") as run:
    model = LGBMClassifier(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.05,
        num_leaves=31,
        min_child_samples=20,
        class_weight="balanced",
        random_state=42,
        verbose=-1,
    )
    model.fit(X_train, y_train)

    # Evaluate
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_proba)

    mlflow.log_metric("test_auc", auc)
    print(f"\nTest AUC: {auc:.4f}")
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred))

    # Log model with signature
    from mlflow.models.signature import infer_signature
    signature = infer_signature(X_train, model.predict(X_train))

    mlflow.sklearn.log_model(
        sk_model=model,
        artifact_path="model",
        signature=signature,
        input_example=X_train.iloc[:3],
        registered_model_name=MODEL_NAME,
    )
    run_id = run.info.run_id
    print(f"\nMLflow run ID: {run_id}")
    print(f"Model registered as: {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Get Latest Model Version

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()

# Get the latest version of the registered model
model_versions = client.search_model_versions(f"name='{MODEL_NAME}'")
latest_version = max(model_versions, key=lambda mv: int(mv.version))
model_version = latest_version.version
print(f"Latest model version: {model_version}")

# Set alias for serving
client.set_registered_model_alias(MODEL_NAME, "champion", model_version)
print(f"Set alias 'champion' on version {model_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Deploy to Model Serving Endpoint

# COMMAND ----------

import requests
import json
import time

# Get workspace URL and token
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
base_url = f"https://{workspace_url}"
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create or update the serving endpoint

# COMMAND ----------

endpoint_config = {
    "name": ENDPOINT_NAME,
    "config": {
        "served_entities": [
            {
                "entity_name": MODEL_NAME,
                "entity_version": str(model_version),
                "workload_size": "Small",
                "scale_to_zero_enabled": True,
            }
        ],
        "auto_capture_config": {
            "catalog_name": CATALOG,
            "schema_name": SCHEMA,
            "table_name_prefix": "demo_fraud_serving",
        },
    },
}

# Check if endpoint already exists
get_resp = requests.get(
    f"{base_url}/api/2.0/serving-endpoints/{ENDPOINT_NAME}",
    headers=headers,
)

if get_resp.status_code == 200:
    print(f"Endpoint '{ENDPOINT_NAME}' exists. Updating config...")
    update_resp = requests.put(
        f"{base_url}/api/2.0/serving-endpoints/{ENDPOINT_NAME}/config",
        headers=headers,
        json=endpoint_config["config"],
    )
    if update_resp.status_code not in (200, 201):
        print(f"Update failed ({update_resp.status_code}): {update_resp.text}")
        raise Exception(f"Failed to update endpoint: {update_resp.text}")
    print("Endpoint config updated.")
else:
    print(f"Creating new endpoint '{ENDPOINT_NAME}'...")
    create_resp = requests.post(
        f"{base_url}/api/2.0/serving-endpoints",
        headers=headers,
        json=endpoint_config,
    )
    if create_resp.status_code not in (200, 201):
        print(f"Create failed ({create_resp.status_code}): {create_resp.text}")
        raise Exception(f"Failed to create endpoint: {create_resp.text}")
    print("Endpoint created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wait for endpoint to be ready

# COMMAND ----------

TIMEOUT_SECONDS = 1200  # 20 minutes
POLL_INTERVAL = 30
start_time = time.time()

print(f"Waiting for endpoint '{ENDPOINT_NAME}' to be ready (timeout: {TIMEOUT_SECONDS}s)...")
while True:
    elapsed = time.time() - start_time
    if elapsed > TIMEOUT_SECONDS:
        raise TimeoutError(
            f"Endpoint '{ENDPOINT_NAME}' did not become ready within {TIMEOUT_SECONDS}s"
        )

    resp = requests.get(
        f"{base_url}/api/2.0/serving-endpoints/{ENDPOINT_NAME}",
        headers=headers,
    )
    status_info = resp.json()
    state = status_info.get("state", {})
    ready = state.get("ready", "NOT_READY")
    config_update = state.get("config_update", "NOT_UPDATING")

    print(f"  [{int(elapsed)}s] ready={ready}  config_update={config_update}")

    if ready == "READY" and config_update != "IN_PROGRESS":
        print(f"\nEndpoint is READY after {int(elapsed)}s.")
        break

    time.sleep(POLL_INTERVAL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Test the Endpoint

# COMMAND ----------

# Build a sample payload using real feature values
sample = X_test.iloc[:3].to_dict(orient="split")
test_payload = {"dataframe_split": sample}

print("Sample payload:")
print(json.dumps(test_payload, indent=2, default=str))

# COMMAND ----------

score_resp = requests.post(
    f"{base_url}/serving-endpoints/{ENDPOINT_NAME}/invocations",
    headers=headers,
    json=test_payload,
)

if score_resp.status_code == 200:
    predictions = score_resp.json()
    print("Predictions from endpoint:")
    print(json.dumps(predictions, indent=2))
else:
    print(f"Scoring failed ({score_resp.status_code}): {score_resp.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

endpoint_url = f"{base_url}/ml/endpoints/{ENDPOINT_NAME}"

print("=" * 70)
print("  FRAUD DETECTION MODEL - DEPLOYMENT COMPLETE")
print("=" * 70)
print(f"  Model:        {MODEL_NAME} (v{model_version})")
print(f"  Test AUC:     {auc:.4f}")
print(f"  Endpoint:     {ENDPOINT_NAME}")
print(f"  Endpoint URL: {endpoint_url}")
print(f"  Status:       READY")
print("=" * 70)
print()
print("To score claims programmatically:")
print(f'  POST {base_url}/serving-endpoints/{ENDPOINT_NAME}/invocations')
print(f'  Headers: {{"Authorization": "Bearer <token>", "Content-Type": "application/json"}}')
print(f'  Body:    {{"dataframe_split": {{"columns": {list(X.columns)}, "data": [[...]]}}}}')
