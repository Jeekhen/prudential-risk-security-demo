# Databricks notebook source
# MAGIC %md
# MAGIC # Cleanup: Remove All Demo Assets
# MAGIC
# MAGIC This notebook removes **everything** created by the Insurance Risk & Security demo:
# MAGIC - 8 Delta tables
# MAGIC - 2 AI/BI Dashboards
# MAGIC - 2 Genie Spaces
# MAGIC - 1 Model Serving endpoint
# MAGIC - 1 Vector Search index + endpoint
# MAGIC - 1 Registered ML model
# MAGIC - 1 Databricks App
# MAGIC
# MAGIC **Set the same CATALOG, SCHEMA, and WORKSPACE_FOLDER you used during setup**, then click Run All.

# COMMAND ----------

dbutils.widgets.text("CATALOG", "users", "Catalog")
dbutils.widgets.text("SCHEMA", "jk_wong", "Schema")
dbutils.widgets.text("WORKSPACE_FOLDER", "", "Workspace Folder (same as setup)")

CATALOG = dbutils.widgets.get("CATALOG")
SCHEMA = dbutils.widgets.get("SCHEMA")
WORKSPACE_FOLDER = dbutils.widgets.get("WORKSPACE_FOLDER")
TABLE_PREFIX = f"{CATALOG}.{SCHEMA}"

print(f"Will clean up all demo assets in: {TABLE_PREFIX}")

# COMMAND ----------

import requests

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
workspace_url = f"https://{ctx.browserHostName().get()}"
token = ctx.apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Drop Tables

# COMMAND ----------

TABLES = [
    "demo_claims_providers",
    "demo_claims_raw",
    "demo_claims_fraud_scores",
    "demo_security_incidents",
    "demo_security_alerts",
    "demo_security_metrics",
    "demo_policy_documents",
    "demo_policy_chunks",
]

for table_name in TABLES:
    fqn = f"{TABLE_PREFIX}.{table_name}"
    try:
        spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        print(f"  Dropped: {fqn}")
    except Exception as e:
        print(f"  Skipped {fqn}: {e}")

print(f"\n{len(TABLES)} tables dropped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Delete AI/BI Dashboards

# COMMAND ----------

DASHBOARD_NAMES = {
    "Claims Fraud Risk Analytics",
    "Security Incident Analytics",
}

page_token = None
deleted_dashboards = 0

while True:
    params = {"page_size": 50}
    if page_token:
        params["page_token"] = page_token

    resp = requests.get(f"{workspace_url}/api/2.0/lakeview/dashboards", headers=headers, params=params)
    data = resp.json()

    for dash in data.get("dashboards", []):
        if dash.get("display_name") in DASHBOARD_NAMES:
            dash_id = dash["dashboard_id"]
            del_resp = requests.delete(
                f"{workspace_url}/api/2.0/lakeview/dashboards/{dash_id}",
                headers=headers,
            )
            if del_resp.status_code in (200, 204):
                print(f"  Deleted dashboard: {dash['display_name']} ({dash_id})")
                deleted_dashboards += 1
            else:
                print(f"  Failed to delete {dash['display_name']}: {del_resp.text}")

    page_token = data.get("next_page_token")
    if not page_token:
        break

if deleted_dashboards == 0:
    print("  No matching dashboards found (already cleaned up?)")
else:
    print(f"\n{deleted_dashboards} dashboard(s) deleted.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Delete Genie Spaces

# COMMAND ----------

GENIE_NAMES = {
    "Claims Fraud Explorer",
    "Security Threat Explorer",
}

resp = requests.get(f"{workspace_url}/api/2.0/data-rooms", headers=headers)
genie_data = resp.json()
deleted_genies = 0

for space in genie_data.get("spaces", genie_data.get("data_rooms", [])):
    if space.get("display_name") in GENIE_NAMES:
        space_id = space.get("id") or space.get("space_id")
        del_resp = requests.delete(
            f"{workspace_url}/api/2.0/data-rooms/{space_id}",
            headers=headers,
        )
        if del_resp.status_code in (200, 204):
            print(f"  Deleted Genie space: {space['display_name']} ({space_id})")
            deleted_genies += 1
        else:
            print(f"  Failed to delete {space['display_name']}: {del_resp.text}")

if deleted_genies == 0:
    print("  No matching Genie spaces found (already cleaned up?)")
else:
    print(f"\n{deleted_genies} Genie space(s) deleted.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Delete Model Serving Endpoint

# COMMAND ----------

endpoint_name = "demo-fraud-scoring"
resp = requests.delete(
    f"{workspace_url}/api/2.0/serving-endpoints/{endpoint_name}",
    headers=headers,
)
if resp.status_code in (200, 204):
    print(f"  Deleted serving endpoint: {endpoint_name}")
elif resp.status_code == 404:
    print(f"  Serving endpoint '{endpoint_name}' not found (already cleaned up?)")
else:
    print(f"  Failed to delete serving endpoint: {resp.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Delete Vector Search Index & Endpoint

# COMMAND ----------

vs_index_name = f"{TABLE_PREFIX}.demo_policy_chunks_index"
vs_endpoint_name = "demo-vs-endpoint"

# Delete the index first
resp = requests.delete(
    f"{workspace_url}/api/2.0/vector-search/indexes/{vs_index_name}",
    headers=headers,
)
if resp.status_code in (200, 204):
    print(f"  Deleted vector search index: {vs_index_name}")
elif resp.status_code == 404:
    print(f"  Vector search index not found (already cleaned up?)")
else:
    print(f"  Failed to delete VS index: {resp.text}")

# Delete the endpoint
resp = requests.delete(
    f"{workspace_url}/api/2.0/vector-search/endpoints/{vs_endpoint_name}",
    headers=headers,
)
if resp.status_code in (200, 204):
    print(f"  Deleted vector search endpoint: {vs_endpoint_name}")
elif resp.status_code == 404:
    print(f"  Vector search endpoint not found (already cleaned up?)")
else:
    print(f"  Failed to delete VS endpoint: {resp.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Delete Registered ML Model

# COMMAND ----------

model_name = f"{TABLE_PREFIX}.demo_fraud_model"
try:
    from mlflow import MlflowClient
    client = MlflowClient(registry_uri="databricks-uc")
    client.delete_registered_model(name=model_name)
    print(f"  Deleted registered model: {model_name}")
except Exception as e:
    print(f"  Could not delete model '{model_name}': {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Delete Databricks App

# COMMAND ----------

app_name = "risk-security-demo"
resp = requests.delete(
    f"{workspace_url}/api/2.0/apps/{app_name}",
    headers=headers,
)
if resp.status_code in (200, 204):
    print(f"  Deleted app: {app_name}")
elif resp.status_code == 404:
    print(f"  App '{app_name}' not found (already cleaned up?)")
else:
    print(f"  Failed to delete app: {resp.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("  CLEANUP COMPLETE")
print("=" * 70)
print()
print(f"  Tables dropped:              {len(TABLES)}")
print(f"  Dashboards deleted:          {deleted_dashboards}")
print(f"  Genie spaces deleted:        {deleted_genies}")
print(f"  Model Serving endpoint:      deleted")
print(f"  Vector Search index/endpoint: deleted")
print(f"  Registered ML model:         deleted")
print(f"  Databricks App:              deleted")
print()
print("  All demo assets have been removed.")
print("=" * 70)
