# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy the Risk & Security Demo App
# MAGIC Deploys a Databricks App with two features:
# MAGIC - **Fraud Scoring** — submit claim details, get real-time ML risk score
# MAGIC - **Compliance Copilot** — chat with risk policies using RAG
# MAGIC
# MAGIC **Prerequisites:** Run `05_Train_Deploy_Model` and `06_Setup_RAG_Pipeline` first.

# COMMAND ----------

import json
import time
import requests

CATALOG = dbutils.widgets.get("CATALOG")
SCHEMA = dbutils.widgets.get("SCHEMA")
WORKSPACE_FOLDER = dbutils.widgets.get("WORKSPACE_FOLDER")

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
workspace_url = f"https://{ctx.browserHostName().get()}"
token = ctx.apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

APP_NAME = "risk-security-demo"
print(f"Deploying app: {APP_NAME}")
print(f"Workspace: {workspace_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Upload App Files to Workspace

# COMMAND ----------

# The app source code is in the ./app/ subdirectory relative to this notebook.
# We need to determine the workspace path for the app source.

# Get current notebook path
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
base_path = "/".join(notebook_path.rsplit("/", 1)[:-1])
app_source_path = f"{base_path}/app"

print(f"App source path: {app_source_path}")

# Verify app files exist
try:
    files = dbutils.fs.ls(f"file:/Workspace{app_source_path}")
    print(f"Found {len(files)} items in app directory")
except Exception:
    print(f"WARNING: Could not list {app_source_path}")
    print("Make sure the 'app' folder was imported alongside the notebooks.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Update app.yaml with Correct Catalog/Schema

# COMMAND ----------

# Read the app.yaml template and update CATALOG/SCHEMA values
app_yaml_path = f"/Workspace{app_source_path}/app.yaml"

with open(app_yaml_path, "r") as f:
    app_yaml_content = f.read()

# Replace default values with the user's actual catalog/schema
import re
app_yaml_content = re.sub(
    r'(name: CATALOG\s+value: )\S+',
    f'\\g<1>{CATALOG}',
    app_yaml_content
)
app_yaml_content = re.sub(
    r'(name: SCHEMA\s+value: )\S+',
    f'\\g<1>{SCHEMA}',
    app_yaml_content
)

with open(app_yaml_path, "w") as f:
    f.write(app_yaml_content)

print("Updated app.yaml:")
print(app_yaml_content)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Create or Update the Databricks App

# COMMAND ----------

# Check if app already exists
resp = requests.get(f"{workspace_url}/api/2.0/apps/{APP_NAME}", headers=headers)

if resp.status_code == 200:
    print(f"App '{APP_NAME}' already exists, will redeploy.")
    app_info = resp.json()
else:
    print(f"Creating app '{APP_NAME}'...")
    create_payload = {
        "name": APP_NAME,
        "description": "Insurance Risk & Security Demo - Fraud Scoring + Compliance Copilot",
    }
    resp = requests.post(
        f"{workspace_url}/api/2.0/apps",
        headers=headers,
        json=create_payload,
    )
    if resp.status_code in (200, 201):
        app_info = resp.json()
        print(f"App created: {app_info.get('name')}")
    else:
        print(f"Error creating app: {resp.status_code} {resp.text}")
        dbutils.notebook.exit(f"Failed to create app: {resp.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Deploy the App

# COMMAND ----------

deploy_payload = {
    "source_code_path": app_source_path,
}

resp = requests.post(
    f"{workspace_url}/api/2.0/apps/{APP_NAME}/deployments",
    headers=headers,
    json=deploy_payload,
)

if resp.status_code in (200, 201):
    deployment = resp.json()
    deployment_id = deployment.get("deployment_id", "")
    print(f"Deployment started: {deployment_id}")
else:
    print(f"Error deploying: {resp.status_code} {resp.text}")
    dbutils.notebook.exit(f"Failed to deploy: {resp.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Wait for Deployment

# COMMAND ----------

timeout_minutes = 15
poll_interval = 30
elapsed = 0

while elapsed < timeout_minutes * 60:
    resp = requests.get(f"{workspace_url}/api/2.0/apps/{APP_NAME}", headers=headers)
    app_data = resp.json()

    status = app_data.get("status", {}).get("state", "UNKNOWN")
    print(f"  [{elapsed}s] App status: {status}")

    if status == "RUNNING":
        app_url = app_data.get("url", f"{workspace_url}/apps/{APP_NAME}")
        print(f"\nApp is RUNNING!")
        print(f"  URL: {app_url}")
        break
    elif status in ("FAILED", "ERROR"):
        msg = app_data.get("status", {}).get("message", "Unknown error")
        print(f"\nApp deployment FAILED: {msg}")
        break

    time.sleep(poll_interval)
    elapsed += poll_interval
else:
    print(f"\nTimed out after {timeout_minutes} minutes. Check the app status manually.")
    app_url = f"{workspace_url}/apps/{APP_NAME}"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary

# COMMAND ----------

print("=" * 70)
print("  APP DEPLOYMENT COMPLETE")
print("=" * 70)
print()
print(f"  App Name:  {APP_NAME}")
print(f"  App URL:   {app_url}")
print()
print("  Features:")
print("    Tab 1: Fraud Scoring  - Submit claims for real-time ML scoring")
print("    Tab 2: Compliance Copilot - Ask questions about risk policies")
print()
print("=" * 70)
