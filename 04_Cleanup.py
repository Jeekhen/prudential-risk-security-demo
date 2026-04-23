# Databricks notebook source
# MAGIC %md
# MAGIC # Cleanup: Remove All Demo Assets
# MAGIC
# MAGIC This notebook removes **everything** created by the Prudential HK Risk & Security demo:
# MAGIC - 6 Delta tables
# MAGIC - 2 AI/BI Dashboards
# MAGIC - 2 Genie Spaces
# MAGIC
# MAGIC **Set the same CATALOG and SCHEMA you used during setup**, then click Run All.

# COMMAND ----------

dbutils.widgets.text("CATALOG", "users", "Catalog")
dbutils.widgets.text("SCHEMA", "jk_wong", "Schema")
dbutils.widgets.text("WORKSPACE_FOLDER", "", "Workspace Folder (same as setup)")

CATALOG = dbutils.widgets.get("CATALOG")
SCHEMA = dbutils.widgets.get("SCHEMA")
WORKSPACE_FOLDER = dbutils.widgets.get("WORKSPACE_FOLDER")
TABLE_PREFIX = f"{CATALOG}.{SCHEMA}"

print(f"Will clean up all prudential_* assets in: {TABLE_PREFIX}")
if WORKSPACE_FOLDER:
    print(f"Will search for dashboards/Genie spaces in: {WORKSPACE_FOLDER}")

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
    "prudential_claims_providers",
    "prudential_claims_raw",
    "prudential_claims_fraud_scores",
    "prudential_security_incidents",
    "prudential_security_alerts",
    "prudential_security_metrics",
]

for table_name in TABLES:
    fqn = f"{TABLE_PREFIX}.{table_name}"
    try:
        spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        print(f"  Dropped: {fqn}")
    except Exception as e:
        print(f"  Skipped {fqn}: {e}")

print("\nAll tables dropped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Delete AI/BI Dashboards

# COMMAND ----------

DASHBOARD_NAMES = {
    "Prudential HK - Claims Fraud Risk Analytics",
    "Prudential HK - Security Incident Analytics",
}

# List all dashboards and find ours by name
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
    "Prudential HK - Claims Fraud Explorer",
    "Prudential HK - Security Threat Explorer",
}

# List Genie spaces and find ours by name
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
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("  CLEANUP COMPLETE")
print("=" * 70)
print()
print(f"  Tables dropped:       {len(TABLES)}")
print(f"  Dashboards deleted:   {deleted_dashboards}")
print(f"  Genie spaces deleted: {deleted_genies}")
print()
print("  All Prudential HK demo assets have been removed.")
print("=" * 70)
