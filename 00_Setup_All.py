# Databricks notebook source
# MAGIC %md
# MAGIC # Insurance Risk & Security AI Demo Setup
# MAGIC
# MAGIC **One-click setup** for the Insurance Risk & Security demo.
# MAGIC
# MAGIC This notebook creates:
# MAGIC - **6 Delta tables** (claims fraud + security incidents data)
# MAGIC - **2 AI/BI Dashboards** (Claims Fraud Risk Analytics, Security Incident Analytics)
# MAGIC - **2 Genie Spaces** (Claims Fraud Explorer, Security Threat Explorer)
# MAGIC
# MAGIC ## Instructions
# MAGIC 1. Set the **CATALOG** and **SCHEMA** widgets above to your target location
# MAGIC 2. Click **Run All**
# MAGIC 3. Links to all created assets will be printed at the end
# MAGIC
# MAGIC **Requirements:** Databricks Runtime 14.3+ with access to Unity Catalog.

# COMMAND ----------

dbutils.widgets.text("CATALOG", "users", "Catalog")
dbutils.widgets.text("SCHEMA", "jk_wong", "Schema")
dbutils.widgets.text("WORKSPACE_FOLDER", "", "Workspace Folder (e.g. /Users/me@company.com/Demos)")

CATALOG = dbutils.widgets.get("CATALOG")
SCHEMA = dbutils.widgets.get("SCHEMA")
WORKSPACE_FOLDER = dbutils.widgets.get("WORKSPACE_FOLDER")

print(f"Target: {CATALOG}.{SCHEMA}")
print(f"All tables will be created as {CATALOG}.{SCHEMA}.demo_*")
if WORKSPACE_FOLDER:
    print(f"Dashboards & Genie spaces will be created in: {WORKSPACE_FOLDER}")
else:
    print(f"Dashboards & Genie spaces will be created in your home folder (default)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Generate Claims Fraud Data (3 tables)

# COMMAND ----------

# MAGIC %run ./01_Generate_Claims_Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate Security Incidents Data (3 tables)

# COMMAND ----------

# MAGIC %run ./02_Generate_Security_Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Dashboards & Genie Spaces

# COMMAND ----------

# MAGIC %run ./03_Create_Dashboards_Genie

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete!
# MAGIC
# MAGIC All assets have been created. Check the output above for direct links to your dashboards and Genie spaces.
