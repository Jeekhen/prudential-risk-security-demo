# Databricks notebook source
# MAGIC %md
# MAGIC # Insurance Risk & Security AI Demo Setup
# MAGIC
# MAGIC **One-click setup** for the Insurance Risk & Security demo.
# MAGIC
# MAGIC This notebook creates:
# MAGIC - **8 Delta tables** (claims fraud + security incidents + policy documents)
# MAGIC - **2 AI/BI Dashboards** (Claims Fraud Risk Analytics, Security Incident Analytics)
# MAGIC - **2 Genie Spaces** (Claims Fraud Explorer, Security Threat Explorer)
# MAGIC - **1 ML Model** trained and deployed to a Model Serving endpoint
# MAGIC - **1 Vector Search index** over policy documents for RAG
# MAGIC - **1 Databricks App** with Fraud Scoring + Compliance Copilot
# MAGIC
# MAGIC ## Instructions
# MAGIC 1. Set the **CATALOG**, **SCHEMA**, and **WORKSPACE_FOLDER** widgets above
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
# MAGIC ## Step 4: Train & Deploy Fraud Scoring Model

# COMMAND ----------

# MAGIC %run ./05_Train_Deploy_Model

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Setup RAG Pipeline (Policy Documents + Vector Search)

# COMMAND ----------

# MAGIC %run ./06_Setup_RAG_Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Deploy the Demo App

# COMMAND ----------

# MAGIC %run ./07_Deploy_App

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete!
# MAGIC
# MAGIC All assets have been created. Check the output above for direct links to:
# MAGIC - AI/BI Dashboards
# MAGIC - Genie Spaces
# MAGIC - Model Serving endpoint
# MAGIC - Demo App (Fraud Scoring + Compliance Copilot)
