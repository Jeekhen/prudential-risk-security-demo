# Databricks notebook source
# MAGIC %md
# MAGIC # Create Dashboards & Genie Spaces
# MAGIC Creates 2 AI/BI Dashboards and 2 Genie Spaces via Databricks REST API.

# COMMAND ----------

import json
import requests

CATALOG = dbutils.widgets.get("CATALOG")
SCHEMA = dbutils.widgets.get("SCHEMA")
TABLE_PREFIX = f"{CATALOG}.{SCHEMA}"

# Get workspace URL and token from notebook context
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
workspace_url = f"https://{ctx.browserHostName().get()}"
token = ctx.apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

print(f"Workspace: {workspace_url}")
print(f"Table prefix: {TABLE_PREFIX}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find a SQL Warehouse

# COMMAND ----------

# Find first available SQL warehouse
wh_resp = requests.get(f"{workspace_url}/api/2.0/sql/warehouses", headers=headers)
warehouses = wh_resp.json().get("warehouses", [])
# Prefer a running serverless warehouse
warehouse_id = None
for wh in warehouses:
    if wh.get("state") == "RUNNING":
        warehouse_id = wh["id"]
        print(f"Using warehouse: {wh['name']} ({wh['id']})")
        break
if not warehouse_id and warehouses:
    warehouse_id = warehouses[0]["id"]
    print(f"Using warehouse (may need to start): {warehouses[0]['name']} ({warehouses[0]['id']})")
if not warehouse_id:
    raise RuntimeError("No SQL warehouse found. Please create one first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper: Replace table references in dashboard JSON

# COMMAND ----------

def replace_table_refs(dashboard_json_str, target_prefix):
    """Replace users.jk_wong with the target catalog.schema in all SQL queries."""
    return dashboard_json_str.replace("users.jk_wong", target_prefix)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard 1: Claims Fraud Risk Analytics

# COMMAND ----------

CLAIMS_DASHBOARD_JSON = '{"datasets": [{"name": "ds_kpi", "displayName": "KPI Summary", "queryLines": ["SELECT   COUNT(*) as total_claims,   SUM(CASE WHEN c.is_flagged THEN 1 ELSE 0 END) as total_flagged,   ROUND(SUM(CASE WHEN c.is_flagged THEN 1 ELSE 0 END) / COUNT(*), 4) as fraud_rate,   ROUND(AVG(c.claim_amount_hkd), 0) as avg_claim_amount FROM users.jk_wong.prudential_claims_raw c "]}, {"name": "ds_overview", "displayName": "Claims Overview", "queryLines": ["SELECT   c.claim_id,   c.claim_type,   c.claim_date,   c.claim_amount_hkd,   c.status,   c.is_flagged,   f.fraud_score,   f.risk_category,   CASE \n", "    WHEN p.region IN ('Central and Western', 'Eastern', 'Southern', 'Wan Chai', 'Islands') THEN 'HK Island'\n", "    WHEN p.region IN ('Kowloon City', 'Kwun Tong', 'Sham Shui Po', 'Wong Tai Sin', 'Yau Tsim Mong') THEN 'Kowloon'\n", "    WHEN p.region IN ('Kwai Tsing', 'North', 'Sai Kung', 'Sha Tin', 'Tai Po', 'Tsuen Wan', 'Tuen Mun', 'Yuen Long') THEN 'New Territories'\n", "    WHEN p.region IN ('Cotai', 'Macau Peninsula', 'Taipa') THEN 'Macau'\n", "    ELSE p.region\n", "  END as area FROM users.jk_wong.prudential_claims_raw c JOIN users.jk_wong.prudential_claims_fraud_scores f ON c.claim_id = f.claim_id JOIN users.jk_wong.prudential_claims_providers p ON c.provider_id = p.provider_id "]}, {"name": "ds_monthly_trend", "displayName": "Monthly Claims Trend", "queryLines": ["SELECT   DATE_TRUNC('MONTH', c.claim_date) as claim_month,   COUNT(*) as total_claims,   SUM(CASE WHEN c.is_flagged THEN 1 ELSE 0 END) as flagged_claims FROM users.jk_wong.prudential_claims_raw c GROUP BY DATE_TRUNC('MONTH', c.claim_date) ORDER BY claim_month "]}, {"name": "ds_fraud_by_area", "displayName": "Fraud Rate by Area", "queryLines": ["SELECT   CASE \n", "    WHEN p.region IN ('Central and Western', 'Eastern', 'Southern', 'Wan Chai', 'Islands') THEN 'HK Island'\n", "    WHEN p.region IN ('Kowloon City', 'Kwun Tong', 'Sham Shui Po', 'Wong Tai Sin', 'Yau Tsim Mong') THEN 'Kowloon'\n", "    WHEN p.region IN ('Kwai Tsing', 'North', 'Sai Kung', 'Sha Tin', 'Tai Po', 'Tsuen Wan', 'Tuen Mun', 'Yuen Long') THEN 'New Territories'\n", "    WHEN p.region IN ('Cotai', 'Macau Peninsula', 'Taipa') THEN 'Macau'\n", "    ELSE p.region\n", "  END as area,   COUNT(*) as total_claims,   SUM(CASE WHEN c.is_flagged THEN 1 ELSE 0 END) as flagged_claims,   ROUND(SUM(CASE WHEN c.is_flagged THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as fraud_rate_pct FROM users.jk_wong.prudential_claims_raw c JOIN users.jk_wong.prudential_claims_providers p ON c.provider_id = p.provider_id GROUP BY area ORDER BY fraud_rate_pct DESC "]}, {"name": "ds_top_providers", "displayName": "Top Suspicious Providers", "queryLines": ["SELECT   provider_name,   provider_type,   region,   total_claims_handled,   ROUND(flagged_claims_pct, 1) as flagged_pct,   risk_tier FROM users.jk_wong.prudential_claims_providers ORDER BY flagged_claims_pct DESC LIMIT 10 "]}, {"name": "ds_fraud_type_area", "displayName": "Fraud by Type and Area", "queryLines": ["SELECT   c.claim_type,   CASE \n", "    WHEN p.region IN ('Central and Western', 'Eastern', 'Southern', 'Wan Chai', 'Islands') THEN 'HK Island'\n", "    WHEN p.region IN ('Kowloon City', 'Kwun Tong', 'Sham Shui Po', 'Wong Tai Sin', 'Yau Tsim Mong') THEN 'Kowloon'\n", "    WHEN p.region IN ('Kwai Tsing', 'North', 'Sai Kung', 'Sha Tin', 'Tai Po', 'Tsuen Wan', 'Tuen Mun', 'Yuen Long') THEN 'New Territories'\n", "    WHEN p.region IN ('Cotai', 'Macau Peninsula', 'Taipa') THEN 'Macau'\n", "    ELSE p.region\n", "  END as area,   SUM(CASE WHEN c.is_flagged THEN 1 ELSE 0 END) as flagged_count FROM users.jk_wong.prudential_claims_raw c JOIN users.jk_wong.prudential_claims_providers p ON c.provider_id = p.provider_id GROUP BY c.claim_type, area ORDER BY flagged_count DESC "]}, {"name": "ds_fraud_indicators", "displayName": "Fraud Indicators", "queryLines": ["SELECT   TRIM(indicator) as fraud_indicator,   COUNT(*) as frequency FROM (   SELECT EXPLODE(SPLIT(fraud_indicators, ',')) as indicator   FROM users.jk_wong.prudential_claims_raw   WHERE fraud_indicators IS NOT NULL AND fraud_indicators != '' ) GROUP BY TRIM(indicator) ORDER BY frequency DESC "]}, {"name": "ds_top_flagged", "displayName": "Top Flagged Claims", "queryLines": ["SELECT   c.claim_id,   c.claim_type,   c.claim_date,   ROUND(c.claim_amount_hkd, 0) as claim_amount_hkd,   c.status,   f.fraud_score,   f.risk_category,   p.provider_name,   CASE \n", "    WHEN p.region IN ('Central and Western', 'Eastern', 'Southern', 'Wan Chai', 'Islands') THEN 'HK Island'\n", "    WHEN p.region IN ('Kowloon City', 'Kwun Tong', 'Sham Shui Po', 'Wong Tai Sin', 'Yau Tsim Mong') THEN 'Kowloon'\n", "    WHEN p.region IN ('Kwai Tsing', 'North', 'Sai Kung', 'Sha Tin', 'Tai Po', 'Tsuen Wan', 'Tuen Mun', 'Yuen Long') THEN 'New Territories'\n", "    WHEN p.region IN ('Cotai', 'Macau Peninsula', 'Taipa') THEN 'Macau'\n", "    ELSE p.region\n", "  END as area FROM users.jk_wong.prudential_claims_raw c JOIN users.jk_wong.prudential_claims_fraud_scores f ON c.claim_id = f.claim_id JOIN users.jk_wong.prudential_claims_providers p ON c.provider_id = p.provider_id WHERE c.is_flagged = true ORDER BY f.fraud_score DESC LIMIT 20 "]}], "pages": [{"name": "claims_overview", "displayName": "Claims Overview & Fraud KPIs", "layout": [{"widget": {"name": "page1-title", "multilineTextboxSpec": {"lines": ["## Prudential HK - Claims Fraud Risk Analytics"]}}, "position": {"x": 0, "y": 0, "width": 6, "height": 1}}, {"widget": {"name": "page1-subtitle", "multilineTextboxSpec": {"lines": ["Claims overview, fraud detection KPIs, and trend analysis across Hong Kong and Macau operations"]}}, "position": {"x": 0, "y": 1, "width": 6, "height": 1}}, {"widget": {"name": "kpi-total-claims", "queries": [{"name": "main_query", "query": {"datasetName": "ds_kpi", "fields": [{"name": "total_claims", "expression": "`total_claims`"}], "disaggregated": true}}], "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": {"fieldName": "total_claims", "displayName": "Total Claims"}}, "frame": {"showTitle": true, "title": "Total Claims"}}}, "position": {"x": 0, "y": 2, "width": 2, "height": 3}}, {"widget": {"name": "kpi-total-flagged", "queries": [{"name": "main_query", "query": {"datasetName": "ds_kpi", "fields": [{"name": "total_flagged", "expression": "`total_flagged`"}], "disaggregated": true}}], "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": {"fieldName": "total_flagged", "displayName": "Flagged Claims"}}, "frame": {"showTitle": true, "title": "Flagged Claims"}}}, "position": {"x": 2, "y": 2, "width": 2, "height": 3}}, {"widget": {"name": "kpi-fraud-rate", "queries": [{"name": "main_query", "query": {"datasetName": "ds_kpi", "fields": [{"name": "fraud_rate", "expression": "`fraud_rate`"}], "disaggregated": true}}], "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": {"fieldName": "fraud_rate", "displayName": "Fraud Rate"}}, "frame": {"showTitle": true, "title": "Fraud Rate"}}}, "position": {"x": 4, "y": 2, "width": 2, "height": 3}}, {"widget": {"name": "section-distribution", "multilineTextboxSpec": {"lines": ["### Claims Distribution"]}}, "position": {"x": 0, "y": 5, "width": 6, "height": 1}}, {"widget": {"name": "claims-by-area", "queries": [{"name": "main_query", "query": {"datasetName": "ds_overview", "fields": [{"name": "area", "expression": "`area`"}, {"name": "count(claim_id)", "expression": "COUNT(`claim_id`)"}], "disaggregated": false}}], "spec": {"version": 3, "widgetType": "bar", "encodings": {"x": {"fieldName": "area", "scale": {"type": "categorical"}, "displayName": "Region"}, "y": {"fieldName": "count(claim_id)", "scale": {"type": "quantitative"}, "displayName": "Number of Claims"}}, "frame": {"showTitle": true, "title": "Claims by Region"}}}, "position": {"x": 0, "y": 6, "width": 3, "height": 6}}, {"widget": {"name": "claims-by-type", "queries": [{"name": "main_query", "query": {"datasetName": "ds_overview", "fields": [{"name": "claim_type", "expression": "`claim_type`"}, {"name": "count(claim_id)", "expression": "COUNT(`claim_id`)"}], "disaggregated": false}}], "spec": {"version": 3, "widgetType": "bar", "encodings": {"x": {"fieldName": "claim_type", "scale": {"type": "categorical"}, "displayName": "Claim Type"}, "y": {"fieldName": "count(claim_id)", "scale": {"type": "quantitative"}, "displayName": "Number of Claims"}}, "frame": {"showTitle": true, "title": "Claims by Type"}}}, "position": {"x": 3, "y": 6, "width": 3, "height": 6}}, {"widget": {"name": "section-trend", "multilineTextboxSpec": {"lines": ["### Monthly Claims Trend"]}}, "position": {"x": 0, "y": 12, "width": 6, "height": 1}}, {"widget": {"name": "monthly-trend", "queries": [{"name": "main_query", "query": {"datasetName": "ds_monthly_trend", "fields": [{"name": "claim_month", "expression": "`claim_month`"}, {"name": "total_claims", "expression": "`total_claims`"}, {"name": "flagged_claims", "expression": "`flagged_claims`"}], "disaggregated": true}}], "spec": {"version": 3, "widgetType": "line", "encodings": {"x": {"fieldName": "claim_month", "scale": {"type": "temporal"}, "displayName": "Month"}, "y": {"scale": {"type": "quantitative"}, "fields": [{"fieldName": "total_claims", "displayName": "Total Claims"}, {"fieldName": "flagged_claims", "displayName": "Flagged Claims"}]}}, "frame": {"showTitle": true, "title": "Monthly Claims Volume with Fraud Flagged Overlay"}}}, "position": {"x": 0, "y": 13, "width": 6, "height": 6}}, {"widget": {"name": "section-status", "multilineTextboxSpec": {"lines": ["### Claim Status & Average Amount"]}}, "position": {"x": 0, "y": 19, "width": 6, "height": 1}}, {"widget": {"name": "claim-status-pie", "queries": [{"name": "main_query", "query": {"datasetName": "ds_overview", "fields": [{"name": "status", "expression": "`status`"}, {"name": "count(claim_id)", "expression": "COUNT(`claim_id`)"}], "disaggregated": false}}], "spec": {"version": 3, "widgetType": "pie", "encodings": {"angle": {"fieldName": "count(claim_id)", "scale": {"type": "quantitative"}, "displayName": "Count"}, "color": {"fieldName": "status", "scale": {"type": "categorical"}, "displayName": "Claim Status"}}, "frame": {"showTitle": true, "title": "Claim Status Distribution"}}}, "position": {"x": 0, "y": 20, "width": 3, "height": 6}}, {"widget": {"name": "kpi-avg-amount", "queries": [{"name": "main_query", "query": {"datasetName": "ds_kpi", "fields": [{"name": "avg_claim_amount", "expression": "`avg_claim_amount`"}], "disaggregated": true}}], "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": {"fieldName": "avg_claim_amount", "displayName": "Avg Claim Amount (HKD)"}}, "frame": {"showTitle": true, "title": "Average Claim Amount (HKD)"}}}, "position": {"x": 3, "y": 20, "width": 3, "height": 3}}, {"widget": {"name": "risk-category-pie", "queries": [{"name": "main_query", "query": {"datasetName": "ds_overview", "fields": [{"name": "risk_category", "expression": "`risk_category`"}, {"name": "count(claim_id)", "expression": "COUNT(`claim_id`)"}], "disaggregated": false}}], "spec": {"version": 3, "widgetType": "pie", "encodings": {"angle": {"fieldName": "count(claim_id)", "scale": {"type": "quantitative"}, "displayName": "Count"}, "color": {"fieldName": "risk_category", "scale": {"type": "categorical"}, "displayName": "Risk Category"}}, "frame": {"showTitle": true, "title": "Risk Category Distribution"}}}, "position": {"x": 3, "y": 23, "width": 3, "height": 3}}], "pageType": "PAGE_TYPE_CANVAS"}, {"name": "fraud_deep_dive", "displayName": "Fraud Deep Dive", "layout": [{"widget": {"name": "page2-title", "multilineTextboxSpec": {"lines": ["## Fraud Deep Dive"]}}, "position": {"x": 0, "y": 0, "width": 6, "height": 1}}, {"widget": {"name": "page2-subtitle", "multilineTextboxSpec": {"lines": ["Detailed fraud analysis by region, provider, claim type, and fraud indicators"]}}, "position": {"x": 0, "y": 1, "width": 6, "height": 1}}, {"widget": {"name": "fraud-rate-by-area", "queries": [{"name": "main_query", "query": {"datasetName": "ds_fraud_by_area", "fields": [{"name": "area", "expression": "`area`"}, {"name": "fraud_rate_pct", "expression": "`fraud_rate_pct`"}], "disaggregated": true}}], "spec": {"version": 3, "widgetType": "bar", "encodings": {"x": {"fieldName": "area", "scale": {"type": "categorical"}, "displayName": "Region"}, "y": {"fieldName": "fraud_rate_pct", "scale": {"type": "quantitative"}, "displayName": "Fraud Rate (%)"}}, "frame": {"showTitle": true, "title": "Fraud Rate by Region (%)"}}}, "position": {"x": 0, "y": 2, "width": 3, "height": 6}}, {"widget": {"name": "fraud-type-area", "queries": [{"name": "main_query", "query": {"datasetName": "ds_fraud_type_area", "fields": [{"name": "claim_type", "expression": "`claim_type`"}, {"name": "area", "expression": "`area`"}, {"name": "flagged_count", "expression": "`flagged_count`"}], "disaggregated": true}}], "spec": {"version": 3, "widgetType": "bar", "encodings": {"x": {"fieldName": "claim_type", "scale": {"type": "categorical"}, "displayName": "Claim Type"}, "y": {"fieldName": "flagged_count", "scale": {"type": "quantitative"}, "displayName": "Flagged Claims"}, "color": {"fieldName": "area", "scale": {"type": "categorical"}, "displayName": "Region"}}, "frame": {"showTitle": true, "title": "Flagged Claims by Type & Region"}}}, "position": {"x": 3, "y": 2, "width": 3, "height": 6}}, {"widget": {"name": "section-providers", "multilineTextboxSpec": {"lines": ["### Suspicious Providers & Fraud Indicators"]}}, "position": {"x": 0, "y": 8, "width": 6, "height": 1}}, {"widget": {"name": "top-providers", "queries": [{"name": "main_query", "query": {"datasetName": "ds_top_providers", "fields": [{"name": "provider_name", "expression": "`provider_name`"}, {"name": "flagged_pct", "expression": "`flagged_pct`"}], "disaggregated": true}}], "spec": {"version": 3, "widgetType": "bar", "encodings": {"x": {"fieldName": "flagged_pct", "scale": {"type": "quantitative"}, "displayName": "Flagged Claims (%)"}, "y": {"fieldName": "provider_name", "scale": {"type": "categorical"}, "displayName": "Provider"}}, "frame": {"showTitle": true, "title": "Top 10 Suspicious Providers"}}}, "position": {"x": 0, "y": 9, "width": 3, "height": 6}}, {"widget": {"name": "fraud-indicators", "queries": [{"name": "main_query", "query": {"datasetName": "ds_fraud_indicators", "fields": [{"name": "fraud_indicator", "expression": "`fraud_indicator`"}, {"name": "frequency", "expression": "`frequency`"}], "disaggregated": true}}], "spec": {"version": 3, "widgetType": "bar", "encodings": {"x": {"fieldName": "frequency", "scale": {"type": "quantitative"}, "displayName": "Frequency"}, "y": {"fieldName": "fraud_indicator", "scale": {"type": "categorical"}, "displayName": "Fraud Indicator"}}, "frame": {"showTitle": true, "title": "Fraud Indicator Frequency"}}}, "position": {"x": 3, "y": 9, "width": 3, "height": 6}}, {"widget": {"name": "section-flagged-claims", "multilineTextboxSpec": {"lines": ["### Top Flagged Claims (Highest Fraud Risk Score)"]}}, "position": {"x": 0, "y": 15, "width": 6, "height": 1}}, {"widget": {"name": "flagged-claims-table", "queries": [{"name": "main_query", "query": {"datasetName": "ds_top_flagged", "fields": [{"name": "claim_id", "expression": "`claim_id`"}, {"name": "claim_type", "expression": "`claim_type`"}, {"name": "claim_date", "expression": "`claim_date`"}, {"name": "claim_amount_hkd", "expression": "`claim_amount_hkd`"}, {"name": "status", "expression": "`status`"}, {"name": "fraud_score", "expression": "`fraud_score`"}, {"name": "risk_category", "expression": "`risk_category`"}, {"name": "provider_name", "expression": "`provider_name`"}, {"name": "area", "expression": "`area`"}], "disaggregated": true}}], "spec": {"version": 2, "widgetType": "table", "encodings": {"columns": [{"fieldName": "claim_id", "displayName": "Claim ID"}, {"fieldName": "claim_type", "displayName": "Type"}, {"fieldName": "claim_date", "displayName": "Date"}, {"fieldName": "claim_amount_hkd", "displayName": "Amount (HKD)"}, {"fieldName": "status", "displayName": "Status"}, {"fieldName": "fraud_score", "displayName": "Fraud Score"}, {"fieldName": "risk_category", "displayName": "Risk"}, {"fieldName": "provider_name", "displayName": "Provider"}, {"fieldName": "area", "displayName": "Region"}]}, "frame": {"showTitle": true, "title": "Top 20 Flagged Claims by Fraud Risk Score"}}}, "position": {"x": 0, "y": 16, "width": 6, "height": 7}}], "pageType": "PAGE_TYPE_CANVAS"}], "uiSettings": {"theme": {"canvasBackgroundColor": {"light": "#FAFAFB", "dark": "#1F272D"}, "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#11171C"}, "widgetBorderColor": {"light": "#FFFFFF", "dark": "#11171C"}, "fontColor": {"light": "#000000", "dark": "#F30000"}, "selectionColor": {"light": "#2272B4", "dark": "#8ACAFF"}, "visualizationColors": ["#077A9D", "#FFAB00", "#00A972", "#FF3621", "#8BCAE7", "#AB4057", "#99DDB4", "#FCA4A1", "#919191", "#BF7080", "#077A9D"], "widgetHeaderAlignment": "CENTER"}}}
'

serialized = replace_table_refs(CLAIMS_DASHBOARD_JSON, TABLE_PREFIX)

# Get current user for path
user_resp = requests.get(f"{workspace_url}/api/2.0/preview/scim/v2/Me", headers=headers)
user_name = user_resp.json().get("userName", "")
dashboard_path = f"/Users/{user_name}"

payload = {
    "display_name": "Prudential HK - Claims Fraud Risk Analytics",
    "serialized_dashboard": serialized,
    "parent_path": dashboard_path,
    "warehouse_id": warehouse_id,
}

resp = requests.post(f"{workspace_url}/api/2.0/lakeview/dashboards", headers=headers, json=payload)
claims_dashboard = resp.json()
claims_dashboard_id = claims_dashboard.get("dashboard_id", "")

if claims_dashboard_id:
    # Publish with embedded credentials
    pub_resp = requests.post(
        f"{workspace_url}/api/2.0/lakeview/dashboards/{claims_dashboard_id}/published",
        headers=headers,
        json={"embed_credentials": True, "warehouse_id": warehouse_id}
    )
    claims_dash_url = f"{workspace_url}/dashboardsv3/{claims_dashboard_id}/published"
    print(f"Claims Fraud Dashboard created and published!")
    print(f"  URL: {claims_dash_url}")
else:
    print(f"ERROR creating claims dashboard: {claims_dashboard}")
    claims_dash_url = "ERROR"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard 2: Security Incident Analytics

# COMMAND ----------

SECURITY_DASHBOARD_JSON = '{"datasets": [{"name": "kpi_summary", "displayName": "KPI Summary", "queryLines": ["SELECT   COUNT(*) as total_incidents,   SUM(CASE WHEN status IN ('open', 'investigating') THEN 1 ELSE 0 END) as open_investigating,   SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) as critical_incidents,   ROUND(AVG(response_time_hours), 1) as avg_mttr_hours FROM users.jk_wong.prudential_security_incidents"]}, {"name": "monthly_trend", "displayName": "Monthly Trend", "queryLines": ["SELECT month, total_incidents, critical_incidents, high_incidents FROM users.jk_wong.prudential_security_metrics ORDER BY month"]}, {"name": "incidents_by_type", "displayName": "Incidents by Type", "queryLines": ["SELECT incident_type, COUNT(*) as incident_count FROM users.jk_wong.prudential_security_incidents GROUP BY incident_type ORDER BY incident_count DESC"]}, {"name": "incidents_by_severity", "displayName": "Incidents by Severity", "queryLines": ["SELECT severity, COUNT(*) as incident_count FROM users.jk_wong.prudential_security_incidents GROUP BY severity ORDER BY CASE severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 WHEN 'low' THEN 4 END"]}, {"name": "detection_methods", "displayName": "Detection Methods", "queryLines": ["SELECT detection_method, COUNT(*) as incident_count FROM users.jk_wong.prudential_security_incidents GROUP BY detection_method ORDER BY incident_count DESC"]}, {"name": "target_systems", "displayName": "Top Targeted Systems", "queryLines": ["SELECT target_system, COUNT(*) as incident_count FROM users.jk_wong.prudential_security_incidents GROUP BY target_system ORDER BY incident_count DESC LIMIT 10"]}, {"name": "source_countries", "displayName": "Attacks by Source Country", "queryLines": ["SELECT source_country, COUNT(*) as attack_count FROM users.jk_wong.prudential_security_incidents GROUP BY source_country ORDER BY attack_count DESC LIMIT 10"]}, {"name": "mttr_trend", "displayName": "MTTR Trend", "queryLines": ["SELECT month, avg_response_time_hours, avg_resolution_time_hours FROM users.jk_wong.prudential_security_metrics ORDER BY month"]}, {"name": "response_by_severity", "displayName": "Response Time by Severity", "queryLines": ["SELECT severity,   ROUND(AVG(response_time_hours), 1) as avg_response_hours,   ROUND(AVG(resolution_time_hours), 1) as avg_resolution_hours FROM users.jk_wong.prudential_security_incidents GROUP BY severity ORDER BY CASE severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 WHEN 'low' THEN 4 END"]}, {"name": "alert_actions", "displayName": "Alert Actions by Source", "queryLines": ["SELECT source_system, status as action_taken, COUNT(*) as alert_count FROM users.jk_wong.prudential_security_alerts GROUP BY source_system, status ORDER BY source_system, alert_count DESC"]}, {"name": "fp_rate_trend", "displayName": "False Positive Rate Trend", "queryLines": ["SELECT month, false_positive_rate_pct FROM users.jk_wong.prudential_security_metrics ORDER BY month"]}, {"name": "recent_critical", "displayName": "Recent Critical/High Incidents", "queryLines": ["SELECT incident_id, incident_date, incident_type, severity, status,   source_country, target_system, detection_method,   ROUND(response_time_hours, 1) as response_hours,   ROUND(resolution_time_hours, 1) as resolution_hours FROM users.jk_wong.prudential_security_incidents WHERE severity IN ('critical', 'high') ORDER BY incident_date DESC LIMIT 25"]}], "pages": [{"name": "security_operations_overview", "displayName": "Security Operations Overview", "layout": [{"widget": {"name": "page1-title", "multilineTextboxSpec": {"lines": ["## Prudential HK - Security Operations Overview"]}}, "position": {"x": 0, "y": 0, "width": 6, "height": 1}}, {"widget": {"name": "page1-subtitle", "multilineTextboxSpec": {"lines": ["Real-time visibility into security incidents, detection methods, and operational metrics"]}}, "position": {"x": 0, "y": 1, "width": 6, "height": 1}}, {"widget": {"name": "kpi-total-incidents", "queries": [{"name": "main_query", "query": {"datasetName": "kpi_summary", "fields": [{"name": "total_incidents", "expression": "`total_incidents`"}], "disaggregated": true}}], "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": {"fieldName": "total_incidents", "displayName": "Total Incidents"}}, "frame": {"showTitle": true, "title": "Total Incidents"}}}, "position": {"x": 0, "y": 2, "width": 2, "height": 3}}, {"widget": {"name": "kpi-open-investigating", "queries": [{"name": "main_query", "query": {"datasetName": "kpi_summary", "fields": [{"name": "open_investigating", "expression": "`open_investigating`"}], "disaggregated": true}}], "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": {"fieldName": "open_investigating", "displayName": "Open / Investigating"}}, "frame": {"showTitle": true, "title": "Open / Investigating"}}}, "position": {"x": 2, "y": 2, "width": 2, "height": 3}}, {"widget": {"name": "kpi-critical-incidents", "queries": [{"name": "main_query", "query": {"datasetName": "kpi_summary", "fields": [{"name": "critical_incidents", "expression": "`critical_incidents`"}], "disaggregated": true}}], "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": {"fieldName": "critical_incidents", "displayName": "Critical Incidents"}}, "frame": {"showTitle": true, "title": "Critical Incidents"}}}, "position": {"x": 4, "y": 2, "width": 2, "height": 3}}, {"widget": {"name": "section-trends", "multilineTextboxSpec": {"lines": ["### Monthly Incident Trend"]}}, "position": {"x": 0, "y": 5, "width": 6, "height": 1}}, {"widget": {"name": "monthly-incident-trend", "queries": [{"name": "main_query", "query": {"datasetName": "monthly_trend", "fields": [{"name": "month", "expression": "`month`"}, {"name": "total_incidents", "expression": "`total_incidents`"}, {"name": "critical_incidents", "expression": "`critical_incidents`"}], "disaggregated": true}}], "spec": {"version": 3, "widgetType": "line", "encodings": {"x": {"fieldName": "month", "scale": {"type": "temporal"}, "displayName": "Month"}, "y": {"scale": {"type": "quantitative"}, "fields": [{"fieldName": "total_incidents", "displayName": "Total Incidents"}, {"fieldName": "critical_incidents", "displayName": "Critical Incidents"}]}}, "frame": {"showTitle": true, "title": "Monthly Incident Trend"}}}, "position": {"x": 0, "y": 6, "width": 6, "height": 5}}, {"widget": {"name": "section-breakdown", "multilineTextboxSpec": {"lines": ["### Incident Breakdown"]}}, "position": {"x": 0, "y": 11, "width": 6, "height": 1}}, {"widget": {"name": "incidents-by-type", "queries": [{"name": "main_query", "query": {"datasetName": "incidents_by_type", "fields": [{"name": "incident_type", "expression": "`incident_type`"}, {"name": "incident_count", "expression": "`incident_count`"}], "disaggregated": true}}], "spec": {"version": 3, "widgetType": "bar", "encodings": {"x": {"fieldName": "incident_type", "scale": {"type": "categorical"}, "displayName": "Incident Type"}, "y": {"fieldName": "incident_count", "scale": {"type": "quantitative"}, "displayName": "Count"}}, "frame": {"showTitle": true, "title": "Incidents by Type"}}}, "position": {"x": 0, "y": 12, "width": 3, "height": 5}}, {"widget": {"name": "incidents-by-severity", "queries": [{"name": "main_query", "query": {"datasetName": "incidents_by_severity", "fields": [{"name": "severity", "expression": "`severity`"}, {"name": "incident_count", "expression": "`incident_count`"}], "disaggregated": true}}], "spec": {"version": 3, "widgetType": "bar", "encodings": {"x": {"fieldName": "severity", "scale": {"type": "categorical"}, "displayName": "Severity"}, "y": {"fieldName": "incident_count", "scale": {"type": "quantitative"}, "displayName": "Count"}}, "frame": {"showTitle": true, "title": "Incidents by Severity"}}}, "position": {"x": 3, "y": 12, "width": 3, "height": 5}}, {"widget": {"name": "detection-method-dist", "queries": [{"name": "main_query", "query": {"datasetName": "detection_methods", "fields": [{"name": "detection_method", "expression": "`detection_method`"}, {"name": "incident_count", "expression": "`incident_count`"}], "disaggregated": true}}], "spec": {"version": 3, "widgetType": "pie", "encodings": {"angle": {"fieldName": "incident_count", "scale": {"type": "quantitative"}, "displayName": "Count"}, "color": {"fieldName": "detection_method", "scale": {"type": "categorical"}, "displayName": "Detection Method"}}, "frame": {"showTitle": true, "title": "Detection Method Distribution"}}}, "position": {"x": 0, "y": 17, "width": 3, "height": 5}}, {"widget": {"name": "top-targeted-systems", "queries": [{"name": "main_query", "query": {"datasetName": "target_systems", "fields": [{"name": "target_system", "expression": "`target_system`"}, {"name": "incident_count", "expression": "`incident_count`"}], "disaggregated": true}}], "spec": {"version": 3, "widgetType": "bar", "encodings": {"x": {"fieldName": "target_system", "scale": {"type": "categorical"}, "displayName": "Target System"}, "y": {"fieldName": "incident_count", "scale": {"type": "quantitative"}, "displayName": "Incidents"}}, "frame": {"showTitle": true, "title": "Top Targeted Systems"}}}, "position": {"x": 3, "y": 17, "width": 3, "height": 5}}], "pageType": "PAGE_TYPE_CANVAS"}, {"name": "threat_intel_response", "displayName": "Threat Intelligence & Response", "layout": [{"widget": {"name": "page2-title", "multilineTextboxSpec": {"lines": ["## Threat Intelligence & Response"]}}, "position": {"x": 0, "y": 0, "width": 6, "height": 1}}, {"widget": {"name": "page2-subtitle", "multilineTextboxSpec": {"lines": ["Geographic threat origins, response performance, and alert quality metrics"]}}, "position": {"x": 0, "y": 1, "width": 6, "height": 1}}, {"widget": {"name": "kpi-avg-mttr", "queries": [{"name": "main_query", "query": {"datasetName": "kpi_summary", "fields": [{"name": "avg_mttr_hours", "expression": "`avg_mttr_hours`"}], "disaggregated": true}}], "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": {"fieldName": "avg_mttr_hours", "displayName": "Avg MTTR (hours)"}}, "frame": {"showTitle": true, "title": "Avg MTTR (hours)"}}}, "position": {"x": 0, "y": 2, "width": 2, "height": 3}}, {"widget": {"name": "kpi-total-incidents-p2", "queries": [{"name": "main_query", "query": {"datasetName": "kpi_summary", "fields": [{"name": "total_incidents", "expression": "`total_incidents`"}], "disaggregated": true}}], "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": {"fieldName": "total_incidents", "displayName": "Total Incidents"}}, "frame": {"showTitle": true, "title": "Total Incidents"}}}, "position": {"x": 2, "y": 2, "width": 2, "height": 3}}, {"widget": {"name": "kpi-critical-p2", "queries": [{"name": "main_query", "query": {"datasetName": "kpi_summary", "fields": [{"name": "critical_incidents", "expression": "`critical_incidents`"}], "disaggregated": true}}], "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": {"fieldName": "critical_incidents", "displayName": "Critical Incidents"}}, "frame": {"showTitle": true, "title": "Critical Incidents"}}}, "position": {"x": 4, "y": 2, "width": 2, "height": 3}}, {"widget": {"name": "attacks-by-country", "queries": [{"name": "main_query", "query": {"datasetName": "source_countries", "fields": [{"name": "source_country", "expression": "`source_country`"}, {"name": "attack_count", "expression": "`attack_count`"}], "disaggregated": true}}], "spec": {"version": 3, "widgetType": "bar", "encodings": {"x": {"fieldName": "source_country", "scale": {"type": "categorical"}, "displayName": "Source Country"}, "y": {"fieldName": "attack_count", "scale": {"type": "quantitative"}, "displayName": "Attacks"}}, "frame": {"showTitle": true, "title": "Attacks by Source Country"}}}, "position": {"x": 0, "y": 5, "width": 3, "height": 5}}, {"widget": {"name": "mttr-trend-chart", "queries": [{"name": "main_query", "query": {"datasetName": "mttr_trend", "fields": [{"name": "month", "expression": "`month`"}, {"name": "avg_response_time_hours", "expression": "`avg_response_time_hours`"}, {"name": "avg_resolution_time_hours", "expression": "`avg_resolution_time_hours`"}], "disaggregated": true}}], "spec": {"version": 3, "widgetType": "line", "encodings": {"x": {"fieldName": "month", "scale": {"type": "temporal"}, "displayName": "Month"}, "y": {"scale": {"type": "quantitative"}, "fields": [{"fieldName": "avg_response_time_hours", "displayName": "Avg Response Time (hrs)"}, {"fieldName": "avg_resolution_time_hours", "displayName": "Avg Resolution Time (hrs)"}]}}, "frame": {"showTitle": true, "title": "MTTR Trend Over Time"}}}, "position": {"x": 3, "y": 5, "width": 3, "height": 5}}, {"widget": {"name": "response-by-severity", "queries": [{"name": "main_query", "query": {"datasetName": "response_by_severity", "fields": [{"name": "severity", "expression": "`severity`"}, {"name": "avg_response_hours", "expression": "`avg_response_hours`"}, {"name": "avg_resolution_hours", "expression": "`avg_resolution_hours`"}], "disaggregated": true}}], "spec": {"version": 3, "widgetType": "bar", "encodings": {"x": {"fieldName": "severity", "scale": {"type": "categorical"}, "displayName": "Severity"}, "y": {"scale": {"type": "quantitative"}, "fields": [{"fieldName": "avg_response_hours", "displayName": "Avg Response (hrs)"}, {"fieldName": "avg_resolution_hours", "displayName": "Avg Resolution (hrs)"}]}}, "mark": {"layout": "group"}, "frame": {"showTitle": true, "title": "Response Time by Severity"}}}, "position": {"x": 0, "y": 10, "width": 3, "height": 5}}, {"widget": {"name": "alert-actions-by-source", "queries": [{"name": "main_query", "query": {"datasetName": "alert_actions", "fields": [{"name": "source_system", "expression": "`source_system`"}, {"name": "action_taken", "expression": "`action_taken`"}, {"name": "alert_count", "expression": "`alert_count`"}], "disaggregated": true}}], "spec": {"version": 3, "widgetType": "bar", "encodings": {"x": {"fieldName": "source_system", "scale": {"type": "categorical"}, "displayName": "Source System"}, "y": {"fieldName": "alert_count", "scale": {"type": "quantitative"}, "displayName": "Alert Count"}, "color": {"fieldName": "action_taken", "scale": {"type": "categorical"}, "displayName": "Action Taken"}}, "frame": {"showTitle": true, "title": "Alert Actions by Source System"}}}, "position": {"x": 3, "y": 10, "width": 3, "height": 5}}, {"widget": {"name": "section-quality", "multilineTextboxSpec": {"lines": ["### Alert Quality & Recent Incidents"]}}, "position": {"x": 0, "y": 15, "width": 6, "height": 1}}, {"widget": {"name": "fp-rate-trend", "queries": [{"name": "main_query", "query": {"datasetName": "fp_rate_trend", "fields": [{"name": "month", "expression": "`month`"}, {"name": "false_positive_rate_pct", "expression": "`false_positive_rate_pct`"}], "disaggregated": true}}], "spec": {"version": 3, "widgetType": "line", "encodings": {"x": {"fieldName": "month", "scale": {"type": "temporal"}, "displayName": "Month"}, "y": {"fieldName": "false_positive_rate_pct", "scale": {"type": "quantitative"}, "displayName": "False Positive Rate (%)"}}, "frame": {"showTitle": true, "title": "False Positive Rate Trend (%)"}}}, "position": {"x": 0, "y": 16, "width": 6, "height": 5}}, {"widget": {"name": "recent-critical-table", "queries": [{"name": "main_query", "query": {"datasetName": "recent_critical", "fields": [{"name": "incident_id", "expression": "`incident_id`"}, {"name": "incident_date", "expression": "`incident_date`"}, {"name": "incident_type", "expression": "`incident_type`"}, {"name": "severity", "expression": "`severity`"}, {"name": "status", "expression": "`status`"}, {"name": "source_country", "expression": "`source_country`"}, {"name": "target_system", "expression": "`target_system`"}, {"name": "response_hours", "expression": "`response_hours`"}, {"name": "resolution_hours", "expression": "`resolution_hours`"}], "disaggregated": true}}], "spec": {"version": 2, "widgetType": "table", "encodings": {"columns": [{"fieldName": "incident_id", "displayName": "Incident ID"}, {"fieldName": "incident_date", "displayName": "Date"}, {"fieldName": "incident_type", "displayName": "Type"}, {"fieldName": "severity", "displayName": "Severity"}, {"fieldName": "status", "displayName": "Status"}, {"fieldName": "source_country", "displayName": "Source"}, {"fieldName": "target_system", "displayName": "Target System"}, {"fieldName": "response_hours", "displayName": "Response (hrs)"}, {"fieldName": "resolution_hours", "displayName": "Resolution (hrs)"}]}, "frame": {"showTitle": true, "title": "Recent Critical & High Severity Incidents"}}}, "position": {"x": 0, "y": 21, "width": 6, "height": 7}}], "pageType": "PAGE_TYPE_CANVAS"}], "uiSettings": {"theme": {"canvasBackgroundColor": {"light": "#FAFAFB", "dark": "#1F272D"}, "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#11171C"}, "widgetBorderColor": {"light": "#FFFFFF", "dark": "#11171C"}, "fontColor": {"light": "#000000", "dark": "#F30000"}, "selectionColor": {"light": "#2272B4", "dark": "#8ACAFF"}, "visualizationColors": ["#077A9D", "#FFAB00", "#00A972", "#FF3621", "#8BCAE7", "#AB4057", "#99DDB4", "#FCA4A1", "#919191", "#BF7080", "#077A9D"], "widgetHeaderAlignment": "CENTER"}}}
'

serialized = replace_table_refs(SECURITY_DASHBOARD_JSON, TABLE_PREFIX)

payload = {
    "display_name": "Prudential HK - Security Incident Analytics",
    "serialized_dashboard": serialized,
    "parent_path": dashboard_path,
    "warehouse_id": warehouse_id,
}

resp = requests.post(f"{workspace_url}/api/2.0/lakeview/dashboards", headers=headers, json=payload)
security_dashboard = resp.json()
security_dashboard_id = security_dashboard.get("dashboard_id", "")

if security_dashboard_id:
    pub_resp = requests.post(
        f"{workspace_url}/api/2.0/lakeview/dashboards/{security_dashboard_id}/published",
        headers=headers,
        json={"embed_credentials": True, "warehouse_id": warehouse_id}
    )
    security_dash_url = f"{workspace_url}/dashboardsv3/{security_dashboard_id}/published"
    print(f"Security Incident Dashboard created and published!")
    print(f"  URL: {security_dash_url}")
else:
    print(f"ERROR creating security dashboard: {security_dashboard}")
    security_dash_url = "ERROR"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genie Space 1: Claims Fraud Explorer

# COMMAND ----------

claims_genie_payload = {
    "display_name": "Prudential HK - Claims Fraud Explorer",
    "description": f"You are a claims fraud analytics assistant for Prudential HK. Help risk analysts explore insurance claims data, identify fraud patterns, and investigate suspicious providers. Claims data spans 2024-2026 across Hong Kong, Macau, Singapore, and Malaysia. Claim amounts are in HKD. Join claims_raw with fraud_scores on claim_id for fraud analysis. Providers with risk_tier suspended or watch warrant attention. Fraud is concentrated in Macau and among repair_shop/legal provider types.",
    "table_identifiers": [
        f"{TABLE_PREFIX}.prudential_claims_raw",
        f"{TABLE_PREFIX}.prudential_claims_fraud_scores",
        f"{TABLE_PREFIX}.prudential_claims_providers",
    ],
    "warehouse_id": warehouse_id,
}

resp = requests.post(f"{workspace_url}/api/2.0/data-rooms", headers=headers, json=claims_genie_payload)
claims_genie = resp.json()
claims_genie_id = claims_genie.get("id") or claims_genie.get("space_id", "")

if claims_genie_id:
    claims_genie_url = f"{workspace_url}/genie/rooms/{claims_genie_id}"
    print(f"Claims Fraud Genie Space created!")
    print(f"  URL: {claims_genie_url}")

    # Add sample questions
    sample_questions = [
        "Which region has the highest fraud rate?",
        "Show me the top 10 providers with the most flagged claims",
        "What is the average claim amount for flagged vs non-flagged claims?",
        "List all critical risk claims over HKD 100,000",
        "Compare fraud rates across claim types",
    ]
    for q in sample_questions:
        requests.post(
            f"{workspace_url}/api/2.0/data-rooms/{claims_genie_id}/sample-questions",
            headers=headers,
            json={"question": q}
        )
    print(f"  Added {len(sample_questions)} sample questions")
else:
    print(f"ERROR creating claims Genie space: {claims_genie}")
    claims_genie_url = "ERROR"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genie Space 2: Security Threat Explorer

# COMMAND ----------

security_genie_payload = {
    "display_name": "Prudential HK - Security Threat Explorer",
    "description": f"You are a cybersecurity analytics assistant for Prudential HK. Help security analysts explore incident data, identify threat patterns, and monitor SOC performance. Data spans Jan 2024 to Mar 2026. Phishing is the most common threat (~35%). Attacks primarily originate from CN and RU. Join alerts to incidents via linked_incident_id (only ~30% of alerts are linked). Response time and resolution time are in hours. Use the metrics table for monthly trend analysis including MTTR and false positive rates.",
    "table_identifiers": [
        f"{TABLE_PREFIX}.prudential_security_incidents",
        f"{TABLE_PREFIX}.prudential_security_alerts",
        f"{TABLE_PREFIX}.prudential_security_metrics",
    ],
    "warehouse_id": warehouse_id,
}

resp = requests.post(f"{workspace_url}/api/2.0/data-rooms", headers=headers, json=security_genie_payload)
security_genie = resp.json()
security_genie_id = security_genie.get("id") or security_genie.get("space_id", "")

if security_genie_id:
    security_genie_url = f"{workspace_url}/genie/rooms/{security_genie_id}"
    print(f"Security Threat Genie Space created!")
    print(f"  URL: {security_genie_url}")

    sample_questions = [
        "How many critical incidents are currently open?",
        "What is our average response time for phishing attacks?",
        "Which countries are the top sources of attacks?",
        "Show me the trend of incidents over the last 12 months",
        "Which target systems are most frequently attacked?",
    ]
    for q in sample_questions:
        requests.post(
            f"{workspace_url}/api/2.0/data-rooms/{security_genie_id}/sample-questions",
            headers=headers,
            json={"question": q}
        )
    print(f"  Added {len(sample_questions)} sample questions")
else:
    print(f"ERROR creating security Genie space: {security_genie}")
    security_genie_url = "ERROR"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("  PRUDENTIAL HK - RISK & SECURITY DEMO SETUP COMPLETE")
print("=" * 70)
print()
print(f"  Data:  {TABLE_PREFIX}.prudential_claims_*  (3 tables)")
print(f"         {TABLE_PREFIX}.prudential_security_* (3 tables)")
print()
print(f"  Dashboards:")
print(f"    Claims Fraud:      {claims_dash_url}")
print(f"    Security Incidents: {security_dash_url}")
print()
print(f"  Genie Spaces:")
print(f"    Claims Explorer:   {claims_genie_url}")
print(f"    Security Explorer: {security_genie_url}")
print()
print("=" * 70)
