# Prudential HK - Risk & Security AI Demo

One-click setup for a Databricks demo showcasing **AI/BI Dashboards** and **Genie Spaces** for insurance risk and cybersecurity use cases.

## What Gets Created

| Asset | Description |
|-------|-------------|
| `prudential_claims_providers` | 200 HK insurance providers with risk tiers |
| `prudential_claims_raw` | 5,000 insurance claims (HKD, 2024-2026) |
| `prudential_claims_fraud_scores` | ML fraud scores for each claim |
| `prudential_security_incidents` | 3,000 cybersecurity incidents |
| `prudential_security_alerts` | 10,000 security alerts |
| `prudential_security_metrics` | 27 monthly aggregated metrics |
| **Dashboard: Claims Fraud Risk Analytics** | 2-page dashboard with KPIs, fraud rates by region, top suspicious providers |
| **Dashboard: Security Incident Analytics** | 2-page dashboard with incident trends, MTTR, threat intel |
| **Genie: Claims Fraud Explorer** | Natural language Q&A over claims data |
| **Genie: Security Threat Explorer** | Natural language Q&A over security data |

## Setup Instructions

1. **Import** this folder into your Databricks workspace (Workspace > Import > DBC or folder)
2. **Open** `00_Setup_All` notebook
3. **Set widgets** at the top:
   - `CATALOG` — your Unity Catalog catalog (e.g., `users`)
   - `SCHEMA` — your schema (e.g., `my_name`)
   - `WORKSPACE_FOLDER` — where to place dashboards & Genie spaces (e.g., `/Users/me@company.com/Demos`). Leave blank to use your home folder.
4. **Attach** to any cluster (DBR 14.3+)
5. **Run All**
6. Links to all dashboards and Genie spaces will be printed at the end

## Requirements

- Databricks workspace with Unity Catalog enabled
- A running SQL warehouse (auto-detected, or the first available will be used)
- Cluster with DBR 14.3+ (for `numpy`, `pandas`)
- Permissions to create tables in the target catalog/schema

## Cleanup

When you're done with the demo, run `04_Cleanup` to remove all created assets:

1. **Open** `04_Cleanup` notebook
2. **Set the same CATALOG and SCHEMA** you used during setup
3. **Run All** — drops all 6 tables, deletes both dashboards, and deletes both Genie spaces

## Demo Narrative

### For Risk Team (Claims Fraud)
- Show the AI/BI Dashboard: KPIs, fraud concentration in Macau, suspicious providers
- Switch to Genie Space: ask "Which region has the highest fraud rate?" or "Show me claims over HKD 100K flagged as high risk"

### For Security Team (Incidents)
- Show the AI/BI Dashboard: incident trends, MTTR by severity, top attack sources (CN, RU)
- Switch to Genie Space: ask "How many critical incidents are open?" or "What's our average response time for phishing?"
