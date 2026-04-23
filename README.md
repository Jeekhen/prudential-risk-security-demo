# Insurance Risk & Security AI Demo

One-click setup for a Databricks demo showcasing **AI/BI Dashboards** and **Genie Spaces** for insurance risk and cybersecurity use cases.

Built for presenting to **Risk** and **Security** teams at insurance companies.

## What Gets Created

| Asset | Description |
|-------|-------------|
| `demo_claims_providers` | 200 insurance providers with risk tiers |
| `demo_claims_raw` | 5,000 insurance claims (HKD, 2024-2026) |
| `demo_claims_fraud_scores` | ML fraud scores for each claim |
| `demo_security_incidents` | 3,000 cybersecurity incidents |
| `demo_security_alerts` | 10,000 security alerts |
| `demo_security_metrics` | 27 monthly aggregated metrics |
| **Dashboard: Claims Fraud Risk Analytics** | 2-page dashboard with KPIs, fraud rates by region, suspicious providers |
| **Dashboard: Security Incident Analytics** | 2-page dashboard with incident trends, MTTR, threat intel |
| **Genie: Claims Fraud Explorer** | Natural language Q&A over claims data |
| **Genie: Security Threat Explorer** | Natural language Q&A over security data |

## Setup Instructions

1. **Import** this folder into your Databricks workspace (Workspace > Import > folder or Git repo)
2. **Open** `00_Setup_All` notebook
3. **Set widgets** at the top:
   - `CATALOG` - your Unity Catalog catalog (e.g., `users`)
   - `SCHEMA` - your schema (e.g., `my_name`)
   - `WORKSPACE_FOLDER` - where to place dashboards & Genie spaces (e.g., `/Users/me@company.com/Demos`). Leave blank to use your home folder.
4. **Attach** to any cluster (DBR 14.3+)
5. **Run All**
6. Links to all dashboards and Genie spaces will be printed at the end

## Requirements

- Databricks workspace with Unity Catalog enabled
- A running SQL warehouse (auto-detected, or the first available will be used)
- Cluster with DBR 14.3+ (for `numpy`, `pandas`)
- Permissions to create tables in the target catalog/schema

## Demo Flow

### Opening (2 min)

> "Today I'd like to show you how Databricks can help your Risk and Security teams get faster, AI-powered insights from your operational data. We'll look at two real scenarios: **claims fraud detection** and **security incident analytics** - and I'll show you two Databricks features that make this possible without writing code."

### Part 1: Claims Fraud Risk - AI/BI Dashboard (5 min)

Open the **Claims Fraud Risk Analytics** dashboard.

**Page 1 - Overview:**
- Point out the **KPI counters** at the top: total claims, flagged claims, fraud rate percentage
- Highlight the **Claims by Region** chart - note the volume distribution across Hong Kong areas and Macau
- Show the **Monthly Claims Trend** - the dual-line chart shows total claims vs flagged claims over 27 months, demonstrating how the ML model continuously scores incoming claims

> "This dashboard runs on live data in the Lakehouse. As new claims come in and get scored by the ML model, the dashboard updates automatically. No ETL pipelines to external BI tools needed."

**Page 2 - Fraud Deep Dive:**
- Show **Fraud Rate by Region** - Macau stands out at ~28% vs ~9-11% elsewhere. Ask: *"If you saw this pattern, what would your risk team investigate?"*
- Point to **Top 10 Suspicious Providers** - repair shops and legal services providers are overrepresented
- Show the **Fraud Indicator Frequency** chart - common flags like duplicate claims, inflated amounts
- Scroll to the **Top Flagged Claims table** - detailed view that an analyst would use to prioritize investigations

> "This is the kind of insight that helps your risk team focus their investigation resources on the highest-value cases rather than reviewing everything manually."

### Part 2: Claims Fraud - Genie Space (5 min)

Switch to the **Claims Fraud Explorer** Genie space.

> "Now, what if an analyst has a question that isn't covered by the dashboard? This is where Genie comes in - it lets anyone ask questions in plain English."

**Demo questions to ask live:**
1. **"Which region has the highest fraud rate?"** - shows Genie generating SQL and returning results
2. **"Show me the top 10 providers with the most flagged claims"** - demonstrates joins across tables
3. **"What is the average claim amount for flagged vs non-flagged claims?"** - shows analytical comparison
4. **"List all critical risk claims over HKD 100,000"** - shows filtering capability
5. **Ask the customer's own question** - invite someone in the room to ask a question about the data

> "Genie understands the data model and relationships. Your analysts don't need to know SQL or the table structure - they just ask questions."

### Part 3: Security Incidents - AI/BI Dashboard (5 min)

Open the **Security Incident Analytics** dashboard.

**Page 1 - Security Operations Overview:**
- **KPI counters**: total incidents, open/investigating, critical incidents
- **Monthly Incident Trend**: show the seasonality - phishing spikes in Jan, Mar, Sep, Nov
- **Incidents by Type**: phishing dominates at ~35%, followed by malware
- **Detection Method Distribution**: SIEM leads detection, followed by automated scans
- **Top Targeted Systems**: which systems are most attacked

> "This gives your SOC team a single pane of glass across all incident data. No more switching between SIEM dashboards and spreadsheets."

**Page 2 - Threat Intelligence & Response:**
- **Attacks by Source Country**: China and Russia are top sources - typical for APAC organizations
- **MTTR Trend**: show how response and resolution times trend over months
- **Response Time by Severity**: critical incidents get fastest response (~2 hrs) vs low priority (~48 hrs)
- **Alert Actions by Source System**: shows the funnel from alerts to actions across firewall, endpoint, IAM
- **False Positive Rate Trend**: track alert quality over time
- **Recent Critical Incidents table**: the detail view for SOC analysts

> "With all your security data unified in the Lakehouse, you can correlate across sources - something that's very difficult with siloed SIEM and ticketing tools."

### Part 4: Security - Genie Space (5 min)

Switch to the **Security Threat Explorer** Genie space.

**Demo questions:**
1. **"How many critical incidents are currently open?"** - real-time operational question
2. **"What's our average response time for phishing attacks?"** - performance metric
3. **"Which countries are the top sources of attacks?"** - threat intelligence
4. **"Show me the trend of incidents over the last 12 months"** - trend analysis
5. **Customer's own question** - invite the audience to ask

> "Imagine your CISO asking 'How are we doing on response times for critical incidents this quarter?' - Genie answers that in seconds, no analyst needed to pull a report."

### Closing (2 min)

> "What you've seen today is:
> 1. **AI/BI Dashboards** - governed, automatically refreshing dashboards that anyone can consume
> 2. **Genie Spaces** - natural language access to your data for ad-hoc questions
>
> Both run on the same unified Lakehouse where your ML models, security logs, and claims data already live. No data movement, no separate BI tool licenses, and everything is governed through Unity Catalog.
>
> The demo you saw today was set up in minutes with a single notebook - the same approach could be applied to your actual data."

## Cleanup

When you're done with the demo, run `04_Cleanup` to remove all created assets:

1. **Open** `04_Cleanup` notebook
2. **Set the same CATALOG, SCHEMA, and WORKSPACE_FOLDER** you used during setup
3. **Run All** - drops all 6 tables, deletes both dashboards, and deletes both Genie spaces

## Notebook Reference

| Notebook | Purpose |
|----------|---------|
| `00_Setup_All` | Main orchestrator - set widgets and Run All |
| `01_Generate_Claims_Data` | Creates 3 claims fraud tables |
| `02_Generate_Security_Data` | Creates 3 security incident tables |
| `03_Create_Dashboards_Genie` | Creates dashboards and Genie spaces via API |
| `04_Cleanup` | Removes all demo assets |
