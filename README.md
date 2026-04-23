# Insurance Risk & Security AI Demo

One-click setup for a Databricks demo showcasing **AI/BI Dashboards**, **Genie Spaces**, **Model Serving**, **RAG**, and a **full-stack App** for insurance risk and cybersecurity use cases.

Built for presenting to **Risk** and **Security** teams at insurance companies.

## What Gets Created

### Data & Analytics
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

### ML & GenAI
| Asset | Description |
|-------|-------------|
| `demo_policy_documents` | 57 sections of synthetic risk/compliance policy documents |
| `demo_policy_chunks` | Chunked policy text for vector search |
| **Model: demo_fraud_model** | LightGBM fraud classifier registered in Unity Catalog |
| **Endpoint: demo-fraud-scoring** | Model Serving endpoint for real-time fraud scoring |
| **Vector Search: demo-vs-endpoint** | Vector Search endpoint with policy document embeddings |
| **App: risk-security-demo** | Full-stack app with Fraud Scoring + Compliance Copilot |

## Setup Instructions

1. **Import** this folder into your Databricks workspace (Workspace > Import > folder or Git repo)
2. **Open** `00_Setup_All` notebook
3. **Set widgets** at the top:
   - `CATALOG` - your Unity Catalog catalog (e.g., `users`)
   - `SCHEMA` - your schema (e.g., `my_name`)
   - `WORKSPACE_FOLDER` - where to place dashboards & Genie spaces (e.g., `/Users/me@company.com/Demos`). Leave blank to use your home folder.
4. **Attach** to any cluster (DBR 14.3+)
5. **Run All** (full setup takes ~20-30 minutes due to model training and endpoint provisioning)
6. Links to all assets will be printed at the end of each step

## Requirements

- Databricks workspace with Unity Catalog enabled
- A running SQL warehouse (auto-detected)
- Cluster with DBR 14.3+ (for `numpy`, `pandas`, `lightgbm`)
- Permissions to create tables, serving endpoints, and apps in the target catalog/schema

## Demo Flow

### Opening (2 min)

> "Today I'd like to show you how Databricks can help your Risk and Security teams get faster, AI-powered insights from your operational data. We'll cover four areas:
> 1. **Dashboards** for at-a-glance monitoring
> 2. **Genie** for ad-hoc natural language questions
> 3. **Real-time ML scoring** for fraud detection
> 4. **A GenAI copilot** for your risk policies
>
> Everything runs on the same unified Lakehouse - no data movement, no separate tools."

---

### Part 1: Claims Fraud Dashboard (5 min)

Open the **Claims Fraud Risk Analytics** dashboard.

**Page 1 - Overview:**
- Point out the **KPI counters**: total claims, flagged claims, fraud rate
- Highlight **Claims by Region** - volume distribution across HK and Macau
- Show **Monthly Claims Trend** - dual-line chart with total vs flagged over 27 months

> "This dashboard runs on live data in the Lakehouse. As new claims come in and get scored, it updates automatically."

**Page 2 - Fraud Deep Dive:**
- **Fraud Rate by Region** - Macau stands out at ~28% vs ~9-11% elsewhere
- **Top Suspicious Providers** - repair shops and legal providers are overrepresented
- **Fraud Indicator Frequency** - common flags: duplicate claims, inflated amounts
- **Top Flagged Claims table** - detailed view for analyst investigation

> "This helps your risk team focus investigation resources on the highest-value cases."

---

### Part 2: Claims Fraud Genie (5 min)

Switch to the **Claims Fraud Explorer** Genie space.

> "What if an analyst has a question not on the dashboard? Genie lets anyone ask in plain English."

**Questions to ask live:**
1. "Which region has the highest fraud rate?"
2. "Show me the top 10 providers with the most flagged claims"
3. "What is the average claim amount for flagged vs non-flagged claims?"
4. Invite someone in the room to ask their own question

> "No SQL knowledge needed - Genie understands the data model and relationships."

---

### Part 3: Security Incidents Dashboard (5 min)

Open the **Security Incident Analytics** dashboard.

**Page 1 - Operations Overview:**
- **KPIs**: total incidents, open/investigating, critical
- **Monthly Trend**: seasonality - phishing spikes in Jan, Mar, Sep, Nov
- **Incidents by Type**: phishing dominates at ~35%
- **Detection Methods**: SIEM leads, followed by automated scans

**Page 2 - Threat Intelligence:**
- **Attacks by Source Country**: China and Russia lead
- **MTTR Trend**: response and resolution times over 27 months
- **Response by Severity**: critical = ~2 hrs, low = ~48 hrs

> "With all security data unified in the Lakehouse, you can correlate across sources."

---

### Part 4: Security Genie (3 min)

Switch to the **Security Threat Explorer** Genie space.

**Questions:**
1. "How many critical incidents are currently open?"
2. "What's our average response time for phishing attacks?"
3. Let the audience ask a question

---

### Part 5: Real-Time Fraud Scoring App (5 min)

Open the **Demo App** > **Fraud Scoring** tab.

> "The dashboards show historical analysis. But what about scoring a new claim in real-time as it comes in? We've trained an ML model on the historical claims data and deployed it as a live API."

**Live demo:**
1. Fill in a **normal claim**: Medical, HKD 5,000, Hong Kong Island, Hospital, online_portal → show low risk score (green)
2. Fill in a **suspicious claim**: Motor, HKD 450,000, Macau, Repair Shop, agent, high provider flagged % → show high risk score (red)
3. Point out the **risk factors** breakdown - explain how the model weighs each feature

> "This scores in milliseconds. In production, this would be called from your claims intake system via API - every new claim gets scored before an adjuster even sees it."

---

### Part 6: Compliance Copilot App (5 min)

Switch to the **Compliance Copilot** tab.

> "Your risk and compliance teams reference dozens of policy documents daily. What if they could just ask questions in natural language?"

**Questions to ask live:**
1. Click a starter chip: **"What are the fraud escalation thresholds?"** - shows answer with source document citations
2. **"What is the incident response SLA for critical security incidents?"** - pulls from the Cybersecurity Incident Response Plan
3. **"What are the data breach notification requirements?"** - shows the 72-hour rule with regulatory context
4. **"What documentation is needed for motor claims?"** - pulls from Claims Processing Policy
5. Ask a follow-up question to show conversation context works

> "This is RAG - Retrieval-Augmented Generation. The AI searches your actual policy documents using vector similarity, then generates an answer grounded in those sources. Every answer includes citations so you can verify."

---

### Closing (2 min)

> "To summarize what you've seen:
>
> | Capability | What it does | Who uses it |
> |------------|-------------|-------------|
> | **AI/BI Dashboards** | Governed, auto-refreshing KPI views | Risk managers, SOC leads |
> | **Genie Spaces** | Natural language ad-hoc queries | Analysts, investigators |
> | **Model Serving** | Real-time ML scoring via API | Claims intake systems |
> | **RAG Copilot** | AI assistant over internal policies | Compliance, legal, analysts |
>
> All of this runs on one platform, governed by Unity Catalog. The entire demo you just saw was set up from a single notebook."

## Cleanup

When done with the demo, run `04_Cleanup` to remove all created assets:

1. **Open** `04_Cleanup` notebook
2. **Set the same CATALOG, SCHEMA, and WORKSPACE_FOLDER** you used during setup
3. **Run All** - drops all tables, deletes dashboards, Genie spaces, serving endpoint, vector search, ML model, and the app

## Notebook Reference

| Notebook | Purpose |
|----------|---------|
| `00_Setup_All` | Main orchestrator - set widgets and Run All |
| `01_Generate_Claims_Data` | Creates 3 claims fraud tables |
| `02_Generate_Security_Data` | Creates 3 security incident tables |
| `03_Create_Dashboards_Genie` | Creates dashboards and Genie spaces via API |
| `04_Cleanup` | Removes all demo assets |
| `05_Train_Deploy_Model` | Trains fraud model, deploys to Model Serving |
| `06_Setup_RAG_Pipeline` | Generates policy docs, creates Vector Search index |
| `07_Deploy_App` | Deploys the Databricks App |
| `app/` | App source code (FastAPI + React) |
