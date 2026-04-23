# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Security Incidents Data
# MAGIC Creates 3 tables: `prudential_security_incidents`, `prudential_security_alerts`, `prudential_security_metrics`

# COMMAND ----------

CATALOG = dbutils.widgets.get("CATALOG")
SCHEMA = dbutils.widgets.get("SCHEMA")
TABLE_PREFIX = f"{CATALOG}.{SCHEMA}"
print(f"Writing to: {TABLE_PREFIX}.prudential_security_*")

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta

SEED = 42
rng = np.random.default_rng(SEED)
INCIDENT_ROWS = 3_000
ALERT_ROWS = 10_000
DATE_START = np.datetime64("2024-01-01")
DATE_END = np.datetime64("2026-03-31")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# Reference data
INCIDENT_TYPES = ["phishing", "malware", "unauthorized_access", "brute_force",
                  "data_exfiltration", "insider_threat", "ransomware", "ddos"]
INCIDENT_WEIGHTS = np.array([35, 15, 12, 10, 8, 7, 7, 6], dtype=np.float64)

SEVERITIES = ["critical", "high", "medium", "low"]
SEVERITY_WEIGHTS = np.array([5, 15, 40, 40], dtype=np.float64)

SOURCE_COUNTRIES = ["CN", "RU", "US", "KR", "IN", "BR", "IR", "NG", "VN", "UA"]
COUNTRY_WEIGHTS = np.array([30, 20, 15, 10, 5, 5, 5, 4, 3, 3], dtype=np.float64)

TARGET_SYSTEMS = ["claims_portal", "policy_admin", "customer_db", "email_gateway",
                  "hr_system", "payment_gateway", "data_warehouse"]

DETECTION_METHODS = ["SIEM", "automated_scan", "anomaly_detection", "user_report", "threat_intel"]
DETECTION_WEIGHTS = np.array([35, 25, 20, 12, 8], dtype=np.float64)

STATUSES = ["resolved", "mitigated", "investigating", "escalated", "closed"]
STATUS_WEIGHTS = np.array([40, 25, 15, 10, 10], dtype=np.float64)

RESPONSE_TIME_RANGES = {"critical": (0.5, 4.0), "high": (2.0, 12.0), "medium": (8.0, 36.0), "low": (24.0, 72.0)}
IMPACT_RANGES = {"critical": (70, 100), "high": (50, 80), "medium": (20, 60), "low": (1, 30)}
RECORDS_RANGES = {"critical": (1000, 50000), "high": (100, 5000), "medium": (10, 500), "low": (1, 50)}

PHISHING_SPIKE_MONTHS = {1, 3, 9, 11}

ALERT_SOURCES = ["firewall", "endpoint", "iam", "email_filter", "waf", "ids"]
ALERT_SOURCE_WEIGHTS = np.array([25, 20, 15, 20, 10, 10], dtype=np.float64)
ALERT_TYPES = ["intrusion_attempt", "malware_detected", "suspicious_login",
               "policy_violation", "anomalous_traffic", "phishing_email",
               "privilege_escalation", "data_transfer"]
ALERT_TYPE_WEIGHTS = np.array([15, 15, 20, 10, 15, 10, 7, 8], dtype=np.float64)
ALERT_SEVERITIES = ["critical", "high", "medium", "low", "info"]
ALERT_SEV_WEIGHTS = np.array([3, 12, 30, 35, 20], dtype=np.float64)

ANALYSTS = [
    "David Chan", "Michelle Wong", "Eric Lau", "Sarah Leung", "Kevin Ng",
    "Jennifer Ho", "Richard Yip", "Catherine Tam", "Andrew Kwok", "Grace Tsui",
    "Patrick Cheng", "Angela Liu", "Raymond Hui", "Vivian Cheung", "Thomas Fung"
]

MITRE_TECHNIQUES = [
    "T1566.001", "T1078", "T1110", "T1048", "T1486",
    "T1071", "T1059", "T1021", "T1190", "T1053",
    "T1036", "T1055", "T1003", "T1027", "T1562"
]

RULE_NAMES = [
    "BRUTE_FORCE_LOGIN", "MALWARE_SIGNATURE_MATCH", "ANOMALOUS_DATA_TRANSFER",
    "PHISHING_URL_DETECTED", "PRIVILEGE_ESCALATION_ATTEMPT", "SQL_INJECTION_ATTEMPT",
    "SUSPICIOUS_OUTBOUND_TRAFFIC", "UNAUTHORIZED_ACCESS_ATTEMPT",
    "CREDENTIAL_STUFFING", "LATERAL_MOVEMENT_DETECTED",
    "RANSOMWARE_BEHAVIOR", "DNS_TUNNELING", "C2_COMMUNICATION",
    "FILE_INTEGRITY_VIOLATION", "POLICY_VIOLATION_DETECTED"
]

# COMMAND ----------

def generate_dates_with_seasonality(n, spike_months, spike_factor=1.8):
    months = []
    for y in range(2024, 2027):
        for m in range(1, 13):
            if np.datetime64(f"{y}-{m:02d}-01") > DATE_END:
                break
            if np.datetime64(f"{y}-{m:02d}-01") >= DATE_START:
                months.append((y, m))
    month_weights = np.array(
        [spike_factor if m in spike_months else 1.0 for _, m in months], dtype=np.float64
    )
    month_weights /= month_weights.sum()
    month_indices = rng.choice(len(months), size=n, p=month_weights)
    dates = []
    for idx in month_indices:
        y, m = months[idx]
        if m == 12:
            eom = np.datetime64(f"{y + 1}-01-01") - np.timedelta64(1, "D")
        else:
            eom = np.datetime64(f"{y}-{m + 1:02d}-01") - np.timedelta64(1, "D")
        som = np.datetime64(f"{y}-{m:02d}-01")
        som = max(som, DATE_START)
        eom = min(eom, DATE_END)
        day_span = (eom - som).astype(int)
        day_offset = rng.integers(0, day_span + 1)
        dates.append(som + np.timedelta64(int(day_offset), "D"))
    return np.array(dates, dtype="datetime64[D]")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incidents (3,000 rows)

# COMMAND ----------

incident_ids_arr = np.array([f"INC-{i:06d}" for i in range(1, INCIDENT_ROWS + 1)])
incident_types_arr = rng.choice(INCIDENT_TYPES, size=INCIDENT_ROWS, p=INCIDENT_WEIGHTS / INCIDENT_WEIGHTS.sum())
incident_dates_arr = generate_dates_with_seasonality(INCIDENT_ROWS, PHISHING_SPIKE_MONTHS)
severities_arr = rng.choice(SEVERITIES, size=INCIDENT_ROWS, p=SEVERITY_WEIGHTS / SEVERITY_WEIGHTS.sum())
source_countries_arr = rng.choice(SOURCE_COUNTRIES, size=INCIDENT_ROWS, p=COUNTRY_WEIGHTS / COUNTRY_WEIGHTS.sum())
target_systems_arr = rng.choice(TARGET_SYSTEMS, size=INCIDENT_ROWS)
detection_methods_arr = rng.choice(DETECTION_METHODS, size=INCIDENT_ROWS, p=DETECTION_WEIGHTS / DETECTION_WEIGHTS.sum())
statuses_arr = rng.choice(STATUSES, size=INCIDENT_ROWS, p=STATUS_WEIGHTS / STATUS_WEIGHTS.sum())
assigned_analysts_arr = rng.choice(ANALYSTS, size=INCIDENT_ROWS)

response_hours = np.zeros(INCIDENT_ROWS, dtype=np.float64)
impact_scores = np.zeros(INCIDENT_ROWS, dtype=np.int64)
affected_records = np.zeros(INCIDENT_ROWS, dtype=np.int64)

for i in range(INCIDENT_ROWS):
    sev = severities_arr[i]
    lo, hi = RESPONSE_TIME_RANGES[sev]
    response_hours[i] = round(rng.uniform(lo, hi), 2)
    impact_scores[i] = rng.integers(*IMPACT_RANGES[sev])
    affected_records[i] = rng.integers(*RECORDS_RANGES[sev])

financial_impact = np.round(affected_records * rng.uniform(5.0, 50.0, size=INCIDENT_ROWS), 2)
resolution_hours = np.round(response_hours * rng.uniform(1.5, 5.0, size=INCIDENT_ROWS), 2)

breach_probs = np.where(
    np.isin(incident_types_arr, ["data_exfiltration", "insider_threat"]), 0.6,
    np.where(np.isin(incident_types_arr, ["ransomware"]), 0.3, 0.05)
)
data_breach = rng.random(INCIDENT_ROWS) < breach_probs
regulatory_report = data_breach | ((severities_arr == "critical") & (rng.random(INCIDENT_ROWS) < 0.8))

source_ips = np.array([
    f"{rng.integers(1,224)}.{rng.integers(0,256)}.{rng.integers(0,256)}.{rng.integers(1,255)}"
    for _ in range(INCIDENT_ROWS)
])

incident_date_py = pd.to_datetime(incident_dates_arr.astype("datetime64[ms]")).date

incidents_pdf = pd.DataFrame({
    "incident_id": incident_ids_arr,
    "incident_date": incident_date_py,
    "incident_type": incident_types_arr,
    "severity": severities_arr,
    "status": statuses_arr,
    "source_country": source_countries_arr,
    "source_ip": source_ips,
    "target_system": target_systems_arr,
    "detection_method": detection_methods_arr,
    "assigned_analyst": assigned_analysts_arr,
    "response_time_hours": response_hours,
    "resolution_time_hours": resolution_hours,
    "impact_score": impact_scores,
    "affected_records": affected_records,
    "financial_impact_usd": financial_impact,
    "data_breach": data_breach,
    "regulatory_report_required": regulatory_report,
})

print(f"Incidents: {len(incidents_pdf)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alerts (10,000 rows)

# COMMAND ----------

alert_ids_arr = np.array([f"ALT-{i:07d}" for i in range(1, ALERT_ROWS + 1)])
alert_dates_arr = generate_dates_with_seasonality(ALERT_ROWS, PHISHING_SPIKE_MONTHS, spike_factor=1.5)
alert_ts_arr = (
    alert_dates_arr.astype("datetime64[s]")
    + rng.integers(0, 86400, size=ALERT_ROWS).astype("timedelta64[s]")
)
alert_source_systems_arr = rng.choice(ALERT_SOURCES, size=ALERT_ROWS, p=ALERT_SOURCE_WEIGHTS / ALERT_SOURCE_WEIGHTS.sum())
alert_types_arr = rng.choice(ALERT_TYPES, size=ALERT_ROWS, p=ALERT_TYPE_WEIGHTS / ALERT_TYPE_WEIGHTS.sum())
alert_severities_arr = rng.choice(ALERT_SEVERITIES, size=ALERT_ROWS, p=ALERT_SEV_WEIGHTS / ALERT_SEV_WEIGHTS.sum())

linked_mask = rng.random(ALERT_ROWS) < 0.30
linked_incident_ids = np.where(linked_mask, rng.choice(incident_ids_arr, size=ALERT_ROWS), None)

alert_statuses_list = ["new", "acknowledged", "investigating", "resolved", "false_positive"]
alert_status_weights = np.array([15, 20, 15, 35, 15], dtype=np.float64)
alert_statuses_arr = rng.choice(alert_statuses_list, size=ALERT_ROWS, p=alert_status_weights / alert_status_weights.sum())

is_false_positive = (alert_statuses_arr == "false_positive")

alert_source_ips = np.array([
    f"{rng.integers(1,224)}.{rng.integers(0,256)}.{rng.integers(0,256)}.{rng.integers(1,255)}"
    for _ in range(ALERT_ROWS)
])
alert_target_systems_arr = rng.choice(TARGET_SYSTEMS, size=ALERT_ROWS)

confidence_scores = np.where(
    is_false_positive,
    np.round(rng.uniform(0.1, 0.45, size=ALERT_ROWS), 3),
    np.round(rng.uniform(0.4, 0.99, size=ALERT_ROWS), 3)
)
mitre_ids = rng.choice(MITRE_TECHNIQUES, size=ALERT_ROWS)
triggered_rules = rng.choice(RULE_NAMES, size=ALERT_ROWS)

alert_ts_py = pd.to_datetime(alert_ts_arr.astype("datetime64[ms]"))

alerts_pdf = pd.DataFrame({
    "alert_id": alert_ids_arr,
    "alert_timestamp": alert_ts_py,
    "source_system": alert_source_systems_arr,
    "alert_type": alert_types_arr,
    "severity": alert_severities_arr,
    "status": alert_statuses_arr,
    "source_ip": alert_source_ips,
    "target_system": alert_target_systems_arr,
    "linked_incident_id": linked_incident_ids,
    "is_false_positive": is_false_positive,
    "confidence_score": confidence_scores,
    "mitre_technique_id": mitre_ids,
    "triggered_rule": triggered_rules,
})

print(f"Alerts: {len(alerts_pdf)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metrics (27 monthly rows)

# COMMAND ----------

incidents_pdf_agg = incidents_pdf.copy()
incidents_pdf_agg["incident_date"] = pd.to_datetime(incidents_pdf_agg["incident_date"])
incidents_pdf_agg["month"] = incidents_pdf_agg["incident_date"].dt.to_period("M").dt.to_timestamp()

inc_agg = incidents_pdf_agg.groupby("month").agg(
    total_incidents=("incident_id", "count"),
    critical_incidents=("severity", lambda x: (x == "critical").sum()),
    high_incidents=("severity", lambda x: (x == "high").sum()),
    avg_response_time_hours=("response_time_hours", "mean"),
    median_response_time_hours=("response_time_hours", "median"),
    avg_resolution_time_hours=("resolution_time_hours", "mean"),
    total_financial_impact_usd=("financial_impact_usd", "sum"),
    data_breach_count=("data_breach", "sum"),
    regulatory_reports_filed=("regulatory_report_required", "sum"),
    avg_impact_score=("impact_score", "mean"),
    total_affected_records=("affected_records", "sum"),
).reset_index()

for col in ["avg_response_time_hours", "median_response_time_hours", "avg_resolution_time_hours",
            "total_financial_impact_usd", "avg_impact_score"]:
    inc_agg[col] = inc_agg[col].round(2)

alerts_pdf_agg = alerts_pdf.copy()
alerts_pdf_agg["month"] = alerts_pdf_agg["alert_timestamp"].dt.to_period("M").dt.to_timestamp()

alt_agg = alerts_pdf_agg.groupby("month").agg(
    total_alerts=("alert_id", "count"),
    false_positive_count=("is_false_positive", "sum"),
    alerts_linked_to_incidents=("linked_incident_id", lambda x: x.notna().sum()),
    avg_confidence_score=("confidence_score", "mean"),
).reset_index()
alt_agg["avg_confidence_score"] = alt_agg["avg_confidence_score"].round(3)

metrics_pdf = inc_agg.merge(alt_agg, on="month", how="outer").sort_values("month").reset_index(drop=True)
metrics_pdf["false_positive_rate_pct"] = (metrics_pdf["false_positive_count"] / metrics_pdf["total_alerts"] * 100).round(2)
metrics_pdf["alert_to_incident_rate_pct"] = (metrics_pdf["alerts_linked_to_incidents"] / metrics_pdf["total_alerts"] * 100).round(2)
metrics_pdf["high_severity_total"] = metrics_pdf["critical_incidents"] + metrics_pdf["high_incidents"]
metrics_pdf["month"] = metrics_pdf["month"].dt.date

for col in ["data_breach_count", "regulatory_reports_filed", "total_affected_records",
            "false_positive_count", "alerts_linked_to_incidents", "high_severity_total",
            "critical_incidents", "high_incidents", "total_incidents", "total_alerts"]:
    metrics_pdf[col] = metrics_pdf[col].astype(int)

print(f"Metrics: {len(metrics_pdf)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Unity Catalog

# COMMAND ----------

tables_to_write = [
    ("prudential_security_incidents", incidents_pdf),
    ("prudential_security_alerts", alerts_pdf),
    ("prudential_security_metrics", metrics_pdf),
]

for table_name, pdf in tables_to_write:
    fqn = f"{TABLE_PREFIX}.{table_name}"
    print(f"Writing {fqn} ({len(pdf):,} rows)...", end=" ", flush=True)
    spark_df = spark.createDataFrame(pdf)
    (spark_df.write.format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(fqn))
    count = spark.table(fqn).count()
    print(f"done ({count:,} rows verified).")

print("\nSecurity data generation complete!")
