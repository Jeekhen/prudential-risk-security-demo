# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Claims Fraud Data
# MAGIC Creates 3 tables: `demo_claims_providers`, `demo_claims_raw`, `demo_claims_fraud_scores`

# COMMAND ----------

CATALOG = dbutils.widgets.get("CATALOG")
SCHEMA = dbutils.widgets.get("SCHEMA")
TABLE_PREFIX = f"{CATALOG}.{SCHEMA}"
print(f"Writing to: {TABLE_PREFIX}.demo_claims_*")

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import date, timedelta

SEED = 42
rng = np.random.default_rng(SEED)
NUM_PROVIDERS = 200
NUM_CLAIMS = 5_000

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Providers (200 rows)

# COMMAND ----------

HK_DISTRICTS = [
    "Central and Western", "Wan Chai", "Eastern", "Southern",
    "Yau Tsim Mong", "Sham Shui Po", "Kowloon City", "Wong Tai Sin",
    "Kwun Tong", "Tsuen Wan", "Tuen Mun", "Yuen Long", "Sha Tin",
    "Tai Po", "Sai Kung", "Kwai Tsing", "North", "Islands",
]
MACAU_DISTRICTS = ["Macau Peninsula", "Taipa", "Cotai", "Coloane"]
ALL_REGIONS = HK_DISTRICTS + MACAU_DISTRICTS

_region_w = np.array(
    [6, 5, 5, 4, 6, 4, 5, 4, 5, 4, 4, 4, 5, 3, 4, 4, 3, 3]
    + [3, 2, 2, 1],
    dtype=np.float64,
)

PROVIDER_TYPES = [
    "hospital", "clinic", "specialist", "pharmacy", "dental",
    "physiotherapy", "optical", "repair_shop", "legal", "lab",
]
_ptype_w = np.array([20, 25, 15, 10, 8, 6, 4, 5, 4, 3], dtype=np.float64)

CLAIM_TYPES = ["medical", "travel", "motor", "property", "life"]
_ctype_w = np.array([35, 20, 20, 15, 10], dtype=np.float64)

CLAIM_STATUSES = ["approved", "pending", "denied", "under_review", "paid"]
_status_w = np.array([40, 15, 10, 20, 15], dtype=np.float64)

FRAUD_INDICATORS = [
    "duplicate_claim", "inflated_amount", "suspicious_timing",
    "phantom_provider", "document_forgery", "multiple_policies",
    "rapid_succession", "out_of_network", "excessive_treatment",
    "known_fraud_ring",
]

HK_PROVIDER_PREFIXES = [
    "Hong Kong", "Kowloon", "New Territories", "Lantau", "Tsim Sha Tsui",
    "Causeway Bay", "Mongkok", "Aberdeen", "Sha Tin", "Tuen Mun",
    "Tai Po", "Yuen Long", "Macau", "Taipa", "Cotai",
]
PROVIDER_SUFFIXES = {
    "hospital": ["General Hospital", "Medical Centre", "Hospital", "Health Centre"],
    "clinic": ["Clinic", "Medical Clinic", "Family Clinic", "Polyclinic"],
    "specialist": ["Specialist Centre", "Specialist Clinic", "Medical Group"],
    "pharmacy": ["Pharmacy", "Dispensary", "Drug Store"],
    "dental": ["Dental Clinic", "Dental Centre", "Dental Surgery"],
    "physiotherapy": ["Physiotherapy Centre", "Rehab Centre", "Sports Clinic"],
    "optical": ["Optical", "Eye Centre", "Vision Centre"],
    "repair_shop": ["Auto Repair", "Motor Works", "Vehicle Services", "Panel Beaters"],
    "legal": ["Legal Services", "Claims Consultancy", "Loss Adjusters"],
    "lab": ["Laboratory", "Diagnostic Centre", "Pathology Lab"],
}

# COMMAND ----------

provider_ids = np.arange(1, NUM_PROVIDERS + 1)
provider_types = rng.choice(PROVIDER_TYPES, size=NUM_PROVIDERS, p=_ptype_w / _ptype_w.sum())
regions = rng.choice(ALL_REGIONS, size=NUM_PROVIDERS, p=_region_w / _region_w.sum())

provider_names = []
for i in range(NUM_PROVIDERS):
    pt = provider_types[i]
    prefix = rng.choice(HK_PROVIDER_PREFIXES)
    suffix = rng.choice(PROVIDER_SUFFIXES[pt])
    provider_names.append(f"{prefix} {suffix}")

license_years = rng.integers(1, 36, size=NUM_PROVIDERS)
total_claims_per_provider = np.clip(rng.poisson(lam=80, size=NUM_PROVIDERS), 5, 500).astype(int)

is_macau = np.isin(regions, MACAU_DISTRICTS)
is_risky_type = np.isin(provider_types, ["repair_shop", "legal"])

base_flag_pct = rng.beta(2, 15, size=NUM_PROVIDERS) * 100
base_flag_pct[is_macau] += rng.uniform(10, 25, size=is_macau.sum())
base_flag_pct[is_risky_type] += rng.uniform(8, 20, size=is_risky_type.sum())
flagged_claims_pct = np.clip(np.round(base_flag_pct, 1), 0, 80).astype(np.float64)

risk_tier = np.where(
    flagged_claims_pct >= 25, "critical",
    np.where(flagged_claims_pct >= 15, "high",
    np.where(flagged_claims_pct >= 8, "medium", "low"))
)

is_active = rng.choice([True, False], size=NUM_PROVIDERS, p=[0.88, 0.12])

providers_pdf = pd.DataFrame({
    "provider_id": provider_ids.astype(int),
    "provider_name": provider_names,
    "provider_type": provider_types,
    "region": regions,
    "license_years": license_years.astype(int),
    "total_claims_handled": total_claims_per_provider.astype(int),
    "flagged_claims_pct": flagged_claims_pct,
    "risk_tier": risk_tier,
    "is_active": is_active,
})

print(f"Providers: {len(providers_pdf)} rows")
print(f"Risk tier distribution:\n{providers_pdf['risk_tier'].value_counts().to_string()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Claims (5,000 rows)

# COMMAND ----------

claim_ids = np.arange(100_000, 100_000 + NUM_CLAIMS)
policyholder_ids = rng.integers(200_000, 202_000, size=NUM_CLAIMS)
claim_provider_ids = rng.choice(provider_ids, size=NUM_CLAIMS).astype(int)
claim_types = rng.choice(CLAIM_TYPES, size=NUM_CLAIMS, p=_ctype_w / _ctype_w.sum())

start_date = date(2024, 1, 1)
end_date = date(2026, 3, 31)
date_span = (end_date - start_date).days
claim_dates = [start_date + timedelta(days=int(d)) for d in rng.integers(0, date_span + 1, size=NUM_CLAIMS)]

raw_amounts = rng.gamma(2.0, 12_000, size=NUM_CLAIMS)
claim_amounts = np.clip(np.round(raw_amounts, 2), 500, 2_000_000)

statuses = rng.choice(CLAIM_STATUSES, size=NUM_CLAIMS, p=_status_w / _status_w.sum())
channels = rng.choice(
    ["online_portal", "mobile_app", "branch", "agent", "email"],
    size=NUM_CLAIMS, p=np.array([35, 25, 15, 15, 10], dtype=np.float64) / 100,
)
processing_days = np.clip(np.round(rng.gamma(2.0, 5.0, size=NUM_CLAIMS)).astype(int), 1, 90)

provider_region_map = dict(zip(provider_ids, regions))
provider_type_map = dict(zip(provider_ids, provider_types))
claim_regions = np.array([provider_region_map[pid] for pid in claim_provider_ids])
claim_ptypes = np.array([provider_type_map[pid] for pid in claim_provider_ids])

fraud_prob = np.full(NUM_CLAIMS, 0.08)
claim_in_macau = np.isin(claim_regions, MACAU_DISTRICTS)
fraud_prob[claim_in_macau] = 0.30
claim_risky_ptype = np.isin(claim_ptypes, ["repair_shop", "legal"])
fraud_prob[claim_risky_ptype] = np.maximum(fraud_prob[claim_risky_ptype], 0.25)
fraud_prob[claim_amounts > 200_000] += 0.10

is_flagged = rng.random(NUM_CLAIMS) < fraud_prob

fraud_indicator_list = []
for flagged in is_flagged:
    if flagged:
        n_indicators = rng.integers(1, 4)
        indicators = rng.choice(FRAUD_INDICATORS, size=n_indicators, replace=False)
        fraud_indicator_list.append(", ".join(indicators))
    else:
        fraud_indicator_list.append(None)

claims_pdf = pd.DataFrame({
    "claim_id": claim_ids.astype(int),
    "policyholder_id": policyholder_ids.astype(int),
    "provider_id": claim_provider_ids.astype(int),
    "claim_type": claim_types,
    "claim_date": claim_dates,
    "claim_amount_hkd": claim_amounts,
    "status": statuses,
    "submission_channel": channels,
    "processing_days": processing_days.astype(int),
    "is_flagged": is_flagged,
    "fraud_indicators": fraud_indicator_list,
})

print(f"Claims: {len(claims_pdf)} rows, fraud rate: {is_flagged.mean():.1%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fraud Scores (5,000 rows)

# COMMAND ----------

base_scores = rng.beta(2, 8, size=NUM_CLAIMS) * 100
base_scores[is_flagged] = rng.beta(6, 3, size=is_flagged.sum()) * 100
fraud_scores = np.round(np.clip(base_scores, 0, 100), 1)

risk_category = np.where(
    fraud_scores >= 75, "critical",
    np.where(fraud_scores >= 50, "high",
    np.where(fraud_scores >= 25, "medium", "low"))
)

model_versions = rng.choice(["v2.1.0", "v2.2.0", "v2.3.0"], size=NUM_CLAIMS, p=[0.2, 0.3, 0.5])
score_dates = [claim_dates[i] + timedelta(days=int(rng.integers(1, 6))) for i in range(NUM_CLAIMS)]

investigation_outcomes = []
for i in range(NUM_CLAIMS):
    if is_flagged[i]:
        outcome = rng.choice(
            ["confirmed_fraud", "suspicious_cleared", "under_investigation", "referred_to_legal"],
            p=[0.45, 0.20, 0.25, 0.10],
        )
        investigation_outcomes.append(outcome)
    else:
        investigation_outcomes.append("cleared" if rng.random() < 0.05 else None)

estimated_loss = np.where(
    is_flagged,
    np.round(claim_amounts * rng.uniform(0.5, 1.0, size=NUM_CLAIMS), 2),
    0.0,
)

provider_flag_map = dict(zip(provider_ids, flagged_claims_pct))
provider_risk_at_score = np.array([provider_flag_map[pid] for pid in claim_provider_ids], dtype=np.float64)

fraud_scores_pdf = pd.DataFrame({
    "claim_id": claim_ids.astype(int),
    "fraud_score": fraud_scores,
    "risk_category": risk_category,
    "model_version": model_versions,
    "score_date": score_dates,
    "investigation_outcome": investigation_outcomes,
    "estimated_loss_hkd": estimated_loss,
    "provider_risk_pct": provider_risk_at_score,
})

print(f"Fraud scores: {len(fraud_scores_pdf)} rows")
print(f"Risk categories:\n{fraud_scores_pdf['risk_category'].value_counts().to_string()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Unity Catalog

# COMMAND ----------

tables = [
    ("demo_claims_providers", providers_pdf),
    ("demo_claims_raw", claims_pdf),
    ("demo_claims_fraud_scores", fraud_scores_pdf),
]

for table_name, pdf in tables:
    fqn = f"{TABLE_PREFIX}.{table_name}"
    print(f"Writing {fqn} ({len(pdf):,} rows)...", end=" ", flush=True)
    spark_df = spark.createDataFrame(pdf)
    (spark_df.write.format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(fqn))
    count = spark.table(fqn).count()
    print(f"done ({count:,} rows verified).")

print("\nClaims data generation complete!")
