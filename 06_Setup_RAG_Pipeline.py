# Databricks notebook source
# MAGIC %md
# MAGIC # Setup RAG Pipeline — Risk Policy Compliance Copilot
# MAGIC Creates tables: `demo_policy_documents`, `demo_policy_chunks`
# MAGIC
# MAGIC Then builds a Vector Search index and tests the end-to-end RAG pipeline.

# COMMAND ----------

CATALOG = dbutils.widgets.get("CATALOG")
SCHEMA = dbutils.widgets.get("SCHEMA")
TABLE_PREFIX = f"{CATALOG}.{SCHEMA}"
print(f"Writing to: {TABLE_PREFIX}.demo_policy_*")

# COMMAND ----------

import json, time, requests

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Workspace URL and token for REST API calls
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
API_BASE = f"https://{workspace_url}/api/2.0"
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
HEADERS = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

print(f"Workspace: {workspace_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate Synthetic Policy Documents

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1a. Claims Processing Policy v3.2

# COMMAND ----------

claims_processing_sections = [
    {
        "doc_id": "POL-CP-001",
        "doc_title": "Claims Processing Policy v3.2",
        "section_title": "1.1 Purpose and Scope",
        "doc_type": "policy",
        "last_updated": "2025-09-15",
        "section_content": """This policy governs all claims processing activities for the Company Hong Kong and Macau operations. It applies to medical, travel, motor, property, and life insurance claims submitted through any channel — online portal, agent submission, postal mail, or walk-in service centres. All claims adjusters, team leads, quality assurance staff, and outsourced processing partners must comply with this policy. The policy was last revised following the Insurance Authority's GL-28 guidance note on fair claims settlement, and supersedes Claims Processing Policy v3.1 dated 2024-11-01. Any deviation from the procedures herein requires written approval from the Head of Claims Operations or the Chief Risk Officer. This document must be reviewed at least annually or upon any material change in regulatory requirements."""
    },
    {
        "doc_id": "POL-CP-001",
        "doc_title": "Claims Processing Policy v3.2",
        "section_title": "1.2 Claims Submission Requirements",
        "doc_type": "policy",
        "last_updated": "2025-09-15",
        "section_content": """All claims must be submitted within 90 calendar days of the incident date for medical and travel claims, and within 30 calendar days for motor and property claims. Life claims must be submitted within 180 calendar days of the date of death. A valid claim submission must include: (a) a completed claim form (Form CP-100 for medical, CP-200 for motor, CP-300 for property, CP-400 for travel, CP-500 for life); (b) original or certified copies of supporting documents as listed in Appendix A; (c) claimant identification — Hong Kong Identity Card or Macau BIR number; (d) policy number and certificate of insurance. Incomplete submissions are returned within 5 business days with a deficiency notice (Form CP-050). Claimants have 30 additional days to resubmit. Electronic submissions via the Mythe Company portal are preferred and receive a processing time advantage of 2 business days. Submissions via registered mail must include the tracking number on the claim form envelope."""
    },
    {
        "doc_id": "POL-CP-001",
        "doc_title": "Claims Processing Policy v3.2",
        "section_title": "1.3 Documentation Requirements by Claim Type",
        "doc_type": "policy",
        "last_updated": "2025-09-15",
        "section_content": """Medical claims require: original medical receipts, referral letters (if specialist), diagnosis report, and discharge summary for inpatient stays exceeding 24 hours. Receipts must show the provider's name, registration number, date of service, itemised charges, and diagnosis code (ICD-10 format preferred). Travel claims require: boarding passes or itinerary confirmation, police report (for theft/loss), medical reports from overseas providers with certified translation if not in English or Chinese, and proof of travel insurance purchase date. Motor claims require: police report number (mandatory for accidents involving injury or damage exceeding HKD 50,000), photographs of damage from at least four angles, repair estimate from an authorised assessor on our panel list, and driver's licence copy. Property claims require: photographs of damage, police report (for theft or vandalism), property valuation report not older than 12 months, and itemised loss schedule. Life claims require: certified death certificate, coroner's report (if applicable), medical records for the 24 months prior to death, and beneficiary identification documents."""
    },
    {
        "doc_id": "POL-CP-001",
        "doc_title": "Claims Processing Policy v3.2",
        "section_title": "1.4 Processing SLAs and Timelines",
        "doc_type": "policy",
        "last_updated": "2025-09-15",
        "section_content": """Claims must be acknowledged within 2 business days of receipt. The target processing timelines from acknowledgement to decision are: simple medical claims (under HKD 20,000) — 5 business days; complex medical claims (HKD 20,000 to HKD 200,000) — 10 business days; high-value medical claims (over HKD 200,000) — 15 business days with mandatory senior review; travel claims — 10 business days; motor claims — 15 business days; property claims — 20 business days; life claims — 20 business days. Payment must be issued within 5 business days of approval. If SLA is breached, the claims supervisor is automatically notified and must provide a written explanation. Monthly SLA compliance must exceed 92% for the team to meet its KPI target. The Quality Assurance team audits a random 10% sample of processed claims each month to verify SLA accuracy and decision quality."""
    },
    {
        "doc_id": "POL-CP-001",
        "doc_title": "Claims Processing Policy v3.2",
        "section_title": "1.5 Fraud Escalation Thresholds",
        "doc_type": "policy",
        "last_updated": "2025-09-15",
        "section_content": """All claims are screened through the automated fraud scoring system (FraudGuard v4.1) at intake. Claims flagged with a fraud score of 70 or above are automatically routed to the Special Investigations Unit (SIU). Manual escalation is required when: (a) the claim amount exceeds HKD 200,000 and involves a provider not on the approved panel; (b) the claimant has more than 3 claims in the past 12 months with a combined value exceeding HKD 150,000; (c) the claim involves a recently onboarded provider (less than 6 months); (d) the claim includes billing codes inconsistent with the stated diagnosis; (e) the claimant or provider appears on any sanctions or watchlist. Senior adjuster review is mandatory for all claims exceeding HKD 500,000 regardless of fraud score. The SIU must acknowledge escalated cases within 4 hours during business hours and provide an initial assessment within 48 hours. Adjusters must not communicate the fraud referral to the claimant or provider."""
    },
    {
        "doc_id": "POL-CP-001",
        "doc_title": "Claims Processing Policy v3.2",
        "section_title": "1.6 Provider Verification Procedures",
        "doc_type": "policy",
        "last_updated": "2025-09-15",
        "section_content": """Before processing any claim, the adjuster must verify the service provider against the Provider Master Database (PMD). Verification includes: (a) confirming the provider's licence is active with the relevant regulatory body — Medical Council of Hong Kong, Dental Council, or Macau Health Bureau; (b) checking the provider's status on our approved panel list; (c) verifying the provider's address matches records; (d) confirming the provider's bank account details have not changed since last verification. Providers not found in the PMD trigger an automatic flag. New provider onboarding requires completion of Form PV-100, site visit by the Provider Relations team (within 30 days for Hong Kong, 60 days for Macau), and anti-fraud screening. Providers under investigation by the SIU are flagged as "restricted" — claims involving restricted providers require SIU clearance before processing. The PMD is refreshed quarterly against the HKMA and Macau SSM provider registries."""
    },
    {
        "doc_id": "POL-CP-001",
        "doc_title": "Claims Processing Policy v3.2",
        "section_title": "1.7 Regional Processing Rules — Hong Kong",
        "doc_type": "policy",
        "last_updated": "2025-09-15",
        "section_content": """Claims arising from services in Hong Kong are processed under Hong Kong Insurance Ordinance (Cap. 41) requirements. All claim decisions must comply with the Insurance Authority's GL-28 Guideline on Claims Handling. Medical claims from Hong Kong public hospitals (Hospital Authority facilities) are processed at gazetted rates published annually by the Hospital Authority. Private hospital claims are assessed against the Reasonable and Customary (R&C) schedule updated semi-annually. The R&C schedule uses the 80th percentile of market rates for each procedure code in the relevant district. Out-of-network claims are reimbursed at 70% of the R&C rate unless pre-authorisation was obtained. Claims involving cross-harbour ambulance transfers require separate pre-authorisation. Motor claims in Hong Kong must reference the police report filed with the Hong Kong Police Force Traffic Branch. The minimum excess for comprehensive motor policies is HKD 2,000."""
    },
    {
        "doc_id": "POL-CP-001",
        "doc_title": "Claims Processing Policy v3.2",
        "section_title": "1.8 Regional Processing Rules — Macau",
        "doc_type": "policy",
        "last_updated": "2025-09-15",
        "section_content": """Claims arising from services in Macau are processed under Macau Insurance Supervision Bureau (ASM) regulations. Currency conversion from MOP to HKD is applied at the Treasury-published rate on the date of service. Macau claims have a supplementary documentation requirement: providers must include their Macau Health Bureau (SSM) registration number on all invoices. Medical claims from Macau government hospitals (Centro Hospitalar Conde de Sao Januario and associated clinics) are processed at SSM-gazetted rates. Private facility claims in Macau follow the same R&C methodology as Hong Kong but use a separate Macau-specific rate schedule reflecting lower market rates. Travel claims originating from Macau casinos and integrated resorts require additional verification — the claimant must provide hotel registration or entry records from the Gaming Inspection and Coordination Bureau (DICJ) if the claim involves an on-premises incident. Motor claims in Macau reference the Macau PSP (Corpo de Policia de Seguranca Publica) incident report."""
    },
    {
        "doc_id": "POL-CP-001",
        "doc_title": "Claims Processing Policy v3.2",
        "section_title": "1.9 Appeal Process",
        "doc_type": "policy",
        "last_updated": "2025-09-15",
        "section_content": """Claimants may appeal a claim decision within 60 calendar days of receiving the decision notice. Appeals must be submitted in writing using Form AP-100, accompanied by any additional evidence supporting the appeal. First-level appeals are reviewed by a senior adjuster not involved in the original decision — the review must be completed within 15 business days. If the first-level appeal is denied, the claimant may escalate to the Claims Appeals Committee, which comprises the Head of Claims, the Chief Medical Officer (for medical claims), and an independent claims assessor. The Committee convenes within 20 business days and issues a binding decision within 10 business days of the hearing. If the claimant remains dissatisfied after the internal process, they may refer the complaint to the Insurance Complaints Bureau (ICB) or the Insurance Authority under the GL-18 complaints handling framework. All appeal outcomes are logged in the Claims Management System and included in the quarterly claims quality report to the Board Risk Committee."""
    },
    {
        "doc_id": "POL-CP-001",
        "doc_title": "Claims Processing Policy v3.2",
        "section_title": "1.10 Dispute Resolution",
        "doc_type": "policy",
        "last_updated": "2025-09-15",
        "section_content": """Disputes between the insurer and policyholders that cannot be resolved through the appeals process are subject to the following resolution hierarchy: (1) Mediation — conducted through the Hong Kong Mediation Centre or Macau Mediation Centre, with costs shared equally between parties for claims under HKD 1,000,000; (2) Arbitration — for claims exceeding HKD 1,000,000, disputes are referred to the Hong Kong International Arbitration Centre (HKIAC) under HKIAC Administered Arbitration Rules. The arbitration panel consists of one arbitrator agreed by both parties or, failing agreement, appointed by HKIAC. The language of arbitration is English or Chinese at the claimant's election. Disputes involving Macau policyholders may alternatively be referred to the Macau World Trade Centre Arbitration Centre. Provider disputes (e.g., fee disagreements, panel status challenges) follow a separate track managed by the Provider Relations team with escalation to the Chief Operations Officer."""
    },
    {
        "doc_id": "POL-CP-001",
        "doc_title": "Claims Processing Policy v3.2",
        "section_title": "1.11 Claims Reserve Management",
        "doc_type": "policy",
        "last_updated": "2025-09-15",
        "section_content": """An initial reserve must be established for every claim at the point of registration. Reserve amounts follow the Reserving Guidelines issued by the Actuarial Department. Minimum initial reserves by claim type: medical — HKD 15,000; motor — HKD 30,000; property — HKD 50,000; travel — HKD 10,000; life — sum assured or HKD 500,000 (whichever is lower for the initial reserve). Reserves must be reviewed at every claim touchpoint and at minimum every 30 calendar days for open claims. Reserve changes exceeding 50% of the prior reserve or any single increase above HKD 200,000 require Team Lead approval. Reserves exceeding HKD 2,000,000 require sign-off from the Head of Claims and the Chief Actuary. The Finance team reconciles reserves against the claims ledger monthly. Any unexplained variance above 5% triggers an investigation. The quarterly reserve adequacy review is presented to the Board Risk Committee."""
    },
    {
        "doc_id": "POL-CP-001",
        "doc_title": "Claims Processing Policy v3.2",
        "section_title": "1.12 Data Quality and Record Keeping",
        "doc_type": "policy",
        "last_updated": "2025-09-15",
        "section_content": """All claim records must be maintained in the Claims Management System (CMS) — ClaimsXpert v6.2. Paper documents must be scanned and uploaded within 2 business days of receipt. The minimum data quality standards require: (a) all mandatory fields completed before a claim progresses past the registration stage; (b) consistent use of ICD-10 diagnosis codes for medical claims; (c) accurate capture of provider registration numbers; (d) policy number validation against the Policy Administration System at intake; (e) claimant identity verification against the CRM. Data quality audits are conducted quarterly by the Business Intelligence team. The target data completeness score is 98% across all claim records. Records must be retained for a minimum of 7 years after claim closure, or 10 years for claims involving litigation or regulatory investigation. Access to claim records is controlled through role-based permissions aligned with the Information Security Policy. Bulk data extracts require approval from the Data Protection Officer."""
    },
    {
        "doc_id": "POL-CP-001",
        "doc_title": "Claims Processing Policy v3.2",
        "section_title": "1.13 Delegated Authority Limits",
        "doc_type": "policy",
        "last_updated": "2025-09-15",
        "section_content": """Claims approval authority is delegated based on role and claim value. Junior Adjuster (Grade A1-A2): approve claims up to HKD 50,000. Senior Adjuster (Grade A3-A4): approve claims up to HKD 200,000. Team Lead: approve claims up to HKD 500,000. Claims Manager: approve claims up to HKD 2,000,000. Head of Claims: approve claims up to HKD 10,000,000. Claims exceeding HKD 10,000,000 require co-approval from the Head of Claims and the Chief Financial Officer. Ex-gratia payments (outside policy terms) require approval one level above the normal authority limit, with a minimum of Team Lead approval. All delegated authority limits are reviewed annually by Internal Audit. Temporary delegations during staff absence must be documented on Form DA-100 and approved by the delegator's direct superior. No individual may approve a claim where they have a personal interest or relationship with the claimant — such cases must be escalated to the next authority level."""
    },
    {
        "doc_id": "POL-CP-001",
        "doc_title": "Claims Processing Policy v3.2",
        "section_title": "1.14 Outsourced Claims Processing",
        "doc_type": "policy",
        "last_updated": "2025-09-15",
        "section_content": """Third-party administrators (TPAs) processing claims on behalf of the Company must comply with this policy in full. TPA contracts include: (a) adherence to all SLAs defined in Section 1.4; (b) mandatory use of our CMS for all claim registrations; (c) daily data submission to our data warehouse; (d) quarterly on-site audits by our Quality Assurance team; (e) staff background checks equivalent to our internal standards. TPAs must not approve claims exceeding HKD 100,000 without referral to our in-house team. The maximum proportion of total claims volume processed by TPAs must not exceed 30% in any calendar quarter. TPA performance is reviewed monthly against a balanced scorecard covering accuracy (target: >97%), turnaround time (target: within SLA 95% of the time), customer satisfaction (target: NPS > 40), and compliance (target: zero material findings). Material underperformance triggers a remediation plan with a 60-day cure period. Persistent underperformance may result in contract termination with 90 days notice."""
    },
    {
        "doc_id": "POL-CP-001",
        "doc_title": "Claims Processing Policy v3.2",
        "section_title": "1.15 Regulatory Reporting of Claims",
        "doc_type": "policy",
        "last_updated": "2025-09-15",
        "section_content": """the Company must submit the following claims-related regulatory returns: (a) Insurance Authority Form C1 — quarterly claims statistics by line of business, due within 30 days of quarter-end; (b) Insurance Authority Form C3 — annual large loss report for individual claims exceeding HKD 5,000,000, due within 60 days of year-end; (c) HKMA suspicious transaction reports (STRs) — within 3 business days of identification; (d) Insurance Fraud Bureau reports — within 10 business days of confirmed fraud; (e) Macau ASM statistical returns — quarterly, due within 45 days of quarter-end. The Compliance team is responsible for compiling and submitting all regulatory returns. The Claims Department must provide underlying data by the 15th of the month following quarter-end. Late submission attracts regulatory penalties of up to HKD 500,000 per instance and is reported to the Board Audit Committee. Any data discrepancy between internal reports and regulatory submissions must be investigated and resolved before filing."""
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1b. Fraud Investigation Guidelines

# COMMAND ----------

fraud_investigation_sections = [
    {
        "doc_id": "POL-FI-002",
        "doc_title": "Fraud Investigation Guidelines",
        "section_title": "2.1 Investigation Triggers and Thresholds",
        "doc_type": "guideline",
        "last_updated": "2025-07-01",
        "section_content": """The Special Investigations Unit (SIU) initiates a formal investigation when any of the following triggers are met: (a) automated fraud score of 70 or above from FraudGuard v4.1; (b) manual referral from a claims adjuster based on professional judgment; (c) tip-off from the fraud hotline (hotline number: 2888-FRAUD); (d) external intelligence from the Insurance Fraud Bureau (IFB), law enforcement, or peer insurers; (e) pattern detection from the quarterly analytics review identifying anomalous clustering. Quantitative thresholds for automatic investigation: single claim exceeding HKD 500,000 with a fraud score above 50; cumulative claims from a single provider exceeding HKD 2,000,000 in a rolling 12-month period with more than 20% above-average billing; more than 5 claims from a single claimant in 6 months where total value exceeds HKD 300,000. The SIU maintains a risk-rated case backlog — Priority 1 (suspected organised fraud ring) cases must commence within 24 hours; Priority 2 (individual high-value) within 48 hours; Priority 3 (pattern-based, lower value) within 5 business days."""
    },
    {
        "doc_id": "POL-FI-002",
        "doc_title": "Fraud Investigation Guidelines",
        "section_title": "2.2 Evidence Collection Procedures",
        "doc_type": "guideline",
        "last_updated": "2025-07-01",
        "section_content": """All evidence must be collected and preserved in accordance with the chain-of-custody protocol to ensure admissibility in legal proceedings. Digital evidence: claim forms, system logs, email communications, and portal activity logs must be extracted using the CMS forensic export tool (ClaimsXpert Forensic Module) and stored in the SIU Evidence Repository with SHA-256 hash verification. Physical evidence: original documents, surveillance footage, and physical artefacts must be catalogued on Form EV-100, photographed, and stored in the SIU secure evidence room (access restricted to SIU Grade S3+ personnel). Witness statements must be recorded on Form EV-200, signed and dated by the witness and the interviewing investigator. Provider site visits require advance authorisation from the SIU Manager using Form EV-300 and must be conducted by at least two investigators. Medical record requests must go through the formal Section 58 request process under the Personal Data (Privacy) Ordinance. All evidence must be indexed in the SIU Case Management System within 24 hours of collection."""
    },
    {
        "doc_id": "POL-FI-002",
        "doc_title": "Fraud Investigation Guidelines",
        "section_title": "2.3 Types of Insurance Fraud",
        "doc_type": "guideline",
        "last_updated": "2025-07-01",
        "section_content": """The SIU classifies fraud into the following categories for investigation and reporting purposes. Opportunistic fraud: exaggeration of legitimate claims — inflated repair costs, extended treatment periods, or additional items on loss schedules. This represents approximately 60% of detected fraud by volume. Premeditated fraud: staged accidents, fabricated injuries, or fictitious claims with no underlying insured event. Organised fraud: coordinated schemes involving multiple parties — fraud rings typically include a policyholder, a provider (often a medical clinic or repair shop), and sometimes an intermediary or agent. Phantom provider fraud: billing from non-existent or de-registered providers. Common indicators include: providers with no physical premises, unusually high volume from a single provider, billing for services on dates the provider was closed, and identical treatment patterns across unrelated patients. Internal fraud: collusion between company staff and external parties. Indicators include adjusters consistently processing claims from the same provider, overriding fraud flags without documented justification, and accessing claim files outside their assigned portfolio."""
    },
    {
        "doc_id": "POL-FI-002",
        "doc_title": "Fraud Investigation Guidelines",
        "section_title": "2.4 Fraud Indicator Red Flags",
        "doc_type": "guideline",
        "last_updated": "2025-07-01",
        "section_content": """Investigators and adjusters should be alert to the following red flags. Claim-level indicators: claim submitted very close to policy inception or renewal; claim amount just below a threshold requiring additional approval; multiple claims for similar incidents in a short period; claimant unusually knowledgeable about claims procedures; claimant pressures for rapid settlement; inconsistencies between the claim narrative and medical/repair reports. Provider-level indicators: provider bills at consistently higher rates than peers; provider has a disproportionate number of claims from a single insurer; provider submits claims for services not typically associated with their specialty; provider address is a residential property or virtual office; provider's patient volume exceeds physical capacity. Policyholder-level indicators: recent increase in coverage shortly before a claim; history of claims across multiple insurers; address in a high-fraud-risk zone (as identified by the quarterly fraud heat map); policyholder has financial difficulties (verified through credit bureau check where permitted). Network indicators: shared phone numbers, addresses, or bank accounts across ostensibly unrelated claims or providers."""
    },
    {
        "doc_id": "POL-FI-002",
        "doc_title": "Fraud Investigation Guidelines",
        "section_title": "2.5 Investigation Methodology",
        "doc_type": "guideline",
        "last_updated": "2025-07-01",
        "section_content": """All SIU investigations follow the structured methodology: Phase 1 — Preliminary Assessment (1-3 days): review the referral, gather available documentation from the CMS, check internal databases and watchlists, and make a proceed/decline decision. Document the assessment on Form INV-100. Phase 2 — Detailed Investigation (5-20 business days depending on priority): conduct data analysis (claims history, provider billing patterns, network analysis), obtain external records (medical records, police reports, financial records where authorised), perform surveillance if warranted (approval required from SIU Manager for Priority 2, SIU Director for Priority 3), and conduct interviews with the claimant, provider, and witnesses. Phase 3 — Analysis and Conclusion (3-5 business days): synthesise findings, prepare the investigation report (Form INV-200), and make a recommendation — confirmed fraud, suspected fraud (insufficient evidence), or no fraud found. Phase 4 — Action: for confirmed fraud cases, recommend claim denial, policy cancellation, recoveries action, and referral to law enforcement where the loss exceeds HKD 100,000 or involves organised activity."""
    },
    {
        "doc_id": "POL-FI-002",
        "doc_title": "Fraud Investigation Guidelines",
        "section_title": "2.6 Escalation Matrix",
        "doc_type": "guideline",
        "last_updated": "2025-07-01",
        "section_content": """The following escalation matrix governs when cases must be elevated beyond the SIU. Escalation to Legal Department: all confirmed fraud cases where recovery action or litigation is contemplated; cases where the suspect has engaged legal representation; cases involving potential defamation risk (e.g., wrongful accusation). Escalation to Compliance: all cases involving suspected money laundering or terrorist financing; cases requiring a Suspicious Transaction Report (STR); cases involving politically exposed persons (PEPs). Escalation to Senior Management (C-suite): any case involving potential loss exceeding HKD 5,000,000; cases involving employees at Grade M1 or above; cases likely to attract media attention; cases involving a broker or agent relationship generating more than HKD 10,000,000 annual premium. Referral to Law Enforcement (Hong Kong Police Commercial Crime Bureau or Macau Judiciary Police): all confirmed fraud cases with losses exceeding HKD 100,000; all cases involving organised fraud rings regardless of value; cases involving forgery of official documents; cases where the suspect poses a flight risk. The SIU Director must approve all law enforcement referrals and notify the Chief Risk Officer within 24 hours."""
    },
    {
        "doc_id": "POL-FI-002",
        "doc_title": "Fraud Investigation Guidelines",
        "section_title": "2.7 Surveillance Guidelines",
        "doc_type": "guideline",
        "last_updated": "2025-07-01",
        "section_content": """Surveillance may be conducted only when there is a reasonable suspicion of fraud and other investigation methods have been exhausted or are insufficient. Authorisation requirements: basic surveillance (public-area observation, social media review) requires SIU Manager approval; extended surveillance (multi-day physical observation, covert operations) requires SIU Director approval and legal review. All surveillance must comply with the Personal Data (Privacy) Ordinance (PDPO) and the Interception of Communications and Surveillance Ordinance (ICSO) where applicable. Surveillance operatives must be licensed investigators registered with the Security and Guarding Services Industry Authority. Maximum surveillance duration: 10 consecutive days per authorisation; extensions require re-approval. Surveillance reports must include: date, time, location, weather conditions, subjects observed, activities documented, and photographic/video evidence log. Social media investigations must be conducted using anonymised accounts and must not involve deceptive interaction with the subject (e.g., sending friend requests under false pretences). All surveillance records are retained for 7 years or the duration of any related legal proceedings, whichever is longer."""
    },
    {
        "doc_id": "POL-FI-002",
        "doc_title": "Fraud Investigation Guidelines",
        "section_title": "2.8 Interview and Statement Taking",
        "doc_type": "guideline",
        "last_updated": "2025-07-01",
        "section_content": """All interviews must be conducted professionally and in accordance with the principles of natural justice. The interviewee must be informed of: the general nature of the enquiry (without disclosing suspicion of fraud), their right to have a support person present (but not a legal advisor acting in a representative capacity at the initial interview stage), and that their statement may be used in any subsequent proceedings. Interviews must be conducted by two SIU investigators — one leading the interview, one taking notes. Audio recording is permitted with the interviewee's consent and is recommended for all interviews in confirmed fraud cases. Video recording requires SIU Director approval. Interview notes must be transcribed onto Form INT-100 within 24 hours and offered to the interviewee for review and signature. Leading questions should be avoided. If the interviewee makes admissions, the interviewer must caution them that any admission may be used in legal or regulatory proceedings and offer the opportunity to have legal representation before continuing. Interpreters must be used when the interviewee's primary language is not the language of the interview. All interview records are classified as Confidential."""
    },
    {
        "doc_id": "POL-FI-002",
        "doc_title": "Fraud Investigation Guidelines",
        "section_title": "2.9 Case Closure Criteria",
        "doc_type": "guideline",
        "last_updated": "2025-07-01",
        "section_content": """An investigation may be closed when one of the following outcomes is reached and documented: (a) Confirmed Fraud — evidence meets the civil standard of proof (balance of probabilities) and has been reviewed by the Legal Department. Actions taken: claim denied, policy cancelled, recovery action initiated, law enforcement referral made (if applicable), and provider delisted (if applicable). (b) Suspected Fraud — Insufficient Evidence: indicators of fraud were identified but evidence does not meet the threshold for action. The case is placed on a 12-month watch list and the claimant/provider profile is flagged in the CMS for enhanced scrutiny on future claims. (c) No Fraud Found — investigation concluded that the claim is legitimate. The claim is returned to normal processing with a priority flag to compensate for any delay. The claimant must not be informed that an investigation was conducted unless required by law. (d) Withdrawn — the claimant withdrew the claim during investigation. The withdrawal is noted, and the case is retained for pattern analysis. Case closure requires SIU Manager approval for Priority 2-3 cases and SIU Director approval for Priority 1 cases. All closed cases are summarised in the monthly SIU Activity Report."""
    },
    {
        "doc_id": "POL-FI-002",
        "doc_title": "Fraud Investigation Guidelines",
        "section_title": "2.10 Recovery and Restitution",
        "doc_type": "guideline",
        "last_updated": "2025-07-01",
        "section_content": """For confirmed fraud cases, recovery of losses is pursued through the following channels in order of preference: (a) direct restitution — the fraudster voluntarily repays the overpaid or fraudulently obtained amount, documented via a settlement agreement prepared by Legal; (b) civil litigation — filed in the District Court for amounts up to HKD 3,000,000, or the Court of First Instance for higher amounts; (c) criminal restitution — requested as part of the criminal prosecution where law enforcement has been engaged; (d) third-party recovery — from complicit providers, agents, or intermediaries under joint and several liability. Recovery targets: 80% of confirmed fraud losses should be subject to active recovery action; the target recovery rate is 40% of total confirmed fraud value. The SIU maintains a recovery ledger reconciled monthly with the Finance Department. Write-off of unrecoverable fraud losses requires approval from the CFO for amounts up to HKD 1,000,000 and the Board Audit Committee for amounts exceeding HKD 1,000,000. Recovery costs (legal fees, investigator costs) are tracked against each case and included in the annual fraud cost analysis."""
    },
    {
        "doc_id": "POL-FI-002",
        "doc_title": "Fraud Investigation Guidelines",
        "section_title": "2.11 Fraud Analytics and Reporting",
        "doc_type": "guideline",
        "last_updated": "2025-07-01",
        "section_content": """The SIU produces the following regular reports: Monthly SIU Activity Report — new referrals, open cases, closed cases, confirmed fraud value, recovery amounts, and case ageing analysis; submitted to the Chief Risk Officer by the 10th of each month. Quarterly Fraud Trend Analysis — fraud patterns by type, region, provider, and product line; emerging schemes; and peer benchmarking against IFB industry data; presented to the Risk Committee. Annual Fraud Report — comprehensive annual summary including total fraud detected, fraud-to-claims ratio (target: keep below 1.5% of gross claims paid), cost-benefit analysis of fraud detection investment, and strategic recommendations; presented to the Board. Ad-hoc alerts — issued immediately when a new fraud scheme or organised ring is identified, distributed to all claims staff and relevant business units. The Data Science team maintains predictive models that are retrained quarterly using confirmed fraud outcomes. Model performance (precision, recall, F1 score) is tracked, and any degradation below agreed thresholds triggers a model review."""
    },
    {
        "doc_id": "POL-FI-002",
        "doc_title": "Fraud Investigation Guidelines",
        "section_title": "2.12 Confidentiality and Information Barriers",
        "doc_type": "guideline",
        "last_updated": "2025-07-01",
        "section_content": """All SIU activities are classified as Confidential or higher. Information about active investigations must not be disclosed to: the subject of the investigation, claims adjusters not directly involved in the case (need-to-know principle), external parties without Legal Department approval, or media under any circumstances (all media enquiries are directed to Corporate Communications). SIU staff must maintain information barriers — they must not discuss active cases in open-plan areas, must use encrypted communication channels (approved email and messaging only, no personal devices), and must store all case materials in the secure SIU section of the document management system. Breach of confidentiality is a disciplinary offence potentially leading to termination. When sharing investigation outcomes with claims processing teams, only the decision (approve, deny, refer) is communicated — not the investigation detail. External sharing with law enforcement or peer insurers under the IFB framework requires a formal information-sharing agreement and SIU Director sign-off. Annual confidentiality training is mandatory for all SIU staff and claims staff with SIU system access."""
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1c. Insurance Authority (IA) Regulatory Requirements

# COMMAND ----------

regulatory_sections = [
    {
        "doc_id": "REG-IA-003",
        "doc_title": "Insurance Authority (IA) Regulatory Requirements",
        "section_title": "3.1 Reporting Obligations for Suspicious Claims",
        "doc_type": "regulation",
        "last_updated": "2025-11-01",
        "section_content": """Under the Anti-Money Laundering and Counter-Terrorist Financing Ordinance (AMLO) Cap. 615, the Company is required to file Suspicious Transaction Reports (STRs) with the Joint Financial Intelligence Unit (JFIU) when there are reasonable grounds to suspect that property is proceeds of an indictable offence or is terrorist property. STRs must be filed within 3 business days of the suspicion being formed, using the JFIU's online filing system (goAML). The compliance officer (MLRO — Money Laundering Reporting Officer) is the sole authorised filer. Claims-related STR triggers include: claim payments directed to jurisdictions on the FATF grey or black list; claims involving known or suspected proceeds of crime; claims where the policyholder's source of funds cannot be satisfactorily verified; and claims involving PEPs (Politically Exposed Persons) as defined in Schedule 2 of AMLO. The Insurance Authority's GL-3 guideline requires that STR filing records be retained for 6 years and that staff training on STR identification be conducted annually. Failure to file an STR carries criminal penalties of up to HKD 500,000 fine and 3 months imprisonment."""
    },
    {
        "doc_id": "REG-IA-003",
        "doc_title": "Insurance Authority (IA) Regulatory Requirements",
        "section_title": "3.2 Anti-Money Laundering Requirements",
        "doc_type": "regulation",
        "last_updated": "2025-11-01",
        "section_content": """the Company's AML programme must comply with the Insurance Authority's GL-3 Guideline on AML/CFT. Key requirements: Customer Due Diligence (CDD) — identity verification at policy inception using reliable, independent source documents; ongoing monitoring of the business relationship including claims patterns; Enhanced Due Diligence (EDD) for high-risk customers including PEPs, customers from high-risk jurisdictions, and complex or unusually large transactions. Claims-specific AML controls: all claim payments exceeding HKD 120,000 are screened against sanctions lists (UN, OFAC, EU, HKMA) before disbursement; claim payments to third parties (other than verified medical providers) require additional verification; cash refund of premiums is prohibited for amounts exceeding HKD 8,000; and claims settled by assignment to a third party require AML screening of the assignee. The compliance monitoring programme includes: transaction monitoring using automated rules (25 rules currently active in the AML monitoring system); quarterly AML risk assessment by business line; annual independent review by Internal Audit or external assessor; and immediate remediation of any findings rated High or Critical."""
    },
    {
        "doc_id": "REG-IA-003",
        "doc_title": "Insurance Authority (IA) Regulatory Requirements",
        "section_title": "3.3 Data Protection — PDPO Compliance",
        "doc_type": "regulation",
        "last_updated": "2025-11-01",
        "section_content": """All claims processing activities must comply with the Personal Data (Privacy) Ordinance (PDPO) Cap. 486. The six Data Protection Principles (DPPs) apply: DPP1 — data collected for claims must be necessary and directly related to the claims purpose; DPP2 — personal data must be accurate and not kept longer than necessary; DPP3 — data must not be used for purposes other than the purpose for which it was collected without consent; DPP4 — reasonable security measures must be in place; DPP5 — policies and practices on personal data handling must be transparent; DPP6 — data subjects have the right to access and correct their data. Claims-specific requirements: claimant medical records are classified as sensitive personal data requiring explicit consent for collection (obtained via the claim form consent clause); data sharing with reinsurers must be covered by the privacy notice provided at policy inception; cross-border transfer of claims data (including to group companies) requires compliance with DPP3 and the recommended model clauses; data retention periods for claims: active claims — retained for duration of the claim; closed claims — 7 years post-closure (10 years for litigated claims); and SIU investigation records — 7 years post-case closure. The Data Protection Officer must be consulted before any new claims data processing activity or system implementation."""
    },
    {
        "doc_id": "REG-IA-003",
        "doc_title": "Insurance Authority (IA) Regulatory Requirements",
        "section_title": "3.4 Record Retention Policies",
        "doc_type": "regulation",
        "last_updated": "2025-11-01",
        "section_content": """The Insurance Authority requires insurers to maintain adequate records of all insurance transactions. Minimum retention periods: policy documents and endorsements — 7 years after policy expiry; claims records (including all supporting documents, assessor reports, and adjuster notes) — 7 years after claim closure; financial records relating to claims payments — 7 years per the Companies Ordinance and Inland Revenue Ordinance requirements; AML/CFT records (including CDD documents and transaction monitoring records) — 6 years per AMLO; SIU investigation files — 7 years after case closure or conclusion of any legal proceedings, whichever is later; regulatory correspondence — 10 years. Records must be stored in a manner that allows retrieval within 5 business days for regulatory examination purposes. Electronic records must be backed up in accordance with the IT Disaster Recovery Plan (RPO: 4 hours, RTO: 24 hours for critical claims systems). Physical records in offsite storage must be retrievable within 10 business days. Annual record disposal must follow the approved disposal schedule and be documented on Form RR-100, with sign-off from the Compliance team. No records may be destroyed if they are subject to a legal hold notice."""
    },
    {
        "doc_id": "REG-IA-003",
        "doc_title": "Insurance Authority (IA) Regulatory Requirements",
        "section_title": "3.5 Policyholder Notification Requirements",
        "doc_type": "regulation",
        "last_updated": "2025-11-01",
        "section_content": """The Insurance Authority's GL-28 guideline mandates specific notifications to policyholders during the claims process. Required notifications: (a) Acknowledgement of claim receipt — within 2 business days, including claim reference number, assigned adjuster contact details, and expected processing timeline; (b) Request for additional information — within 5 business days of identifying a documentation gap, clearly specifying what is required and the deadline for submission; (c) Claim decision notice — within 3 business days of the decision, including: the amount payable (or reason for denial), detailed calculation of the settlement amount, deductions applied (excess, co-payment, policy limits), and appeal rights and process; (d) Payment confirmation — within 2 business days of payment issuance, including payment method, amount, and expected clearing date. All notifications must be in the policyholder's preferred language (English or Chinese). Notifications involving denial or partial settlement must include reference to the specific policy terms applied. The Insurance Authority may request evidence of notification compliance during examinations. Non-compliance with GL-28 notification requirements may result in a public reprimand or fine of up to HKD 10,000,000."""
    },
    {
        "doc_id": "REG-IA-003",
        "doc_title": "Insurance Authority (IA) Regulatory Requirements",
        "section_title": "3.6 Conduct of Business Requirements",
        "doc_type": "regulation",
        "last_updated": "2025-11-01",
        "section_content": """Under the Insurance Ordinance and the IA's GL-15 guideline, all claims handling must adhere to principles of fair dealing. Specific requirements: claims must be assessed objectively based on policy terms and available evidence; adjusters must not adopt an adversarial stance toward claimants; settlement offers must not be unreasonably low with the intent to discourage genuine claims; claimants must not be pressured to accept settlement offers; all material facts relevant to the claim decision must be considered. Conflicts of interest: adjusters must declare any personal or financial interest in a claim or relationship with the claimant, provider, or broker — such cases must be reassigned. The IA conducts thematic reviews of claims handling practices approximately every 2-3 years. Recent focus areas include: turnaround times for medical claims, adequacy of denial explanations, and treatment of vulnerable customers (elderly, disabled, non-native language speakers). Findings from IA examinations are reported to the Board with a mandatory remediation action plan. Persistent non-compliance may result in conditions being imposed on the insurer's licence."""
    },
    {
        "doc_id": "REG-IA-003",
        "doc_title": "Insurance Authority (IA) Regulatory Requirements",
        "section_title": "3.7 Capital Adequacy and Claims Reserving Regulations",
        "doc_type": "regulation",
        "last_updated": "2025-11-01",
        "section_content": """The Insurance Authority's risk-based capital (RBC) framework requires adequate reserving for outstanding claims. The appointed actuary must certify claims reserves annually using methods compliant with the Actuarial Society of Hong Kong (ASHK) standards. Reserve calculations must include: case reserves for reported claims, incurred but not reported (IBNR) reserves, claims handling expense reserves, and a risk margin. The IA's minimum capital requirement includes a claims risk charge calculated as a percentage of net claims reserves (currently 10% for short-tail lines, 15% for long-tail lines). the Company maintains a target solvency ratio of 150% above the IA's prescribed capital amount. If the solvency ratio falls below 150%, the Chief Actuary must notify the CFO and prepare a capital restoration plan. If it falls below 120%, the IA must be notified within 14 days. Claims reserve adequacy is tested quarterly using run-off analysis, and any deterioration exceeding 5% of prior quarter reserves triggers an actuarial review. The appointed actuary presents the annual reserve certification to the Board Audit Committee, including commentary on uncertainty ranges and sensitivity to key assumptions."""
    },
    {
        "doc_id": "REG-IA-003",
        "doc_title": "Insurance Authority (IA) Regulatory Requirements",
        "section_title": "3.8 Complaints Handling — GL-18 Compliance",
        "doc_type": "regulation",
        "last_updated": "2025-11-01",
        "section_content": """The Insurance Authority's GL-18 guideline establishes requirements for complaints handling. All complaints related to claims must be: acknowledged within 3 business days; assigned a dedicated complaints handler independent of the original decision-maker; investigated and responded to within 30 calendar days (if complex, an interim response must be provided with a revised timeline); recorded in the central complaints register with categorisation (e.g., delay, quantum dispute, denial dispute, service quality, communication); and analysed for root causes and systemic issues. The complaints handler must have authority to overturn the original claim decision if warranted. If the complainant is dissatisfied with the internal resolution, they must be informed of their right to refer the matter to the Insurance Complaints Bureau (ICB) for claims up to HKD 1,200,000 or to the Insurance Authority for regulatory complaints. The Compliance team reports quarterly complaints statistics to the IA as part of the conduct returns. Trends indicating systemic issues (e.g., more than 5% complaint rate on a product line) trigger a product review by the Product Governance Committee. Annual complaints data is published in the corporate governance report as required by the IA's disclosure requirements."""
    },
    {
        "doc_id": "REG-IA-003",
        "doc_title": "Insurance Authority (IA) Regulatory Requirements",
        "section_title": "3.9 Outsourcing and Third-Party Risk — GL-14",
        "doc_type": "regulation",
        "last_updated": "2025-11-01",
        "section_content": """Under the IA's GL-14 guideline, outsourcing of material claims processing activities requires: prior notification to the IA (30 days before commencement for material outsourcing); a documented risk assessment covering operational, compliance, concentration, and data security risks; a legally binding outsourcing agreement covering service levels, audit rights, data protection obligations, business continuity requirements, and termination provisions; ongoing monitoring including quarterly performance reviews and annual on-site assessments; and a viable exit strategy ensuring service continuity if the outsourcing arrangement is terminated. Material outsourcing is defined as any arrangement where the outsourced activity, if disrupted, would materially impact policyholders or the insurer's ability to manage risks. The outsourcing register must be maintained by the Compliance team and updated within 10 business days of any change. Cross-border outsourcing (e.g., claims processing in mainland China or Southeast Asia) requires enhanced oversight including data localisation compliance and local regulatory requirements. The Board retains ultimate responsibility for outsourced activities and must receive an annual outsourcing risk report from the Chief Risk Officer."""
    },
    {
        "doc_id": "REG-IA-003",
        "doc_title": "Insurance Authority (IA) Regulatory Requirements",
        "section_title": "3.10 Climate Risk and ESG Disclosure",
        "doc_type": "regulation",
        "last_updated": "2025-11-01",
        "section_content": """The Insurance Authority requires insurers to integrate climate-related risks into their risk management frameworks, aligned with the Task Force on Climate-Related Financial Disclosures (TCFD) recommendations. Claims-related climate risk considerations: increasing frequency and severity of natural catastrophe claims (typhoons, flooding) in Hong Kong and Macau; potential for correlated losses across property, motor, and business interruption lines; need for forward-looking claims reserving that accounts for climate trends rather than relying solely on historical experience. the Company must: conduct an annual climate risk stress test covering physical risks (acute events such as Signal 10 typhoons and chronic changes such as sea-level rise) and transition risks (policy and regulatory changes affecting insured assets); disclose climate risk exposures in the annual report using the TCFD four-pillar framework (Governance, Strategy, Risk Management, Metrics and Targets); and report to the IA on the financial impact of extreme weather events on claims within 30 days of a declared natural disaster. The appointed actuary must consider climate trends in the annual reserving exercise and document any adjustments made for climate-related claims experience."""
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1d. Cybersecurity Incident Response Plan

# COMMAND ----------

cybersecurity_sections = [
    {
        "doc_id": "POL-CS-004",
        "doc_title": "Cybersecurity Incident Response Plan",
        "section_title": "4.1 Incident Classification — Severity Levels",
        "doc_type": "sop",
        "last_updated": "2025-10-01",
        "section_content": """All cybersecurity incidents are classified into four severity levels. P1 — Critical: active data breach involving customer PII or financial data; ransomware affecting production systems; complete loss of a critical business system (claims portal, policy admin, payment gateway); or compromise of privileged administrative accounts. Response time: Security Operations Centre (SOC) acknowledgement within 15 minutes; Incident Commander on-call activation within 30 minutes; initial containment actions within 1 hour. P2 — High: attempted data exfiltration detected and blocked; malware infection on multiple endpoints (more than 10); partial system outage affecting business operations; or compromise of standard user accounts with access to sensitive data. Response time: SOC acknowledgement within 30 minutes; investigation commenced within 2 hours; containment within 4 hours. P3 — Medium: isolated malware infection (fewer than 10 endpoints); successful phishing attack with credential compromise but no evidence of lateral movement; policy violation involving data handling; or vulnerability exploitation attempt on non-critical systems. Response time: SOC acknowledgement within 2 hours; investigation within 8 hours; remediation within 48 hours. P4 — Low: unsuccessful attack attempts; minor policy violations; vulnerability scan findings requiring patching; or suspicious but unconfirmed activity. Response time: logged and queued for review within 24 hours; remediation per standard patching cycle."""
    },
    {
        "doc_id": "POL-CS-004",
        "doc_title": "Cybersecurity Incident Response Plan",
        "section_title": "4.2 Response Time SLAs by Severity",
        "doc_type": "sop",
        "last_updated": "2025-10-01",
        "section_content": """Detailed Service Level Agreements for incident response. P1 Critical SLAs: detection to acknowledgement — 15 minutes (24/7); initial assessment completed — 30 minutes; Incident Commander engaged — 30 minutes; executive notification (CISO, CRO, CEO) — 1 hour; initial containment action — 1 hour; status update to stakeholders — every 2 hours; full containment — 4 hours; root cause identification — 24 hours; full remediation — 72 hours; post-incident review meeting — within 5 business days. P2 High SLAs: detection to acknowledgement — 30 minutes; initial assessment — 2 hours; team lead engaged — 2 hours; CISO notification — 4 hours; containment — 4 hours; status updates — every 4 hours; remediation — 48 hours; post-incident review — within 10 business days. P3 Medium SLAs: acknowledgement — 2 hours; assessment — 8 hours; containment — 24 hours; remediation — 5 business days; review — monthly batch review. P4 Low SLAs: acknowledgement — 24 hours; remediation — per standard schedule; review — quarterly batch review. SLA compliance is tracked in the ITSM platform (ServiceNow) and reported monthly to the Technology Risk Committee. SLA breaches trigger automatic escalation to the next management level."""
    },
    {
        "doc_id": "POL-CS-004",
        "doc_title": "Cybersecurity Incident Response Plan",
        "section_title": "4.3 Incident Response Team Structure",
        "doc_type": "sop",
        "last_updated": "2025-10-01",
        "section_content": """The Cybersecurity Incident Response Team (CSIRT) comprises the following roles. Incident Commander (IC): the senior security officer on rotation who assumes overall command of the incident response. The IC has authority to make containment decisions including system isolation, account suspension, and network segmentation. SOC Analysts (L1/L2/L3): L1 analysts perform initial triage and classification; L2 analysts conduct detailed investigation and threat hunting; L3 analysts handle advanced forensics and malware analysis. Threat Intelligence Analyst: provides context from threat feeds, dark web monitoring, and industry ISACs (Information Sharing and Analysis Centres). Communications Lead: manages internal and external communications, coordinates with Corporate Communications for media enquiries, and manages regulatory notifications. IT Operations Liaison: coordinates with infrastructure, application, and network teams for containment and recovery actions. Legal Advisor: provides guidance on legal obligations, evidence preservation, and regulatory notification requirements. Business Continuity Manager: activates business continuity plans if the incident impacts service delivery. The CSIRT maintains a 24/7 on-call rotation with a maximum response time of 30 minutes for P1 activations. All CSIRT members must complete annual incident response training and participate in at least two tabletop exercises per year."""
    },
    {
        "doc_id": "POL-CS-004",
        "doc_title": "Cybersecurity Incident Response Plan",
        "section_title": "4.4 Escalation Procedures",
        "doc_type": "sop",
        "last_updated": "2025-10-01",
        "section_content": """Escalation follows a structured path based on severity and time elapsed. Level 1 — SOC: all incidents start here; SOC has authority to handle P3 and P4 incidents independently. Level 2 — CSIRT Manager: automatic escalation for all P2 incidents and any P3 incident not contained within SLA. Level 3 — CISO: automatic escalation for all P1 incidents; any P2 incident not contained within 4 hours; any incident involving customer data exposure; any incident requiring external notification. Level 4 — Crisis Management Team (CMT): activated for all P1 incidents with confirmed data breach; incidents likely to result in regulatory action; incidents with estimated financial impact exceeding HKD 5,000,000; and incidents requiring public disclosure. The CMT comprises: CEO, CFO, CRO, CISO, General Counsel, Head of Corporate Communications, and Head of Operations. The CMT convenes within 2 hours of activation (in person or video conference) and meets at minimum every 6 hours during an active P1 incident. External escalation: the Insurance Authority must be notified within 72 hours of a confirmed data breach affecting policyholders; the Office of the Privacy Commissioner must be notified if personal data is compromised; law enforcement (Hong Kong Police Cyber Security and Technology Crime Bureau) is engaged for criminal activity."""
    },
    {
        "doc_id": "POL-CS-004",
        "doc_title": "Cybersecurity Incident Response Plan",
        "section_title": "4.5 Data Breach Notification — 72-Hour Rule",
        "doc_type": "sop",
        "last_updated": "2025-10-01",
        "section_content": """When a data breach involving personal data is confirmed, the following notification obligations apply. Regulatory notification (72-hour rule): the Office of the Privacy Commissioner for Personal Data (PCPD) must be notified within 72 hours of breach confirmation. The notification must include: nature of the breach, categories and approximate number of data subjects affected, categories of personal data compromised, likely consequences, and measures taken or proposed to mitigate impact. The Insurance Authority must be notified in parallel for breaches affecting policyholders, using the IA's incident notification template. If the breach involves data subjects in Macau, the Macau GPDP (Office for Personal Data Protection) must also be notified. Individual notification: affected individuals must be notified without undue delay when the breach is likely to result in high risk to their rights — for example, exposure of identity document numbers, financial account details, or health information. The notification must include: description of the breach in plain language, name and contact details of the DPO, likely consequences, and measures individuals can take to protect themselves (e.g., monitoring financial accounts, changing passwords). Communication channels: email to the registered address, followed by postal mail if email delivery fails. For breaches affecting more than 10,000 individuals, a public notice on the company website is also required. Template notifications are pre-approved by Legal and stored in the Incident Response Toolkit (SharePoint: /CSIRT/Templates/)."""
    },
    {
        "doc_id": "POL-CS-004",
        "doc_title": "Cybersecurity Incident Response Plan",
        "section_title": "4.6 Containment Strategies",
        "doc_type": "sop",
        "last_updated": "2025-10-01",
        "section_content": """Containment is the highest priority after classification. Short-term containment (within SLA): isolate affected systems from the network using firewall rules or VLAN segmentation; disable compromised user accounts and revoke active sessions; block malicious IP addresses, domains, and URLs at the perimeter firewall and web proxy; quarantine infected endpoints using the EDR platform (CrowdStrike Falcon); preserve forensic images of affected systems before remediation (mandatory for P1 and P2). Long-term containment: implement temporary mitigating controls while root cause is being addressed — for example, enhanced monitoring, additional authentication requirements, or restricted access to affected systems; apply emergency patches for exploited vulnerabilities; rotate credentials for all accounts that may have been compromised (for P1 incidents, this includes all privileged accounts in the affected environment). Containment decision authority: P4/P3 — SOC analyst with team lead approval; P2 — CSIRT Manager; P1 — Incident Commander with CISO notification. Emergency system shutdown (pulling a system offline entirely) requires Incident Commander approval for P1, and CISO approval for P2. System restoration must follow the Recovery Procedures in Section 4.9 and requires sign-off from both the CSIRT and the system owner."""
    },
    {
        "doc_id": "POL-CS-004",
        "doc_title": "Cybersecurity Incident Response Plan",
        "section_title": "4.7 Evidence Preservation and Forensics",
        "doc_type": "sop",
        "last_updated": "2025-10-01",
        "section_content": """Digital forensic evidence must be preserved to support investigation, regulatory response, and potential legal action. For P1 and P2 incidents: full disk images of affected systems must be captured before any remediation action using forensically sound tools (FTK Imager or equivalent); memory dumps must be captured for systems with active malware or suspected compromise; network packet captures from the relevant time window must be extracted from the SIEM (Splunk) and NDR (Darktrace); all logs from affected systems, authentication systems, and security tools must be preserved and exported to the forensic investigation environment; chain of custody must be maintained using the Digital Evidence Log (Form DF-100) — each transfer of evidence must be documented with date, time, handler name, and purpose. For P3 incidents: log preservation is mandatory; full disk imaging is at the CSIRT Manager's discretion. Forensic analysis must be performed in an isolated forensic workstation not connected to the production network. External forensic firms (contracted: PwC Cybersecurity and Kroll) may be engaged for P1 incidents at the CISO's discretion. Forensic reports must be marked Attorney-Client Privileged when prepared in anticipation of litigation — Legal must be consulted before initiating the forensic engagement to establish privilege."""
    },
    {
        "doc_id": "POL-CS-004",
        "doc_title": "Cybersecurity Incident Response Plan",
        "section_title": "4.8 Communication Protocol During Incidents",
        "doc_type": "sop",
        "last_updated": "2025-10-01",
        "section_content": """Internal communications: all incident communications use the secure incident channel (Microsoft Teams: CSIRT-Incidents, encrypted). Status updates follow the SITREP (Situation Report) format: current status, actions taken, next steps, and estimated time to resolution. P1 SITREPs are distributed every 2 hours to the CMT distribution list. P2 SITREPs are distributed every 4 hours to the CISO and IT leadership. Email must not be used for incident communications if email systems may be compromised — fall back to the out-of-band communication channel (Signal group: CSIRT Emergency). External communications: no external communication about an active incident may be made without approval from the Communications Lead and General Counsel. Media enquiries are handled exclusively by Corporate Communications using approved talking points prepared by the Communications Lead. Customer-facing communications (e.g., service disruption notices) must be approved by the CMT for P1 incidents. Regulatory communications follow the templates and timelines in Section 4.5. Social media monitoring is activated during P1/P2 incidents to detect public disclosure or misinformation. The Communications Lead maintains a stakeholder notification matrix updated quarterly, listing contact details and preferred channels for all internal and external stakeholders."""
    },
    {
        "doc_id": "POL-CS-004",
        "doc_title": "Cybersecurity Incident Response Plan",
        "section_title": "4.9 Recovery Procedures",
        "doc_type": "sop",
        "last_updated": "2025-10-01",
        "section_content": """System recovery follows a structured process to ensure security before restoration of normal operations. Pre-recovery checklist: root cause identified and documented; vulnerability exploited has been patched or mitigated; compromised credentials have been rotated; forensic evidence has been preserved; and CSIRT has confirmed the threat actor is no longer active in the environment. Recovery steps: (a) rebuild affected systems from known-good images or backups (backups must be verified as uncompromised); (b) apply all outstanding security patches; (c) restore data from the most recent clean backup (verified by comparing checksums with pre-incident baselines where available); (d) conduct security validation testing — vulnerability scan, configuration review, and penetration test of the recovered systems; (e) implement enhanced monitoring for 30 days post-recovery (increased logging, additional alerting rules, and daily review by L2/L3 analysts); (f) gradual restoration of access — start with a limited user group, expand after 24 hours of stable operation. Recovery priority order for business systems: Tier 1 (RTO 4 hours) — claims portal, payment gateway, policy administration; Tier 2 (RTO 12 hours) — email, customer service systems, data warehouse; Tier 3 (RTO 48 hours) — reporting systems, development environments, non-critical applications. Recovery sign-off requires approval from both the CSIRT Manager and the business system owner."""
    },
    {
        "doc_id": "POL-CS-004",
        "doc_title": "Cybersecurity Incident Response Plan",
        "section_title": "4.10 Post-Incident Review Process",
        "doc_type": "sop",
        "last_updated": "2025-10-01",
        "section_content": """Every P1 and P2 incident requires a formal post-incident review (PIR). The PIR must be conducted within 5 business days of incident closure for P1 and 10 business days for P2. PIR participants: all CSIRT members involved, IT operations staff who participated in containment/recovery, affected business unit representatives, and Compliance (for incidents involving regulatory obligations). The PIR follows a blameless format focused on systemic improvements. PIR agenda: (a) timeline reconstruction — minute-by-minute for P1, hour-by-hour for P2; (b) what went well — effective controls, quick detection, good coordination; (c) what could be improved — gaps in detection, delays in response, communication breakdowns; (d) root cause analysis using the "5 Whys" methodology; (e) lessons learned and action items with owners and deadlines. The PIR report (Form PIR-100) is distributed to: CISO, CRO, Technology Risk Committee, and Board Risk Committee (for P1 incidents). Action items are tracked in the CSIRT action register and reviewed at the monthly security operations meeting. Trends from PIRs feed into the annual update of this Incident Response Plan. A tabletop exercise based on the most significant incident of the year is conducted annually, involving senior management and the CMT."""
    },
    {
        "doc_id": "POL-CS-004",
        "doc_title": "Cybersecurity Incident Response Plan",
        "section_title": "4.11 Third-Party Vendor Incident Management",
        "doc_type": "sop",
        "last_updated": "2025-10-01",
        "section_content": """When a cybersecurity incident originates from or affects a third-party vendor, the following procedures apply. Vendor notification obligation: all contracts with vendors handling the Company data must include a clause requiring the vendor to notify the Company of any security incident within 24 hours of detection. The notification must include: nature of the incident, the Company data potentially affected, containment actions taken, and a designated point of contact for the investigation. Upon receiving vendor notification: (a) assess the impact on the Company data and systems — classify using our P1-P4 framework; (b) request the vendor's incident report and forensic findings; (c) if the Company customer data is affected, treat as a the Company incident with full CSIRT activation; (d) engage the vendor management team and Legal to assess contractual obligations and potential liability; (e) consider activating backup vendors or manual processes if the affected vendor's service is disrupted. Key vendor categories requiring enhanced incident management: cloud infrastructure providers (AWS), SaaS applications processing claims or customer data, outsourced claims processing TPAs, and payment processing partners. The Vendor Risk team maintains a critical vendor register with incident contact details, tested annually during the vendor management review cycle. Vendor incident performance (notification timeliness, cooperation, remediation speed) is factored into the annual vendor risk assessment score."""
    },
    {
        "doc_id": "POL-CS-004",
        "doc_title": "Cybersecurity Incident Response Plan",
        "section_title": "4.12 Business Continuity During Cyber Incidents",
        "doc_type": "sop",
        "last_updated": "2025-10-01",
        "section_content": """When a cybersecurity incident results in system unavailability, the Business Continuity Plan (BCP) is activated in parallel with incident response. BCP activation criteria: any P1 incident affecting Tier 1 systems; any incident causing system outage exceeding 2 hours for Tier 1 or 8 hours for Tier 2 systems; ransomware affecting more than one business unit. Manual fallback procedures: claims processing — paper-based claim registration using Form BCP-CP01, manual logging in the offline claims register (spreadsheet on the secure BCP laptop), and batch entry into the CMS upon restoration; payment processing — manual payment authorisation via the BCP payment approval chain, with dual approval required for all payments; customer service — redirect online enquiries to the call centre, extend call centre hours, and activate the BCP customer communication script. Work-from-home restrictions during incidents: during P1 incidents, all CSIRT and IT operations staff must work from the primary office or designated DR site to ensure secure communications; business users may be directed to cease accessing corporate systems from personal devices until the all-clear is given. The BCP Coordinator conducts a daily impact assessment during extended outages and reports to the CMT on business impact, customer complaints, and estimated restoration timeline. BCP effectiveness is tested annually through a combined cyber-BCP exercise simulating a ransomware scenario with system isolation."""
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1e. Risk Appetite Framework

# COMMAND ----------

risk_appetite_sections = [
    {
        "doc_id": "POL-RA-005",
        "doc_title": "Risk Appetite Framework",
        "section_title": "5.1 Risk Tolerance Thresholds by Category",
        "doc_type": "policy",
        "last_updated": "2025-08-15",
        "section_content": """the Company's Risk Appetite Framework defines tolerance thresholds across five risk categories. Insurance Risk: loss ratio must remain within 55-70% across all product lines; combined ratio must not exceed 98%; single-event loss retention is capped at HKD 50,000,000 net (excess is ceded to reinsurance); catastrophe loss tolerance for a 1-in-200-year event is HKD 200,000,000. Credit Risk: investment portfolio credit quality must maintain a weighted average rating of A or better; maximum single-issuer exposure is 5% of the investment portfolio; maximum exposure to sub-investment-grade bonds is 10% of the portfolio. Operational Risk: operational risk losses must not exceed 2% of gross premium in any rolling 12-month period; the target for material operational risk incidents (loss > HKD 500,000) is fewer than 5 per year. Cyber Risk: zero tolerance for data breaches involving more than 100,000 customer records; system availability target for critical systems is 99.9%; maximum acceptable downtime for critical systems is 4 hours. Compliance Risk: zero tolerance for material regulatory breaches; target for regulatory examination findings rated "Significant" or higher is zero; all regulatory returns must be submitted on time (100% target). These thresholds are reviewed annually by the Board Risk Committee and recalibrated based on the strategic plan, market conditions, and regulatory developments."""
    },
    {
        "doc_id": "POL-RA-005",
        "doc_title": "Risk Appetite Framework",
        "section_title": "5.2 Key Risk Indicators (KRIs) and Limits",
        "doc_type": "policy",
        "last_updated": "2025-08-15",
        "section_content": """The following Key Risk Indicators are monitored continuously with defined green/amber/red thresholds. Claims KRIs: (KRI-01) Claims loss ratio — Green: <65%, Amber: 65-70%, Red: >70%; (KRI-02) Claims processing SLA compliance — Green: >95%, Amber: 90-95%, Red: <90%; (KRI-03) Fraud detection rate — Green: >1.0% of claims, Amber: 0.5-1.0%, Red: <0.5% (indicating possible detection gap); (KRI-04) Claims reserve adequacy ratio — Green: 100-110%, Amber: 95-100% or 110-120%, Red: <95% or >120%; (KRI-05) Large loss frequency (claims >HKD 1M) — Green: <10 per quarter, Amber: 10-15, Red: >15. Cybersecurity KRIs: (KRI-06) Mean time to detect (MTTD) — Green: <1 hour, Amber: 1-4 hours, Red: >4 hours; (KRI-07) Mean time to contain (MTTC) — Green: <4 hours, Amber: 4-12 hours, Red: >12 hours; (KRI-08) Phishing click rate — Green: <3%, Amber: 3-5%, Red: >5%; (KRI-09) Patch compliance (critical patches within SLA) — Green: >95%, Amber: 90-95%, Red: <90%; (KRI-10) Privileged account ratio — Green: <2% of total accounts, Amber: 2-3%, Red: >3%. All KRIs are reported monthly to the Enterprise Risk Committee and dashboarded in the Risk Management Information System (RMIS). Amber triggers require a management action plan within 10 business days. Red triggers require immediate escalation to the CRO and a Board Risk Committee briefing at the next scheduled meeting or ad hoc if the CRO deems it urgent."""
    },
    {
        "doc_id": "POL-RA-005",
        "doc_title": "Risk Appetite Framework",
        "section_title": "5.3 Breach Reporting and Remediation",
        "doc_type": "policy",
        "last_updated": "2025-08-15",
        "section_content": """When a risk appetite threshold is breached (KRI enters red zone), the following mandatory process applies. Immediate actions (within 24 hours): the risk owner must notify the CRO and Enterprise Risk Management (ERM) team; an incident record must be created in the RMIS; and an initial impact assessment must be documented. Remediation planning (within 5 business days): the risk owner must prepare a remediation plan including root cause analysis, specific corrective actions with owners and deadlines, interim mitigating controls, and an estimated timeline for returning to the green zone. The remediation plan must be approved by the CRO for risks within delegated authority or the Board Risk Committee for risks involving: solvency ratio impact exceeding 5 percentage points; potential regulatory censure; estimated financial impact exceeding HKD 10,000,000; or reputational risk rated "high" or above on the reputational risk assessment matrix. Monitoring during breach: the affected KRI must be monitored weekly (rather than monthly) until it returns to the amber zone, and daily if the breach is worsening. Escalation for persistent breaches: if a KRI remains in the red zone for more than 60 days, the Board Risk Committee must be briefed at its next meeting with an updated remediation plan. If a KRI remains red for more than 90 days, an independent review by Internal Audit or an external advisor is commissioned. All risk appetite breaches are reported in the quarterly CRO Report to the Board."""
    },
    {
        "doc_id": "POL-RA-005",
        "doc_title": "Risk Appetite Framework",
        "section_title": "5.4 Board Reporting Requirements",
        "doc_type": "policy",
        "last_updated": "2025-08-15",
        "section_content": """The Board receives risk appetite information through the following reporting cadence. Monthly: Executive Risk Dashboard — one-page summary of all KRIs with traffic light status, produced by the ERM team and distributed to the Executive Committee and Board Risk Committee Chair; includes trend arrows (improving, stable, deteriorating) for each KRI. Quarterly: CRO Report to the Board Risk Committee — comprehensive risk report covering: KRI performance with commentary on amber and red indicators; risk appetite breaches and remediation status; emerging risks assessment; risk event log (material operational risk events with loss > HKD 100,000); stress test results; and regulatory engagement summary. The Board Risk Committee meets quarterly and has the authority to recommend changes to risk appetite thresholds to the full Board. Annually: Risk Appetite Statement — formal statement approved by the full Board, setting out the qualitative risk appetite (risk culture, strategic risk preferences) and quantitative risk appetite (the specific thresholds in Section 5.1); Own Risk and Solvency Assessment (ORSA) — comprehensive assessment of the company's risk profile relative to its risk appetite, capital position, and solvency projections under base and stress scenarios; and the Internal Audit report on the effectiveness of the risk management framework. Ad-hoc: any material risk event (estimated impact > HKD 20,000,000 or regulatory censure) triggers an immediate Board notification from the CRO via the Board Risk Committee Chair."""
    },
    {
        "doc_id": "POL-RA-005",
        "doc_title": "Risk Appetite Framework",
        "section_title": "5.5 Stress Testing and Scenario Analysis",
        "doc_type": "policy",
        "last_updated": "2025-08-15",
        "section_content": """the Company conducts regular stress testing to assess resilience against adverse scenarios. Mandatory stress tests: (a) Insurance stress test — conducted semi-annually: scenarios include a 1-in-50-year natural catastrophe (Super Typhoon scenario with estimated gross loss of HKD 500,000,000), pandemic scenario (30% increase in medical claims for 12 months), and economic downturn (25% increase in motor and property claims combined with 15% investment portfolio decline). (b) Cyber stress test — conducted annually: scenarios include a major ransomware attack with 2-week system outage, a data breach affecting all policyholder records, and a supply chain attack compromising the claims processing TPA. (c) Integrated stress test — conducted annually for the ORSA: combines insurance, market, credit, and operational risk scenarios in a severe but plausible narrative. (d) Reverse stress test — identifies scenarios that would cause the business to become unviable (solvency ratio below 100%). Results must demonstrate: solvency ratio remains above 120% under all scenarios except reverse stress test; recovery actions identified for each scenario; management actions are realistic and implementable within the timeframes assumed. The Chief Actuary presents stress test results to the Board Risk Committee, and they form a key input to the annual capital planning process and reinsurance programme renewal."""
    },
    {
        "doc_id": "POL-RA-005",
        "doc_title": "Risk Appetite Framework",
        "section_title": "5.6 Risk Culture and Governance",
        "doc_type": "policy",
        "last_updated": "2025-08-15",
        "section_content": """A strong risk culture is fundamental to operating within risk appetite. Governance structure: the Board has ultimate responsibility for setting risk appetite; the Board Risk Committee provides detailed oversight; the CRO, reporting directly to the CEO with a dotted line to the Board Risk Committee Chair, leads the risk management function; the Enterprise Risk Committee (executive-level) meets monthly to review risk performance; Business Unit Risk Committees meet monthly to manage risks within their domains. Three lines of defence model: First line — business units (claims, underwriting, IT) own and manage their risks; Second line — Enterprise Risk Management, Compliance, and Information Security provide oversight, frameworks, and challenge; Third line — Internal Audit provides independent assurance. Risk culture initiatives: mandatory annual risk awareness training for all staff (completion target: 100%); risk KPIs embedded in performance scorecards for all managers at Grade M1 and above (weighting: 10% of variable compensation); annual risk culture survey with action plans for areas scoring below 3.5/5.0; whistleblowing programme (anonymous hotline and online portal) with a non-retaliation policy; and quarterly risk awareness communications from the CRO to all staff. The Board conducts an annual self-assessment of its risk governance effectiveness, benchmarked against the IA's Corporate Governance Code."""
    },
    {
        "doc_id": "POL-RA-005",
        "doc_title": "Risk Appetite Framework",
        "section_title": "5.7 Emerging Risks Management",
        "doc_type": "policy",
        "last_updated": "2025-08-15",
        "section_content": """The ERM team maintains an Emerging Risks Register, updated quarterly, capturing risks that may materialise over a 1-5 year horizon and are not yet fully reflected in the risk appetite metrics. Current emerging risks under monitoring: (a) Artificial Intelligence risk — use of AI in claims automation may introduce model bias, explainability challenges, and regulatory scrutiny; mitigation: AI governance framework established with mandatory model validation before deployment. (b) Geopolitical risk — escalation of regional tensions affecting supply chains, investment portfolio (exposure to affected regions), and reinsurance availability; mitigation: scenario analysis and concentration limits on geographic exposures. (c) Regulatory change risk — anticipated tightening of data protection regulations (e.g., proposed amendments to PDPO), new insurance conduct requirements, and evolving AML expectations; mitigation: regulatory horizon scanning by Compliance team, with impact assessments within 30 days of proposed changes. (d) Climate and sustainability risk — increasing natural catastrophe frequency, transition risks from carbon-intensive investment exposures, and greenwashing risk in product marketing; mitigation: climate risk integration as per Section 3.10. (e) Talent and workforce risk — difficulty attracting and retaining cybersecurity and data science talent; mitigation: enhanced compensation benchmarking, training programmes, and partnership with universities. Each emerging risk is assigned an owner, a likelihood/impact assessment, and a monitoring plan. Emerging risks that crystallise are transitioned to the main risk register with appropriate KRIs and appetite thresholds."""
    },
    {
        "doc_id": "POL-RA-005",
        "doc_title": "Risk Appetite Framework",
        "section_title": "5.8 Risk Appetite Calibration Process",
        "doc_type": "policy",
        "last_updated": "2025-08-15",
        "section_content": """The risk appetite thresholds are calibrated annually through a structured process. Step 1 — Strategic context (Q1): the CEO and CFO present the strategic plan priorities and financial targets to the Board Risk Committee, establishing the context for risk appetite. Step 2 — Bottom-up risk assessment (Q1-Q2): each business unit submits its risk profile, including current KRI performance, forward-looking risk assessment, and proposed appetite adjustments. Step 3 — Top-down calibration (Q2): the CRO and Chief Actuary analyse the aggregate risk profile against the capital plan, reinsurance programme, and regulatory requirements; propose threshold adjustments using quantitative modelling (stochastic simulations for insurance risk, VaR for market risk, scenario-based for operational and cyber risk). Step 4 — Board workshop (Q3): the Board Risk Committee reviews the proposed risk appetite in a dedicated workshop, challenging assumptions and considering qualitative factors (reputation, strategic positioning, stakeholder expectations). Step 5 — Board approval (Q3): the full Board approves the updated Risk Appetite Statement, which is effective from 1 January of the following year. Step 6 — Cascading (Q4): the approved appetite is cascaded into business unit risk limits, delegated authority frameworks, and individual performance targets. The ERM team updates the RMIS with the new thresholds and provides training to all risk owners. Mid-year recalibration may occur if triggered by a material change in the operating environment (e.g., a major regulatory change, M&A activity, or a severe loss event exceeding 50% of the annual risk budget)."""
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combine and Save All Policy Documents

# COMMAND ----------

all_sections = (
    claims_processing_sections
    + fraud_investigation_sections
    + regulatory_sections
    + cybersecurity_sections
    + risk_appetite_sections
)
print(f"Total sections generated: {len(all_sections)}")

# COMMAND ----------

import pandas as pd

pdf = pd.DataFrame(all_sections)
df = spark.createDataFrame(pdf)
df.write.mode("overwrite").saveAsTable(f"{TABLE_PREFIX}.demo_policy_documents")
print(f"Created table: {TABLE_PREFIX}.demo_policy_documents ({df.count()} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Chunked Table for Vector Search

# COMMAND ----------

from pyspark.sql import functions as F

chunks_df = (
    spark.table(f"{TABLE_PREFIX}.demo_policy_documents")
    .withColumn("chunk_id", F.concat_ws("-", F.col("doc_id"), F.monotonically_increasing_id().cast("string")))
    .select("chunk_id", "doc_id", "doc_title", "section_title", F.col("section_content").alias("chunk_text"), "doc_type")
)
chunks_df.write.mode("overwrite").saveAsTable(f"{TABLE_PREFIX}.demo_policy_chunks")
print(f"Created table: {TABLE_PREFIX}.demo_policy_chunks ({chunks_df.count()} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Enable Change Data Feed on the chunks table
# MAGIC
# MAGIC Required for Delta Sync Vector Search index.

# COMMAND ----------

spark.sql(f"ALTER TABLE {TABLE_PREFIX}.demo_policy_chunks SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
print("Enabled Change Data Feed on demo_policy_chunks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Vector Search Endpoint and Index

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4a. Create (or reuse) Vector Search endpoint

# COMMAND ----------

VS_ENDPOINT_NAME = "demo-vs-endpoint"
VS_INDEX_NAME = f"{TABLE_PREFIX}.demo_policy_chunks_index"

def _vs_api(method, path, **kwargs):
    """Helper for Vector Search REST API calls."""
    url = f"{API_BASE}/vector-search{path}"
    resp = getattr(requests, method)(url, headers=HEADERS, **kwargs)
    return resp

# Check if endpoint exists
resp = _vs_api("get", f"/endpoints/{VS_ENDPOINT_NAME}")
if resp.status_code == 200:
    ep_status = resp.json().get("endpoint_status", {}).get("state", "UNKNOWN")
    print(f"Endpoint '{VS_ENDPOINT_NAME}' already exists (state: {ep_status})")
else:
    print(f"Creating Vector Search endpoint: {VS_ENDPOINT_NAME}")
    create_resp = _vs_api("post", "/endpoints", json={
        "name": VS_ENDPOINT_NAME,
        "endpoint_type": "STANDARD"
    })
    if create_resp.status_code in (200, 201):
        print(f"Endpoint creation initiated: {create_resp.json()}")
    else:
        print(f"Endpoint creation response ({create_resp.status_code}): {create_resp.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. Wait for the endpoint to be ready

# COMMAND ----------

print(f"Waiting for endpoint '{VS_ENDPOINT_NAME}' to become ONLINE...")
for i in range(60):  # Up to 30 minutes
    resp = _vs_api("get", f"/endpoints/{VS_ENDPOINT_NAME}")
    if resp.status_code == 200:
        state = resp.json().get("endpoint_status", {}).get("state", "UNKNOWN")
        print(f"  [{i*30}s] Endpoint state: {state}")
        if state == "ONLINE":
            print("Endpoint is ONLINE and ready.")
            break
    time.sleep(30)
else:
    print("WARNING: Endpoint did not reach ONLINE state within 30 minutes. Continuing anyway...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4c. Create Delta Sync Vector Search Index

# COMMAND ----------

# Check if index already exists
resp = _vs_api("get", f"/indexes/{VS_INDEX_NAME}")
if resp.status_code == 200:
    idx_status = resp.json().get("status", {}).get("ready", False)
    print(f"Index '{VS_INDEX_NAME}' already exists (ready: {idx_status})")
else:
    print(f"Creating Delta Sync index: {VS_INDEX_NAME}")
    create_idx_resp = _vs_api("post", "/indexes", json={
        "name": VS_INDEX_NAME,
        "endpoint_name": VS_ENDPOINT_NAME,
        "primary_key": "chunk_id",
        "index_type": "DELTA_SYNC",
        "delta_sync_index_spec": {
            "source_table": f"{TABLE_PREFIX}.demo_policy_chunks",
            "pipeline_type": "TRIGGERED",
            "embedding_source_columns": [
                {
                    "name": "chunk_text",
                    "embedding_model_endpoint_name": "databricks-bge-large-en"
                }
            ]
        }
    })
    if create_idx_resp.status_code in (200, 201):
        print(f"Index creation initiated: {json.dumps(create_idx_resp.json(), indent=2)}")
    else:
        print(f"Index creation response ({create_idx_resp.status_code}): {create_idx_resp.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4d. Wait for the index to be ready

# COMMAND ----------

print(f"Waiting for index '{VS_INDEX_NAME}' to sync and become ready...")
for i in range(90):  # Up to 45 minutes
    resp = _vs_api("get", f"/indexes/{VS_INDEX_NAME}")
    if resp.status_code == 200:
        status = resp.json().get("status", {})
        ready = status.get("ready", False)
        indexed = status.get("indexed_row_count", 0)
        msg = status.get("message", "")
        print(f"  [{i*30}s] Ready: {ready} | Indexed rows: {indexed} | {msg}")
        if ready:
            print("Index is READY.")
            break
    time.sleep(30)
else:
    print("WARNING: Index did not become ready within 45 minutes. You can proceed — it may still be syncing.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Test the RAG Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a. Query the Vector Search Index

# COMMAND ----------

test_question = "What are the fraud escalation thresholds and when should cases be referred to law enforcement?"

# Query the vector search index
search_resp = _vs_api("post", f"/indexes/{VS_INDEX_NAME}/query", json={
    "columns": ["chunk_id", "doc_title", "section_title", "chunk_text", "doc_type"],
    "query_text": test_question,
    "num_results": 5
})

if search_resp.status_code == 200:
    results = search_resp.json().get("result", {}).get("data_array", [])
    columns = search_resp.json().get("manifest", {}).get("columns", [])
    col_names = [c["name"] for c in columns]
    print(f"Retrieved {len(results)} relevant chunks:\n")
    for i, row in enumerate(results):
        row_dict = dict(zip(col_names, row))
        print(f"  [{i+1}] {row_dict.get('doc_title', 'N/A')} — {row_dict.get('section_title', 'N/A')}")
        print(f"      Type: {row_dict.get('doc_type', 'N/A')}")
        print()
else:
    print(f"Vector search query failed ({search_resp.status_code}): {search_resp.text}")
    results = []
    col_names = []

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. Call the Foundation Model API with Retrieved Context

# COMMAND ----------

if results and col_names:
    # Build context from retrieved chunks
    context_parts = []
    sources = []
    for i, row in enumerate(results[:4]):  # Top 4 chunks
        row_dict = dict(zip(col_names, row))
        context_parts.append(
            f"[Source {i+1}: {row_dict.get('doc_title', '')} — {row_dict.get('section_title', '')}]\n"
            f"{row_dict.get('chunk_text', '')}"
        )
        sources.append(f"{row_dict.get('doc_title', '')} — {row_dict.get('section_title', '')}")

    context_text = "\n\n---\n\n".join(context_parts)

    system_prompt = """You are a Risk Policy Compliance Copilot for an insurance company operating in Hong Kong and Macau.
Answer questions accurately based ONLY on the provided policy document context.
Always cite the specific document and section you are referencing.
If the context does not contain enough information to fully answer the question, say so clearly.
Be concise but thorough. Use bullet points for lists of requirements or thresholds."""

    user_prompt = f"""Context from internal policy documents:

{context_text}

---

Question: {test_question}

Please provide a detailed answer with specific thresholds, procedures, and source citations."""

    # Call Foundation Model API
    fm_resp = requests.post(
        f"{API_BASE}/serving-endpoints/databricks-meta-llama-3-3-70b-instruct/invocations",
        headers=HEADERS,
        json={
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "max_tokens": 1024,
            "temperature": 0.1
        }
    )

    if fm_resp.status_code == 200:
        answer = fm_resp.json()["choices"][0]["message"]["content"]
        print("=" * 80)
        print("QUESTION:", test_question)
        print("=" * 80)
        print()
        print(answer)
        print()
        print("-" * 80)
        print("SOURCES:")
        for s in sources:
            print(f"  - {s}")
        print("-" * 80)
    else:
        print(f"Foundation Model API call failed ({fm_resp.status_code}): {fm_resp.text}")
else:
    print("Skipping RAG test — no vector search results available.")
    print("The index may still be syncing. Re-run this cell once the index is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary

# COMMAND ----------

print("=" * 80)
print("RAG PIPELINE SETUP COMPLETE")
print("=" * 80)
print()
print(f"Tables created:")
print(f"  - {TABLE_PREFIX}.demo_policy_documents  ({len(all_sections)} sections across 5 documents)")
print(f"  - {TABLE_PREFIX}.demo_policy_chunks      (chunked for vector search)")
print()
print(f"Vector Search:")
print(f"  - Endpoint:  {VS_ENDPOINT_NAME}")
print(f"  - Index:     {VS_INDEX_NAME}")
print(f"  - Embedding: databricks-bge-large-en (Databricks-managed)")
print(f"  - Sync mode: TRIGGERED (Delta Sync)")
print()
print(f"Foundation Model:")
print(f"  - Endpoint:  databricks-meta-llama-3-3-70b-instruct")
print()
print("Documents indexed:")
print("  1. Claims Processing Policy v3.2         (15 sections)")
print("  2. Fraud Investigation Guidelines         (12 sections)")
print("  3. IA Regulatory Requirements             (10 sections)")
print("  4. Cybersecurity Incident Response Plan   (12 sections)")
print("  5. Risk Appetite Framework                 (8 sections)")
print()
print("To query the RAG pipeline from another notebook:")
print("  1. Use the Vector Search REST API to search for relevant chunks")
print("  2. Pass the retrieved context to the Foundation Model API")
print("  3. The model will answer based on the policy documents with citations")
