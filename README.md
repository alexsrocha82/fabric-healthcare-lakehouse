# Healthcare Lakehouse — End-to-End Data Engineering on Microsoft Fabric

An end-to-end data engineering project built entirely on **Microsoft Fabric (Free Trial)**, simulating a real-world healthcare data platform with the medallion architecture, SCD Type 2, PII protection, row/column-level security, and automated orchestration.

> **Purpose:** This project was designed as both a learning exercise and a portfolio piece. Every design decision is documented with the *why*, not just the *how*.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        OneLake (single tenant store)                   │
│                                                                        │
│  ┌──────────────────── Lakehouse: lh_healthcare ────────────────────┐  │
│  │                                                                    │  │
│  │  Files/                          Tables/                           │  │
│  │  ├── landing/                    ├── bronze_departments             │  │
│  │  │   ├── departments/            ├── bronze_encounters             │  │
│  │  │   ├── encounters/             ├── bronze_insurance_plans        │  │
│  │  │   ├── insurance_plans/        ├── bronze_patients               │  │
│  │  │   ├── patients/               ├── bronze_providers              │  │
│  │  │   └── providers/              ├── silver_departments            │  │
│  │  └── _generator_state/           ├── silver_encounters             │  │
│  │                                  ├── silver_insurance_plans        │  │
│  │                                  ├── silver_patients (no PII)      │  │
│  │                                  ├── silver_patients_pii (vault)   │  │
│  │                                  ├── silver_providers              │  │
│  │                                  ├── gold_dim_date                 │  │
│  │                                  ├── gold_dim_department           │  │
│  │                                  ├── gold_dim_insurance            │  │
│  │                                  ├── gold_dim_patient (SCD2)       │  │
│  │                                  ├── gold_dim_provider (SCD2)      │  │
│  │                                  └── gold_fact_encounters          │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                                                        │
│  ┌──────────────────── Warehouse: wh_healthcare ───────────────────┐  │
│  │  Views (DirectLake from Gold)    │  Table                        │  │
│  │  ├── dim_date                    │  └── patients_pii (DDM)       │  │
│  │  ├── dim_department              │                                │  │
│  │  ├── dim_insurance               │  Security                     │  │
│  │  ├── dim_patient                 │  ├── RLS (by region)          │  │
│  │  ├── dim_provider                │  ├── CLS (column restrict)    │  │
│  │  └── fact_encounters             │  └── DDM (mask PII fields)    │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                                                        │
│  Semantic Model: sm_healthcare_analytics (DirectLake)                  │
│  Report: rpt_healthcare_dashboard                                      │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow (Pipeline)

```
nb_00 Generate Source Data  ──►  nb_01 Bronze Ingestion  ──►  nb_02 Silver Transformation  ──►  nb_03 Gold Dimensional
      (1m 27s)                        (2m 52s)                       (3m 22s)                        (3m 08s)
```

Orchestrated by `pl_healthcare_medallion` Data Pipeline with **On Success** dependencies. Total runtime: ~10 minutes end-to-end.

---

## Medallion Layers

### Bronze — Raw Ingestion
- **Principle:** Ingest as-is, never transform.
- Reads all partitioned CSVs from `Files/landing/<entity>/load_date=YYYY-MM-DD/`.
- All columns preserved as **string** (`inferSchema=false`) to prevent silent data loss from type inference failures.
- Adds three audit columns: `_source_file`, `_load_date`, `_ingested_at`.
- Append-only, idempotent (full overwrite from landing on each run).

### Silver — Trusted, Clean Data
- **Principle:** Make data trustworthy. This is where the engineer adds the most value.
- **Type casting** with failure detection: original value preserved, cast failure flagged.
- **Data quality checks** with quarantine pattern: rows failing DQ rules go to `silver_dq_quarantine` (not discarded).
- **PII pseudonymization:** SHA-256 hash of SSN (with salt) creates `patient_hash_id`.
- **PII vault pattern:** sensitive fields (name, SSN, email, phone, address) physically separated into `silver_patients_pii`. The main `silver_patients` table contains NO PII — only the hash key for linking.
- **CDC deduplication** via window functions (`ROW_NUMBER` partitioned by business key, ordered by `_load_date DESC`). Silver represents the *current state* of each entity.

### Gold — Dimensional Model (Star Schema)
- **dim_date:** Generated calendar dimension (2023–2025).
- **dim_department / dim_insurance:** Simple dimensions using business keys directly (no surrogate keys — see Design Decisions below).
- **dim_patient / dim_provider:** **SCD Type 2** dimensions built from Bronze CDC history. Each version has `effective_start`, `effective_end`, and `is_current` flag. Surrogate keys (`patient_key`, `provider_key`) uniquely identify each version.
- **fact_encounters:** Central fact table referencing all dimensions. SCD2 dimensions are joined using the **range join pattern**: `encounter_date BETWEEN effective_start AND effective_end`, ensuring each encounter links to the dimension version active on that date.

---

## Security & Governance (Defense in Depth)

Three layers of PII protection, each serving a different purpose:

| Layer | Where | What it does | Who it protects against |
|-------|-------|-------------|----------------------|
| **PII Vault** (architecture) | Lakehouse Silver | Physically separates PII into a restricted table | Analysts querying `silver_patients` — PII simply doesn't exist there |
| **DDM** (access control) | Warehouse `patients_pii` | Masks SSN, email, phone, name at display time | Users with vault access but no `UNMASK` permission |
| **RLS** (row filtering) | Warehouse `fact_encounters` | Filters encounters by region per user | Analysts restricted to their region only |

> **Important:** In Microsoft Fabric, RLS/CLS/DDM are enforced only for **Viewers**. Admins, Members, and Contributors bypass all security policies by design. In production, analysts and data consumers should be assigned the Viewer role.

---

## Design Decisions & Rationale

### Surrogate Keys: Modern Approach
In traditional Kimball-era warehouses (1990s–2000s), integer surrogate keys were essential for JOIN performance on row-based RDBMS engines. In modern lakehouse architectures (Delta Lake, Parquet columnar storage), **Z-ordering, bloom filters, and predicate pushdown** make JOINs on string business keys nearly as fast as integer SKs.

**Decision:** Surrogate keys are used only for **SCD2 dimensions** (where they uniquely identify each *version* of a record). Simple dimensions use business keys directly.

### Silver Dedup vs Gold SCD2
Silver applies CDC deduplication to produce the **current state** of each entity — useful for operational queries, ad-hoc analysis, DQ validation, and feeding downstream systems. Gold reads from **Bronze** (which preserves the full CDC history) to build SCD2 version chains.

**Why not read Gold from Silver?** Because Silver's dedup discards historical versions that SCD2 needs. The two layers serve different purposes: Silver = "what is true now", Gold = "what was true at any point in time".

### Views vs Tables in Warehouse
Dimension and fact tables in the Warehouse are implemented as **views** over Lakehouse Gold tables (cross-database queries), not materialized copies. This avoids data duplication and eliminates sync pipelines. The only materialized table is `patients_pii`, because Dynamic Data Masking requires `ALTER TABLE` which only works on real tables.

### Auto Loader Equivalent in Fabric
Databricks `format("cloudFiles")` (Auto Loader) is proprietary and not available in Fabric. The equivalent is **Spark Structured Streaming** with `trigger(availableNow=True)` and `checkpointLocation`, which provides the same incremental file processing behavior. The current Bronze uses full-overwrite for simplicity; the incremental variant is documented for production use.

### DirectLake Semantic Model
The Power BI semantic model uses **DirectLake mode**, which reads directly from Delta/Parquet files in OneLake without importing data. This provides near-zero latency to data changes (no refresh needed) and zero data duplication. DirectLake is automatically configured when creating semantic models from Fabric Lakehouses.

---

## Healthcare Domain Context

### Entities
| Entity | Type | Volume | CDC Behavior |
|--------|------|--------|-------------|
| Departments | Static dimension | 8 | Created once (day 1) |
| Insurance Plans | Static dimension | 6 | Created once (day 1) |
| Providers | Slowly changing | ~47 unique | New hires + department transfers |
| Patients | Slowly changing | ~890 unique | Address/insurance/provider changes + soft deletes |
| Encounters | Fact (event) | ~1,685 | Daily new encounters + status updates |

### SCD2 Tracked Attributes
- **Patients:** address (city, state, zip), insurance_plan, primary_provider_id
- **Providers:** department_id, specialty

### ICD-10 Diagnosis Codes
12 realistic codes including E11.9 (Type 2 diabetes), I10 (Essential hypertension), F41.1 (Generalized anxiety disorder), and others commonly seen in outpatient healthcare.

---

## How to Reproduce

### Prerequisites
- Microsoft Azure account (Pay-As-You-Go, free tier sufficient)
- Microsoft Fabric Trial (60 days, F64 capacity, free)
- GitHub account

### Step-by-Step
1. **Azure & Fabric Setup:** Create an Azure account → Create a user in Microsoft Entra ID → Sign in to `app.fabric.microsoft.com` → Activate Fabric Trial (choose region: Brazil South or nearest).
2. **Workspace:** Create workspace `healthcare-lakehouse-project` with Fabric Trial capacity.
3. **Lakehouse:** Create `lh_healthcare` in the workspace.
4. **Environment:** Create `env_healthcare` → Add `faker` library from PyPI → Publish.
5. **Notebooks:** Create notebooks `nb_00` through `nb_03`, attach `lh_healthcare` as default lakehouse, set environment to `env_healthcare`.
6. **Run pipeline** or execute notebooks manually in order: `nb_00` → `nb_01` → `nb_02` → `nb_03`.
7. **Warehouse:** Create `wh_healthcare` → Run SQL scripts for views, DDM, and RLS.
8. **Semantic Model:** From `lh_healthcare`, create `sm_healthcare_analytics` selecting all `gold_*` tables → Define relationships.
9. **Report:** Create `rpt_healthcare_dashboard` from the semantic model.

### Important Notes
- The `%pip install faker` cell in `nb_00` must be commented out when running via pipeline. Use the `env_healthcare` Environment instead.
- All data is synthetic and generated by `nb_00`. No external data sources are needed.
- The project is fully reproducible: clone the repo, create the Fabric items, and run.

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Platform | Microsoft Fabric (Trial, F64) |
| Storage | OneLake (Delta Lake / Parquet) |
| Processing | Apache Spark (PySpark) |
| Orchestration | Fabric Data Pipelines |
| Warehouse | Fabric Warehouse (T-SQL) |
| BI | Power BI (DirectLake semantic model) |
| Data Generation | Python + Faker library |
| Version Control | GitHub + Fabric Git Integration |
| Security | RLS, CLS, DDM, PII Vault Pattern |

---

## Project Structure

```
healthcare-lakehouse-project/
├── lh_healthcare.Lakehouse/           # Lakehouse definition
├── nb_00_generate_synthetic_source/   # Synthetic CDC data generator
├── nb_01_bronze_ingestion/            # Raw ingestion with audit columns
├── nb_02_silver_transformation/       # Cleaning, DQ, PII vault, dedup
├── nb_03_gold_dimensional/            # Star schema with SCD2
├── nb_99_sandbox/                     # Ad-hoc exploration notebook
├── wh_healthcare.Warehouse/           # Serving layer with security
├── pl_healthcare_medallion/           # Orchestration pipeline
├── sm_healthcare_analytics/           # DirectLake semantic model
└── rpt_healthcare_dashboard/          # Power BI report
```

---

## Key Learnings

1. **OneLake is one per tenant**, not per workspace. Workspaces are organizational units within the single OneLake.
2. **Notebooks are workspace-level items**, not lakehouse items. They must be attached to a lakehouse to access its tables.
3. **`cloudFiles` (Auto Loader) is Databricks-proprietary** and unavailable in Fabric. Use Structured Streaming with `availableNow` trigger instead.
4. **RLS/CLS/DDM only affect Viewers** in Fabric. Admins, Members, and Contributors bypass all security policies.
5. **Surrogate keys are optional** in modern lakehouse architectures for simple dimensions, thanks to columnar storage and predicate pushdown. They remain valuable for SCD2 dimensions where they uniquely identify version rows.
6. **DirectLake is Fabric's killer feature** for BI — zero-copy, near real-time, no scheduled refresh needed.
7. **PII protection requires multiple layers** (defense in depth): architectural separation (vault pattern) + access control (DDM/CLS) + row filtering (RLS).

---

## Author

**Alex Rocha** — Data Engineer

- GitHub: [@alexsrocha82](https://github.com/alexsrocha82)
- Project: [fabric-healthcare-lakehouse](https://github.com/alexsrocha82/fabric-healthcare-lakehouse)

---

*Built with Microsoft Fabric Trial. All data is synthetic — no real patient information was used.*
