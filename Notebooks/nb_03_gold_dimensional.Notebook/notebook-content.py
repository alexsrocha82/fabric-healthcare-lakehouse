# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "96a64e5c-e76a-41e7-86fc-aad44f7f95fc",
# META       "default_lakehouse_name": "lh_healthcare",
# META       "default_lakehouse_workspace_id": "41842032-fc55-48e8-82d5-cb6347b22db0",
# META       "known_lakehouses": [
# META         {
# META           "id": "96a64e5c-e76a-41e7-86fc-aad44f7f95fc"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "1f443f42-5ae5-bd4f-43d7-b342b62ea618",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# ===================================================================
# nb_03_gold_dimensional (inline version)
# -------------------------------------------------------------------
# Builds a star schema (dimensional model) from Silver/Bronze tables.
#
# Cell 1: Imports
# Cell 2: dim_date           (generated calendar dimension)
# Cell 3: dim_department     (simple dimension from Silver)
# Cell 4: dim_insurance      (simple dimension from Silver)
# Cell 5: dim_provider       (SCD Type 2 — tracks dept changes)
# Cell 6: dim_patient        (SCD Type 2 — tracks address/insurance)
# Cell 7: fact_encounters    (fact table with dimension keys)
# Cell 8: Validation summary
#
# KEY CONCEPT — SCD Type 2:
# Instead of overwriting old data when something changes, we keep
# EVERY version with date ranges (effective_start → effective_end).
# This lets us answer: "What was the patient's insurance plan when
# they visited the ER on January 5th?"
# ===================================================================

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 1 — Imports ===================================================
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    DateType, TimestampType, BooleanType, DecimalType, IntegerType
)
 
# PII salt (same as Silver, for consistent hash keys)
PII_SALT = "healthcare-lakehouse-project-2024"
 
print("Imports loaded. Ready to build Gold layer.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 2 — dim_date ==================================================
# A calendar dimension is generated, not extracted from source data.
# Every star schema needs one. It lets reports slice by year, quarter,
# month, day of week, etc. without calculating these on the fly.
#
# We generate dates from 2023-01-01 to 2025-12-31 to cover our
# simulated encounter dates (2024-01-01 to 2024-01-14) plus margin.
 
df_date = (
    spark.sql("""
        SELECT explode(sequence(
            to_date('2023-01-01'),
            to_date('2025-12-31'),
            interval 1 day
        )) AS full_date
    """)
)
 
df_date = (
    df_date
    # date_key: integer in YYYYMMDD format (e.g. 20240115)
    # This is the standard surrogate key for date dimensions.
    .withColumn("date_key", F.date_format(F.col("full_date"), "yyyyMMdd").cast(IntegerType()))
    .withColumn("year", F.year(F.col("full_date")))
    .withColumn("quarter", F.quarter(F.col("full_date")))
    .withColumn("month", F.month(F.col("full_date")))
    .withColumn("month_name", F.date_format(F.col("full_date"), "MMMM"))
    .withColumn("day", F.dayofmonth(F.col("full_date")))
    .withColumn("day_of_week", F.dayofweek(F.col("full_date")))
    .withColumn("day_name", F.date_format(F.col("full_date"), "EEEE"))
    .withColumn("is_weekend", F.dayofweek(F.col("full_date")).isin(1, 7))
    .withColumn("year_month", F.date_format(F.col("full_date"), "yyyy-MM"))
)
 
df_date.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("gold_dim_date")
 
print(f"gold_dim_date: {spark.table('gold_dim_date').count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 3 — dim_department (simple dimension) =========================
# No SCD2 needed — departments are static in our simulation.
#
# NOTE ON SURROGATE KEYS (SK):
# In traditional Kimball-era warehouses (1990s-2000s), integer SKs
# were essential for JOIN performance on row-based RDBMS engines.
# In modern lakehouse architectures (Delta Lake, Parquet columnar
# storage), Z-ordering, bloom filters, and predicate pushdown make
# JOINs on string business keys nearly as fast as integer SKs.
# For simple (non-SCD2) dimensions, we use the business key directly.
# SKs are kept only for SCD2 dimensions (patient, provider) where
# they uniquely identify each VERSION of a record.
 
df_dept = spark.table("silver_departments")
 
df_dept_gold = (
    df_dept
    .select(
        "department_id",      # business key, used as FK in fact table
        "department_name",
        "clinic_location",
        "region",             # drives RLS later
        "is_active",
    )
)
 
df_dept_gold.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("gold_dim_department")
 
print(f"gold_dim_department: {spark.table('gold_dim_department').count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 4 — dim_insurance (simple dimension) ==========================
# Same rationale as dim_department: no SK needed in modern lakehouse.
# Business key (insurance_id) used directly.
 
df_ins = spark.table("silver_insurance_plans")
 
df_ins_gold = (
    df_ins
    .select(
        "insurance_id",       # business key
        "insurance_name",     # used for joining from patient's insurance_plan
        "is_active",
    )
)
 
df_ins_gold.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("gold_dim_insurance")
 
print(f"gold_dim_insurance: {spark.table('gold_dim_insurance').count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 5 — dim_provider (SCD Type 2) =================================
# Providers can transfer between departments. Each transfer creates a
# new SCD2 version. This lets us answer: "Which department was Dr.
# Smith in when the patient visited on January 5th?"
#
# HOW SCD2 WORKS (step by step):
#
# 1. Read ALL versions from Bronze (not just the latest from Silver)
# 2. Clean and cast types (same as Silver)
# 3. For each provider, order all versions by date
# 4. Use LEAD() window function to find the NEXT version's date
# 5. Set date ranges:
#    - effective_start = date of THIS version
#    - effective_end   = date of NEXT version - 1 day
#                        (or 9999-12-31 if there is no next version)
# 6. is_current = True only for the latest version
#
# EXAMPLE for provider PRV0001:
#   Version 1: Cardiology, effective 2024-01-01 → 2024-01-06, current=false
#   Version 2: Neurology,  effective 2024-01-07 → 9999-12-31, current=true
 
# Step 1: Read ALL CDC events from Bronze (we need the full history)
df_prov_all = spark.table("bronze_providers")
 
# Step 2: Clean and cast (same logic as Silver, applied here because
# we're reading from Bronze, not Silver)
df_prov_all = (
    df_prov_all
    .withColumn("provider_id", F.trim(F.col("provider_id")))
    .withColumn("full_name", F.trim(F.col("full_name")))
    .withColumn("specialty", F.trim(F.col("specialty")))
    .withColumn("department_id", F.trim(F.col("department_id")))
    .withColumn("license_number", F.trim(F.col("license_number")))
    .withColumn("hire_date", F.col("hire_date").cast(DateType()))
    .withColumn("is_active", F.col("is_active").cast(BooleanType()))
    .withColumn("source_updated_at", F.col("source_updated_at").cast(TimestampType()))
    .withColumn("_load_date", F.col("_load_date").cast(DateType()))
)
 
# Step 3: Deduplicate within the same day (if a provider has 2 events
# on the same day, keep only the latest by source_updated_at)
window_day_dedup = (
    Window
    .partitionBy("provider_id", "_load_date")
    .orderBy(F.col("source_updated_at").desc())
)
 
df_prov_all = (
    df_prov_all
    .withColumn("_rn", F.row_number().over(window_day_dedup))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)
 
# Step 4: For each provider, find the NEXT version's date using LEAD()
# LEAD() looks at the next row in the window (ordered by date) and
# returns its _load_date. If there is no next row, it returns null.
window_scd = (
    Window
    .partitionBy("provider_id")
    .orderBy(F.col("_load_date").asc())
)
 
df_prov_all = df_prov_all.withColumn(
    "_next_version_date",
    F.lead("_load_date").over(window_scd)
)
 
# Step 5: Set effective date ranges
# - effective_start: the date this version became active
# - effective_end: the day before the next version (or 9999-12-31)
# - is_current: True only if there is no next version
df_prov_scd2 = (
    df_prov_all
    .withColumn("effective_start", F.col("_load_date"))
    .withColumn(
        "effective_end",
        F.when(
            F.col("_next_version_date").isNotNull(),
            F.date_sub(F.col("_next_version_date"), 1)
        ).otherwise(F.lit("9999-12-31").cast(DateType()))
    )
    .withColumn("is_current", F.col("_next_version_date").isNull())
)
 
# Step 6: Add surrogate key
df_prov_scd2 = (
    df_prov_scd2
    .withColumn("provider_key", F.monotonically_increasing_id() + 1)
    .select(
        "provider_key",
        "provider_id",
        "full_name",
        "specialty",
        "department_id",
        "license_number",
        "hire_date",
        "is_active",
        "effective_start",
        "effective_end",
        "is_current",
    )
)
 
df_prov_scd2.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("gold_dim_provider")
 
total = spark.table("gold_dim_provider").count()
current = spark.table("gold_dim_provider").filter(F.col("is_current")).count()
print(f"gold_dim_provider: {total} rows total, {current} current versions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 6 — dim_patient (SCD Type 2) ==================================
# Same SCD2 pattern as providers. Tracked attributes:
#   - address (city, state, zip)
#   - insurance_plan
#   - primary_provider_id
#
# When ANY of these changes, a new version is created.
# PII is NOT included here (no name, SSN, email, phone, street).
# The patient_hash_id links to silver_patients_pii if PII is needed.
 
# Step 1: Read ALL CDC events from Bronze
df_pat_all = spark.table("bronze_patients")
 
# Step 2: Clean and cast
df_pat_all = (
    df_pat_all
    .withColumn("patient_id", F.trim(F.col("patient_id")))
    .withColumn("ssn", F.trim(F.col("ssn")))
    .withColumn("gender", F.upper(F.trim(F.col("gender"))))
    .withColumn("birth_date", F.col("birth_date").cast(DateType()))
    .withColumn("address_city", F.trim(F.col("address_city")))
    .withColumn("address_state", F.upper(F.trim(F.col("address_state"))))
    .withColumn("address_zip", F.trim(F.col("address_zip")))
    .withColumn("insurance_plan", F.trim(F.col("insurance_plan")))
    .withColumn("primary_provider_id", F.trim(F.col("primary_provider_id")))
    .withColumn("is_active", F.col("is_active").cast(BooleanType()))
    .withColumn("source_updated_at", F.col("source_updated_at").cast(TimestampType()))
    .withColumn("_load_date", F.col("_load_date").cast(DateType()))
    # PII hash for linking to the PII vault (same salt as Silver)
    .withColumn("patient_hash_id", F.sha2(F.concat(F.lit(PII_SALT), F.col("ssn")), 256))
)
 
# Step 3: Deduplicate within the same day
window_day_dedup = (
    Window
    .partitionBy("patient_id", "_load_date")
    .orderBy(F.col("source_updated_at").desc())
)
 
df_pat_all = (
    df_pat_all
    .withColumn("_rn", F.row_number().over(window_day_dedup))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)
 
# Step 4: LEAD() to find next version date
window_scd = (
    Window
    .partitionBy("patient_id")
    .orderBy(F.col("_load_date").asc())
)
 
df_pat_all = df_pat_all.withColumn(
    "_next_version_date",
    F.lead("_load_date").over(window_scd)
)
 
# Step 5: Set effective date ranges
df_pat_scd2 = (
    df_pat_all
    .withColumn("effective_start", F.col("_load_date"))
    .withColumn(
        "effective_end",
        F.when(
            F.col("_next_version_date").isNotNull(),
            F.date_sub(F.col("_next_version_date"), 1)
        ).otherwise(F.lit("9999-12-31").cast(DateType()))
    )
    .withColumn("is_current", F.col("_next_version_date").isNull())
)
 
# Step 6: Add surrogate key and select final columns (NO PII)
df_pat_scd2 = (
    df_pat_scd2
    .withColumn("patient_key", F.monotonically_increasing_id() + 1)
    .select(
        "patient_key",
        "patient_hash_id",      # links to PII vault
        "patient_id",
        "birth_date",
        "gender",
        "address_city",
        "address_state",
        "address_zip",
        "insurance_plan",
        "primary_provider_id",
        "is_active",
        "effective_start",
        "effective_end",
        "is_current",
    )
)
 
df_pat_scd2.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("gold_dim_patient")
 
total = spark.table("gold_dim_patient").count()
current = spark.table("gold_dim_patient").filter(F.col("is_current")).count()
historical = total - current
print(f"gold_dim_patient: {total} rows total ({current} current + {historical} historical)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 7 — fact_encounters ===========================================
# The fact table is the center of the star. Each row is one encounter.
#
# Dimension references:
#   - dim_date:       via date_key (YYYYMMDD integer from encounter_date)
#   - dim_patient:    via patient_key (SCD2 surrogate key)
#   - dim_provider:   via provider_key (SCD2 surrogate key)
#   - dim_department: via department_id (business key — no SK needed)
#   - dim_insurance:  via insurance_plan (business key from patient dim)
#
# The SCD2 joins use date ranges:
#   encounter_date BETWEEN effective_start AND effective_end
# This links to the dimension version ACTIVE on the encounter day.
 
# Step 1: Read cleaned encounters from Silver (already deduped)
df_enc = spark.table("silver_encounters")
 
# Step 2: Generate date_key from encounter_date
df_fact = df_enc.withColumn(
    "date_key",
    F.date_format(F.col("encounter_date"), "yyyyMMdd").cast(IntegerType())
)
 
# Step 3: Join with dim_patient (SCD2 join!)
# KEY PATTERN: match on business key AND date range.
# encounter_date must fall BETWEEN effective_start AND effective_end.
# We also pick up the patient's insurance_plan from the dimension,
# because it may change over time (SCD2 tracked attribute).
df_dim_pat = spark.table("gold_dim_patient")
 
df_fact = (
    df_fact
    .join(
        df_dim_pat.select(
            F.col("patient_key"),
            F.col("patient_id").alias("dim_patient_id"),
            F.col("insurance_plan").alias("patient_insurance_plan"),
            F.col("effective_start").alias("pat_eff_start"),
            F.col("effective_end").alias("pat_eff_end"),
        ),
        on=(
            (F.col("patient_id") == F.col("dim_patient_id"))
            & (F.col("encounter_date") >= F.col("pat_eff_start"))
            & (F.col("encounter_date") <= F.col("pat_eff_end"))
        ),
        how="left"
    )
    .drop("dim_patient_id", "pat_eff_start", "pat_eff_end")
)
 
# Step 4: Join with dim_provider (SCD2 join — same pattern)
df_dim_prov = spark.table("gold_dim_provider")
 
df_fact = (
    df_fact
    .join(
        df_dim_prov.select(
            F.col("provider_key"),
            F.col("provider_id").alias("dim_provider_id"),
            F.col("effective_start").alias("prov_eff_start"),
            F.col("effective_end").alias("prov_eff_end"),
        ),
        on=(
            (F.col("provider_id") == F.col("dim_provider_id"))
            & (F.col("encounter_date") >= F.col("prov_eff_start"))
            & (F.col("encounter_date") <= F.col("prov_eff_end"))
        ),
        how="left"
    )
    .drop("dim_provider_id", "prov_eff_start", "prov_eff_end")
)
 
# Steps 5-6: dim_department and dim_insurance
# No joins needed! department_id is already in the encounter, and
# insurance_plan comes from the patient SCD2 join above.
# In the Power BI model, fact joins to these dimensions via:
#   fact.department_id → dim_department.department_id
#   fact.insurance_plan → dim_insurance.insurance_name
 
# Step 7: Select final fact columns
df_fact_final = (
    df_fact
    .select(
        # Dimension keys
        "date_key",                          # → dim_date
        "patient_key",                       # → dim_patient (SCD2 SK)
        "provider_key",                      # → dim_provider (SCD2 SK)
        "department_id",                     # → dim_department (business key)
        F.col("patient_insurance_plan")
         .alias("insurance_plan"),           # → dim_insurance (business key)
        # Degenerate dimensions (IDs for traceability)
        "encounter_id",
        "patient_id",
        "provider_id",
        # Facts / measures
        "encounter_date",
        "encounter_type",
        "status",
        "diagnosis_code",
        "diagnosis_desc",
        "procedure_code",
        "total_amount",
        # Audit
        "source_created_at",
        "source_updated_at",
    )
)
 
df_fact_final.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("gold_fact_encounters")
 
fact_count = spark.table("gold_fact_encounters").count()
null_patient_key = spark.table("gold_fact_encounters").filter(F.col("patient_key").isNull()).count()
null_provider_key = spark.table("gold_fact_encounters").filter(F.col("provider_key").isNull()).count()
print(f"gold_fact_encounters: {fact_count} rows")
print(f"  null patient_key: {null_patient_key} (should be 0)")
print(f"  null provider_key: {null_provider_key} (should be 0)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 8 — Validation summary ========================================
print("=" * 60)
print("GOLD DIMENSIONAL MODEL SUMMARY")
print("=" * 60)
 
gold_tables = [
    "gold_dim_date",
    "gold_dim_department",
    "gold_dim_insurance",
    "gold_dim_provider",
    "gold_dim_patient",
    "gold_fact_encounters",
]
 
for t in gold_tables:
    count = spark.table(t).count()
    print(f"  {t:<28} {count:>6} rows")
 
print("-" * 60)
print("\nSCD2 validation:")
 
# Show providers with multiple versions (proof that SCD2 works)
prov_versions = (
    spark.table("gold_dim_provider")
    .groupBy("provider_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)
print(f"  Providers with version history: {prov_versions}")
 
# Show patients with multiple versions
pat_versions = (
    spark.table("gold_dim_patient")
    .groupBy("patient_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)
print(f"  Patients with version history:  {pat_versions}")
 
print("\nPeek at a patient with multiple versions (SCD2 in action):")
# Find a patient with >1 version and display their history
multi_version_patient = (
    spark.table("gold_dim_patient")
    .groupBy("patient_id")
    .count()
    .filter(F.col("count") > 1)
    .select("patient_id")
    .limit(1)
    .collect()
)
 
if multi_version_patient:
    sample_id = multi_version_patient[0]["patient_id"]
    display(
        spark.table("gold_dim_patient")
        .filter(F.col("patient_id") == sample_id)
        .select(
            "patient_key", "patient_id",
            "address_city", "address_state", "insurance_plan",
            "effective_start", "effective_end", "is_current"
        )
        .orderBy("effective_start")
    )
else:
    print("  (No patients with multiple versions found)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
