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
# nb_02_silver_transformation
# -------------------------------------------------------------------
# Transforms Bronze tables into clean, typed, quality-checked Silver
# tables. This is the layer where data becomes trustworthy:
#
#  1. Type casting   — explicit string → proper types, no silent nulls
#  2. Cleaning       — trim, normalize, standardize values
#  3. DQ tests       — validate business rules; failures → quarantine
#  4. PII pseudonym. — SHA-256 hash of SSN; PII vault pattern
#  5. CDC dedup      — keep only the latest change per key per day
#
# Output tables:
#   silver_departments, silver_insurance_plans, silver_providers,
#   silver_patients, silver_patients_pii (vault), silver_encounters,
#   silver_dq_quarantine (failed DQ rows)
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
    StringType, IntegerType, DateType, TimestampType,
    DecimalType, BooleanType
)
 
# Salt for PII hashing. In production this comes from Azure Key Vault.
PII_SALT = "healthcare-lakehouse-project-2024"
 
print("Imports loaded. Ready to process.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 2 — Silver: departments =======================================
# Simple static dimension. Just clean, cast types, and validate.
 
# Step 1: Read from Bronze
df_dept = spark.table("bronze_departments")
 
# Step 2: Clean and cast types
df_dept = (
    df_dept
    .withColumn("department_id", F.trim(F.col("department_id")))
    .withColumn("department_name", F.trim(F.col("department_name")))
    .withColumn("clinic_location", F.trim(F.col("clinic_location")))
    .withColumn("region", F.trim(F.upper(F.col("region"))))
    .withColumn("is_active", F.col("is_active").cast(BooleanType()))
    .withColumn("source_created_at", F.col("source_created_at").cast(TimestampType()))
    .withColumn("source_updated_at", F.col("source_updated_at").cast(TimestampType()))
    .withColumn("_silver_loaded_at", F.current_timestamp())
    .drop("_source_file", "_ingested_at")
)
 
# Step 3: Data quality checks
# Flag rows that fail any rule. Good rows go to silver, bad to quarantine.
df_dept = (
    df_dept
    .withColumn("_dq_dept_id_null", F.col("department_id").isNull())
    .withColumn("_dq_dept_name_null", F.col("department_name").isNull())
    .withColumn("_dq_region_null", F.col("region").isNull())
)
 
# Combine: a row fails if ANY flag is True
df_dept = df_dept.withColumn(
    "_dq_failed",
    F.col("_dq_dept_id_null") | F.col("_dq_dept_name_null") | F.col("_dq_region_null")
)
 
# Split into passed and quarantined
df_dept_quarantine = (
    df_dept.filter(F.col("_dq_failed"))
    .withColumn("_dq_entity", F.lit("departments"))
    .withColumn("_dq_checked_at", F.current_timestamp())
)
 
df_dept_clean = (
    df_dept.filter(~F.col("_dq_failed"))
    .drop("_dq_dept_id_null", "_dq_dept_name_null", "_dq_region_null", "_dq_failed")
)
 
# Step 4: Write to Silver
df_dept_clean.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("silver_departments")
 
dept_count = spark.table("silver_departments").count()
dept_q_count = df_dept_quarantine.count()
print(f"silver_departments: {dept_count} rows written, {dept_q_count} quarantined")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 3 — Silver: insurance_plans ===================================
 
# Step 1: Read from Bronze
df_ins = spark.table("bronze_insurance_plans")
 
# Step 2: Clean and cast types
df_ins = (
    df_ins
    .withColumn("insurance_id", F.trim(F.col("insurance_id")))
    .withColumn("insurance_name", F.trim(F.col("insurance_name")))
    .withColumn("is_active", F.col("is_active").cast(BooleanType()))
    .withColumn("source_created_at", F.col("source_created_at").cast(TimestampType()))
    .withColumn("source_updated_at", F.col("source_updated_at").cast(TimestampType()))
    .withColumn("_silver_loaded_at", F.current_timestamp())
    .drop("_source_file", "_ingested_at")
)
 
# Step 3: Data quality checks
df_ins = (
    df_ins
    .withColumn("_dq_ins_id_null", F.col("insurance_id").isNull())
    .withColumn("_dq_ins_name_null", F.col("insurance_name").isNull())
)
 
df_ins = df_ins.withColumn(
    "_dq_failed",
    F.col("_dq_ins_id_null") | F.col("_dq_ins_name_null")
)
 
df_ins_quarantine = (
    df_ins.filter(F.col("_dq_failed"))
    .withColumn("_dq_entity", F.lit("insurance_plans"))
    .withColumn("_dq_checked_at", F.current_timestamp())
)
 
df_ins_clean = (
    df_ins.filter(~F.col("_dq_failed"))
    .drop("_dq_ins_id_null", "_dq_ins_name_null", "_dq_failed")
)
 
# Step 4: Write to Silver
df_ins_clean.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("silver_insurance_plans")
 
ins_count = spark.table("silver_insurance_plans").count()
ins_q_count = df_ins_quarantine.count()
print(f"silver_insurance_plans: {ins_count} rows written, {ins_q_count} quarantined")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 4 — Silver: providers =========================================
# Providers have CDC updates (department transfers). Here we introduce
# the CDC DEDUP pattern: when a provider appears in multiple daily
# loads (insert on day 1, update on day 5), we keep ONLY the latest
# version. This gives us the "current state" of each provider.
 
# Step 1: Read from Bronze
df_prov = spark.table("bronze_providers")
 
# Step 2: Clean and cast types
df_prov = (
    df_prov
    .withColumn("provider_id", F.trim(F.col("provider_id")))
    .withColumn("full_name", F.trim(F.col("full_name")))
    .withColumn("specialty", F.trim(F.col("specialty")))
    .withColumn("department_id", F.trim(F.col("department_id")))
    .withColumn("license_number", F.trim(F.col("license_number")))
    .withColumn("hire_date", F.col("hire_date").cast(DateType()))
    .withColumn("is_active", F.col("is_active").cast(BooleanType()))
    .withColumn("source_created_at", F.col("source_created_at").cast(TimestampType()))
    .withColumn("source_updated_at", F.col("source_updated_at").cast(TimestampType()))
    .withColumn("_silver_loaded_at", F.current_timestamp())
    .drop("_source_file", "_ingested_at")
)
 
# Step 3: Data quality checks
df_prov = (
    df_prov
    .withColumn("_dq_prov_id_null", F.col("provider_id").isNull())
    .withColumn("_dq_name_null", F.col("full_name").isNull())
    .withColumn("_dq_dept_null", F.col("department_id").isNull())
    .withColumn("_dq_hire_null", F.col("hire_date").isNull())
)
 
df_prov = df_prov.withColumn(
    "_dq_failed",
    (
        F.col("_dq_prov_id_null") | F.col("_dq_name_null")
        | F.col("_dq_dept_null") | F.col("_dq_hire_null")
    )
)
 
df_prov_quarantine = (
    df_prov.filter(F.col("_dq_failed"))
    .withColumn("_dq_entity", F.lit("providers"))
    .withColumn("_dq_checked_at", F.current_timestamp())
)
 
df_prov_clean = (
    df_prov.filter(~F.col("_dq_failed"))
    .drop("_dq_prov_id_null", "_dq_name_null", "_dq_dept_null", "_dq_hire_null", "_dq_failed")
)
 
# Step 4: CDC DEDUP — keep only the latest version per provider
# HOW IT WORKS:
#   - We partition (group) by provider_id
#   - We order by _load_date DESC (most recent first),
#     then by source_updated_at DESC (latest update first)
#   - ROW_NUMBER assigns 1 to the most recent row per provider
#   - We keep only row_number = 1 (the latest version)
#
# Example: provider PRV0001 appears in 3 loads:
#   _load_date=2024-01-01, op=I  (created)      -> row_number=3
#   _load_date=2024-01-05, op=U  (transferred)   -> row_number=2
#   _load_date=2024-01-10, op=U  (transferred)   -> row_number=1 ← KEEP
#
window_prov = (
    Window
    .partitionBy("provider_id")
    .orderBy(F.col("_load_date").desc(), F.col("source_updated_at").desc())
)
 
df_prov_clean = (
    df_prov_clean
    .withColumn("_row_number", F.row_number().over(window_prov))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
)
 
# Step 5: Write to Silver
df_prov_clean.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("silver_providers")
 
prov_count = spark.table("silver_providers").count()
prov_q_count = df_prov_quarantine.count()
print(f"silver_providers: {prov_count} rows written, {prov_q_count} quarantined")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 5 — Silver: patients ==========================================
# The most complex entity. Introduces three new concepts:
#   1. Cast with failure detection (birth_date)
#   2. PII hashing (SHA-256 of SSN)
#   3. PII vault pattern (split into two tables)
# Plus DQ checks and CDC dedup (same as providers).
 
# Step 1: Read from Bronze
df_pat = spark.table("bronze_patients")
 
# Step 2: Clean and cast types
# For birth_date, we detect cast failures: if the original string is
# not null but the casted value IS null, the cast failed (bad data).
df_pat = (
    df_pat
    .withColumn("_original_birth_date", F.col("birth_date"))  # keep original
    .withColumn("birth_date", F.col("birth_date").cast(DateType()))
    .withColumn(
        "_cast_failed_birth_date",
        F.when(
            F.col("_original_birth_date").isNotNull() & F.col("birth_date").isNull(),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
    .drop("_original_birth_date")
    .withColumn("patient_id", F.trim(F.col("patient_id")))
    .withColumn("full_name", F.trim(F.col("full_name")))
    .withColumn("ssn", F.trim(F.col("ssn")))
    .withColumn("gender", F.upper(F.trim(F.col("gender"))))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("phone", F.trim(F.col("phone")))
    .withColumn("address_street", F.trim(F.col("address_street")))
    .withColumn("address_city", F.trim(F.col("address_city")))
    .withColumn("address_state", F.upper(F.trim(F.col("address_state"))))
    .withColumn("address_zip", F.trim(F.col("address_zip")))
    .withColumn("insurance_plan", F.trim(F.col("insurance_plan")))
    .withColumn("primary_provider_id", F.trim(F.col("primary_provider_id")))
    .withColumn("is_active", F.col("is_active").cast(BooleanType()))
    .withColumn("source_created_at", F.col("source_created_at").cast(TimestampType()))
    .withColumn("source_updated_at", F.col("source_updated_at").cast(TimestampType()))
    .withColumn("_silver_loaded_at", F.current_timestamp())
    .drop("_source_file", "_ingested_at")
)
 
# Step 3: Data quality checks
df_pat = (
    df_pat
    .withColumn("_dq_pat_id_null", F.col("patient_id").isNull())
    .withColumn("_dq_ssn_null", F.col("ssn").isNull())
    .withColumn("_dq_ssn_format", ~F.col("ssn").rlike(r"^\d{3}-\d{2}-\d{4}$"))
    .withColumn("_dq_birth_null", F.col("birth_date").isNull())
    .withColumn("_dq_birth_future", F.col("birth_date") > F.current_date())
    .withColumn("_dq_gender_invalid", ~F.col("gender").isin("M", "F"))
    .withColumn("_dq_name_null", F.col("full_name").isNull())
)
 
df_pat = df_pat.withColumn(
    "_dq_failed",
    (
        F.col("_dq_pat_id_null") | F.col("_dq_ssn_null") | F.col("_dq_ssn_format")
        | F.col("_dq_birth_null") | F.col("_dq_birth_future")
        | F.col("_dq_gender_invalid") | F.col("_dq_name_null")
    )
)
 
df_pat_quarantine = (
    df_pat.filter(F.col("_dq_failed"))
    .withColumn("_dq_entity", F.lit("patients"))
    .withColumn("_dq_checked_at", F.current_timestamp())
)
 
dq_cols_pat = [
    "_dq_pat_id_null", "_dq_ssn_null", "_dq_ssn_format",
    "_dq_birth_null", "_dq_birth_future", "_dq_gender_invalid",
    "_dq_name_null", "_dq_failed"
]
df_pat_clean = df_pat.filter(~F.col("_dq_failed")).drop(*dq_cols_pat)
 
# Step 4: CDC DEDUP — keep only the latest version per patient
window_pat = (
    Window
    .partitionBy("patient_id")
    .orderBy(F.col("_load_date").desc(), F.col("source_updated_at").desc())
)
 
df_pat_clean = (
    df_pat_clean
    .withColumn("_row_number", F.row_number().over(window_pat))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
)
 
# Step 5: PII HASHING
# Generate a SHA-256 hash of the SSN with a salt. This creates a
# stable key (same SSN always produces the same hash) that can be
# used as a join key WITHOUT exposing the actual SSN.
#
# Example:
#   SSN "882-24-3172" + salt → "a3f7b2c1d4e5..."  (64-char hex string)
#
df_pat_clean = df_pat_clean.withColumn(
    "patient_hash_id",
    F.sha2(F.concat(F.lit(PII_SALT), F.col("ssn")), 256)
)
 
# Step 6: PII VAULT PATTERN — split into two tables
#
# TABLE 1: silver_patients (SAFE — no PII)
#   Contains: patient_hash_id, demographics, insurance, flags
#   Access: open to analysts, BI tools, reports
#
# TABLE 2: silver_patients_pii (RESTRICTED — contains PII)
#   Contains: patient_hash_id + name, SSN, email, phone, street
#   Access: only authorized users (enforced via CLS/DDM in Warehouse)
#
# To get the full picture, you JOIN them on patient_hash_id.
# But most analytics NEVER need the PII table.
 
# PII vault table (restricted)
df_pii_vault = df_pat_clean.select(
    "patient_hash_id",
    "patient_id",
    "full_name",        # PII
    "ssn",              # PII
    "email",            # PII
    "phone",            # PII
    "address_street",   # PII
    "_silver_loaded_at",
)
 
# Clean analytics table (no PII)
df_pat_silver = df_pat_clean.select(
    "patient_hash_id",
    "patient_id",
    "birth_date",
    "gender",
    "address_city",
    "address_state",
    "address_zip",
    "insurance_plan",
    "primary_provider_id",
    "is_active",
    "op",
    "_load_date",
    "source_created_at",
    "source_updated_at",
    "_silver_loaded_at",
    "_cast_failed_birth_date",
)
 
# Step 7: Write both tables
df_pat_silver.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("silver_patients")
df_pii_vault.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("silver_patients_pii")
 
pat_count = spark.table("silver_patients").count()
pii_count = spark.table("silver_patients_pii").count()
pat_q_count = df_pat_quarantine.count()
print(f"silver_patients:     {pat_count} rows written")
print(f"silver_patients_pii: {pii_count} rows (vault)")
print(f"patients quarantined: {pat_q_count} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 6 — Silver: encounters ========================================
# Fact table. Type casting with failure detection, DQ checks, and
# CDC dedup (same pattern as providers/patients).
 
# Step 1: Read from Bronze
df_enc = spark.table("bronze_encounters")
 
# Step 2: Clean and cast types (with failure detection for key fields)
df_enc = (
    df_enc
    # encounter_date: cast with failure detection
    .withColumn("_orig_encounter_date", F.col("encounter_date"))
    .withColumn("encounter_date", F.col("encounter_date").cast(DateType()))
    .withColumn(
        "_cast_failed_encounter_date",
        F.when(
            F.col("_orig_encounter_date").isNotNull() & F.col("encounter_date").isNull(),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
    .drop("_orig_encounter_date")
    # total_amount: cast with failure detection
    .withColumn("_orig_total_amount", F.col("total_amount"))
    .withColumn("total_amount", F.col("total_amount").cast(DecimalType(10, 2)))
    .withColumn(
        "_cast_failed_total_amount",
        F.when(
            F.col("_orig_total_amount").isNotNull() & F.col("total_amount").isNull(),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
    .drop("_orig_total_amount")
    # Clean string columns
    .withColumn("encounter_id", F.trim(F.col("encounter_id")))
    .withColumn("patient_id", F.trim(F.col("patient_id")))
    .withColumn("provider_id", F.trim(F.col("provider_id")))
    .withColumn("department_id", F.trim(F.col("department_id")))
    .withColumn("encounter_type", F.lower(F.trim(F.col("encounter_type"))))
    .withColumn("status", F.lower(F.trim(F.col("status"))))
    .withColumn("diagnosis_code", F.trim(F.col("diagnosis_code")))
    .withColumn("diagnosis_desc", F.trim(F.col("diagnosis_desc")))
    .withColumn("procedure_code", F.trim(F.col("procedure_code")))
    .withColumn("source_created_at", F.col("source_created_at").cast(TimestampType()))
    .withColumn("source_updated_at", F.col("source_updated_at").cast(TimestampType()))
    .withColumn("_silver_loaded_at", F.current_timestamp())
    .drop("_source_file", "_ingested_at")
)
 
# Step 3: Data quality checks
df_enc = (
    df_enc
    .withColumn("_dq_enc_id_null", F.col("encounter_id").isNull())
    .withColumn("_dq_patient_null", F.col("patient_id").isNull())
    .withColumn("_dq_provider_null", F.col("provider_id").isNull())
    .withColumn("_dq_date_null", F.col("encounter_date").isNull())
    .withColumn("_dq_date_future", F.col("encounter_date") > F.current_date())
    .withColumn("_dq_amount_invalid", ~(F.col("total_amount") > 0))
    .withColumn("_dq_status_invalid", ~F.col("status").isin(
        "scheduled", "completed", "cancelled", "no_show"
    ))
)
 
df_enc = df_enc.withColumn(
    "_dq_failed",
    (
        F.col("_dq_enc_id_null") | F.col("_dq_patient_null")
        | F.col("_dq_provider_null") | F.col("_dq_date_null")
        | F.col("_dq_date_future") | F.col("_dq_amount_invalid")
        | F.col("_dq_status_invalid")
    )
)
 
df_enc_quarantine = (
    df_enc.filter(F.col("_dq_failed"))
    .withColumn("_dq_entity", F.lit("encounters"))
    .withColumn("_dq_checked_at", F.current_timestamp())
)
 
dq_cols_enc = [
    "_dq_enc_id_null", "_dq_patient_null", "_dq_provider_null",
    "_dq_date_null", "_dq_date_future", "_dq_amount_invalid",
    "_dq_status_invalid", "_dq_failed"
]
df_enc_clean = df_enc.filter(~F.col("_dq_failed")).drop(*dq_cols_enc)
 
# Step 4: CDC DEDUP — keep only the latest version per encounter
window_enc = (
    Window
    .partitionBy("encounter_id")
    .orderBy(F.col("_load_date").desc(), F.col("source_updated_at").desc())
)
 
df_enc_clean = (
    df_enc_clean
    .withColumn("_row_number", F.row_number().over(window_enc))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
)
 
# Step 5: Write to Silver
df_enc_clean.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("silver_encounters")
 
enc_count = spark.table("silver_encounters").count()
enc_q_count = df_enc_quarantine.count()
print(f"silver_encounters: {enc_count} rows written, {enc_q_count} quarantined")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 7 — Quarantine table ==========================================
# Collect all quarantined rows into a single audit table.
# In production you would alert on this (e.g. if quarantine > 5%).
 
from functools import reduce
from pyspark.sql import DataFrame
 
quarantine_dfs = [
    df_dept_quarantine,
    df_ins_quarantine,
    df_prov_quarantine,
    df_pat_quarantine,
    df_enc_quarantine,
]
 
# Select only common columns and union
common_cols = ["_dq_entity", "_dq_checked_at"]
non_empty = [df.select(*common_cols) for df in quarantine_dfs if df.head(1)]
 
if non_empty:
    df_quarantine_all = reduce(DataFrame.unionByName, non_empty)
    df_quarantine_all.write.mode("overwrite").format("delta").saveAsTable("silver_dq_quarantine")
    q_count = spark.table("silver_dq_quarantine").count()
    print(f"silver_dq_quarantine: {q_count} rows")
else:
    print("silver_dq_quarantine: 0 rows (all data passed DQ checks)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 8 — Validation summary ========================================
print("=" * 60)
print("SILVER TRANSFORMATION SUMMARY")
print("=" * 60)
 
tables = [
    "silver_departments",
    "silver_insurance_plans",
    "silver_providers",
    "silver_patients",
    "silver_patients_pii",
    "silver_encounters",
]
 
for t in tables:
    count = spark.table(t).count()
    print(f"  {t:<28} {count:>6} rows")
 
print("-" * 60)
print("\nPII Vault pattern validation:")
print("  silver_patients     -> NO PII (hash key + demographics)")
print("  silver_patients_pii -> PII fields (name, SSN, email, phone)")
print("\nPeek at silver_patients (notice: no name, no SSN, no email):")
display(spark.table("silver_patients").limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
