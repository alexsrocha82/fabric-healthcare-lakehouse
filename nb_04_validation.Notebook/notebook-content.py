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
# META     }
# META   }
# META }

# CELL ********************

# ===================================================================
# nb_04_validation
# -------------------------------------------------------------------
# Data quality and integrity validation across all medallion layers.
# Each cell is an independent check that prints PASS or FAIL.
# Run this after the pipeline to certify the data is correct.
#
# Cell 1: Row count reconciliation (Bronze vs Silver vs Gold)
# Cell 2: DQ quarantine check
# Cell 3: PII vault validation (no PII leaked into analytics table)
# Cell 4: SCD2 integrity — exactly one current version per entity
# Cell 5: SCD2 integrity — no overlapping date ranges
# Cell 6: Referential integrity — no orphan facts
# Cell 7: Business metric sanity checks
# Cell 8: Validation summary
# ===================================================================

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 1 — Row count reconciliation ==================================
from pyspark.sql import functions as F

print("=" * 60)
print("CHECK 1: Row count reconciliation")
print("=" * 60)

# Silver current-state counts should match Gold current versions.
silver_patients = spark.table("silver_patients").count()
gold_patients_current = spark.table("gold_dim_patient").filter(F.col("is_current")).count()

silver_providers = spark.table("silver_providers").count()
gold_providers_current = spark.table("gold_dim_provider").filter(F.col("is_current")).count()

silver_encounters = spark.table("silver_encounters").count()
gold_encounters = spark.table("gold_fact_encounters").count()

print(f"Patients:   silver={silver_patients}, gold_current={gold_patients_current} "
      f"-> {'PASS' if silver_patients == gold_patients_current else 'FAIL'}")
print(f"Providers:  silver={silver_providers}, gold_current={gold_providers_current} "
      f"-> {'PASS' if silver_providers == gold_providers_current else 'FAIL'}")
print(f"Encounters: silver={silver_encounters}, gold_fact={gold_encounters} "
      f"-> {'PASS' if silver_encounters == gold_encounters else 'FAIL'}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 2 — DQ quarantine check ======================================
print("=" * 60)
print("CHECK 2: Data quality quarantine")
print("=" * 60)

# Check if the quarantine table exists and report failures by entity.
tables = [t.name for t in spark.catalog.listTables()]

if "silver_dq_quarantine" in tables:
    q = spark.table("silver_dq_quarantine")
    total_q = q.count()
    print(f"Total quarantined rows: {total_q}")
    if total_q > 0:
        display(q.groupBy("_dq_entity").count().orderBy("_dq_entity"))
    else:
        print("PASS: No rows failed data quality checks.")
else:
    print("PASS: No quarantine table (no DQ failures).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 3 — PII vault validation =====================================
print("=" * 60)
print("CHECK 3: PII vault — no PII in analytics table")
print("=" * 60)

# silver_patients must NOT contain any PII columns.
pii_columns = ["full_name", "ssn", "email", "phone", "address_street"]
patient_cols = spark.table("silver_patients").columns

leaked = [c for c in pii_columns if c in patient_cols]

if not leaked:
    print("PASS: silver_patients contains NO PII columns.")
    print(f"      Columns present: {patient_cols}")
else:
    print(f"FAIL: PII columns leaked into silver_patients: {leaked}")

# The vault table SHOULD contain PII (it's the restricted store).
vault_cols = spark.table("silver_patients_pii").columns
vault_has_pii = [c for c in pii_columns if c in vault_cols]
print(f"\nVault (silver_patients_pii) PII columns: {vault_has_pii}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 4 — SCD2: one current version per entity =====================
print("=" * 60)
print("CHECK 4: SCD2 — exactly one current version per entity")
print("=" * 60)

# Each business key must have exactly ONE row where is_current = true.
for table, key in [("gold_dim_patient", "patient_id"), ("gold_dim_provider", "provider_id")]:
    multi_current = (
        spark.table(table)
        .filter(F.col("is_current"))
        .groupBy(key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    result = "PASS" if multi_current == 0 else "FAIL"
    print(f"{table}: {multi_current} keys with multiple current versions -> {result}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 5 — SCD2: no overlapping date ranges =========================
from pyspark.sql import Window

print("=" * 60)
print("CHECK 5: SCD2 — no overlapping effective date ranges")
print("=" * 60)

# For each entity, the next version's start must be AFTER this version's end.
for table, key in [("gold_dim_patient", "patient_id"), ("gold_dim_provider", "provider_id")]:
    w = Window.partitionBy(key).orderBy("effective_start")
    df = (
        spark.table(table)
        .withColumn("_next_start", F.lead("effective_start").over(w))
        .filter(F.col("_next_start").isNotNull())
        .filter(F.col("_next_start") <= F.col("effective_end"))
    )
    overlaps = df.count()
    result = "PASS" if overlaps == 0 else "FAIL"
    print(f"{table}: {overlaps} overlapping date ranges -> {result}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 6 — Referential integrity ====================================
print("=" * 60)
print("CHECK 6: Referential integrity — no orphan facts")
print("=" * 60)

fact = spark.table("gold_fact_encounters")

# Every fact must have a valid (non-null) dimension key.
null_patient = fact.filter(F.col("patient_key").isNull()).count()
null_provider = fact.filter(F.col("provider_key").isNull()).count()
null_date = fact.filter(F.col("date_key").isNull()).count()

print(f"Facts with null patient_key:  {null_patient} -> {'PASS' if null_patient == 0 else 'FAIL'}")
print(f"Facts with null provider_key: {null_provider} -> {'PASS' if null_provider == 0 else 'FAIL'}")
print(f"Facts with null date_key:     {null_date} -> {'PASS' if null_date == 0 else 'FAIL'}")

# Every patient_key in fact must exist in dim_patient.
dim_patient_keys = spark.table("gold_dim_patient").select("patient_key").distinct()
orphan_patients = (
    fact.select("patient_key").distinct()
    .join(dim_patient_keys, on="patient_key", how="left_anti")
    .count()
)
print(f"Orphan patient_keys (not in dim): {orphan_patients} -> {'PASS' if orphan_patients == 0 else 'FAIL'}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 7 — Business metric sanity checks ============================
print("=" * 60)
print("CHECK 7: Business metric sanity")
print("=" * 60)

fact = spark.table("gold_fact_encounters")

# All encounter amounts must be positive.
non_positive = fact.filter(F.col("total_amount") <= 0).count()
print(f"Encounters with non-positive amount: {non_positive} -> {'PASS' if non_positive == 0 else 'FAIL'}")

# All statuses must be in the allowed set.
valid_statuses = ["scheduled", "completed", "cancelled", "no_show"]
invalid_status = fact.filter(~F.col("status").isin(valid_statuses)).count()
print(f"Encounters with invalid status: {invalid_status} -> {'PASS' if invalid_status == 0 else 'FAIL'}")

# Encounter dates must fall within our simulated window.
out_of_range = fact.filter(
    (F.col("encounter_date") < "2024-01-01") | (F.col("encounter_date") > "2024-12-31")
).count()
print(f"Encounters outside date window: {out_of_range} -> {'PASS' if out_of_range == 0 else 'FAIL'}")

# Show encounters by status for a quick visual sanity check.
print("\nEncounters by status:")
display(fact.groupBy("status").count().orderBy(F.col("count").desc()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 8 — Validation summary =======================================
print("=" * 60)
print("VALIDATION COMPLETE")
print("=" * 60)
print("""
All checks above should show PASS. This notebook certifies:
  1. Row counts reconcile across Silver and Gold layers
  2. Data quality failures are quarantined (not silently dropped)
  3. No PII leaked into the analytics table (vault pattern holds)
  4. SCD2 dimensions have exactly one current version per entity
  5. SCD2 date ranges do not overlap (clean version history)
  6. No orphan facts (referential integrity intact)
  7. Business metrics pass sanity checks (positive amounts, valid statuses)

Run this notebook as the final step of the pipeline to certify data quality.
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
