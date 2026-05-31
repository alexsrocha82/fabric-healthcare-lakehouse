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

# Fabric notebook source
 
# ===================================================================
# nb_01_bronze_ingestion
# -------------------------------------------------------------------
# Ingests raw CDC change files from Files/landing/ into Delta tables
# bronze_<entity>, preserving every source column AS-IS (string) and
# adding lineage/audit columns. No cleaning, typing, or dedup here:
# that is the Silver layer's job. The Bronze layer is the immutable,
# replayable record of "what we received".
#
# Design choices:
#  * Read all columns as STRING  -> never let Spark guess types; the
#    Silver layer owns explicit casting. Prevents silent corruption.
#  * Append-only intent, idempotent run -> we overwrite the whole
#    bronze table from the full landing scan, so re-running never
#    duplicates. (Incremental variant comes with orchestration.)
#  * Audit columns prefixed with "_" to separate them from business
#    columns: _source_file, _ingested_at, _load_date.
# ===================================================================

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
 
# Entities to ingest from the landing zone. Order is irrelevant in
# Bronze (no dependencies between tables at this layer).
ENTITIES = ["departments", "insurance_plans", "providers", "patients", "encounters"]
 
LANDING_BASE = "Files/landing"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ingest_to_bronze(entity: str) -> int:
    """
    Read all partitioned CSVs for one entity from the landing zone and
    write a Delta table bronze_<entity>. Returns the row count written.
    """
    source_path = f"{LANDING_BASE}/{entity}"
 
    # recursiveFileLookup=true: descend into every load_date=*/ folder.
    # All columns read as string; header row provides column names.
    df = (
        spark.read
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .option("inferSchema", "false")  # everything stays string
        .csv(source_path)
    )
 
    # --- Audit / lineage columns -------------------------------------
    # input_file_name() captures the physical file each row came from.
    # We parse load_date out of the partition folder name in the path.
    df = (
        df
        .withColumn("_source_file", F.input_file_name())
        .withColumn(
            "_load_date",
            F.regexp_extract(F.col("_source_file"), r"load_date=(\d{4}-\d{2}-\d{2})", 1),
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )
 
    target_table = f"bronze_{entity}"
    (
        df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")  # schema may evolve between runs
        .format("delta")
        .saveAsTable(target_table)
    )
 
    count = spark.table(target_table).count()
    return count

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run ingestion for every entity and print a summary.
print("Bronze ingestion starting...\n")
results = {}
for ent in ENTITIES:
    n = ingest_to_bronze(ent)
    results[ent] = n
    print(f"  bronze_{ent:<16} {n:>6} rows")
 
print("\nBronze ingestion complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Quick validation: peek at bronze_patients to confirm structure.
# You should see original columns (op, patient_id, full_name, ssn, ...)
# all as string, plus the three audit columns at the end.
display(spark.table("bronze_patients").limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sanity check: row counts per load_date for patients. The first day
# is the big initial load; later days are smaller CDC change sets.
display(
    spark.table("bronze_patients")
    .groupBy("_load_date", "op")
    .count()
    .orderBy("_load_date", "op")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
