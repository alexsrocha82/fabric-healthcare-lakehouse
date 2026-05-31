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

# See all tables available
display(spark.sql("SHOW TABLES"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.read.table("bronze_departments").limit(3)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table("bronze_departments").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
