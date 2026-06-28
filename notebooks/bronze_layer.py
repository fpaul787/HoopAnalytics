# Databricks notebook source
# DBTITLE 1,Notebook Summary
# MAGIC %md
# MAGIC # Bronze Layer — NBA Raw Data Ingestion
# MAGIC
# MAGIC This notebook ingests raw NBA data from Azure Data Lake Storage (ADLS Gen2) into Delta tables in the `hooplakehouse.hoop` schema using **Auto Loader** (Structured Streaming).
# MAGIC
# MAGIC ## Steps
# MAGIC 1. **Auth** — Configures the ADLS account key via Databricks Secrets (`fplakehouse` scope).
# MAGIC 2. **Schema setup** — Creates the `hooplakehouse.hoop` schema if it does not exist.
# MAGIC 3. **Player & Team box scores** — Streams all parquet files from the `player_box/` and `team_box/` folders into `bronze_player_box` and `bronze_team_box` tables.
# MAGIC 4. **Schedules** — Streams NBA schedule files for seasons **2000–2026** (glob-filtered) into `bronze_schedules`.
# MAGIC
# MAGIC ## Key Details
# MAGIC - **Trigger**: `availableNow=True` — designed for incremental daily runs (processes only new files)
# MAGIC - **Schema inference**: `cloudFiles.inferColumnTypes` enabled; schema evolution tracked per table in `_schemas/`
# MAGIC - **Checkpoints**: Stored under `_checkpoints/` on ADLS to track ingestion progress

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.fabricdatastoragefp787.dfs.core.windows.net",
    dbutils.secrets.get(scope="fplakehouse", key="azurestoragekey"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS hooplakehouse.hoop;

# COMMAND ----------

# DBTITLE 1,Ingest each folder as a Bronze table
# Auto Loader: player_box and team_box (all parquet files)

storage_base = "abfss://hoopr-nba-storage@fabricdatastoragefp787.dfs.core.windows.net"

for folder in ["player_box", "team_box"]:
    source_path = f"{storage_base}/{folder}/parquet"
    target_table = f"hooplakehouse.hoop.bronze_{folder}"
    checkpoint = f"{storage_base}/_checkpoints/{folder}"
    schema_location = f"{storage_base}/_schemas/{folder}"

    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
    )

    query = (df.writeStream
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    query.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Read schedules with glob pattern
# Auto Loader: schedules (only seasons 2000-2026)

storage_base = "abfss://hoopr-nba-storage@fabricdatastoragefp787.dfs.core.windows.net"
source_path = f"{storage_base}/schedules/parquet"
checkpoint = f"{storage_base}/_checkpoints/schedules"
schema_location = f"{storage_base}/_schemas/schedules"

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("pathGlobFilter", "nba_schedule_{200[0-9],201[0-9],202[0-6]}.parquet")
    .load(source_path)
)

query = (df.writeStream
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable("hooplakehouse.hoop.bronze_schedules")
)
query.awaitTermination()

# COMMAND ----------


