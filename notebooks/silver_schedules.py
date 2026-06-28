# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — `silver_schedules`
# MAGIC
# MAGIC Transforms `hooplakehouse.hoop.bronze_schedules` into a clean, analytics-ready table.
# MAGIC
# MAGIC **Steps**
# MAGIC 1. Load bronze source
# MAGIC 2. Cast mis-typed columns
# MAGIC 3. Parse date strings
# MAGIC 4. Normalize nulls & codes
# MAGIC 5. Drop cosmetic / redundant columns
# MAGIC 6. Deduplicate
# MAGIC 7. Add derived columns
# MAGIC 8. Write to Delta as `silver.schedules`
# MAGIC 9. Validation

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Bronze

# COMMAND ----------

df = spark.read.table("hooplakehouse.hoop.bronze_schedules")
print(f"Bronze row count: {df.count():,}")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cast Mis-Typed Columns
# MAGIC
# MAGIC `attendance`, `venue_capacity`, `status_clock`, `status_period`, and `format_regulation_periods`
# MAGIC were ingested as `double` but represent whole-number values.

# COMMAND ----------

double_to_int_cols = [
    "attendance",
    "venue_capacity",
    "status_clock",
    "status_period",
    "format_regulation_periods",
]

for col in double_to_int_cols:
    df = df.withColumn(col, F.col(col).cast(IntegerType()))

null_counts = (
    df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in double_to_int_cols])
      .collect()[0]
      .asDict()
)
print("Null counts after cast:", null_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Parse Date Strings
# MAGIC
# MAGIC `date` and `start_date` are ISO-8601 strings (e.g. `"2005-06-24T00:00Z"`). 
# MAGIC Parse them to `timestamp`. `game_date` and `game_date_time` are already correctly typed and kept as-is.

# COMMAND ----------

df = (
    df
    .withColumn("date",       F.to_timestamp(F.col("date"),       "yyyy-MM-dd'T'HH:mmX"))
    .withColumn("start_date", F.to_timestamp(F.col("start_date"), "yyyy-MM-dd'T'HH:mmX"))
)

# Verify parse success
bad_dates = df.filter(F.col("date").isNull() | F.col("start_date").isNull()).count()
print(f"Rows with unparseable date/start_date: {bad_dates}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Normalize Nulls & Codes

# COMMAND ----------

# Empty strings in notes columns -> null (not every game has notes)
df = (
    df
    .withColumn("notes_type",     F.nullif(F.trim(F.col("notes_type")),     F.lit("")))
    .withColumn("notes_headline", F.nullif(F.trim(F.col("notes_headline")), F.lit("")))
)

# season_type integer code -> human-readable label (consistent with silver_team_box)
df = df.withColumn(
    "season_type_label",
    F.when(F.col("season_type") == 1, "Preseason")
     .when(F.col("season_type") == 2, "Regular Season")
     .when(F.col("season_type") == 3, "Playoffs")
     .otherwise("Unknown")
)

# Rename conference_competition for clarity
df = df.withColumnRenamed("conference_competition", "is_conference_game")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Drop Cosmetic / Redundant Columns
# MAGIC
# MAGIC Removed:
# MAGIC - `uid`, `home_uid`, `away_uid` — internal ESPN identifiers
# MAGIC - `home_color`, `home_alternate_color`, `home_logo`, `home_short_display_name` — UI assets
# MAGIC - `away_color`, `away_alternate_color`, `away_logo`, `away_short_display_name` — UI assets
# MAGIC - `game_json_url` — raw data pipeline artifact, not analytical
# MAGIC - `game_json` — boolean flag for pipeline use only
# MAGIC - `type_abbreviation` — redundant with `season_type_label`
# MAGIC - `status_display_clock` — string representation of `status_clock` (e.g. `"0.0"`), redundant
# MAGIC - `status_type_name` — verbose form of `status_type_state` (e.g. STATUS_FINAL vs post)
# MAGIC - `recent` — pipeline-relative boolean, not stable for analysis

# COMMAND ----------

drop_cols = [
    "uid", "home_uid", "away_uid",
    "home_color", "home_alternate_color", "home_logo", "home_short_display_name",
    "away_color", "away_alternate_color", "away_logo", "away_short_display_name",
    "game_json_url", "game_json",
    "type_abbreviation",
    "status_display_clock",
    "status_type_name",
    "recent",
]

df = df.drop(*drop_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Deduplicate
# MAGIC
# MAGIC Primary key is `game_id` — one row per game.

# COMMAND ----------

before = df.count()
df = df.dropDuplicates(["game_id"])
after = df.count()
print(f"Rows before dedup: {before:,} | after: {after:,} | dropped: {before - after:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Derived Columns

# COMMAND ----------

# ── Scoring ───────────────────────────────────────────────────────────────────

# Total combined score
df = df.withColumn(
    "total_score",
    F.col("home_score") + F.col("away_score")
)

# Point differential from home team's perspective (positive = home win)
df = df.withColumn(
    "point_differential",
    F.col("home_score") - F.col("away_score")
)

# ── Game Context ──────────────────────────────────────────────────────────────

# Did the game go to overtime? (periods played > regulation periods)
df = df.withColumn(
    "went_to_overtime",
    F.when(
        F.col("status_period").isNotNull() & F.col("format_regulation_periods").isNotNull(),
        F.col("status_period") > F.col("format_regulation_periods")
    ).otherwise(False)
)

# Number of overtime periods played (0 if regulation)
df = df.withColumn(
    "overtime_periods",
    F.when(
        F.col("went_to_overtime"),
        F.col("status_period") - F.col("format_regulation_periods")
    ).otherwise(0)
)

# ── Venue ─────────────────────────────────────────────────────────────────────

# Attendance as a percentage of venue capacity
df = df.withColumn(
    "attendance_pct_capacity",
    F.when(
        F.col("venue_capacity") > 0,
        F.round(F.col("attendance") / F.col("venue_capacity"), 4)
    ).otherwise(None)
)

# ── Data Completeness ─────────────────────────────────────────────────────────

# Flag rows where all three supplementary data sources are available.
# Useful for filtering in downstream joins to silver_team_box / player_box.
df = df.withColumn(
    "data_complete",
    F.col("PBP") & F.col("team_box") & F.col("player_box")
)

print("Derived columns added.")
df.select(
    "game_id", "home_abbreviation", "away_abbreviation",
    "home_score", "away_score", "total_score", "point_differential",
    "went_to_overtime", "overtime_periods",
    "attendance", "venue_capacity", "attendance_pct_capacity",
    "data_complete"
).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Final Schema Review

# COMMAND ----------

df.printSchema()
print(f"Final row count: {df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Write to Silver
# MAGIC
# MAGIC Partitioned by `season` and `season_type` — consistent with `silver_team_box`
# MAGIC for efficient partition-pruning on joins between the two tables.

# COMMAND ----------

# CREATE SILVER SCHEMA
spark.sql("""
CREATE SCHEMA IF NOT EXISTS hooplakehouse.silver
""")

# COMMAND ----------

(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .partitionBy("season", "season_type")
      .saveAsTable("hooplakehouse.hoop.silver_schedules")
)

print("silver.schedules written successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Validation

# COMMAND ----------

silver = spark.sql("SELECT * FROM hooplakehouse.hoop.silver_schedules")

print(f"Row count: {silver.count():,}")

# Duplicate check
dup_count = silver.groupBy("game_id").count().filter(F.col("count") > 1).count()
print(f"Duplicate game_id rows: {dup_count}")

# Score integrity: home_winner should align with point_differential
winner_mismatch = silver.filter(
    F.col("status_type_completed") == True
).filter(
    (F.col("home_winner") == True)  & (F.col("point_differential") <= 0) |
    (F.col("home_winner") == False) & (F.col("point_differential") >= 0)
).count()
print(f"home_winner / point_differential mismatches (completed games): {winner_mismatch}")

# OT distribution
print("\nOvertime breakdown:")
silver.groupBy("overtime_periods").count().orderBy("overtime_periods").show()

# Data completeness summary
print("Data completeness (PBP + team_box + player_box):")
silver.groupBy("data_complete").count().show()

# Season / season_type distribution
silver.groupBy("season", "season_type", "season_type_label") \
      .count() \
      .orderBy("season", "season_type") \
      .show(50, truncate=False)

display(silver)
