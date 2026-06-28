# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — `silver_team_box`
# MAGIC
# MAGIC Transforms `hooplakehouse.hoop.bronze_team_box` into a clean, analytics-ready table.
# MAGIC
# MAGIC **Steps**
# MAGIC 1. Load bronze source
# MAGIC 2. Cast mis-typed columns
# MAGIC 3. Normalize flags & codes
# MAGIC 4. Drop cosmetic / redundant columns
# MAGIC 5. Deduplicate
# MAGIC 6. Add derived / efficiency metrics
# MAGIC 7. Write to Delta as `hooplakehouse.hoop.silver_team_box`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Bronze

# COMMAND ----------

df = spark.read.table("hooplakehouse.hoop.bronze_team_box")
print(f"Bronze row count: {df.count():,}")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cast Mis-Typed Columns
# MAGIC
# MAGIC The following columns contain numeric values but were ingested as `string` in bronze.
# MAGIC Cast them to `IntegerType` and report null counts introduced by invalid values.

# COMMAND ----------

string_to_int_cols = [
    "fast_break_points",
    "points_in_paint",
    "turnover_points",
    "largest_lead",
]

for col in string_to_int_cols:
    df = df.withColumn(col, F.col(col).cast(IntegerType()))

# Sanity check: count nulls introduced by bad casts
# null_counts = {c: df.filter(F.col(c).isNull()).count() for c in string_to_int_cols}
null_counts = (
    df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in string_to_int_cols]).collect()[0].asDict()
)
print("Null counts after cast:", null_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Normalize Flags & Codes

# COMMAND ----------

# team_home_away -> boolean is_home
df = df.withColumn(
    "is_home",
    F.when(F.lower(F.col("team_home_away")) == "home", True)
     .when(F.lower(F.col("team_home_away")) == "away", False)
     .otherwise(None)
).drop("team_home_away")

# season_type integer code -> human-readable label
# ESPN codes: 1 = Preseason, 2 = Regular Season, 3 = Playoffs
df = df.withColumn(
    "season_type_label",
    F.when(F.col("season_type") == 1, "Preseason")
     .when(F.col("season_type") == 2, "Regular Season")
     .when(F.col("season_type") == 3, "Playoffs")
     .otherwise("Unknown")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Drop Cosmetic / Redundant Columns
# MAGIC
# MAGIC The following columns are display assets or are redundant with other fields:
# MAGIC - `team_uid` / `opponent_team_uid` — internal ESPN identifiers, not useful for analytics
# MAGIC - `team_slug` / `opponent_team_slug` — redundant with `team_abbreviation`
# MAGIC - `team_color`, `team_alternate_color`, `team_logo` — UI assets
# MAGIC - `opponent_team_color`, `opponent_team_alternate_color`, `opponent_team_logo` — UI assets
# MAGIC - `team_short_display_name` / `opponent_team_short_display_name` — redundant with `team_name`

# COMMAND ----------

drop_cols = [
    "team_uid", "team_slug", "team_color", "team_alternate_color",
    "team_logo", "team_short_display_name",
    "opponent_team_uid", "opponent_team_slug", "opponent_team_color",
    "opponent_team_alternate_color", "opponent_team_logo",
    "opponent_team_short_display_name",
]

df = df.drop(*drop_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Deduplicate
# MAGIC
# MAGIC Primary key is `(game_id, team_id)` — one row per team per game.

# COMMAND ----------

before = df.count()
df = df.dropDuplicates(["game_id", "team_id"])
after = df.count()
print(f"Rows before dedup: {before:,} | after: {after:,} | dropped: {before - after:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Derived & Efficiency Columns

# COMMAND ----------

# ── Scoring ──────────────────────────────────────────────────────────────────

# Point differential (positive = won, negative = lost)
df = df.withColumn(
    "point_differential",
    F.col("team_score") - F.col("opponent_team_score")
)

# Categorised margin: useful for filtering without writing CASE every time
df = df.withColumn(
    "score_margin_category",
    F.when(F.abs(F.col("point_differential")) <= 3,  "OT-Range (1-3)")
     .when(F.abs(F.col("point_differential")) <= 8,  "Close (4-8)")
     .when(F.abs(F.col("point_differential")) <= 15, "Comfortable (9-15)")
     .otherwise("Blowout (16+)")
)

# ── Shooting Efficiency ───────────────────────────────────────────────────────

# Effective FG% = (FGM + 0.5 * 3PM) / FGA  — weights 3-pointers properly
df = df.withColumn(
    "effective_fg_pct",
    F.when(
        F.col("field_goals_attempted") > 0,
        F.round(
            (F.col("field_goals_made") + 0.5 * F.col("three_point_field_goals_made"))
            / F.col("field_goals_attempted"),
            4
        )
    ).otherwise(None)
)

# True Shooting % = PTS / (2 * (FGA + 0.44 * FTA))  — accounts for FTs
df = df.withColumn(
    "true_shooting_pct",
    F.when(
        (F.col("field_goals_attempted") + F.col("free_throws_attempted")) > 0,
        F.round(
            F.col("team_score")
            / (2 * (F.col("field_goals_attempted") + 0.44 * F.col("free_throws_attempted"))),
            4
        )
    ).otherwise(None)
)

# 3-Point Rate = 3PA / FGA  — share of shots from deep
df = df.withColumn(
    "three_point_rate",
    F.when(
        F.col("field_goals_attempted") > 0,
        F.round(
            F.col("three_point_field_goals_attempted") / F.col("field_goals_attempted"),
            4
        )
    ).otherwise(None)
)

# Free Throw Rate = FTA / FGA  — ability to get to the line
df = df.withColumn(
    "free_throw_rate",
    F.when(
        F.col("field_goals_attempted") > 0,
        F.round(
            F.col("free_throws_attempted") / F.col("field_goals_attempted"),
            4
        )
    ).otherwise(None)
)

# ── Ball Control ──────────────────────────────────────────────────────────────

# Assist-to-Turnover Ratio
df = df.withColumn(
    "assist_to_turnover_ratio",
    F.when(
        F.col("turnovers") > 0,
        F.round(F.col("assists") / F.col("turnovers"), 2)
    ).otherwise(None)
)

# ── Rebounding ────────────────────────────────────────────────────────────────

# Data quality flag: offensive + defensive should equal total rebounds
df = df.withColumn(
    "rebound_count_mismatch",
    (F.col("offensive_rebounds") + F.col("defensive_rebounds")) != F.col("total_rebounds")
)

print("Derived columns added.")
df.select(
    "game_id", "team_abbreviation", "team_score", "opponent_team_score",
    "point_differential", "score_margin_category",
    "effective_fg_pct", "true_shooting_pct",
    "three_point_rate", "free_throw_rate",
    "assist_to_turnover_ratio", "rebound_count_mismatch"
).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Final Schema Review

# COMMAND ----------

df.printSchema()
print(f"Final row count: {df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write to Silver
# MAGIC
# MAGIC Partition by `season` and `season_type` for efficient downstream filtering.
# MAGIC Using `mergeSchema=True` so future bronze additions can flow through without breaking the write.

# COMMAND ----------

# CREATE SILVER SCHEMA
spark.sql("""
CREATE SCHEMA IF NOT EXISTS hooplakehouse.silver
""")

(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("mergeSchema", "true")
      .partitionBy("season", "season_type")
      .saveAsTable("hooplakehouse.hoop.silver_team_box")
)

print("hoop.silver_team_box written successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validation

# COMMAND ----------

silver = spark.sql("SELECT * FROM hooplakehouse.hoop.silver_team_box")

print(f"Row count: {silver.count():,}")

# Duplicate check
dup_count = silver.groupBy("game_id", "team_id").count().filter(F.col("count") > 1).count()
print(f"Duplicate (game_id, team_id) pairs: {dup_count}")

# Rebound mismatch summary
mismatch = silver.filter(F.col("rebound_count_mismatch") == True).count()
print(f"Rows with rebound count mismatch: {mismatch}")

# Season / season_type distribution
silver.groupBy("season", "season_type", "season_type_label") \
      .count() \
      .orderBy("season", "season_type") \
      .show(truncate=False)

display(silver)

# COMMAND ----------


