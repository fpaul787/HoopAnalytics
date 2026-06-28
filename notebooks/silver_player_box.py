# Databricks notebook source
df = spark.read.table("hooplakehouse.hoop.bronze_player_box")

# COMMAND ----------

df = df.dropDuplicates([
    "game_id",
    "athlete_id",
    "team_id"
])

# COMMAND ----------

df = df.dropna(
    subset=[
        "game_id",
        "season",
        "athlete_id",
        "team_id"
    ]
)

# COMMAND ----------

from pyspark.sql.functions import coalesce, lit, col

stat_columns = [
    "points",
    "rebounds",
    "assists",
    "steals",
    "blocks",
    "turnovers",
    "minutes",
    "field_goals_made",
    "field_goals_attempted",
    "free_throws_made",
    "free_throws_attempted"
]

df = df.withColumns({c: coalesce(col(c), lit(0)) for c in stat_columns})

# COMMAND ----------

from pyspark.sql.functions import upper

df = df.withColumn(
    "home_away",
    upper(col("home_away"))
)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col

df = df.withColumn(
    "plus_minus",
    col("plus_minus").cast("int")
)

# COMMAND ----------

from pyspark.sql.functions import when

df = df.withColumn(
    "fg_pct",
    when(
        col("field_goals_attempted") > 0,
        col("field_goals_made") /
        col("field_goals_attempted")
    )
)

# COMMAND ----------

from pyspark.sql.functions import expr
from pyspark.sql.functions import when, col

df = df.withColumn(
    "double_double",
    (
        when(col("points") >= 10, 1).otherwise(0) +
        when(col("rebounds") >= 10, 1).otherwise(0) +
        when(col("assists") >= 10, 1).otherwise(0) +
        when(col("steals") >= 10, 1).otherwise(0) +
        when(col("blocks") >= 10, 1).otherwise(0)
    ) >= 2
)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df = df.withColumn(
    "silver_load_timestamp",
    current_timestamp()
)

(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable("hooplakehouse.hoop.silver_player_box")
)

# COMMAND ----------


