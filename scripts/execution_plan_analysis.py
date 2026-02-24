#!/usr/bin/env python3

# -------------------------------------------------------------------------
# Task 11: Execution Plan Analysis
# For at least 3 major queries:
# Use:
# df.explain("extended")

# Identify:
# Exchange
# BroadcastHashJoin
# SortMergeJoin
# WholeStageCodegen
# Explain each component
# -------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Start Spark Session
# ---------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("ExecutionPlan") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------------------------------
# Read Data
# ---------------------------------------------------------------------------
df = spark.read.parquet('hdfs:///data/covid/staging/full_grouped_parquet')

# ---------------------------------------------------------------------------
# 1. Aggregation Query
# ---------------------------------------------------------------------------
death_df = df.groupBy("Country_Region") \
             .agg(sum("Deaths").alias("total_deaths"))

death_df.explain("extended")

# Execution Plan Components to Identify:
# - Exchange:
#   Indicates shuffle due to groupBy (wide transformation).
#   Data is hash-partitioned by Country/Region.
#
# - HashAggregate:
#   Performs partial + final aggregation.
#
# - WholeStageCodegen:
#   Spark combines multiple physical operators into a single JVM function
#   for CPU efficiency.

# ---------------------------------------------------------------------------
# 2. Join Query
# ---------------------------------------------------------------------------
world_df = spark.read.parquet("hdfs:///data/covid/staging/worldometer_data_parquet")

joined_df = df.join(
    world_df,
    df["Country_Region"] == world_df["Country_Region"],
    "inner"
)
joined_df.explain("extended")

# Execution Plan Components to Identify:
#
# - Exchange:
#   Shuffle on both sides if Spark chooses SortMergeJoin.
# - SortMergeJoin:
#   Used when both datasets are large.
#   Requires shuffle + sort before merge.
# - BroadcastHashJoin (if applicable):
#   Appears when smaller dataset is auto-broadcasted.
#   Avoids shuffle on small side.
# - WholeStageCodegen:
#   Optimizes execution by generating compiled JVM bytecode.

# ---------------------------------------------------------------------------
# 3. Window Function Query
# ---------------------------------------------------------------------------
windowSpec = Window.partitionBy("Country_Region") \
                   .orderBy("Date") \
                   .rowsBetween(-6, 0)

rolling_df = df.withColumn(
    "rolling_avg_recovery",
    avg("Recovered").over(windowSpec)
)
rolling_df.explain("extended")

# Execution Plan Components to Identify:
# - Exchange:
#   Occurs if partitioning requires redistribution by Country/Region.
# - Sort:
#   Required due to orderBy(Date) inside window.
# - Window:
#   Applies rolling aggregation within partition.
# - WholeStageCodegen:
#   Combines sort + window operators into optimized execution pipeline.