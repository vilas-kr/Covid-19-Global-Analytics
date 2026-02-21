#!/usr/bin/env python3

# -------------------------------------------------------------------------
# Task 6: Global Time-Series Analysis

# Using day_wise.csv:
# 1. Global daily average new cases.
# 2. Detect spike days using Z-score.
# 3. Identify peak death date globally.
# 4. Month-over-Month death growth rate.
# -------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

# -------------------------------------------------------------------------
# 1. Spark Session
# -------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName('Global Time Series Analysis') \
    .getOrCreate()
    
spark.sparkContext.setLogLevel('ERROR')

STAGING_PATH = 'hdfs:///data/covid/staging/'
ANALYTICS_PATH = 'hdfs:///data/covid/analytics/'

# -------------------------------------------------------------------------
# 2. Read dataset from hadoop 
# -------------------------------------------------------------------------
df_day_wise = spark.read.parquet(STAGING_PATH + 'day_wise_parquet')

# -------------------------------------------------------------------------
# 3. Global daily average new cases
# -------------------------------------------------------------------------
daily_avg_new_cases = df_day_wise.withColumn(
        'avg_new_cases',
        avg('new_cases').over(
            Window.orderBy(col('date')).rowsBetween(
            Window.unboundedPreceding, 
            Window.currentRow
            )
        )
    ).withColumn(
        'avg_new_cases',
        round(col('avg_new_cases'), 2)
    ).select('date', 'new_cases', 'avg_new_cases')


    