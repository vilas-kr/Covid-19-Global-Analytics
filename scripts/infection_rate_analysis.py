#!/usr/bin/env python3

# -------------------------------------------------------------------------
# Task 4: Infection Rate Analysis

# Using worldometer_data:
# Confirmed cases per 1000 population.
# Active cases per 1000 population.
# Top 10 countries by infection rate.
# WHO region infection ranking.
# -------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.functions import * 

# -------------------------------------------------------------------------
# 1. Spark Session
# -------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName('Infection Analysis') \
    .getOrCreate()
    
STAGING_PATH = 'hdfs:///data/covid/staging/'
ANALYTICS_PATH = 'hdfs:///data/covid/analytics/'

# -------------------------------------------------------------------------
# 2. Read dataset from hadoop 
# -------------------------------------------------------------------------
df_full_grouped = spark.read.parquet(STAGING_PATH + 'full_grouped_parquet')
df_worldometer_data = spark.read.parquet(STAGING_PATH + \
    'worldometer_data_parquet')

# -------------------------------------------------------------------------
# 3. Confirmed cases per 1000 population
# -------------------------------------------------------------------------
df_worldometer_data = df_worldometer_data.withColumn(
    'confirmed_cases_per(1000)',
    round(
            (col('total_cases') / col('population')) * 1000, 2
        )
    )

# -------------------------------------------------------------------------
# 4. Active cases per 1000 population
# -------------------------------------------------------------------------
df_worldometer_data = df_worldometer_data.withColumn(
    'active_cases_per(1000)',
    round(
            (col('active_cases') / col('population')) * 1000, 2
        )
    )
