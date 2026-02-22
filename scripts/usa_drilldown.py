#!/usr/bin/env python3

# -------------------------------------------------------------------------
# Task 7: USA Drilldown Analysis

# Using usa_county_wise.csv:
# 1. Aggregate county data to state level.
# 2. Identify top 10 affected states.
# 3. Detect data skew across states.
# 4. Explain skew impact in distributed systems.
# -------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

# -------------------------------------------------------------------------
# 1. Spark Session
# -------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName('USA drill down Analysis') \
    .getOrCreate()
    
spark.sparkContext.setLogLevel('ERROR')

STAGING_PATH = 'hdfs:///data/covid/staging/'
ANALYTICS_PATH = 'hdfs:///data/covid/analytics/'

# -------------------------------------------------------------------------
# 2. Read dataset from hadoop 
# -------------------------------------------------------------------------
df_usa_county_wise = spark.read.parquet(STAGING_PATH + 
                        'usa_county_wise_parquet')

# -------------------------------------------------------------------------
# 3. Aggregate county data to state level
# -------------------------------------------------------------------------
state_level_aggregation = df_usa_county_wise.groupBy(
        'province_state'
    ).agg(
        avg(col('lat')).alias('avg_lat'),
        avg(col('long')).alias('avg_long'),
        count_distinct(col('admin2')).alias('total_admin'),
        sum(col('confirmed')).alias('total_confirmed'),
        sum(col('deaths')).alias('total_deaths')
    )
    
print('Aggregated state level data')
state_level_aggregation.show()

# -------------------------------------------------------------------------
# 4. Identify top 10 affected states
# -------------------------------------------------------------------------
top_affected_states = state_level_aggregation.sort(
        col('total_confirmed').desc(),
        col('total_deaths').desc()
    ).select(
        col('province_state'),
        col('total_confirmed'),
        col('total_deaths')
    )
    
print('Top 10 affected states')
top_affected_states.show(10)

