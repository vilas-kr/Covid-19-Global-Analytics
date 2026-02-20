#!/usr/bin/env python3

# -------------------------------------------------------------------------
# Task 5: Recovery Efficiency

# Recovered percentage per country.
# 7-day rolling recovery average (Window function).
# Country with fastest recovery growth.
# Peak recovery day per country
# -------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# -------------------------------------------------------------------------
# 1. Spark Session
# -------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName('Recovery Analysis') \
    .getOrCreate()
    
spark.sparkContext.setLogLevel('ERROR')

STAGING_PATH = 'hdfs:///data/covid/staging/'
ANALYTICS_PATH = 'hdfs:///data/covid/analytics/'

# -------------------------------------------------------------------------
# 2. Read dataset from hadoop 
# -------------------------------------------------------------------------
df_full_grouped = spark.read.parquet(STAGING_PATH + 'full_grouped_parquet')
df_day_wise = spark.read.parquet(STAGING_PATH + 'day_wise_parquet')

# -------------------------------------------------------------------------
# 3. Recovered percentage per country
# -------------------------------------------------------------------------
country_recovered_percentage = df_full_grouped.groupBy(
     col('country_region')
    ).agg(
        sum(col('confirmed')).alias('total_confirmed'),
        sum(col('recovered')).alias('total_recovered')
    ).withColumn('recovered_percentage'
        when(col('total_confirmed') > 0, 
             round((col('total_recovered') / col('total_confirmed')) * 100, 
                2)
        ).otherwise(0)
    )

# -------------------------------------------------------------------------
# 4. 7-day rolling recovery average
# -------------------------------------------------------------------------
rolling_recovery = df_day_wise.withColumn(
        '7_days_rolling_recovery_avg',
        avg(col('recovered')).over(
            Window.orderBy(col('date')).rowsBetween(-6, Window.currentRow)
        )
    )



