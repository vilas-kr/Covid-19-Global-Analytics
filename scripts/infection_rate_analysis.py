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
from pyspark.sql.functions import *
from pyspark.sql.window import Window 

# -------------------------------------------------------------------------
# 1. Spark Session
# -------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName('Infection Analysis') \
    .getOrCreate()
    
spark.sparkContext.setLogLevel('ERROR')

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
        'confirmed_cases_1000',
        round((col('total_cases') / col('population')) * 1000, 2)
    )

# -------------------------------------------------------------------------
# 4. Active cases per 1000 population
# -------------------------------------------------------------------------
df_worldometer_data = df_worldometer_data.withColumn(
        'active_cases_1000',
        round((col('active_cases') / col('population')) * 1000, 2)
    )

# -------------------------------------------------------------------------
# 5. Top 10 countries by infection rate
# -------------------------------------------------------------------------
print(f'Top 10 infected countries')
df_worldometer_data.sort(
        col('confirmed_cases_1000').desc()
    ).select(
        col('country_region').alias('country'), 
        col('confirmed_cases_1000').alias('infection_rate')
    ).show(10)

# -------------------------------------------------------------------------
# 6. WHO region infection rank
# -------------------------------------------------------------------------
who_infection_rate = df_worldometer_data.join(
        df_full_grouped.select('country_region', 'who_region'),
        df_full_grouped['country_region'] == df_worldometer_data['country_region'],
        'inner'
    ).groupBy(col('who_region')).agg(
        sum(col('total_cases')).alias('total_cases'),
        sum(col('population')).alias('total_population')
    ).withColumn(
        'infection_rate_1000',
        (col('total_cases') / col('total_population')) * 1000
    ).withColumn(
        'Rank',
        dense_rank().over(
            Window.orderBy(col('infection_rate_1000').desc())
        )
    )
    
print(f'WHO Region infection ranking')   
who_infection_rate.show()

# -------------------------------------------------------------------------
# 7. Store result into HDFS
# -------------------------------------------------------------------------
who_infection_rate.write \
    .mode('overwrite') \
    .parquet(ANALYTICS_PATH + 'WHO_infection_rate_parquet')

df_worldometer_data.write \
    .mode('overwrite') \
    .parquet(ANALYTICS_PATH + 'worldometer_data_parquet')
