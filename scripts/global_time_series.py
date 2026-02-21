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

# -------------------------------------------------------------------------
# 4. Detect spike days using Z-score
# -------------------------------------------------------------------------
# Calculate standard deviation and mean
stat = df_day_wise.select(
        stddev(col('new_cases')).alias('std'), 
        avg(col('new_cases')).alias('mean')
    ).collect()[0]

spike_days = df_day_wise.withColumn(
        'z_score',
        ((col('new_cases') - stat['mean']) / stat['std'])
    ).select(
        'date', 'new_cases', 'z_score'
    ).filter( col('z_score') > 2 )

print(f'Spike days using Z-score :')
spike_days.show()

# -------------------------------------------------------------------------
# 5. Identify peak death date globally
# -------------------------------------------------------------------------
print(f'Peak Death date : ')

df_day_wise.sort(
        col('new_deaths').desc()
    ).select(
        'date', col('new_deaths').alias('deaths')
    ).limit(1).show()

# -------------------------------------------------------------------------
# 6. Month-over-Month death growth rate
# -------------------------------------------------------------------------
months_death_growth = df_day_wise.withColumn(
        'month', 
        month(col('date'))
    ).withColumn(
        'year',
        year(col('date'))
    ).groupBy(
        col('year'), col('month')
    ).agg(
        sum('new_deaths').alias('total_deaths')
    ).withColumn(
        'previous_total_deaths',
        lag('total_deaths').over(
            Window.orderBy(col('year'), col('month'))
        )
    ).withColumn(
        'deaths_growth',
        ((col('total_deaths') - col('previous_total_deaths')) 
            / col('previous_total_deaths')) * 100
    )

print(f'Month-over-Month death growth rate : ')
months_death_growth.show() 
