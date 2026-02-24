#!/usr/bin/env python3

# -------------------------------------------------------------------------
# Task 9: Spark SQL Implementation

# Create temporary views.
# Write SQL queries for:
# 1. Top 10 infection countries
# 2. Death percentage ranking
# 3. Rolling 7-day average
# 4. Compare physical plans with DataFrame API
# -------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# -------------------------------------------------------------------------
# 1. Spark Session
# -------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName('Spark SQL') \
    .getOrCreate()
    
spark.sparkContext.setLogLevel('ERROR')

STAGING_PATH = 'hdfs:///data/covid/staging/'

# -------------------------------------------------------------------------
# 2. Read dataset from hadoop 
# -------------------------------------------------------------------------
df_worldometer_data = spark.read.parquet(STAGING_PATH + \
    'worldometer_data_parquet')

# -------------------------------------------------------------------------
# 3. Create temporary views
# -------------------------------------------------------------------------
df_worldometer_data.createOrReplaceTempView('worldometer')

# -------------------------------------------------------------------------
# 4. Top 10 infection countries
# -------------------------------------------------------------------------
top_10_countries = spark.sql('''
        SELECT country_region, round( ((total_cases / population) * 1000),
            2) AS infection_rate_per_1000
        FROM worldometer
        ORDER BY infection_rate_per_1000 DESC
        LIMIT 10
        ''')

print('\nTop 10 Infected countries : ')
top_10_countries.show()

# -------------------------------------------------------------------------
# 5. Death percentage ranking
# -------------------------------------------------------------------------
country_death_ranking = spark.sql('''
    SELECT country_region, ROUND( (total_deaths / total_cases) * 1000, 2) 
        AS death_rate_per_1000, DENSE_RANK() OVER(
            ORDER BY ROUND( (total_deaths / total_cases) * 1000, 2) DESC
            ) AS death_ranking
        FROM worldometer
    ''')

print('Country wise Death Ranking : ')
country_death_ranking.show()

# -------------------------------------------------------------------------
# 6. Rolling 7-day average
# -------------------------------------------------------------------------
df_day_wise = spark.read.parquet(STAGING_PATH + 'day_wise_parquet')

df_day_wise.createOrReplaceTempView('day_wise')

rolling_avg = spark.sql('''
    SELECT date, confirmed, AVG(confirmed) OVER(ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS confirmed_rolling_avg,
        deaths, AVG(deaths) OVER(ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS death_rolling_avg, 
        recovered, AVG(recovered) OVER(ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS recovered_rolling_avg
    FROM day_wise
    ''')

print('7 days Rolling Average : ')
rolling_avg.show()

# -------------------------------------------------------------------------
# 7. Compare physical plans with DataFrame API
# -------------------------------------------------------------------------
top_10_df_api = df_worldometer_data \
    .select(
        col("country_region"),
        round((col("total_cases") / col("population")) * 1000, 2)
        .alias("infection_rate_per_1000")
    ) \
    .orderBy(col("infection_rate_per_1000").desc()) \
    .limit(10)

print("\nPhysical Plan (SQL Version):")
top_10_countries.explain()

print("\nPhysical Plan (DataFrame API Version):")
top_10_df_api.explain()

