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
    ).withColumn('recovered_percentage',
        when(col('total_confirmed') != 0, 
             round((col('total_recovered') / col('total_confirmed')) * 100, 
                2),
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

# -------------------------------------------------------------------------
# 5. Country with fastest recovery growth
# -------------------------------------------------------------------------
# calculated daily recovery growth
df_growth = df_full_grouped.withColumn(
        "Previous_Recovered",
        lag("Recovered").over(
            Window.partitionBy(col('country_region'))\
                .orderBy(col('date'))
        )
    ).withColumn(
        "Daily_Recovery_Growth",
        when(col('previous_recovered') != 0,
                (col("new_recovered") / col('previous_recovered')) * 100,
            ).otherwise(0)
    ).fillna(0)

# calculate country average recovery growth 
country_recovery_growth = df_growth.groupBy('country_region').agg(
        avg(col('daily_recovery_growth')).alias("avg_growth")
    ).sort(col('avg_growth').desc())

print('''
---------------------------------------------------------------------------
country with fastest recovery growth
''')
country_recovery_growth.show(1)

# -------------------------------------------------------------------------
# 6. Peak recovery day per country
# -------------------------------------------------------------------------
contry_peak_recovery_day = df_growth.withColumn(
        'rank',
        row_number().over(
            Window.partitionBy('country_region').orderBy(
                col('daily_recovery_growth').desc()
                )
            )
    ).filter(col('rank') == 1).select(
        'date', 'country_region', 
        col('daily_recovery_growth').alias('peak_recovery_percent')
    )
    
# -------------------------------------------------------------------------
# 7. Store result into HDFS
# -------------------------------------------------------------------------
country_recovered_percentage.write \
    .mode('overwrite') \
    .parquet(ANALYTICS_PATH + 'country_recovered_percentage_parquet')

rolling_recovery.write \
    .mode('overwrite') \
    .parquet(ANALYTICS_PATH + 'rolling_recovery_parquet')

country_recovery_growth.write \
    .mode('overwrite') \
    .parquet(ANALYTICS_PATH + 'country_recovery_growth_parquet')
    
contry_peak_recovery_day.write \
    .mode('overwrite') \
    .parquet(ANALYTICS_PATH + 'contry_peak_recovery_day_parquet')
