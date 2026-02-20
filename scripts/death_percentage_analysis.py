#!/usr/bin/env python3

# -------------------------------------------------------------------------
# Task 3: Death Percentage Analysis

# Using full_grouped.csv:
# Compute daily death percentage per country:
# Deaths / Confirmed * 100
# Compute global daily death percentage.
# Compute continent-wise death percentage (join with worldometer_data).

# Identify:
# Country with highest death percentage
# Top 10 countries by deaths per capita
# All results must be written to HDFS under /data/covid/analytics.
# -------------------------------------------------------------------------

from pyspark.sql import *
from pyspark.sql.functions import *
import subprocess

# -------------------------------------------------------------------------
# 1. Spark Session
# -------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName('Death Analysis') \
    .getOrCreate()
    
spark.sparkContext.setLogLevel('ERROR')

STAGING_PATH = 'hdfs:///data/covid/staging/'
ANALYTICS_PATH = 'hdfs:///data/covid/analytics/'

# -------------------------------------------------------------------------
# 2. Read dataset from hadoop 
# -------------------------------------------------------------------------
df_full_grouped = spark.read.parquet(STAGING_PATH + 'full_grouped_parquet')
df_worldometer_data = spark.read.parquet(STAGING_PATH + 'worldometer_data_parquet')

# -------------------------------------------------------------------------
# 3. Compute daily death percentage per country
# -------------------------------------------------------------------------
df_full_grouped = df_full_grouped.withColumn(
    'Death_percentage', 
        when( col('confirmed') > 0, 
            ( col('deaths') / col('confirmed') ) * 100, 
        ).otherwise(0) 
    )   

country_daily_deaths = df_full_grouped.select('date', 'country_region', 
    'confirmed', 'deaths', 'Death_percentage')

# -------------------------------------------------------------------------
# 4. Compute global daily death percentage
# -------------------------------------------------------------------------
global_daily_deaths = df_full_grouped.groupBy('Date').agg( 
    sum('confirmed').alias('total_confirmed'), 
    sum('deaths').alias('total_deaths') 
    ).withColumn('death_percentage', 
        when( col('total_confirmed') > 0,  
             ( col('total_deaths') / col('total_confirmed') ) * 100, 
        ).otherwise(0) 
    )


# -------------------------------------------------------------------------
# 5. Compute continent-wise death percentage
# -------------------------------------------------------------------------
continent_deaths = df_full_grouped.join( df_worldometer_data, 
    df_full_grouped['country_region'] == df_worldometer_data['country_region'], 
    'inner' 
    ).groupBy( col('continent') ).agg( 
    sum('confirmed').alias('total_confirmed'), 
    sum('deaths').alias('total_deaths') 
    ).withColumn('death_percentage', 
        when( col('total_confirmed') > 0, 
              ( col('total_deaths') / col('total_confirmed') ) * 100, 
        ).otherwise(0) \
    )

# -------------------------------------------------------------------------
# 6. Country with highest death percentage
# -------------------------------------------------------------------------
country_deaths = df_full_grouped.groupBy( col('country_region') ).agg(
    sum( col('confirmed') ).alias('total_confirmed'),
    sum( col('deaths') ).alias('total_deaths')
    ).withColumn('death_percentage',
        when( col('total_confirmed') > 0,
              ( col('total_deaths') / col('total_confirmed') ) * 100,
        ).otherwise(0)
    ).orderBy( col('death_percentage').desc() )

print('''
---------------------------------------------------------------------------
Country with highest death percentage
''')
country_deaths.show(1)

# -------------------------------------------------------------------------
# 7. Top 10 countries by deaths per capita
# -------------------------------------------------------------------------
df_worldometer_data = df_worldometer_data.withColumn(
    'deaths_per_capita(1000000)',
    when( col('population') > 0,
          round((( col('total_deaths') / col('population') ) * 1000000), 2),
        ).otherwise(0)
    ).sort( col('deaths_per_capita(1000000)').desc() )

# Select only the required column
deaths_per_capita = df_worldometer_data.select( col('country_region'), col('population'), col('total_deaths'), col('deaths_per_capita(1000000)') )

print('''
---------------------------------------------------------------------------
Top 10 countries by deaths per capita
''')
deaths_per_capita.show(10)

# -------------------------------------------------------------------------
# 8. Store result into HDFS
# -------------------------------------------------------------------------
country_daily_deaths.write \
    .mode('overwrite') \
    .parquet(ANALYTICS_PATH + 'country_daily_deaths_parquet')

global_daily_deaths.write \
    .mode('overwrite') \
    .parquet(ANALYTICS_PATH + 'global_daily_deaths_parquet')
    
continent_deaths.write \
    .mode('overwrite') \
    .parquet(ANALYTICS_PATH + 'continent_deaths_parquet')

country_deaths.write \
    .mode('overwrite') \
    .parquet(ANALYTICS_PATH + 'country_deaths_parquet')

deaths_per_capita.write \
    .mode('overwrite') \
    .parquet(ANALYTICS_PATH + 'deaths_per_capita_parquet')


    
