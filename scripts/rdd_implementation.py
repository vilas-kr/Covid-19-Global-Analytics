#!/usr/bin/env python3

# -------------------------------------------------------------------------
# Task 8: RDD-Based Implementation
# Using RDD API:
# 1.Calculate total confirmed per country.
# 2.Calculate total deaths per country.
# 3.Compute death percentage using reduceByKey.
# 4.Compare RDD performance vs DataFrame.

# Explain:
# Why reduceByKey is preferred over groupByKey
# When RDD should be avoided
# -------------------------------------------------------------------------

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# -------------------------------------------------------------------------
# 1. Spark Session
# -------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("RDD Implementation") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

RAW_PATH = 'hdfs:///data/covid/raw/'
ANALYTICS_PATH = 'hdfs:///data/covid/analytics/'

# -------------------------------------------------------------------------
# 2. Read dataset from hadoop 
# -------------------------------------------------------------------------
rdd = spark.sparkContext.textFile(RAW_PATH + 'full_grouped.csv')
header = rdd.first()

# Remove header 
data = rdd.filter(lambda row: row != header)

#split columns
rdd_full_grouped = data.map(lambda x: x.split(","))

# -------------------------------------------------------------------------
# 3. Calculate total confirmed per country
# -------------------------------------------------------------------------
country_confirmed = rdd_full_grouped.map(
        lambda x: (x[1], int(x[2]))
    )

total_confirmed = country_confirmed.reduceByKey(
        lambda a, b: a + b
    )

result = total_confirmed.take(10)

print('\nConfirmed cases per country')
print(f'\
---------------------------------------------------------------------- \n\
Country -> Total confimed \n\
---------------------------------------------------------------------- ')
for country, confirmed in result:
    print(f'{country} -> {confirmed}')

# -------------------------------------------------------------------------
# 4. Calculate total deaths per country
# -------------------------------------------------------------------------
country_deaths = rdd_full_grouped.map(
        lambda x: (x[1], int(x[3]))
    )

total_deaths = country_deaths.reduceByKey(
        lambda a, b: a + b
    )

result = total_deaths.take(10)

print('\nDeaths per country')
print(f'\
---------------------------------------------------------------------- \n\
Country -> Total Deaths \n\
----------------------------------------------------------------------')
for country, deaths in result:
    print(f'{country} -> {deaths}')
    
# -------------------------------------------------------------------------
# 5. Compute death percentage using reduceByKey
# -------------------------------------------------------------------------
aggregated = total_confirmed.join(total_deaths)

death_percentage = aggregated.mapValues(
        lambda x: (x[1] / x[0]) * 100 if x[0] != 0 else 0
    )

country_death_percent = death_percentage.toDF(['Country', 
        'Death_Percentage'])

print('\nDeath percent per country')
country_death_percent.show()

# -------------------------------------------------------------------------
# 6. Compare RDD performance vs DataFrame
# -------------------------------------------------------------------------

# Calculated RDD performance time
start_time = time.time()

rdd = spark.sparkContext.textFile(RAW_PATH + 'full_grouped.csv')
header = rdd.first()

# Remove header 
data = rdd.filter(lambda row: row != header)

rdd_country_confirmed = data.map(lambda line: line.split(",")) \
    .map(lambda columns: (columns[1], int(columns[2]))) \
    .reduceByKey(lambda confirmed1, confirmed2: confirmed1 + confirmed2)
rdd_country_confirmed.count()
rdd_time = time.time() - start_time
print(f"RDD Execution Time: {rdd_time:.2f} seconds")

# Calculate Dataframe performance time
start_time = time.time()
df_full_grouped = spark.read.csv(RAW_PATH + 'full_grouped.csv',
                    header=True,inferSchema=True)
df_country_confirmed = df_full_grouped.groupBy("Country/Region").sum("Confirmed")
df_country_confirmed.count()
df_time = time.time() - start_time
print(f"DataFrame Execution Time: {df_time:.2f} seconds")
  
# -------------------------------------------------------------------------
# 7. Store result into HDFS
# -------------------------------------------------------------------------  
country_death_percent.write \
    .mode("overwrite") \
    .parquet(ANALYTICS_PATH + 'country_death_percent_parquet')

'''
spark-submit \
  --master yarn \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/mnt/d/bridgelabs/spark/covid_analytics/spark_env/bin/python \
  --conf spark.executorEnv.PYSPARK_PYTHON=/mnt/d/bridgelabs/spark/covid_analytics/spark_env/bin/python \
  scripts/rdd_implementation.py
'''
# -------------------------------------------------------------------------
# Explain:
# -------------------------------------------------------------------------
# 1. Why reduceByKey is preferred over groupByKey
# reduceByKey is preferred over groupByKey because it performs local 
# aggregation on each partition before shuffling data across the network.
# In groupByKey, all values associated with a key are shuffled across 
# the network and stored in memory, which increases network traffic 
# and memory usage. This can lead to performance degradation and 
# even Out Of Memory (OOM) errors for large datasets. On the other hand, 
# reduceByKey reduces the data at the partition level first and only 
# transfers the aggregated result, thereby:
# -> Reducing shuffle size
# -> Lowering memory consumption
# -> Improving performance

# 2. When RDD should be avoided
# RDD should be avoided when working with structured or semi-structured data, 
# such as CSV, JSON, Parquet, or database tables.

# Modern Spark applications prefer DataFrames or Datasets because they:
# -> Use the Catalyst Optimizer
# -> Provide better execution planning
# -> Offer improved memory management
# -> Support SQL queries
# -> Deliver better performance

# RDD does not support automatic optimization and requires manual handling
# of transformations, making it less efficient for production-level big 
# data processing.
# RDD is mainly used for:
# -> Low-level transformations
# -> Fine-grained control
# -> Learning Spark fundamentals