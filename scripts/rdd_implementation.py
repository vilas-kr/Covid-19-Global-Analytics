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
    
