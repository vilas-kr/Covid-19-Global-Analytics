# Task 2: Data Ingestion & Optimization
# -------------------------------------------------------------------------
# Read all raw CSV files from HDFS.
# Apply proper schema instead of inferSchema.
# Handle null values.
# Convert raw CSV files into Parquet format.
# Store them in /data/covid/staging.

# Compare CSV vs Parquet:
# File size
# Read performance
# Execution plan
# Explain why Parquet performs better.
# -------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import time
import subprocess


# -------------------------------------------------------------------------
# 1. Spark Session
# -------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName('Data Ingestion') \
    .getOrCreate()
    
RAW_PATH = 'hdfs:///data/covid/raw/'
STAGING_PATH = 'hdfs:///data/covid/staging/'

# -------------------------------------------------------------------------
# 2. Define schema for all files
# -------------------------------------------------------------------------
country_wise_latest_schema = StructType([
    StructField('Country/Region', StringType(), True),
    StructField('Confirmed', LongType(), True),
    StructField('Deaths', LongType(), True),
    StructField('Recovered', LongType(), True),
    StructField('Active', LongType(), True),
    StructField('New cases', LongType(), True),
    StructField('New deaths', LongType(), True),
    StructField('New recovered', LongType(), True),
    StructField('Deaths / 100 Cases', DoubleType(), True),
    StructField('Recovered / 100 Cases', DoubleType(), True)
])

covid_19_clean_complete_schema = StructType([
    StructField('Province/State', StringType(), True),
    StructField('Country/Region', StringType(), True),
    StructField('Lat', DoubleType(), True),
    StructField('Long', DoubleType(), True),
    StructField('Date', DateType(), True),
    StructField('Confirmed', LongType(), True),
    StructField('Deaths', LongType(), True),
    StructField('Recovered', LongType(), True),
    StructField('Active', LongType(), True),
    StructField('WHO Region', StringType(), True)
])

day_wise_schema = StructType([
    StructField('Date', DateType(), True),
    StructField('Confirmed', LongType(), True),
    StructField('Deaths', LongType(), True),
    StructField('Recovered', LongType(), True),
    StructField('Active', LongType(), True),
    StructField('New cases', LongType(), True),
    StructField('New deaths', LongType(), True),
    StructField('New recovered', LongType(), True),
    StructField('Deaths / 100 Cases', DoubleType(), True),
    StructField('Recovered / 100 Cases', DoubleType(), True)
])

full_grouped_schema = StructType([
    StructField('Date',DateType(), True),
    StructField('Country/Region',StringType(), True),
    StructField('Confirmed',LongType(), True),
    StructField('Deaths',LongType(), True),
    StructField('Recovered',LongType(), True),
    StructField('Active',LongType(), True),
    StructField('New cases',LongType(), True),
    StructField('New deaths',LongType(), True),
    StructField('New recovered',LongType(), True),
    StructField('WHO Region',StringType(), True)
])

usa_country_wise_schema = StructType([
    StructField('UID', LongType(), True),
    StructField('iso2', StringType(), True),
    StructField('iso3', StringType(), True),
    StructField('code3', IntegerType(), True),
    StructField('FIPS', DoubleType(), True),
    StructField('Admin2', StringType(), True),
    StructField('Province_State', StringType(), True),
    StructField('Country_Region', StringType(), True),
    StructField('Lat', DoubleType(), True),
    StructField('Long_', DoubleType(), True)
])

worldometer_data_schema = StructType([
    StructField('Country/Region', StringType(), True),
    StructField('Continent', StringType(), True),
    StructField('Population', LongType(), True),
    StructField('TotalCases', LongType(), True),
    StructField('NewCases', LongType(), True),
    StructField('TotalDeaths', LongType(), True),
    StructField('NewDeaths', LongType(), True),
    StructField('TotalRecovered', LongType(), True),
    StructField('NewRecovered', LongType(), True),
    StructField('ActiveCases', LongType(), True)
])

# -------------------------------------------------------------------------
# 3. Read dataset from hadoop with specified schema
# -------------------------------------------------------------------------
df_country_wise_latest = spark.read \
    .option('header', True) \
    .schema(country_wise_latest_schema) \
    .csv(RAW_PATH + 'country_wise_latest.csv')

df_covid_19_clean_complete = spark.read \
    .option('header', True) \
    .schema(covid_19_clean_complete_schema) \
    .csv(RAW_PATH + 'covid_19_clean_complete.csv')

df_day_wise = spark.read \
    .option('header', True) \
    .schema(day_wise_schema) \
    .csv(RAW_PATH + 'day_wise.csv')

df_full_grouped = spark.read \
    .option('header', True) \
    .schema(full_grouped_schema) \
    .csv(RAW_PATH + 'full_grouped.csv')

df_usa_country_wise = spark.read \
    .option('header', True) \
    .schema(usa_country_wise_schema) \
    .csv(RAW_PATH + 'usa_county_wise.csv')

df_worldometer_data = spark.read \
    .option('header', True) \
    .schema(worldometer_data_schema) \
    .csv(RAW_PATH + 'worldometer_data.csv')

# -------------------------------------------------------------------------
# 4. Rename columns
# -------------------------------------------------------------------------
df_country_wise_latest = df_country_wise_latest \
    .withColumnRenamed('Country/Region', 'country_region') \
    .withColumnRenamed('New cases', 'new_cases') \
    .withColumnRenamed('New deaths', 'new_deaths') \
    .withColumnRenamed('New recovered', 'new_recovered') \
    .withColumnRenamed('Deaths / 100 Cases', 'deaths_per_100_cases') \
    .withColumnRenamed('Recovered / 100 Cases', 'recovered_per_100_cases')
    
df_covid_19_clean_complete = df_covid_19_clean_complete \
    .withColumnRenamed('Province/State', 'province_state') \
    .withColumnRenamed('Country/Region', 'country_region') \
    .withColumnRenamed('WHO Region', 'WHO_region') 

df_day_wise = df_day_wise \
    .withColumnRenamed('New cases', 'new_cases') \
    .withColumnRenamed('New deaths', 'new_deaths') \
    .withColumnRenamed('New recovered', 'new_recovered') \
    .withColumnRenamed('Deaths / 100 Cases', 'deaths_per_100_cases') \
    .withColumnRenamed('Recovered / 100 Cases', 'recovered_per_100_cases')

df_full_grouped = df_full_grouped \
    .withColumnRenamed('Country/Region', 'country_region') \
    .withColumnRenamed('New cases', 'new_cases') \
    .withColumnRenamed('New deaths', 'new_deaths') \
    .withColumnRenamed('New recovered', 'new_recovered') \
    .withColumnRenamed('WHO Region', 'WHO_region') 

df_usa_country_wise = df_usa_country_wise \
    .withColumnRenamed('Long_', 'long')
    
df_worldometer_data = df_worldometer_data \
    .withColumnRenamed('Country/Region', 'country_region') \
    .withColumnRenamed('TotalCases', 'total_cases') \
    .withColumnRenamed('NewCases', 'new_cases') \
    .withColumnRenamed('TotalDeaths', 'total_deaths') \
    .withColumnRenamed('NewDeaths', 'new_deaths') \
    .withColumnRenamed('TotalRecovered', 'total_recovered') \
    .withColumnRenamed('NewRecovered', 'new_recovered') \
    .withColumnRenamed('ActiveCases', 'active_cases') 
    
# -------------------------------------------------------------------------
# 5. Handle null values 
# -------------------------------------------------------------------------
df_country_wise_latest = df_country_wise_latest.na.drop(subset=['country_region'])
df_country_wise_latest = df_country_wise_latest.na.fill({
    'confirmed' : 0,
    'Deaths' : 0,
    'Recovered' : 0,
    'Active' : 0,
    'new_cases' : 0,
    'new_deaths' : 0,
    'new_recovered' : 0,
    'deaths_per_100_cases' : 0.0,
    'recovered_per_100_cases' : 0.0
})

df_covid_19_clean_complete = df_covid_19_clean_complete.na.drop(subset=['country_region', 'date'])
df_covid_19_clean_complete = df_covid_19_clean_complete.na.fill({
    'province_state' : 'Unknown',
    'lat' : 0.0,
    'long' : 0.0,
    'confirmed' : 0,
    'Deaths' : 0,
    'Recovered' : 0,
    'Active' : 0,
    'who_region' : 'unknown'
})

df_day_wise = df_day_wise.na.drop(subset=['date'])
df_day_wise = df_day_wise.na.fill({
    'confirmed' : 0,
    'Deaths' : 0,
    'Recovered' : 0,
    'Active' : 0,
    'new_cases' : 0,
    'new_deaths' : 0,
    'new_recovered' : 0,
    'deaths_per_100_cases' : 0.0,
    'recovered_per_100_cases' : 0.0
})

df_full_grouped = df_full_grouped.na.drop(subset=['date', 'country_region'])
df_full_grouped = df_full_grouped.na.fill({
    'confirmed' : 0,
    'Deaths' : 0,
    'Recovered' : 0,
    'Active' : 0,
    'new_cases' : 0,
    'new_deaths' : 0,
    'new_recovered' : 0,
    'who_region' : 'unknown'
})

df_usa_country_wise = df_usa_country_wise.na.drop(subset=['uid', 'country_region'])
df_usa_country_wise = df_usa_country_wise.na.fill({
    'iso2' : 'unknown',
    'iso3' : 'unknown',
    'code3' : 0,
    'FIPS' : 0.0,
    'Admin2' : 'unknown',
    'Province_State' : 'unknown',
    'Lat' : 0.0,
    'long' : 0.0 
})

df_worldometer_data = df_worldometer_data.na.drop(subset=['country_region'])
df_worldometer_data = df_worldometer_data.na.fill({
    'continent' : 'unknown',
    'population' : 0,
    'total_cases' : 0,
    'new_cases' : 0,
    'total_deaths' : 0,
    'new_deaths' : 0,
    'total_recovered' : 0,
    'new_recovered' : 0,
    'active_cases' : 0
})

# -------------------------------------------------------------------------
# 6. Store Filtered data into hadoop
# -------------------------------------------------------------------------
df_country_wise_latest.write \
    .mode('overwrite') \
    .parquet(STAGING_PATH + 'country_wise_latest_parquet')

df_covid_19_clean_complete.write \
    .mode('overwrite') \
    .parquet(STAGING_PATH + 'covid_19_clean_complete_parquet')
    
df_day_wise.write \
    .mode('overwrite') \
    .parquet(STAGING_PATH + 'day_wise_parquet')

df_full_grouped.write \
    .mode('overwrite') \
    .parquet(STAGING_PATH + 'full_grouped_parquet')
    
df_usa_country_wise.write \
    .mode('overwrite') \
    .parquet(STAGING_PATH + 'usa_country_wise_parquet')
    
df_worldometer_data.write \
    .mode('overwrite') \
    .parquet(STAGING_PATH + 'worldometer_data_parquet')

# -------------------------------------------------------------------------
# 7. Compare CSV and Parquet file size
# -------------------------------------------------------------------------
print('''
---------------------------------------------------------------------------
Comparing CSV and Parquet file size
---------------------------------------------------------------------------
CSV File size : ''')

file_path = RAW_PATH + "country_wise_latest.csv"
command = f"hdfs dfs -du -h {file_path}"
subprocess.run(command, shell=True)

file_path = RAW_PATH + "covid_19_clean_complete.csv"
command = f"hdfs dfs -du -h {file_path}"
subprocess.run(command, shell=True)

file_path = RAW_PATH + "day_wise.csv"
command = f"hdfs dfs -du -h {file_path}"
subprocess.run(command, shell=True)

file_path = RAW_PATH + "full_grouped.csv"
command = f"hdfs dfs -du -h {file_path}"
subprocess.run(command, shell=True)

file_path = RAW_PATH + "usa_county_wise.csv"
command = f"hdfs dfs -du -h {file_path}"
subprocess.run(command, shell=True)

file_path = RAW_PATH + "worldometer_data.csv"
command = f"hdfs dfs -du -h {file_path}"
subprocess.run(command, shell=True)

print('''
Paraquet file size : ''')

file_path = STAGING_PATH + "country_wise_latest_parquet"
command = f"hdfs dfs -du -h {file_path}"
subprocess.run(command, shell=True)

file_path = STAGING_PATH + "covid_19_clean_complete_parquet"
command = f"hdfs dfs -du -h {file_path}"
subprocess.run(command, shell=True)

file_path = STAGING_PATH + "day_wise_parquet"
command = f"hdfs dfs -du -h {file_path}"
subprocess.run(command, shell=True)

file_path = STAGING_PATH + "full_grouped_parquet"
command = f"hdfs dfs -du -h {file_path}"
subprocess.run(command, shell=True)

file_path = STAGING_PATH + "usa_country_wise_parquet"
command = f"hdfs dfs -du -h {file_path}"
subprocess.run(command, shell=True)

file_path = STAGING_PATH + "worldometer_data_parquet"
command = f"hdfs dfs -du -h {file_path}"
subprocess.run(command, shell=True)

# -------------------------------------------------------------------------
# 8. Measure read performance
# -------------------------------------------------------------------------
print('''
Measure read performance
---------------------------------------------------------------------------''')
start_time = time.time()
df_country_wise_latest = spark.read \
    .option('header', True) \
    .schema(country_wise_latest_schema) \
    .csv(RAW_PATH + 'country_wise_latest.csv') \
    .count()

df_covid_19_clean_complete = spark.read \
    .option('header', True) \
    .schema(covid_19_clean_complete_schema) \
    .csv(RAW_PATH + 'covid_19_clean_complete.csv') \
    .count()

df_day_wise = spark.read \
    .option('header', True) \
    .schema(day_wise_schema) \
    .csv(RAW_PATH + 'day_wise.csv') \
    .count()

df_full_grouped = spark.read \
    .option('header', True) \
    .schema(full_grouped_schema) \
    .csv(RAW_PATH + 'full_grouped.csv') \
    .count()

df_usa_country_wise = spark.read \
    .option('header', True) \
    .schema(usa_country_wise_schema) \
    .csv(RAW_PATH + 'usa_county_wise.csv') \
    .count()

df_worldometer_data = spark.read \
    .option('header', True) \
    .schema(worldometer_data_schema) \
    .csv(RAW_PATH + 'worldometer_data.csv') \
    .count()
    
end_time = time.time()
print(f'''
---------------------------------------------------------------------------
CSV read time {end_time - start_time} seconds
''')

start_time = time.time()
df_country_wise_latest = spark.read.parquet(STAGING_PATH + "country_wise_latest_parquet").count()
df_covid_19_clean_complete = spark.read.parquet(STAGING_PATH + "covid_19_clean_complete_parquet").count()
df_day_wise = spark.read.parquet(STAGING_PATH + "day_wise_parquet").count()
df_full_grouped = spark.read.parquet(STAGING_PATH + "full_grouped_parquet").count()
df_usa_county_wise = spark.read.parquet(STAGING_PATH + "usa_country_wise_parquet").count()
df_worldometer_data = spark.read.parquet(STAGING_PATH + "worldometer_data_parquet").count()
end_time = time.time()
print(f'''
---------------------------------------------------------------------------
Parquet read time {end_time - start_time} seconds
''')
