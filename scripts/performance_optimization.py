# Task 10: Performance Optimization (Mandatory)
# ---------------------------------------------------------------------------
# Import Statements
# ---------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.storagelevel import StorageLevel


# ---------------------------------------------------------------------------
# Starting Spark Session
# ---------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("Performance Optimization") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------------------------------
# Define Paths
# ---------------------------------------------------------------------------
STAGING_PATH = "hdfs:///data/covid/staging/"
ANALYTICS_PATH = "hdfs:///data/covid/analytics/"

# ---------------------------------------------------------------------------
# Read Data
# ---------------------------------------------------------------------------
worldometer_data = spark.read.parquet(STAGING_PATH + "worldometer_data_parquet")
full_grouped = spark.read.parquet(STAGING_PATH + "full_grouped_parquet")


# ===========================================================================
# 1. Partition Strategy
# ===========================================================================
# Repartition by Date (full shuffle)
full_grouped_by_date = full_grouped.repartition("Date")

# Repartition by Country/Region
worldometer_by_country = worldometer_data.repartition("Country_Region")

# Write partitioned parquet
worldometer_by_country.write \
    .mode("overwrite") \
    .partitionBy("Country_Region") \
    .parquet(ANALYTICS_PATH + "partitioned_by_country")

full_grouped_by_date.write \
    .mode("overwrite") \
    .partitionBy("Date") \
    .parquet(ANALYTICS_PATH + "full_grouped_by_date")

# Explanation:
# repartition() → Full shuffle, can increase/decrease partitions.
# coalesce() → Reduces partitions without full shuffle (narrow transformation).

#===========================================================================
# 2. Data Skew Handling
#===========================================================================
# Identify skewed countries
country_distribution = worldometer_data.groupBy("Country_Region") \
    .count() \
    .orderBy(col("count").desc())

country_distribution.show()

# Salting Technique
salted_df = worldometer_data.withColumn(
    "salt",
    floor(rand() * 5)
)

# Example salted aggregation
salted_agg = salted_df.groupBy("Country_Region", "salt") \
    .agg(sum("Total_Cases").alias("salted_sum")) \
    .groupBy("Country_Region") \
    .agg(sum("salted_sum").alias("final_sum"))

salted_agg.show()

# Impact of Skew:
# - Uneven partition sizes
# - Long shuffle stages
# - Straggler tasks
# - Memory spill
# - Increased job completion time


# ===========================================================================
# 3. Broadcast Join Optimization
# ===========================================================================
small_df = worldometer_data.select("Country_Region", "Population")

joined_df = full_grouped.join(
    broadcast(small_df),
    on="Country_Region",
    how="inner"
)

# Verify physical plan
joined_df.explain("formatted")

# ===========================================================================
# 4. Shuffle Optimization
# ===========================================================================
# Tune shuffle partitions (default is 200)
spark.conf.set("spark.sql.shuffle.partitions", 50)

print("Shuffle Partitions:",
      spark.conf.get("spark.sql.shuffle.partitions"))

# Avoid unnecessary wide transformations
country_df = full_grouped.select(
    "Country_Region",
    "Date",
    "Confirmed",
    "Deaths",
    "Recovered"
)

# Combine filters BEFORE join 
filtered_full_grouped = country_df.filter(col("Confirmed") > 1000)

optimized_join = filtered_full_grouped.join(
    broadcast(small_df),
    on="Country_Region",
    how="inner"
)

optimized_join.explain("formatted")

# Why shuffle is expensive:
# - Network data transfer between executors
# - Disk I/O due to spill
# - Serialization / deserialization cost
# - Sorting for SortMergeJoin
# - Stage barrier creation

# ===========================================================================
# 5. Caching Strategy
# ===========================================================================
# Cache frequently reused DataFrame
worldometer_data.persist(StorageLevel.MEMORY_AND_DISK)

# Trigger action
worldometer_data.count()

# When caching degrades performance:
# - Dataset too large for memory
# - Cached but rarely reused
# - GC pressure increases
# - Useful cached data gets evicted