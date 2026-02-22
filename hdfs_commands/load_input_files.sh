#!/bin/bash
# -------------------------------------------------------------------------
# Task 1: Hadoop Integration
# Create proper HDFS directory structure:

# /data/covid/raw
# /data/covid/staging
# /data/covid/curated
# /data/covid/analytics

# Upload all CSV files into /data/covid/raw.
# Verify using HDFS commands.
# All Spark jobs must read from HDFS paths only.
# -------------------------------------------------------------------------

echo "Uploading all the files to Hadoop..."

echo "Creating HDFS directory structure..."
hdfs dfs -mkdir -p /data/covid/raw
hdfs dfs -mkdir -p /data/covid/staging
hdfs dfs -mkdir -p /data/covid/curated
hdfs dfs -mkdir -p /data/covid/analytics

echo -e "\nLoading input files to the HDFS..."

echo "Loading country wise latest data to HDFS..."
hdfs dfs -put ./dataset/country_wise_latest.csv /data/covid/raw

echo "Loading covid 19 clean data to HDFS..."
hdfs dfs -put ./dataset/covid_19_clean_complete.csv /data/covid/raw

echo "Loading day wise data to HDFS..."
hdfs dfs -put ./dataset/day_wise.csv /data/covid/raw

echo "Loading full grouped data to HDFS..."
hdfs dfs -put ./dataset/full_grouped.csv /data/covid/raw

echo "Loading usa country wise data to HDFS..."
hdfs dfs -put ./dataset/usa_county_wise.csv /data/covid/raw

echo "Loading worldometer data to HDFS..."
hdfs dfs -put ./dataset/worldometer_data.csv /data/covid/raw

echo -e "\nVerifying files are uploaded..."
echo "---------------------------------------------------------------------"
hdfs dfs -ls /data/covid/raw/

echo -e "\nAnalyzing block allocation..."
echo "---------------------------------------------------------------------"
hdfs fsck /data/covid/raw/country_wise_latest.csv -files -blocks | grep "Total blocks"
hdfs fsck /data/covid/raw/covid_19_clean_complete.csv -files -blocks | grep "Total blocks"
hdfs fsck /data/covid/raw/day_wise.csv -files -blocks | grep "Total blocks"
hdfs fsck /data/covid/raw/full_grouped.csv -files -blocks | grep "Total blocks"
hdfs fsck /data/covid/raw/usa_county_wise.csv -files -blocks | grep "Total blocks"
hdfs fsck /data/covid/raw/worldometer_data.csv -files -blocks | grep "Total blocks"

echo -e "\nHDFS file directory and input files setup complete."


