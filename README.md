# COVID-19 Global Analytics Platform
## Project Overview
A distributed big data analytics platform built using Apache Spark (PySpark) integrated with Hadoop (HDFS + YARN) in pseudo-distributed mode.

This project simulates a real-world Data Engineering use case for analyzing global COVID-19 datasets at scale using distributed processing techniques.

## Project Objective
Design, implement, optimize, and analyze a distributed COVID analytics platform where:

- All data is stored in HDFS
- All Spark jobs run on YARN
- Data is processed using:
    - DataFrame API
    - Spark SQL
    - RDD API
- Performance tuning and execution plan analysis are implemented

## Tech Stack
```
Language: Python
Framework: Apache Spark (PySpark)
Storage: Hadoop HDFS
Resource Manager: YARN
Processing APIs:
    - DataFrame API
    - Spark SQL
    - RDD API
File Format: CSV, Parquet
Tools: VSCode, Git
```

## Hadoop Directory Structure (HDFS)
```
/data/covid/raw
/data/covid/staging
/data/covid/curated
/data/covid/analytics
```
Note: All Spark jobs strictly read from and write to HDFS.

## Project Structure
```
covid-19-global-analytics /
|
|-- hdfs_commands /
|   |-- setup.sh
|   |-- start_hadoop.sh
|   |-- stop_hadoop.sh
|   |-- load_input_files.sh
|
|-- scripts /
|   |-- csv_to_parquet.py
|   |-- death_percentage_analysis.py
|   |-- execution_plan_analysis.py
|   |-- global_time_series.py
|   |-- infection_rate_analysis.py
|   |-- performance_optimization.py
|   |-- rdd_implementation.py
|   |-- recovery_effeciency.py
|   |-- spark_sql_queries.py
|   |-- usa_drilldown.py
|   |-- resource_and_memory.txt
|
|-- docs /
|   |-- resource_and_memory.txt
|
|-- data /
|   |-- country_wise_latest.csv
|   |-- covid_19_clean_complete.csv
|   |-- day_wise.csv
|   |-- full_grouped.csv
|   |-- usa_county_wise.csv
|   |-- worldometer_data.csv
|
|-- .gitignore
|-- README.md
|-- requirements.txt
```

## Datasets Used
- country_wise_latest.csv
- covid_19_clean_complete.csv
- day_wise.csv
- full_grouped.csv
- usa_county_wise.csv
- worldometer_data.csv

Kaggle Dataset : https://www.kaggle.com/datasets/imdevskp/corona-virus-report

## Installation
```
Clone the repository
-> git clone https://github.com/vilas-kr/covid-19-global-analytics

Move to project folder
-> cd covid-19-global-analytics

Install dependencies:
pip install -r requirements.txt

Download datasets from below link:
https://www.kaggle.com/datasets/imdevskp/corona-virus-report

Then move this files to data folder

Make all shell scripts executable
-> chmod +x hdfs_commands/*.sh

Now run 'setup.sh' to setup the environment for the project
-> hdfs_commands/setup.sh
```

## How to Run

### 1. Start Hadoop Services
```
hdfs_commands/start_hadoop.sh
```
This will start service and list the running services

### 2. Upload Data to HDFS
```
hdfs_commands/load_input_files.sh
```
This will load all the input files to HDFS

### 3. Submit Spark Job
Note: All jobs must run using
```
spark-submit --master yarn
```
```
spark-submit --master yarn ./scripts/csv_to_parquet.py
spark-submit --master yarn ./scripts/death_percentage_analysis.py
spark-submit --master yarn ./scripts/infection_rate_analysis.py
spark-submit --master yarn ./scripts/recovery_efficiency.py
spark-submit --master yarn ./scripts/global_time_series.py
spark-submit --master yarn ./scripts/usa_drilldown.py
spark-submit --master yarn ./scripts/rdd_implementation.py
spark-submit --master yarn ./scripts/spark_sql_queries.py
spark-submit --master yarn ./scripts/performance_optimization.py
spark-submit --master yarn ./scripts/execution_plan_analysis.py
```
Note: Hadoop services should be running to execute these.

### 4. Stop Hadoop Services
```
hdfs_commands/stop_hadoop.sh
``` 
This will stop service and list the running services

## Tasks Implemented
- Task 1: Hadoop Integration
- Task 2: Data Ingestion & Optimization
- Task 3: Death Percentage Analysis
- Task 4: Infection Rate Analysis
- Task 5: Recovery Efficiency
- Task 6: Global Time-Series Analysis
- Task 7: USA Drilldown Analysis
- Task 8: RDD-Based Implementation
- Task 9: Spark SQL Implementation
- Task 10: Performance Optimization
- Task 11: Execution Plan Analysis
- Task 12: Resource & Memory Planning

## Key Learning Outcomes
- Distributed data processing with Spark
- Shuffle mechanics & execution plan analysis
- Join optimization strategies
- Data skew handling in distributed systems
- Resource tuning in YARN
- Production-grade Spark performance engineering

## Output Location
All final analytics written to : 
```
hdfs:///data/covid/analytics/
```

## Author
```
Vilas K R
GitHub: vilas-kr
```
