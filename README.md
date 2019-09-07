# Summary
This project builds a data lake using Spark in the analytics context of the startup called Sparkify. Specifically, the repository presents the pyspark scripts to:

* Load song and log files from S3
* Make a ETL pipeline for the data lake in a Spark Environment
* Save the processed tables written as parquet files in directories, partitioned by a criterion, on S3

## The purpose of this data lake and the analytical goals.

* Analyze the user's daily activities and
* Provide future song recommendations to users

## Files in the repository

#### Python script etl.py
* Load necessary libraries and configurations
* def create_spark_session() 
* def process_data(spark, input_data, output_data)

#### Remote Access Control File “dl.cfg”
* NO QUOTES FOR THE KEYS
* Use the keys associated to the AWS user that has the right s3 accesss policy

#### Before create these file
* Create a Jupyter Notebook on a local machine and a sample data set to prototype the ETL 
* If eventually, the ETL will be deployed on AWS, create AWS EMR Cluster and test the ETL prototype on Notebooks (Referring to AWS documentation)

### Statement and justification of the database schema design and ETL pipeline.

* The STAR schema (i.e., One fact table and four dimension tables) being generated is optimized for song play analysis)
* Fact Table: songplays
* Dimension Tables: users, songs, artists and time

## To run the Python scripts

### Option 1: Access EMR via SSH and run the script

* Create EMR, associated S3 buckets and AWS user keys
* Refer to EMR – cluster – ssh instructions to get access to the EMR (Tried PuTTY for Windows 10, didn’t work out though)
* In a terminal on local machine: ssh spark-emr
* /usr/bin/spark-submit –master yarn ./etl.py
* Terminate Cluster: clusters – Terminate

### Option 2: Run etl.py on local machine (or Udacity workspace) with a Spark environment pre-installed

* Create EMR S3 buckets and AWS user keys
* In a local machine terminal: python etl.py

## Improvement Points

* Add code blocks or images that show example queries and results that the analytics team would find useful
* Handle duplicate entries with the same id but different values for other columns
