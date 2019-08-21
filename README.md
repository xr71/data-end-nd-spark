# Udacity Project - Data Lake - Spark on S3

## Introduction
Sparkify is a streaming song company that stores its event logs in JSON format on S3. To make these large semi-structured files usable for anlytics and downstream applications, we will be transforming the files using Spark (SparkSQL) and then loading the files back into S3 as Parquet files.

## Directory Structure
This directory contains three files:
  * `etl.py` is the main script file for performing the ETL process from S3 to Spark and back to S3. 
  * `aws_s3_to_pyspark_gc.ipynb` is a prototype interactive notebook that processes a very limited subset of the JSON files to ensure that the process is working. 
  * `dl.cfg` is a configuration file that contains a placeholder for you to input your AWS credentials. Depending on whether you are running this script locally or in an EMR cluster with the correct IAM roles, you may not need to use this file. If you are submitting this file as a `spark-submit` inside an EMR cluster with access to your S3 environment, you may ignore this file and also remove any references in `etl.py` that uses the config file to load the AWS credentials.  

## Instructions
  * `git clone` this repository and `cd` into the directory
  * open up the `aws_s3_to_pyspark_gc.ipynb` file first in your own Jupyter environment or via Google Colab. 
    * This prototype notebook was written in Google Colab so it setups the local pyspark files needed temporarily for the VM to perform all pyspark functionality.
    * If you are running this file locally and has pyspark provisioned locally, feel free to ignore or delete all references to JDK 8 and pyspark set up in the first few cells of the notebook.
    * Go through this notebook section by section to get an understanding of how the ETL process works and what transformations are performed. 
    * Use this notebook to test that writing back to your own S3 output bucket location is working correctly.
  * Finally, simply run `python etl.py` to perform the full etl process if you are working locally or `spark-submit etl.py` if you are working inside an EMR node. 
