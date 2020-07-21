import pandas as pd
import os
import configparser
from pyspark.sql import SparkSession
from datetime import datetime, timedelta, date
from pyspark.sql import types as t

# AWS credentials from configure file
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']     = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
    
"""
Creating a Apache spark session
Output: Spark session.
"""
def create_spark_session():
    
    spark = SparkSession.builder\
                    .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
                    .enableHiveSupport().getOrCreate()
    return spark

"""
handle_i94SAS will read 12 SAS files from ../../data/ and write parquet file,
which will be copied to AWS S3 to read for immigreation_etl.
I had to create this utility function because in the environment saurfang.sas.spark doesn't work with
S3 read/write!  
"""
def handle_i94SAS(spark):
    
   months         = ['apr','aug','dec','feb','jan','jul','jun','mar','may','nov','oct','sep']
   i94ParquetPath = "Output_Data/i94.parquet" 
    
   for m in months:
        inputf = "{}{}{}".format("../../data/18-83510-I94-Data-2016/i94_",m,"16_sub.sas7bdat")
        print(inputf)
        df_spark_i94 = spark.read.format('com.github.saurfang.sas.spark').load(inputf)
        df_spark_i94.write.mode('append').partitionBy("i94mon").parquet(i94ParquetPath)
    
"""
Creates Spark session, processes song and log data.
Creates songs, artists, users, time, songplays table by extracting input from song and log raw files,
transforming as necessary and loading in parquet files.
"""
def main():
        
    # Create Spark session
    spark = create_spark_session()
    
    # Read 2016 monthly (12) SAS files in sas7bdat format and write them as parquet files.
    handle_i94SAS(spark)
    
if __name__ == "__main__":
    main()