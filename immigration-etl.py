import configparser
import pandas as pd
import os
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from datetime import datetime, timedelta, date
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear

# AWS credentials from configure file
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']     = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
    
"""
Creating a Apache spark session on AWS to process the input data.
Output: Spark session.
"""
def create_spark_session():
    
    spark = SparkSession.builder\
                    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
                    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")\
                    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])\
                    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])\
                    .enableHiveSupport().getOrCreate()

    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID'])
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY'])

    return spark

"""
To covert arrival and departure date from df_i94 double to date
Input: Number 
Output: datetime 
"""
def convert_datetime(x):
    try:
        if x == 'null':
            x = 0
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None

"""
Processing I94 SAS data (read/clean up) to create Immigration Fact table
Input: spark session, input data to extract i94 - local or S3
Output: immigration_table 
"""
def create_immigration_table(spark, input_data):
    
    # Read SAS files
    df_i94 = spark.read.parquet(input_data)
    df_i94.dropna()

    # Data Clean up
    udf_datetime_from_sas = udf(lambda x: convert_datetime(x), T.DateType())
    df_i94 = df_i94.withColumn('arrdate', udf_datetime_from_sas(df_i94.arrdate))
    df_i94 = df_i94.withColumn('depdate', udf_datetime_from_sas(df_i94.depdate))
    
    df_i94 = df_i94.withColumn('i94port', trim(col('i94port')))
    
    df_i94 = df_i94.na.fill({\
         'i94yr':      0.0,\
         'i94addr':   'NA',\
         'depdate':   'NA',\
         'i94bir':    'NA',\
         'i94visa':    0.0,\
         'count':      0.0,\
         'dtadfile':  'NA',\
         'visapost':  'NA',\
         'occup':     'NA',\
         'entdepa':   'NA',\
         'entdepd':   'NA',\
         'entdepu':   'NA',\
         'matflag':   'NA',\
         'biryear':   'NA',\
         'dtaddto':   'NA',\
         'gender':    'NA',\
         'insnum':    'NA',\
         'airline':   'NA',\
         'admnum':     0.0,\
         'fltno':     'NA',\
         'visatype':  'NA'\
    })
    # Stage and create table.
    df_i94.createOrReplaceTempView("staging_immigration_table")
    immigration_table = spark.sql("""SELECT 
                                    cast(cicid AS INT)     AS cicid,
                                    cast(i94cit AS INT)    AS country_code,
                                    i94port                AS iata_code,
                                    i94mode                AS i94model,
                                    i94addr                AS State,
                                    arrdate                AS arrival_date,
                                    depdate                AS departure_date,
                                    i94visa                AS visa,
                                    visatype               AS visa_type,
                                    airline                AS airline,
                                    fltno                  AS flight_number,
                                    gender                 AS gender,
                                    cast(biryear AS INT)   AS birth_year,
                                    occup                  AS occupation,
                                    insnum                 AS ins_number,
                                    cast(admnum AS FLOAT)  AS admission_number
                                 FROM staging_immigration_table ORDER BY cicid
                             """).dropDuplicates()
    
    return immigration_table
 
"""
Processing Airport data, csv file and create airport_table - dimension table
Input: spark session, input data to airport codes
Output: airport_table
"""    
def create_airport_table(spark, input_datapath):
    
    # Read Airport data 
    df_airports = spark.read.format('csv').options(header='true').load(input_datapath)
    df_airports.dropna()

    #Basic clean up, remove invalid iata codes
    expr = r'^[A-Z]'
    df_airports = df_airports.filter(df_airports.iata_code.isNotNull())
    df_airports = df_airports.filter(df_airports['iata_code'].rlike(expr))
    df_airports = df_airports.filter(df_airports.iata_code.isNotNull())
    
    df_airports.createOrReplaceTempView("staging_airport_table")
    airport_table = spark.sql("""SELECT 
                                    name         AS airport_name,
                                    iso_region   AS region,
                                    municipality AS muncipality,
                                    iata_code    AS iata_code,
                                    SPLIT(coordinates,',')[0]  AS coordinate_x,
                                    SPLIT(coordinates,',')[1]  AS coordinate_y
                             FROM staging_airport_table ORDER BY region
                            """).dropDuplicates()

    return airport_table

"""
Processing City demographics data, join with IATA look up to add airport code
Input: spark session, input data to city demographics table and iata_code lookup
Output: city_table
"""    
def create_city_table(spark, input_datapath_city, input_datapath_iata):
    
    # Read City Demographics data and basic clean up
    df_city = spark.read.format('csv').options(header='true',delimiter=';').load(input_datapath_city)
    df_city.dropna()
    df_city = df_city.filter(df_city.City.isNotNull())

    # Read IATA code look up
    df_iata = spark.read.format('csv').options(header='true').load(input_datapath_iata)
    df_iata.dropna()
    df_iata = df_iata.filter(df_iata.Code.isNotNull())
  
    df_city.createOrReplaceTempView("staging_city")
    df_iata.createOrReplaceTempView("staging_iata")
    city_table = spark.sql("""SELECT 
                             sc.City               AS city,
                             sc.State              AS state,
                             sc.`State Code`       AS state_abbr,
                             si.Code               AS iata_code,
                             si.Name               AS airport_name,
                             sc.`Median Age`       AS median_age,
                             sc.`Total Population` AS total_population,
                             sc.`Foreign-born`     AS foreign_born
                           FROM staging_city AS sc
                           JOIN staging_iata AS si ON sc.City = si.City 
                           ORDER BY city
                           """).dropDuplicates()
    
    return city_table
    

"""
Take immigration table data and add country_of_origin for immigrant from I94CIT lookup
Input: spark session, immigration table and cit_code lookup
Output: immigrant_table
"""    
def create_immigrant_table(spark, immigration_table, input_datapath):
     
    # Look up table to go from CIT code to Country Name e.g. 582 -> Mexico!
    df_i94cit = spark.read.format('csv').options(header='true').load(input_datapath)
    df_i94cit = df_i94cit.filter(df_i94cit.I94CIT.isNotNull())
    df_i94cit.dropna()    
    df_i94cit.createOrReplaceTempView("staging_cit")       
        
    immigration_table.createOrReplaceTempView("staging_immigration")
    immigrant_table = spark.sql("""SELECT 
                                    it.cicid,
                                    cit.Country  AS country_of_origin,
                                    it.iata_code AS port_of_entry,
                                    it.arrival_date,
                                    it.visa,  
                                    CASE it.visa
                                        WHEN '1.0' THEN 'Business'
                                        WHEN '2.0' THEN 'Pleasure'
                                        WHEN '3.0' THEN 'Student'
                                        ELSE 'Unknown'
                                    END AS visa_for,
                                    it.visa_type,             
                                    it.gender,   
                                    it.birth_year,
                                    it.occupation
                                 FROM staging_immigration AS it
                                 JOIN staging_cit         AS cit ON (it.country_code = cit.i94CIT)
                                 ORDER BY cicid
                             """).dropDuplicates()
        
    return immigrant_table


"""
Write Output data to parquet files
Input: immigrant, airport, city - dimension tables, and output path
Output: None. Parquet files written for immigration, airport, city table
"""    
def write_output(immigration_table, immigrant_table, airport_table, city_table, output_datapath):

    # Parquet file write path
    immigrationParquetPath = "{}{}".format(output_datapath, 'immigration')
    immigrantParquetPath   = "{}{}".format(output_datapath, 'immigrant')
    airportParquetPath     = "{}{}".format(output_datapath, 'airport')                                     
    cityParquetPath        = "{}{}".format(output_datapath, 'city')                                     

    print(immigrationParquetPath)
    
    immigration_table.write.mode('overwrite').partitionBy('iata_code').parquet(immigrationParquetPath) 
    # Commenting out to save time, cost on AWS S3 storage
    #immigrant_table.write.mode('overwrite').partitionBy('country_of_origin').parquet(immigrantParquetPath) 
    #airport_table.write.mode('overwrite').partitionBy('iata_code').parquet(airportParquetPath)           
    #city_table.write.mode('overwrite').partitionBy('city').parquet(cityParquetPath)                                                        
    
    
"""
Creates Spark session, processes song and log data.
Creates songs, artists, users, time, songplays table by extracting input from song and log raw files,
transforming as necessary and loading in parquet files.
"""
def main():
    
    # Input from local system OR S3
    DATA_LOCAL = False
    
    if DATA_LOCAL:
        input_datapath_i94                      = config['LOCAL_DATAPATH']['INPUT_DATA_I94']
        input_datapath_airportcode              = config['LOCAL_DATAPATH']['INPUT_DATA_AIRPORT_CODE']
        input_datapath_us_city_demographies     = config['LOCAL_DATAPATH']['INPUT_DATA_US_CITY']
        input_datapath_citcode                  = config['LOCAL_DATAPATH']['INPUT_DATA_CIT_CODE']
        input_datapath_iatacode                 = config['LOCAL_DATAPATH']['INPUT_DATA_IATA_CITY']
        output_datapath                         = config['LOCAL_DATAPATH']['OUTPUT_DATA']
    else:
        input_datapath_i94                      = config['S3_DATAPATH']['INPUT_DATA_I94']
        input_datapath_airportcode              = config['S3_DATAPATH']['INPUT_DATA_AIRPORT_CODE']
        input_datapath_us_city_demographies     = config['S3_DATAPATH']['INPUT_DATA_US_CITY']
        input_datapath_citcode                  = config['S3_DATAPATH']['INPUT_DATA_CIT_CODE']
        input_datapath_iatacode                 = config['S3_DATAPATH']['INPUT_DATA_IATA_CITY']
        output_datapath                         = config['S3_DATAPATH']['OUTPUT_DATA']

    # Create Spark session
    spark = create_spark_session()

    # Read input data in spark frame and create tables
    
    # immigration - fact table (i94 sas data)
    immigration_table    = create_immigration_table(spark, input_datapath_i94)
    
    # airport - dimension tables (airport codes data)
    airport_table        = create_airport_table(spark, input_datapath_airportcode)
    
    # city - dimension table, (city demographics data, adding iata code for airport in the city)
    city_table           = create_city_table(spark, input_datapath_us_city_demographies, input_datapath_iatacode) 
    
    #immigrant - dimension table (immigration table and cit code look up to add country_of_origin for immigrant)
    immigrant_table      = create_immigrant_table(spark, immigration_table, input_datapath_citcode )
    
    # Parquet file writes
    write_output(immigration_table, immigrant_table, airport_table, city_table, output_datapath)
    
    
if __name__ == "__main__":
    main()