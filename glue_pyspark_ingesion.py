import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import *
import boto3,json
import pandas as pd
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_data(spark, input_data, output_data):
    """
    Description:
        Process the searches data files of json format and create parquet files  for search table.
    :param spark: a spark session instance
    :param input_data: input file path
    :param output_data: output file path
    """
    
    #in_data = input_data + "/order_lines.csv"

    df = spark.read.option("inferSchema",True).csv("s3://sl-asessment-s3/data/products/variant.csv")
    #df.write.parquet("s3://sl-asessment-s3/data/output/" ) 

    df.write.mode("append").format("jdbc")\
    .option("url", "jdbc:postgresql://mydb.cch5yeioln81.ap-southeast-1.rds.amazonaws.com:5432/") \
    .option("driver", "org.postgresql.Driver").option("database", "sl_asessment").option("dbtable", "public.variant") \
    .option("user", "postgres").option("password", "Aadhya143").mode("overwrite").save()
    
    dfprqt = spark.read.format("jdbc").options(url= "jdbc:postgresql://mydb.cch5yeioln81.ap-southeast-1#.rds.amazonaws.com:5432/"\
    ,driver="org.postgresql.Driver",dbtable="public.orders",user="postgres",password="Aadhya143" ).load()
    
    dfprqt.write.parquet("s3://sl-asessment-s3/data/output/" )   
    
def main():
    spark = create_spark_session()
    
    input_data = "s3://sl-asessment-s3/data/"
    output_data = "s3://sl-asessment-s3/outbound/"
    
    process_data(spark, input_data, output_data)
    

    
if __name__ == "__main__":
    main()


    
job.commit()