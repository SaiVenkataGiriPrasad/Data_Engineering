import os
from datetime import datetime, timedelta
from time import strftime
import requests
import psycopg2
from sqlalchemy import create_engine
import pandas as pd

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def ingest_data(date):
    """Ingest ACLED data for a given date"""

    conf = SparkConf()
    conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4')
    conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    conf.set('spark.hadoop.fs.s3a.access.key', aws_access_key)
    conf.set('spark.hadoop.fs.s3a.secret.key', aws_secret_key)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    df = pd.read_sql_query(f"SELECT * FROM acled where event_date > '{date}'::date - '10 days'::interval and event_date <= '{date}'::date", engine)
    dataframe = spark.createDataFrame(df)
    
    if dataframe.count() == 0:
        return "No data"

    dataframe = dataframe.withColumn("event_date", F.to_date("event_date", "yyyy-MM-dd"))
    dataframe = dataframe.withColumn("fatalities", dataframe["fatalities"].cast("int"))
    dataframe = dataframe.withColumn("geo_precision", dataframe["geo_precision"].cast("int"))
    dataframe = dataframe.withColumn("inter1", dataframe["inter1"].cast("int"))
    dataframe = dataframe.withColumn("interaction", dataframe["interaction"].cast("int"))
    dataframe = dataframe.withColumn("latitude", dataframe["latitude"].cast("double"))
    dataframe = dataframe.withColumn("longitude", dataframe["longitude"].cast("double"))
    dataframe = dataframe.withColumn("time_precision", dataframe["time_precision"].cast("int"))
    dataframe = dataframe.withColumnRenamed("timestamp", "upload_date")
    dataframe = dataframe.withColumn("upload_date", F.from_unixtime("upload_date", "yyyy-MM-dd HH:mm:ss"))
    dataframe = dataframe.withColumn("upload_date", F.to_timestamp("upload_date", "yyyy-MM-dd HH:mm:ss"))

    for column in dataframe.columns:
        dataframe = dataframe.withColumn(column, F.when(F.col(column) == "", None).otherwise(F.col(column)))

    dataframe.coalesce(1).write.partitionBy('event_date').mode("append").option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false").parquet(f's3a://{acled_bucket}/acled_{owner}/')
    return "Success"