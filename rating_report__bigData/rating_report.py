#!/usr/bin/env python
# -*- coding: utf-8 -*-

# PACKAGES, see https://pypi.org/ & individual documentation for each package
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from datetime import datetime, timedelta
import pandas as pd
# import traceback
# boto 3 is a software dev kit (SDK) for AWS (S3 & EC2), used here to connect s3 and read/delete files
import boto3

# check for files in Amazon S3 
def reading_test():
    try:
        sqlContext.read.format("csv") \
                        .options(header='True', delimiter=';') \
                        .load(input_s3_path)
        return True
    except:
        return False

# read S3 files and generates str_lists of file_names
def file_name_list(list_name):
    # s3 conexion instance
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY)
    # target object search and retrival of csv_file paths list 
    objects_input = s3_client\
        .list_objects(
            Bucket=bucket_name, 
            Prefix=object_name)
    # iteration variable
    obj_content_len = len(objects_input["Contents"]) 
    # generation of list of file_name_and_extension or file_name_only
    if list_name == 'file_name_and_extension':
        file_name_and_extension = []
        for i in range(1,obj_content_len):
            temporary_file_name = objects_input["Contents"][i]['Key'] \
                            .replace(objects_input["Contents"][0]['Key'], '')   
            file_name_and_extension += [temporary_file_name]
        return file_name_and_extension   
    elif list_name == 'file_name_only':
        file_name_only = []
        for i in range(1,obj_content_len):
            temporary_file_name = objects_input["Contents"][i]['Key'] \
                            .replace(objects_input["Contents"][0]['Key'], '')   
            file_name_only += [temporary_file_name.replace('.csv', '')]
        return file_name_only
    else:
        print("$$$$$$$$$$ wrong parameter, try 'file_name_only' or 'file_name_and_extension'")


# data extraction from "cable epg" according to dates (date_list) 
# and to the request made by the client as input in an s3 object (csv file).# to the request 
def main_process(date_list, request_df):

    # SEARCH FOR RATING DATA ON DATES DETERMINED BY THE INPUT
    # Read the cable_epg.parquet target files by requested date.
    cable_epg_path = cable_epg \
        +str(date_list[0].__getitem__('year')).zfill(4)+'/' \
        +str(date_list[0].__getitem__('month')).zfill(2)+'/' \
        +str(date_list[0].__getitem__('day')).zfill(2)+'/*.parquet' 
    # A first df, using just the period min date (start_date), is generated outside the loop 
    # in order to have a schema of the df prior the total date range iteration
    start_date_df = sqlContext.read.parquet(cable_epg_path)
    start_date_df.createOrReplaceTempView("start_date_df_view")
    rating_df = sqlContext.sql ('''
        -- In a first step, the fields of interest are extracted and data is prepared for rating calculation
        WITH table AS (
            SELECT                        
                year,
                month,
                day,
                hour,
                minute,
                channel,
                channel_number,
                SUM(COALESCE(mac_addr_channel, 0)*location_weight) AS mac_addr_channel__location_weight, 
                MAX(deco_count) AS deco_count -- We take the MAX value since there may be more than one data per location according to the calculation of "deco_count" (the count of cable boxes)
            FROM
                start_date_df_view
            WHERE
                location = 'city_name'
            GROUP BY
                year,
                month,
                day,
                hour,
                minute,
                channel,
                channel_number    
            )
        -- tabla final
        SELECT
            CAST(year AS INT) AS year,
            CAST(month AS INT) AS month,
            CAST(hour AS INT) AS hour,
            CAST(day AS INT) AS day,
            CAST(minute AS INT) AS minute,
            channel,
            channel_number,
            ROUND(mac_addr_channel__location_weight/deco_count, 3) AS rating
        FROM 
            table
        ''')

    # loop para resto de fechas de interés agrengado filas al df previamente armado (rating_df)
    for i in range(1, len(date_list)):
        parquet_path = cable_epg \
            +str(date_list[i].__getitem__('year')).zfill(4)+'/' \
            +str(date_list[i].__getitem__('month')).zfill(2)+'/' \
            +str(date_list[i].__getitem__('day')).zfill(2)+'/*.parquet' 
        try:
            # prueba de apertura de archivo
            sqlContext.read.parquet(parquet_path)
        except:
            print("$$$$$$$$$$ Error al leer particion: " +  parquet_path)
        # escritura de df a partir del parquet-input
        ana_epg_df = sqlContext.read.parquet(parquet_path)
        ana_epg_df.createOrReplaceTempView("ana_epg_df_view")
        temporary_rating_df = sqlContext.sql ('''
            -- In a first step, the fields of interest are extracted and data is prepared for rating calculation
            WITH table AS (
                SELECT                        
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    channel,
                    channel_number,
                    SUM(COALESCE(mac_addr_channel, 0)*location_weight) AS mac_addr_channel__location_weight, 
                    MAX(deco_count) AS deco_count -- tomamos MAX dado que puede haber mas de un dato por localidad de acurdo al cálculo de "total cajas"
                FROM
                    ana_epg_df_view
                WHERE
                    location = 'city_name'
                GROUP BY
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    channel,
                    channel_number   
                )
            -- second step
            SELECT
                CAST(year AS INT) AS year,
                CAST(month AS INT) AS month,
                CAST(hour AS INT) AS hour,
                CAST(day AS INT) AS day,
                CAST(minute AS INT) AS minute,
                channel,
                channel_number,
                ROUND(mac_addr_channel__location_weight/deco_count, 3) AS rating
            FROM 
                table
            ORDER BY 
                year,
                month,
                day,
                hour,
                minute,
                channel
            ''')
        # finaly, append of the partial_df (temporary_rating_df) to the total_df (rating_df)
        rating_df = rating_df.union(temporary_rating_df)    
  
    # join btw rating data and client request df's
    rating_df.createOrReplaceTempView("rating_df")
    request_df.createOrReplaceTempView("request_df")
    rating_report = sqlContext.sql ('''

        WITH table AS (
            SELECT
                a.year,
                a.month,
                a.day,
                a.hour,
                a.minute,
                COALESCE(a.channel_number, 'sin datos') AS channel_number, 
                COALESCE(r.channel, 'sin datos') AS channel, 
                COALESCE(r.rating, 0) AS rating
            FROM 
                request_df AS a
            LEFT JOIN
                rating_df AS r 
                    ON  a.year = r.year
                    AND a.month = r.month
                    AND a.day = r.day
                    AND a.hour = r.hour
                    AND a.minute = r.minute
                    AND a.channel_number = r.channel_number
        )
        -- data type modification to, subsequently, sort dates 
        SELECT *
        FROM (
            SELECT
                CAST(year AS INT) AS year,
                CAST(month AS INT) AS month,
                CAST(day AS INT) AS day,
                CAST(hour AS INT) AS hour,
                CAST(minute AS INT) AS minute,
                channel_number,
                channel,
                ROUND(CAST(rating AS FLOAT), 3) AS rating
            FROM 
                table)
        ORDER BY 
            year ASC,
            month ASC,
            day ASC,
            hour ASC,
            minute ASC,
            rating DESC
        ''')
    
    return rating_report

# read input, report generation, and write output
def rating_report_build():
    try:
        for i in range(len(file_name_list('file_name_and_extension'))):
            print('$$$$$$$$$$ read & write process #'+str(i+1))
            print('$$$$$$$$$$ S3 input reading: '+ input_s3_path + file_name_list('file_name_and_extension')[i])              
            # file search in s3
            request_df = sqlContext.read \
                .format("csv") \
                .options(header='True', delimiter=';') \
                .load(input_s3_path + file_name_list('file_name_and_extension')[i])
            # working dates extraction and editing
            fechas_df = request_df['year','month', 'day'].distinct()
            fechas_df = fechas_df.orderBy(col("year").asc(), col("month").asc(), col("day").asc())
            # date list gen
            date_list = fechas_df.collect()    
            # output report gen accoring to date_list, and request_df, created above
            rating_report = main_process(date_list, request_df)
            # output report writing
            print('$$$$$$$$$$ S3 output writing: '+ output_s3_path + file_name_list('file_name_only')[i] + '_' + str(datetime.now().date()))
            rating_report.repartition(1).write \
                                        .format('csv') \
                                        .mode('overwrite') \
                                        .options(header='True', delimiter=';') \
                                        .save(output_s3_path + file_name_list('file_name_only')[i] + '_' + str(datetime.now().date()))
    except:
        pass
        # add send email with error information as body and filename in subject

# S3 input deletion
def delete_input():
    s3_client = boto3.client(
        "s3",
        aws_access_key_id = ACCESS_KEY,
        aws_secret_access_key = SECRET_KEY)

    for file in file_name_list('file_name_and_extension'):
        s3_client.delete_object(
            Bucket = bucket_name, 
            Key = object_name + file)
        print('$$$$$$$$$$ S3 input deletion: ' + file)


if __name__ == '__main__':

    input_s3_path = 's3a://reports_bucket/rating/city_name/input/'
    output_s3_path = 's3a://reports_bucket/rating/city_name/output/'

    bucket_name = 'reports_bucket'
    object_name = 'rating/city_name/input/'

    ACCESS_KEY='FOOBARFOOBARFOOBAR'
    SECRET_KEY='Foo0bar0Foo0bar0Foo0bar0Foo0bar0Foo0bar0Foo'

    cable_epg = 'hdfs://master:8020/cable_epg/daily_epg_report/'

    # spark init
    try:
        # an instance is defined to configure Spark
        conf = SparkConf().setAppName("rating report: city_name")
        # S3 config for subsequent sending of files, necessary for work in crontab
        # (crontab does not get the keys from the spark config as if we run this directly, outside a cron-job)
        conf.set("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        conf.set("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        conf.set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        # to avoid the success_file gen when using "write attribute" from spark
        conf.set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        # sc object definition to give the execution context according to the applied config
        sc = SparkContext(conf=conf)
        # creation of the sqlContext working object
        sqlContext = SQLContext(sc)
    except: 
        print("$$$$$$$$$$ Spark init error")

    # process start
    if reading_test() == False:
        print('$$$$$$$$$$ input file not found')
    elif reading_test() == True:
        rating_report_build()
        delete_input()
    # process end
