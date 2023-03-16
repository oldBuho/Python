#!/usr/bin/env python
# -*- coding: utf-8 -*-

# PACKAGES, see https://pypi.org/ & individual documentation for each package
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from datetime import datetime, timedelta
import pandas as pd
import traceback

def channel_list():
    try:
        # spark init
        try:
            # an instance is defined to configure Spark
            conf = SparkConf().setAppName("channel list gen")
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

        ##### channel list reading from cable_epg.parquet data set from a HDFS 

        # use today()-1 to avoid cases where the last file is not yet present when read
        extraction_date = datetime.now() - timedelta(days = 1)

        cable_epg_path = cable_epg \
            +str(extraction_date.year).zfill(4)+'/' \
            +str(extraction_date.month).zfill(2)+'/' \
            +str(extraction_date.day).zfill(2)+'/*.parquet' 

        cable_epg_df = sqlContext.read.parquet(cable_epg_path)

        cable_epg_df.createOrReplaceTempView("cable_epg_view")

        channel_df = sqlContext.sql('SELECT DISTINCT channel, channel_number FROM cable_epg_view ORDER BY channel ASC')

        ##### ESCRITURA DEL OUTPUT EN S3
        channel_df.repartition(1).write  \
                                .format('csv') \
                                .mode('overwrite') \
                                .options(header='True', delimiter=';') \
                                .save(s3_path)
    
    except:
        pass
        # add send email with error information as body and filename in subject

if __name__ == '__main__':
      
    ACCESS_KEY='FOOBARFOOBARFOOBAR'
    SECRET_KEY='Foo0bar0Foo0bar0Foo0bar0Foo0bar0Foo0bar0Foo'

    cable_epg = 'hdfs://master:8020/cable_epg/daily_epg_report/'
    
    s3_path = 's3a://reports_bucket/rating/channel_list/'

    # run process
    channel_list()