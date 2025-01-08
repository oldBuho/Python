#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# define spark context
from pyspark.sql import SparkSession
# dataframe schema
from pyspark.sql.types import StructField, StructType, StringType
# lit to add a new column to DataFrame by assigning a literal or constant value
from pyspark.sql.functions import lit
# boto 3 is a software dev kit (SDK) for AWS (S3 & EC2), used here to connect s3 and download a file
import boto3
# error handler
import traceback
# access to variables and functions related to the interpreter/system.
import sys
# date and time handler
import datetime
# import utilities from testCompany.json
sys.path.append("/home/ec2-user/testCompany/testCompany")
from confing_and_email import send_email, get_config_testCompany

def process():

    try: 

        # declaration of usage paths.
        bucket_name = 'testCompany-input'
        csv_file_key = 'tcc/raw/epg/unprocessed/'
        csv_file_key_processed = 'tcc/raw/epg/processed/'
        parquet_file_key = 'tcc/raw/epg/parquet/'

        try:
            # SparkSession init
            # Adding of config to avoid "succcess file" generation
            spark = SparkSession.builder.appName("EPG: CSV to Parquet") \
                .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
                .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
                .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
                .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
                .getOrCreate()
        except:
            print("ERROR al inicializar SparkSession")

        # Preparation of the working DataFrame schema.
        # There are two fewer columns compared to V1; here, date & time are combined into a single 'datetime' column.

        schema = StructType([
            StructField("epg_id", StringType()),
            StructField("senal", StringType()),
            StructField("datetime_from", StringType()),
            StructField("datetime_to", StringType()),
            StructField("epg_title", StringType()),
            StructField("cat", StringType())
        ])

        try:
            # boto3 for S3 client init (session init)
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_KEY)

            # search for the target object and retrieve the path of the CSV to be read.
            epg_objects = s3_client\
                .list_objects_v2(
                    Bucket=bucket_name, 
                    Prefix=csv_file_key)
        except:
            print("S3 client ERROR")

        # list of CSVs in the "unprocessed" folder.
        csv_list = [obj["Key"].split("/")[-1] for obj in epg_objects["Contents"] if obj["Key"].endswith('.csv')]
        
        for i in range(len(csv_list)):

            # S3 path for file in "unprocessed" folder
            # s3_csv_path = "s3a://"+bucket_name+"/"+csv_file_key+csv_list[i]
            s3_csv_path = f"s3a://{bucket_name}/{csv_file_key}{csv_list[i]}"
            
            print("Archivo de trabajo: " + s3_csv_path)
            
            # extraction of just the file name within "unprocessed" folder
            file_name = csv_list[i].split(".")[0]
            
            # Read the CSV file using Spark.
            # sep by default = ',' (comma).
            # The CSV must have the columns in the same order as the schema defined here.
            # Added removal of entirely null rows, if any exist.

            df = spark.read.csv(s3_csv_path, header=True, schema=schema)\
                .na.drop(how="all")\
                .withColumn('s3_file', lit(file_name).cast(StringType()))    
            
            # dS3 path for the new parquet file
            # s3_parquet_path = "s3a://"+bucket_name+"/"+parquet_file_key+file_name
            s3_parquet_path = f"s3a://{bucket_name}/{parquet_file_key}{file_name}"
            # parquet writing, default compression "snappy"
            df.write.parquet(s3_parquet_path, mode="overwrite")
            # print("Parquet file generated: " + s3_parquet_path)
            print(f"Parquet file generated: {s3_parquet_path}")

            # move file from "unprocessed" to "processed"
            # https://repost.aws/questions/QUcAzX1YTqTuSrNDGmiF3gfw/what-is-the-ideal-way-to-copy-objects-from-one-s3-to-another-in-a-different-region-via-boto3
            csv_file_key_filename = csv_file_key+csv_list[i]
            csv_file_key_processed_filename = csv_file_key_processed+csv_list[i]
            copy_source = {
                'Bucket': bucket_name,
                'Key': csv_file_key_filename
            }
            s3_client.copy_object(
                CopySource = copy_source,
                Bucket = bucket_name, 
                Key = csv_file_key_processed_filename
                )
            # print("File copied to 'processed' folder: " + bucket_name +"/"+ csv_file_key_processed_filename)
            print(f"File copied to 'processed' folder: {bucket_name}/{csv_file_key_processed_filename}")

            # delete the file that has already been processed in the "unprocessed" folder.
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_object.html
            s3_client.delete_object(Bucket=bucket_name, Key=csv_file_key_filename)
            # print("File deleted: " + s3_csv_path)
            print(f"File deleted: {s3_csv_path}")
                
    except:

        # print("ERROR: "+ traceback.format_exc())
        # subject = "environment " + environment + ", error executing the EPG ingestion process - " + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # body = "error executing script " + sys.argv[0] + ":  "+ traceback.format_exc()

        print(f"ERROR: {traceback.format_exc()}")
        subject = f"environment {environment}, error executing the EPG ingestion process - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        body = f"error executing script {sys.argv[0]}: {traceback.format_exc()}"

        send_email(subject, body)
        print(body)        

if __name__ == '__main__':

    try:
        # get testCompany config
        config_testCompany = get_config_testCompany()
        environment = config_testCompany["environment"]["name"]
    except:
        environment = 'No definido'

    ACCESS_KEY = config_testCompany["s3_key"]["ACCESS_KEY"]
    SECRET_KEY = config_testCompany["s3_key"]["SECRET_KEY"]


    process()