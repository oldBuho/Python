#!/usr/bin/env python
# -*- coding: utf-8 -*-

# file handler, simil path
import pathlib
import shutil
from glob import glob
# flask
from flask import Flask
from flask import request
from flask_cors import CORS
# http error handler
from werkzeug.exceptions import HTTPException
# boto 3 is a software dev kit (SDK) for AWS (S3 & EC2), used here to connect s3 and download a file
import boto3
# date handler
from datetime import date, timedelta, datetime
# data analytics tools
import pandas as pd
import numpy as np
# json handler
import json
# para ana-planificador, importo este input/output handler
import io
# handler de sql queries para pandas-df
import duckdb

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

cors = CORS(app)

pd.options.mode.chained_assignment = None

# S3 psw
ACCESS_KEY='FOOBARFOOBARFOOBAR'
SECRET_KEY='Foo0bar0Foo0bar0Foo0bar0Foo0bar0Foo0bar0Foo'


# ~~~~~~~~~~ PLANNER PROJECT _ START ~~~~~~~~~~ 

# endpoint that looks in S3 for the filters_template to be modified by the web user 
@app.route("/api/parameters", methods = ['GET'])
def return_parametros():
    planner_s3bucket = 'api'
    filters_s3_object = 'planner/filters/' 
    # s3 client connect
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY)

    # s3 list_object search, in this case, the "filters"
    object_list_myfilter = s3_client\
        .list_objects(
            Bucket = planner_s3bucket,
            Prefix = filters_s3_object)
    '''
    Select the second item, the filters.
    If the "success" file, created by default when Spark writes a parquet file, 
    it's off, this following part of the code can be avoided.
    How to avoid the success file: when starting the spark session add this line >> conf.set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    '''
    filter_key = [obj["Key"] for obj in object_list_myfilter["Contents"]][1]
    # Search parquet file, now an individual object (not list)
    object_filter = s3_client.get_object(
            Bucket = planner_s3bucket,
            Key = filter_key)
    # It reads the file, but it's not a json file, it's a byte type of file. 
    byteType_filter = object_filter['Body'].read()
    # Parse to string
    strType_filter = byteType_filter.decode("utf-8")
    # Build of the json structure, and removal of "\n" scape character (added by s3 by default)
    strType_filter_v2 = '{"parametros":[' \
                        + (strType_filter.replace("}\n{", "},{")) \
                            .replace('\n','') \
                    + ']}'

    filter_final = json.loads(strType_filter_v2)

    #response return
    return filter_final, 200, {'content-type': 'application/json'}

# Once the user modifies the filters as desired (request), sends that back to the server,
# the app processes it, and returns a "response" json file (vector data)
@app.route("/api/data", methods = ['POST'])
def return_data():

    # s3 client connect
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY)
    # S3 bucket
    planner_s3bucket="api"

    # the request sent by the webpage, it contains the filters modified by the user
    json_input = request.get_json()

    # Prior moving fwd whit the ET process, a test is performed on the request json file to avoid/catch errors
    def test():

        # Lists of names (keys) and dates are generated to be compared with ideal conditions
        name_list = []
        date_range_test = []
        json_input_value_test = json_input['request']

        for index in range(len(json_input_value_test)):
            name_list.append(json_input_value_test[index]['name'])
            if json_input_value_test[index]['name'] == 'date':
                date_range_test = json_input_value_test[index]['values']

        start_date_test_test=''
        end_date_test_test=''
        for date in date_range_test:
            if date_range_test[0] < date_range_test[1]:
                start_date_test_test = datetime.strptime(date_range_test[0], '%Y-%m-%dT%H:%M:%S.%f')
                end_date_test_test = datetime.strptime(date_range_test[1], '%Y-%m-%dT%H:%M:%S.%f') 
            elif date_range_test[0] > date_range_test[1]:
                start_date_test_test = datetime.strptime(date_range_test[1], '%Y-%m-%dT%H:%M:%S.%f')
                end_date_test_test = datetime.strptime(date_range_test[0], '%Y-%m-%dT%H:%M:%S.%f') 

        # Ideal conditions of names and dates
        name_template_list = ['date', 'sex', 'age_group', 'social_class']

        # Test on key name
        def test_1():
            # To campare str lists we don't need order items; if "sort" method added the script fails
            if name_template_list == name_list:
                return True
            else:
                return False
        # Data range test
        def test_2():
            if start_date_test_test >= (datetime.now() + timedelta(days = -30)) \
                and \
                end_date_test_test <= datetime.now():
                return True
            else:
                return False

        if test_1()==True and test_2()==True:
            return True
        else:
            return False
            
    # Data table gen, according to request
    def output():

        # applied filters extraction
        date_range = []
        age_filter = []
        sex_filter = []
        social_filter = []
        json_input_value = json_input['request']

        for index in range(len(json_input_value)):
            try:
                if json_input_value[index]['name'] == 'date':
                    date_range = json_input_value[index]['values']

                elif json_input_value[index]['name'] == 'sex':
                    sex_filter = json_input_value[index]['values']   

                elif json_input_value[index]['name'] == 'age_group':
                    age_filter = json_input_value[index]['values']

                elif json_input_value[index]['name'] == 'social_class':
                    social_filter = json_input_value[index]['values']   
            except:
                pass

        # start date y end date extraction
        start_date=''
        end_date=''

        if date_range[0] < date_range[1]:
            start_date = datetime.strptime(date_range[0], '%Y-%m-%dT%H:%M:%S.%f')
            end_date = datetime.strptime(date_range[1], '%Y-%m-%dT%H:%M:%S.%f') 

        elif date_range[0] > date_range[1]:
            start_date = datetime.strptime(date_range[1], '%Y-%m-%dT%H:%M:%S.%f')
            end_date = datetime.strptime(date_range[0], '%Y-%m-%dT%H:%M:%S.%f') 

        # sociodemographic data search on S3
        sociodemog_s3object = \
            'planner/data/sociodemographic_data/location_sex_age_social'
        objects_sociodemog = s3_client\
            .list_objects(
                Bucket = planner_s3bucket, 
                Prefix = sociodemog_s3object)
        # second item selection (read line ~64 in this script for further explanation)
        sociodemog_key = [obj["Key"] for obj in objects_sociodemog["Contents"]][1]

        # download selected object
        sociodemog_buffer = io.BytesIO()

        s3_client.download_fileobj(
            Bucket = planner_s3bucket,
            # "key" refers to the object name
            Key = sociodemog_key,
            Fileobj = sociodemog_buffer)

        # parquet file to pandas dataframe
        sociodemographic_df = pd.read_parquet(sociodemog_buffer)

        # take the sociodemographic data required by the user
        sociodemographic_semiFiltered = sociodemographic_df[
                                        (sociodemographic_df['age_group'].isin(age_filter)) 
                                        & (sociodemographic_df['social_class'].isin(social_filter))
                                        & (sociodemographic_df['sex'].isin(sex_filter))]

        # the output is a Series, where the index is the location and the social_class the data
        # this is used in a sql query, between dataframes, using duckdb package
        sociodemographic_filtered = sociodemographic_semiFiltered \
                                        .groupby(['id_location','social_class']) \
                                        ['probability_factor'].sum() \
                                        .reset_index()

        # data search in main_table located in S3
        start_date__main_table_object = \
            'planner/data/main_table/'+str(start_date.year).zfill(4)+'/' \
            +str(start_date.month).zfill(2)+'/'+str(start_date.day).zfill(2)

        start_date_objects = s3_client\
            .list_objects(
                Bucket = planner_s3bucket, 
                Prefix = start_date__main_table_object)
        # second item selection, parquet file
        # first item is the "success" file; this file write can be avoided editing the conf in the spark session init
        start_date_obj_key = [obj["Key"] for obj in start_date_objects["Contents"]][1]
        # download the target object
        start_date_buffer = io.BytesIO()

        s3_client.download_fileobj(
            Bucket = planner_s3bucket,
            # key refers to the object name
            Key= start_date_obj_key,
            Fileobj = start_date_buffer)

        # .parquet to Pandas df
        main_table = pd.read_parquet(start_date_buffer)

        # Gen of the rest of DFs for each day reaching end_date.
        # Each new day is added via "union" to the main_table df. 
        loop_date = start_date + timedelta(days = 1)

        while (loop_date <= end_date):

            # main_table data search based on the date specified in the loop
            dia_main_table_object = \
                'planner/data/main_table/'+str(loop_date.year).zfill(4)+'/' \
                +str(loop_date.month).zfill(2)+'/'+str(loop_date.day).zfill(2)   

            dia_object = s3_client\
                .list_objects(
                    Bucket = planner_s3bucket, 
                    Prefix = dia_main_table_object)

            # second item selection, parquet file
            dia_object_key = [obj["Key"] for obj in dia_object["Contents"]][1]
            # download target object
            dia_tabla_buffer = io.BytesIO()

            s3_client.download_fileobj(
                Bucket = planner_s3bucket,
                # key refers to the object name
                Key= dia_object_key,
                Fileobj = dia_tabla_buffer)

            # .parquet to Pandas df
            dia_main_table = pd.read_parquet(dia_tabla_buffer)

            # add the partial df to the total one
            main_table = pd.concat([main_table, dia_main_table],
                                        ignore_index=True, sort=False, levels=None)

            # move to the next day until getting to end_date
            loop_date = loop_date + timedelta(days=1)

        # DOW column gen
        main_table['day'] = pd.to_datetime(main_table['fecha'], format = '%Y-%m-%d') \
                                .dt.day_name()

        rating_df = duckdb.query('''

        -- Join of the needed tables
        -- The "probability_factor" is required for data break-down purposes
        WITH join_df AS (
            SELECT                        
                t.day, -- agrupo por day retirando fecha
                t.channel,
                t.timestamp_from AS epg_start,
                t.timestamp_to AS epg_end,
                t.epg_title,
                d.social_class,
                SUM(t.cable_box_action*d.probability_factor) AS cable_box_action_probFac,
                SUM(t.cable_box_count*d.probability_factor) AS cable_box_count_probFac
            FROM main_table AS t
            LEFT JOIN
                sociodemographic_filtered AS d
                ON t.id_location = d.id_location
            GROUP BY 
                t.day,
                t.channel,
                t.timestamp_from,
                t.timestamp_to,
                t.epg_title,
                d.social_class
            ),

        -- secondly, generate new columns for rating according to socioeconomic group and DOW
        intermediate_table AS (
            SELECT
                channel,
                epg_title,
                epg_start, 
                epg_end,
                cable_box_action_probFac/cable_box_count_probFac*100 AS rating,
                CASE
                    WHEN social_class = 'Alto' THEN cable_box_action_probFac/cable_box_count_probFac*100
                    ELSE 0            
                END rating_upper_class,
                CASE
                    WHEN social_class = 'Medio' THEN cable_box_action_probFac/cable_box_count_probFac*100
                    ELSE 0            
                END rating_middle_class,
                CASE
                    WHEN social_class = 'Bajo' THEN cable_box_action_probFac/cable_box_count_probFac*100
                    ELSE 0            
                END rating_lower_class,
                CASE
                    WHEN day = 'Sunday' THEN cable_box_action_probFac/cable_box_count_probFac*100
                    ELSE 0            
                END rating_sunday,
                CASE
                    WHEN day = 'Monday' THEN cable_box_action_probFac/cable_box_count_probFac*100
                    ELSE 0            
                END rating_monday,
                CASE
                    WHEN day = 'Tuesday' THEN cable_box_action_probFac/cable_box_count_probFac*100
                    ELSE 0            
                END rating_tuesday,
                CASE
                    WHEN day = 'Wednesday' THEN cable_box_action_probFac/cable_box_count_probFac*100
                    ELSE 0            
                END rating_wednesday,
                CASE
                    WHEN day = 'Thursday' THEN cable_box_action_probFac/cable_box_count_probFac*100
                    ELSE 0            
                END rating_thursday,
                CASE
                    WHEN day = 'Friday' THEN cable_box_action_probFac/cable_box_count_probFac*100
                    ELSE 0            
                END rating_friday,
                CASE
                    WHEN day = 'Saturday' THEN cable_box_action_probFac/cable_box_count_probFac*100
                    ELSE 0            
                END rating_saturday
            FROM
                join_df AS t
            ORDER BY
                channel,
                epg_title,
                epg_start,
                epg_end
            )

        -- se tiene en cuenta el solo el primer dato de horario por epg_title como referencia
        SELECT
            channel,
            epg_title,
            MIN(epg_start) AS epg_start,
            MIN(epg_end) AS epg_end,
            SUM(rating) AS rating,
            SUM(rating_upper_class) AS rating_upper_class,
            SUM(rating_middle_class) AS rating_middle_class,
            SUM(rating_lower_class) AS rating_lower_class,
            SUM(rating_sunday) AS rating_sunday,
            SUM(rating_monday) AS rating_monday,
            SUM(rating_tuesday) AS rating_tuesday,
            SUM(rating_wednesday) AS rating_wednesday,
            SUM(rating_thursday) AS rating_thursday,
            SUM(rating_friday) AS rating_friday,
            SUM(rating_saturday) AS rating_saturday
        FROM 
            intermediate_table
        GROUP BY
            channel,
            epg_title
        ''').df()

        # epg_duration row gen
        rating_df['epg_duration'] = rating_df['epg_end'] - rating_df['epg_start'] 
        rating_df['epg_duration'] = rating_df['epg_duration']/np.timedelta64(1,'m')

        # Delete "seconds" in epg_start and epg_end for particular epg_titles
        # Rating normalization by doing a rate with epg_duration
        # Field sort
        final_table = duckdb.query('''
            SELECT 
                channel,
                epg_title,
                LEFT(CAST(epg_start AS string), LENGTH(epg_start)-3) AS epg_start,
                LEFT(CAST(epg_end AS string), LENGTH(epg_end)-3) AS epg_end,
                epg_duration,
                ROUND(rating/epg_duration, 2) rating_promedio,
                ROUND(rating_upper_class/epg_duration, 2) rating_upper_class,
                ROUND(rating_middle_class/epg_duration, 2) rating_middle_class,
                ROUND(rating_lower_class/epg_duration, 2) rating_lower_class,
                ROUND(rating_sunday/epg_duration, 2) rating_sunday,
                ROUND(rating_monday/epg_duration, 2) rating_monday,
                ROUND(rating_tuesday/epg_duration, 2) rating_tuesday,
                ROUND(rating_wednesday/epg_duration, 2) rating_wednesday,
                ROUND(rating_thursday/epg_duration, 2) rating_thursday,
                ROUND(rating_friday/epg_duration, 2) rating_friday,
                ROUND(rating_saturday/epg_duration, 2) rating_saturday
            FROM 
                rating_df
            ORDER BY 
                channel, epg_title, epg_start
                ''').df()

        # parse to json
        output_data = final_table. to_json()

        # JSON structure build, removing "new lines" scape characters (\n) added by default by S3.
        # Final JSON edit as a vector: {"data":["..."]}
        output_data_final = '{"data":[' \
                            + (output_data.replace("}\n{", "},{")) \
                                .replace('\n','') \
                        + ']}'
        
        return output_data_final, 200, {'content-type': 'application/json'}

    # msg if request fails 
    message = '''
        <!DOCTYPE html>
        <html lang="en-us">
            <head>
                <title>400 Bad Request</title>
                <style>
                    body {
                        color: #0097a9;
                        font-family: Nunito,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica Neue,Arial,Noto Sans,sans-serif,Apple Color Emoji,Segoe UI Emoji,Segoe UI Symbol,Noto Color Emoji;
                    }
                    li {
                        font-size:20px;
                        padding-left: 20px;
                    }
                </style>
            </head>
            <body>
                <h1 style="font-size:40px">Request Error.<h1>
                <p style="font-size:25px">Possible causes:</p>
                    <ul>
                        <li>Typo in "name" (eg. age_range instead of age_group),</li>
                        <li>"Date range" error; this should be [-30 days ; hoy]</li>
                    </ul>
                <h2 style="font-size:20px">ANA</h2>
            </body>
        </html>
        '''    

    if test()==False:
        return message, 400, {'content-type': 'text/html'}
    elif test()==True:
        return output()

# ~~~~~~~~~~ PLANNER PROJECT END ~~~~~~~~~~ 

# ~~~~~~~~~~ ERROR HANDLER START ~~~~~~~~~~ 

@app.errorhandler(404)
def page_not_found(e):
    message = '''
        <!DOCTYPE html>
        <html lang="en-us">
            <head>
                <title>404 Page not Found</title>
                <style>
                    body {
                        color: #0097a9;
                        font-family: Nunito,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica Neue,Arial,Noto Sans,sans-serif,Apple Color Emoji,Segoe UI Emoji,Segoe UI Symbol,Noto Color Emoji;
                    }
                </style>
            </head>
            <body>
                <h1 style="font-size:40px">Page not Found!<h1>
                <p style="font-size:30px">Check for typos in the URL.</p>
                <h2 style="font-size:20px">;)</h2>
            </body>
        </html>
        '''    
    return message, 404, {'content-type': 'text/html'}

@app.errorhandler(500)
def internal_server_error(e):
    message = '''
        <!DOCTYPE html>
        <html lang="en-us">
            <head>
                <title>500 Internal Server Error</title>
                <style>
                    body {
                        color: #0097a9;
                        font-family: Nunito,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica Neue,Arial,Noto Sans,sans-serif,Apple Color Emoji,Segoe UI Emoji,Segoe UI Symbol,Noto Color Emoji;
                    }
                    li {
                        font-size:20px;
                        padding-left: 20px;
                    }
                </style>
            </head>
            <body>
                <h1 style="font-size:40px">Waiting for server response.</h1>
                    <p style="font-size:25px">Possible solutions:</p>
                        <ul>
                            <li>Refresh the website (F5).</li>
                            <li>Clear cache.</li>
                            <li>Clear cookies.</li>
                            <li>If you still have issues accessing, contact us!</li>
                        <ul>
                <h2 style="font-size:20px">;)</h2>
            </body>
        </html>
        '''    
    return message, 500, {'content-type': 'text/html'}

@app.errorhandler(HTTPException)
def handle_exception(e):
    # Return JSON, instead of HTML, for HTTP errors.
    # Start with the correct headers and status code from the error
    response = e.get_response()
    # Replace the body with JSON
    response.data = json.dumps({
        "code": e.code,
        "name": e.name,
        "description": e.description,
    })
    response.content_type = "application/json"
    return response

# ~~~~~~~~~~ ERROR HANDLER END ~~~~~~~~~~        

# run app 
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
