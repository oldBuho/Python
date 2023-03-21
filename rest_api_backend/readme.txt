REST API _ backend
I was in charge of generating the endpoints for the API, + the ETL involved to provide the required data. 

NOTE
The code in the python file is not that of the application, most of the key processes are missing and those described are modified to establish a case study.

PROCESS
The ETL runs as part of a Linux-cronjob process in an Amazon EC2 instance; it reads/writes parquet files (big data) from a Hadoop DFS, 
modifies them with Spark and Pandas, and writes/deletes (using Boto) JSON files in Amazon S3 buckets to subsequently be used by the app. 
The app reads a JSON template with sociodemographic conditions as slicers for the user (endpoint #1). 
The user applies changes to the slicer and sends its request; the app gives back a response with EPG  and rating data (endpoint #2)
