1_ the user uploads a csv file, based on a template file, to a s3 bucket. The file contains date, time, and channels numbers as a request.

The user takes the channel numbers from another csv file generated from a channel_list.py program. 
That file contains channel names and numbers. 

2_ the process in this py file, here presented, reads that bucket and process the data. It takes data from a "epg data set" allocated in a HDFS as a parquet file. 

The output is the same input file but with an additional field containing "rating" data for each row (each row is a date~time~channel study case).

The output is written in a separated S3 bucket. 

3_ finally, the input is deleted from S3. 


-> This program runs every half hour, as a cron job, on an EC2 instance. 