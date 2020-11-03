### Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task is a building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to. 

### Datasets

You'll be working with two datasets that reside in S3. Here are the S3 links for each:

Song data: <s3://udacity-dend/song_data>

Log data: <s3://udacity-dend/log_data>

Each file of sond dataset is in JSON format and contains metadata about a song and the artist of that song. 
The log files in the dataset you'll be working with are partitioned by year and month. 

The task is need to create a star schema optimized for queries on song play analysis. This includes the following tables.

Fact Table

<ul>
<li>songplays - records in log data associated with song plays i.e. records with page NextSong</li>
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
</ul>

Dimension Tables

<ul>
<li>users - users in the app</li>
    user_id, first_name, last_name, gender, level
<li>songs - songs in music database</li>
    song_id, title, artist_id, year, duration
<li>artists - artists in music database</li>
    artist_id, name, location, lattitude, longitude
    <li>time - timestamps of records in songplays broken down into specific units</li>
    start_time, hour, day, week, month, year, weekday
</ul>

### ETL process

Using the AWS Credentials data are loaded from the S3 bucket and transfromed by means of Spark to the star schema.
The final tables are loaded back to S3 bucket. 


Project files:

 - etl.py - file load data from s3 bucket and transform them to the parquets files and and writes them back to S3
 - dl.cfg - configuration file that contain information for connection to AWS resources
 

Run etl.py to load data from S3 bucket to data lake and transform it.