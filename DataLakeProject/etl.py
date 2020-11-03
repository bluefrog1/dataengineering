import configparser
from datetime import datetime
import os
from pyspark.sql.types  import IntegerType, DateType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['CONFIG']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['CONFIG']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Description: This function can be used to create Spark object.

    Arguments:
        None

    Returns:
        spark:the spark object 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function can be used to read song data file
    and extract necessary information for creating songs and artists tables.

    Arguments:
        spark: the spark object. 
        input_data: song data file path. 
        output_data: path for output parquets file

    Returns:
        None
    """
    
    # filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # reading song data file
    df = spark.read.json(song_data).dropDuplicates()

    # extracting columns to create songs table
    df.createOrReplaceTempView("songs_data_view")
    songs_table = spark.sql("SELECT song_id, \
                                    title, \
                                    artist_id, \
                                    year, \
                                    duration \
                             FROM songs_data_view \
                             ORDER BY (year, artist_id)")
   
    # writing songs table to parquet files partitioned by year and artist
    songsParquetPath = os.path.join(output_data, "songs")
    songs_table.write.partitionBy("year", "artist_id").parquet(songsParquetPath)

    # extracting columns to create artists table
    artists_table = spark.sql("SELECT artist_id,\
                                      artist_name, \
                                      artist_location, \
                                      artist_latitude, \
                                      artist_longitude \
                               FROM songs_data_view")
    
    # writing artists table to parquet files
    artistsParquetPath = os.path.join(output_data, "artists")
    artists_table.write.parquet(artistsParquetPath)

def process_log_data(spark, input_data, output_data):
    """
    Description: This function can be used to read log data file
    and extract necessary information for creating users, time and songplays tables.

    Arguments:
        spark: the spark object. 
        input_data: song data file path. 
        output_data: path for output parquets file

    Returns:
        None
    """
    
    # getting filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # reading log data file
    df = spark.read.json(log_data).dropDuplicates()
    
    # filtering by actions for song plays
    df = df.filter(df.page == "NextSong")

    df.createOrReplaceTempView("log_data_view")

    # extracting columns for users table    
    users_table = spark.sql("SELECT userId, \
                                    firstName, \
                                    lastName, \
                                    gender, \
                                    level \
                             FROM log_data_view")
    
    # writing users table to parquet files
    usersParquetPath = os.path.join(output_data, "users")
    users_table.write.parquet(usersParquetPath)

    # creating timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(datetime.fromtimestamp(x / 1000.0)))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # creating datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn("datetime", get_datetime(df.ts))
    df.createOrReplaceTempView("log_data_view")
    
    # extracting columns to create time table
    time_table = spark.sql("SELECT log_data_view.timestamp            as start_time, \
                                   hour(log_data_view.datetime)       as hour, \
                                   dayofmonth(log_data_view.datetime) as day, \
                                   weekofyear(log_data_view.datetime) as week, \
                                   month(log_data_view.datetime)      as month, \
                                   year(log_data_view.datetime)       as year, \
                                   dayofweek(log_data_view.datetime)  as weekday \
                            FROM log_data_view")
    
    # writing time table to parquet files partitioned by year and month
    timeParquetPath = os.path.join(output_data, "time")
    time_table.write.partitionBy("year", "month").parquet(timeParquetPath)

    # reading in song data to use for songplays table
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    song_df = spark.read.json(song_data).dropDuplicates()

    # extracting columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_name) & \
                                       (df.length == song_df.duration), 'left_outer')
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())
    songplays_table.createOrReplaceTempView("songplays_table_view")
    songplays_table = spark.sql("SELECT  songplay_id as songplay_id, \
                                         timestamp   as start_time, \
                                         userId      as user_id, \
                                         level       as level, \
                                         song_id     as song_id, \
                                         artist_id   as artist_id, \
                                         sessionId   as session_id, \
                                         location    as location, \
                                         userAgent   as user_agent, \
                                         month(songplays_table_view.datetime)      as month,  \
                                         year(songplays_table_view.datetime)       as year \
                                 FROM songplays_table_view ")

    # writing songplays table to parquet files partitioned by year and month
    songplaysParquetPath = os.path.join(output_data, "songplays")
    songplays_table.write.partitionBy('year', 'month').parquet(songplaysParquetPath)

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/parquets/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
