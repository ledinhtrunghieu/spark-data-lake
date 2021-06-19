import configparser
from datetime import datetime
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import DateType


def create_spark_session():
    """Creates a Spark Session.
    Args:
        None
        
    Returns:
        None
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Processes song data and creates the song and artist tables
    Args:
        spark: SparkSession
        input_data: Input Files Link
        output_data: Storage Link
        
    Returns:
        None
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df =  spark.read.json(song_data)
    df.createOrReplaceTempView("songs")
    
    # extract columns to create songs table
    songs_table = spark.sql("SELECT distinct song_id, title, artist_id, year, duration FROM songs")

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year', 'artist_id']).parquet(path = os.path.join(output_data, 'songs'),mode = 'overwrite') 

    # extract columns to create artists table
    artists_table = spark.sql("SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM songs")
    
    # write artists table to parquet files
    artists_table.write.parquet(path = os.path.join(output_data, 'artists'),mode = 'overwrite')


def process_log_data(spark, input_data, output_data):
    """ Processes log data and creates the users, time and songplays tables
    Args:
        spark: SparkSession
        input_data: Input Files Link
        output_data: Storage Link
        
    Returns:
        None
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data) 
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    df.createOrReplaceTempView("logs")
    
    # extract columns for users table    
    users_table = spark.sql("SELECT distinct userId, firstName, lastName, gender, level FROM logs")
  
    # write users table to parquet files
    users_table.write.parquet(path = os.path.join(output_data, 'users'),mode = 'overwrite')


    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x // 1000), DateType())
    
    # create datetime column from original timestamp column
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = df.select(
        col('datetime').alias('start_time'),
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year'),
        date_format('datetime', 'u').alias('weekday')
    ) 
    time_table = time_table.drop_duplicates(subset=['start_time'])
   
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year', 'month']).parquet(path = os.path.join(output_data, 'time'),mode ='overwrite')


    # read in song data to use for songplays table
    song_df = spark.sql("SELECT DISTINCT song_id, title, artist_id FROM songs")

    # extract columns from joined song and log datasets to create songplays table 
    df = spark.sql("SELECT datetime, userId, level, song, artist, sessionId, location, userAgent")

    df = df.join(song_df, df.song == song_df.title)

    songplays_table = df.select(
        monotonically_increasing_id().alias('songplay_id'),
        col('datetime').alias('start_time'),
        col('userId').alias('user_id'),
        'level',
        'song_id',
        'artist_id',
        col('sessionId').alias('session_id'),
        'location',
        col('userAgent').alias('user_agent')
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year', 'month']).parquet(path = os.path.join(output_data, 'songplays'),mode = 'overwrite')



def main():
    """Main Script run in AWS cluster
    Args:
        None
        
    Returns:
        None
    """
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://hieuleoutputbucket/"

    spark = create_spark_session()

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
