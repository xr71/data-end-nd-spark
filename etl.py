import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as F

# grab credentials if outside of AWS EC2/EMR
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['CREDENTIAL']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['CREDENTIAL']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        This creates the spark session object that will be used by the rest of this script to perform
        the ETL process. 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        This function reads multiple JSON files as specified in the input_data location
        and processes it for loading. It takes a spark session object as a parameter 
        and will write the post-transformed tables as Parquet files as specified in the 
        output_data parameter.
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("stg_song_raw")
    
    songs_table = spark.sql("""
        select distinct song_id
              ,title
              ,artist_id
              ,year
              ,duration
        from stg_song_raw
        where song_id is not null
            and song_id <> ''    
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "dim_songs/")

    # extract columns to create artists table
    artists_table = spark.sql("""
        select distinct artist_id
              ,artist_name as name
              ,artist_location as location
              ,artist_latitude as latitude
              ,artist_longitude as longitude
        from stg_song_raw
        where artist_id is not null
            and artist_id <> ''
    """)
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "dim_artists/")
    

def process_log_data(spark, input_data, output_data):
    """
        This function reads multiple event logs JSON files as specified in the input_data
        location. It takes in a spark session object as a parameter and will process
        the event logs and then write the post-transformed tables as Parquet files 
        to the specified output_data location. 
    """
    # get filepath to log data file
    log_data = input_data + "log_data/2018/11/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df["page"] == "NextSong")

    # extract columns for users table    
    df.createOrReplaceTempView("stg_event_raw")
    
    users_table = spark.sql("""
        select distinct userId as user_id
              ,firstName as first_name
              ,lastName as last_name
              ,gender
              ,level
        from stg_event_raw
        where userId is not null
            and userId <> ''
    """)
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "dim_users/")

    # extract columns to create time table
    time_table = spark.sql("""
        select start_time
              ,hour(start_time) as hour
              ,dayofmonth(start_time) as day
              ,weekofyear(start_time) as week
              ,month(start_time) as month
              ,year(start_time) as year
              ,dayofweek(start_time) as weekday
        from
        (
            select distinct to_timestamp(ts/1000) as start_time
            from stg_event_raw
            where ts is not null
        )
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "dim_time/")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        select monotonically_increasing_id() as songplay_id
            ,to_timestamp(log.ts/1000) as start_time
            ,month(to_timestamp(log.ts/1000)) as month
            ,year(to_timestamp(log.ts/1000)) as year
            ,log.userId as user_id
            ,log.level 
            ,song.song_id
            ,song.artist_id
            ,log.sessionId as session_id
            ,log.location
            ,log.userAgent as user_agent
        from stg_event_raw as log
        join stg_song_raw as song
            on log.artist = song.artist_name
            and log.song = song.title
        where page = 'NextSong'
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write().mode("overwrite").partitionBy("year", "month").parquet(output_data + "fact_songplays/")


def main():
    """
        This is the main entrypoint of the script and will orchestrate all 
        spark session creation and the subsequent two ETL processing functions.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://xuren-data-eng-nd/spark_dl/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
