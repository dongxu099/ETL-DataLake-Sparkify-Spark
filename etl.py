import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('CREDENTIALS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('CREDENTIALS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Read song.json; transform it and save as parquet formate in the provided location
    :param spark: Spark session
    :param input_data: Input data location
    :param output_data: Output data location
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json' ## caution to the subdirectory level and the file type
    #song_data = input_data + 'song_data/A/A/A/*.json' ## test on a small set of data

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    song_cols = ['song_id', 
                 'title', 
                 'artist_id', 
                 'year', 
                 'duration']
    songs_table = df.select(*song_cols) \
                    .where(col('song_id').isNotNull()).dropDuplicates() ## exclude duplicates and NULLs here
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id') \
               .parquet(output_data + 'songs', mode='overwrite')
    print("--- songs.parquet completed ---") ## For local debugging purpose only
    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", 
                                  "artist_name as name", 
                                  "artist_location as location", 
                                  "artist_latitude as latitude", 
                                  "artist_longitude as longitude") \
                      .where(col('artist_id').isNotNull()).dropDuplicates() ## exclude duplicates and NULLs here        
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists", mode="overwrite")
    print("--- artists.parquet completed ---")
    print("*** process_song_data completed ***\n\n")

def process_log_data(spark, input_data, output_data):
    """
    Read log from json files and read procceded song data from the parquet file generated from 
    the previous method; transform them and save as parquet formate in the provided location
    :param spark: Spark session
    :param input_data: Input data location
    :param output_data: Output data location
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'
    #log_data = input_data + 'log_data/2018/11/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select(col("userId").alias("user_id"),
                            col("firstName").alias("first_name"),
                            col("lastName").alias("last_name"),
                            "gender", 
                            "level") \
                    .where(col("userId").isNotNull()).dropDuplicates() ## exclude duplicates and NULLs here
        
    # write users table to parquet files
    users_table.write.parquet(output_data + "users", mode="overwrite")
    print("--- users.parquet completed ---")
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000.0)
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000.0))
    df = df.withColumn("datetime", get_datetime(col("ts")))
    
    # extract columns to create time table
    time_table = df.select(col("timestamp").alias('start_time'), 
                           hour("datetime").alias('hour'),
                           dayofmonth("datetime").alias('day'),
                           weekofyear("datetime").alias('week'),
                           month("datetime").alias('month'),
                           year("datetime").alias('year'),
                           date_format('datetime', 'F').alias('weekday')) \
                    .where(col("timestamp").isNotNull()).dropDuplicates() ## exclude duplicates and NULLs here    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
              .parquet(output_data + "time", mode="overwrite")
    print("--- time.parquet completed ---")
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) 
                                     & (df.length == song_df.duration), 'left') \
                        .withColumn("songplay_id", monotonically_increasing_id()) \
                        .select(df.datetime.alias('start_time'), 
                                df.userId.alias('user_id'), 
                                'level', 
                                'song_id', 
                                'artist_id', 
                                df.sessionId.alias('session_id'), 
                                'location', 
                                df.userAgent.alias('user_agent'),
                                year('datetime').alias('year'),
                                month('datetime').alias('month'))
        
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month') \
                   .parquet(output_data + "songplays", mode="overwrite")
    print("--- songplays.parquet completed ---")
    print("*** process_log_data completed ***\n\nEND")

def main():
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    output_data = 's3a://sparkifydl/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
