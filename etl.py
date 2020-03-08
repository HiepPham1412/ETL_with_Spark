import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')


os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    :param spark: spark connection instance
    :param input_data: str, input data path
    :param output_data: str, output data path
    """
    # get filepath to song data file
    song_data =  os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)
        
    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write\
               .partitionBy(['year', 'artist_id'])\
               .parquet(os.path.join(output_data, 'songs'), 'overwrite')
    
    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 
                             'artist_latitude', 'artist_longitude']).dropDuplicates()
    artists_cols = {'artist_name': 'name', 'artist_location': 'location',
                   'artist_latitude': 'latitude','artist_longitude': 'longitude'}
    artists_table = rename_columns(artists_table, artists_cols)
    
    # write artists table to parquet files
    artists_table.write\
                 .parquet(os.path.join(output_data, 'artists'),  'overwrite')
    
    return None


def rename_columns(df, columns):
    """rename columns in a spark data frame
    
    :param df: spark data frame
    :param columns: a dictionary where the key is the old name and the value is the new name
    
    :return spark data frame with column names changed
    """
    
    if isinstance(columns, dict):
        for old_name, new_name in columns.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df
    else:
        raise ValueError("'columns' should be a dict")

def process_log_data(spark, input_data, output_data):
    """Extract some dimentional tables (users, time) and fact table (songplays) from log data file
    
    :param spark: a spark instance
    :param input_data: str, input data path
    :param output data: str, output data path
    
    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*.json')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter('page== "NextSong"')

    # rename columns 
    cols = {'firstName': 'first_name', 'lastName': 'last_name', 'sessionId': 'session_id', 
            'userAgent': 'user_agent', 'userId': 'user_id'}
    df = rename_columns(df, cols)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(datetime.fromtimestamp(int(x/1000))))
    df = df.withColumn('start_time', get_timestamp(df.ts))
    
    # create a temp view for futher analytics
    df.createOrReplaceTempView('log_table')
    
    
    # extract columns for users table. Update user's level with the latest level status
    latest_user_info_query = """
                            SELECT log_table.user_id, 
                                   log_table.first_name, 
                                   log_table.last_name, 
                                   log_table.gender,
                                   log_table.level
                            FROM 
                                (SELECT user_id, MAX(start_time) AS start_time
                                 FROM log_table
                                 GROUP BY user_id) latest
                                LEFT JOIN log_table
                                ON latest.user_id = log_table.user_id AND latest.start_time = log_table.start_time
                                WHERE latest.start_time IS NOT NULL
                            """
    
    users_table = spark.sql(latest_user_info_query)

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'), 'overwrite')


    # extract columns to create time table
    time_table = df.select('start_time').withColumn('hour', hour(df.start_time))\
                                        .withColumn('day', dayofmonth(df.start_time))\
                                        .withColumn('weekofyear', weekofyear(df.start_time))\
                                        .withColumn('month', month(df.start_time))\
                                        .withColumn('year', year(df.start_time))\
                                        .withColumn('weekday', dayofweek(df.start_time))\
                                        .dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write\
              .partitionBy(['year', 'month'])\
              .parquet(os.path.join(output_data, 'time'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song_data/*/*/*/*.json'))
    song_df = song_df.select(['artist_id', 'artist_name','song_id', 'title']).dropDuplicates()

    # extract columns from joined song and log datasets to create songplays table 
    df = df.join(song_df, [df.song == song_df.title, df.artist == song_df.artist_name], how='left')
    df = df.withColumn('songplay_id', monotonically_increasing_id())\
           .withColumn('year', year(df.start_time))\
           .withColumn('month', month(df.start_time))
    
    songplays_cols = ['songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 
                     'session_id', 'location', 'user_agent', 'year', 'month']
    songplays_table = df.select(songplays_cols)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write\
                   .partitionBy(['year', 'month'])\
                   .parquet(os.path.join(output_data, 'songplays'), 'overwrite')
    
    return None


def main():
    """Main file to execute ETL process using Spark
    
    - json song data is loaded from an input source (S3)
    - songs and artists table are extracted and written to the output data path
    - json log data is loaded from input source (S3)
    - users, time and songplays table are extracted and writetten to the output data path  
    
    """
    spark = create_spark_session()
    input_data = config['DATA']['INPUT_DATA']
    output_data = config['DATA']['OUTPUT_DATA']
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()