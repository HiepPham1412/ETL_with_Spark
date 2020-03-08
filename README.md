# ETL with Spark

## Objective

Create an ETL process using Data Lake architechture with data storage on S3, Spark is run on a ERM cluster.
Spark loads raw data from S3, process the data and write back to a S3 bucket.


## Raw data sets

**Song Dataset**

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

Song data location: s3://udacity-dend/song_data
     ```
     song_data/A/B/C/TRABCEI128F424C983.json
     song_data/A/A/B/TRAABJL12903CDCF1A.json
     ```
     
**Log Dataset**
Log data set contains all user activities 

Log data location: s3://udacity-dend/log_data

Example:
    ```
    log_data/2018/11/2018-11-12-events.json
    log_data/2018/11/2018-11-13-events.json
    ```


## Data Schema

**Fact Table**

songplays - records in log data associated with song plays i.e. records with page NextSong
        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**

users - users in the app
        user_id, first_name, last_name, gender, level
songs - songs in music database
        song_id, title, artist_id, year, duration
        
artists - artists in music database
        artist_id, name, location, lattitude, longitude
        
time - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday

        
## How to run

- Update your AWS key and secret, input and output data path in dl.cfg file
- in your shell terminal execulet etl.py script







