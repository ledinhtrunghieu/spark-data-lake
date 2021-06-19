# Project: Data Lake with Spark - Udacity Data Engineer Nanodegree

## 1. Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## 2. Project structure 
```
postgres-data-modeling
│   README.md             # Project description
|   dl.cfg                # Configuration file
|   requirements.txt      # Python dependencies
│   etl.py                # ETL script
|   setup.py              # Setup buckets, clusters and run ETL.
│   
```


## 3. Datalake schema

### Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong
    * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


### Dimension Tables
1. users - users in the app
    * user_id, first_name, last_name, gender, level
2. songs - songs in music database
    * song_id, title, artist_id, year, duration
3. artists - artists in music database
    * artist_id, name, location, lattitude, longitude
4. time - timestamps of records in songplays broken down into specific units
    *start_time, hour, day, week, month, year, weekday

## 4. Instruction to run

### Setup everything and run etl.py in cluster

The script creates S3 buckets and EMR cluster, send etl.py and run it.
```
python setup.py
```

#### Remember to Shut down EMR cluster
Remember to shut down EMR cluster and check every used regions. I lost almost 10 $ because of this.