import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_IAM_ARN = config.get("DWH", "DWH_IAM_ARN")

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
    artist text,
    auth text,
    firstName text,
    gender text,
    itemInSession text,
    lastName text,
    length float,
    level text,
    location text,
    method text,
    page text,
    registration text,
    sessionId integer,
    song text,
    status text,
    ts timestamp,
    userAgent text,
    userID int
)
;""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs text,
    artist_id text,
    latitude numeric,
    longitude numeric,
    location text,
    name text,
    song_id text,
    title text,
    duration float,
    year integer
)
;""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int IDENTITY(0,1), 
    start_time time, 
    user_id int distkey,
    level varchar,
    song_id varchar,
    artist_id text sortkey,
    session_id int,
    location varchar,
    user_agent varchar,
    PRIMARY KEY(songplay_id)
    )
;""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
    user_id int sortkey distkey,
    first_name varchar,
    last_name varchar,
    gender varchar, 
    level varchar
    )
;""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    song_id varchar,
    title varchar,
    artist_id text,
    year int,
    duration float
    )
    diststyle all
;""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id text,
    name varchar,
    location varchar,
    latitude numeric,
    longitude numeric
    )
    diststyle all
;""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time timestamp distkey,
    hour int,
    day int,
    week int, 
    month int, 
    year int,
    weekday int
    )
;""")

# STAGING TABLES
staging_events_copy = ("""copy staging_events from 's3://udacity-dend/log_data'
                            credentials 'aws_iam_role={}'
                             region 'us-west-2'
                             timeformat as 'epochmillisecs'
                             truncatecolumns blanksasnull emptyasnull
                             json 's3://udacity-dend/log_json_path.json'  
;""").format(DWH_IAM_ARN)

staging_songs_copy = ("""copy staging_songs from 's3://udacity-dend/song_data'
                            credentials 'aws_iam_role={}'
                            format as json 'auto' 
                            region 'us-west-2'
;""").format(DWH_IAM_ARN)


# INSERT STATEMENTS

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
SELECT 
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
FROM staging_songs
;""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
SELECT
    artist_id,
    name,
    location,
    latitude,
    longitude
FROM staging_songs
;""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
SELECT 
    ts as start_time,
    extract(hour from ts) as hour,
    extract(d from ts) as day, --day of month from 1 to 30/31
    extract(w from ts) as week,
    extract(mon from ts) as month, 
    extract(yr from ts) as year,
    extract(weekday from ts) as weekday -- day of week from 0 to 6
FROM staging_events
WHERE page = 'NextSong' 
;""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
SELECT DISTINCT 
    userID as user_id,
    firstName as first_name,
    lastName as last_name,
    gender,
    level
FROM staging_events
WHERE page = 'NextSong'
GROUP BY userID, firstName, lastName, gender, level, ts
ORDER BY user_id, ts DESC
;""")

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT   events.ts,
         events.userID,
         events.level,
         songs.song_id,
         songs.artist_id,
         events.sessionId,
         events.location,
         events.userAgent
FROM staging_events AS events
JOIN staging_songs AS songs
     ON (events.artist = songs.name)
     AND (events.song = songs.title)
     AND (events.length = songs.duration)
     WHERE events.page = 'NextSong'
;""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [song_table_insert, artist_table_insert, time_table_insert, user_table_insert,songplay_table_insert]

# insert_table_queries = [song_table_insert, artist_table_insert, time_table_insert, user_table_insert, songplay_table_insert]
# copy_table_queries = [staging_events_copy, staging_songs_copy]