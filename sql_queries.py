import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS tbl_Staging_Events"
staging_songs_table_drop = "DROP TABLE IF EXISTS tbl_Staging_Songs"
songplay_table_drop = "DROP TABLE IF EXISTS tbl_Songplay"
user_table_drop = "DROP TABLE IF EXISTS tbl_User"
song_table_drop = "DROP TABLE IF EXISTS tbl_Song"
artist_table_drop = "DROP TABLE IF EXISTS tbl_Artist"
time_table_drop = "DROP TABLE IF EXISTS tbl_Time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS tbl_Staging_Events (
    artist text, 
    auth text, 
    first_name text, 
    gender text, 
    item_in_session int,
    last_name text, 
    length float, 
    level text, 
    location text, 
    method text,
    page text, 
    registration text, 
    session_id int, 
    song text, 
    status int,
    start_time BIGINT, 
    user_agent text, 
    user_id int)
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS tbl_Staging_Songs (
    num_songs int,
    artist_id text, 
    latitude float, 
    longitude float, 
    location text, 
    name text, 
    song_id text, 
    title text, 
    duration float, 
    year int)
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS tbl_Songplay (
    songplay_id int IDENTITY(0,1) PRIMARY KEY, 
    start_time TIMESTAMP NOT NULL, 
    user_id int, 
    level text, 
    song_id text, 
    artist_id text, 
    session_id text, 
    location text, 
    user_agent text)    
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS tbl_User (
    user_id int NOT NULL PRIMARY KEY, 
    first_name text, 
    last_name text, 
    gender text, 
    level text)
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS tbl_Song (
    song_id text PRIMARY KEY, 
    title text, 
    artist_id text, 
    year int, 
    duration float)
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS tbl_Artist (
    artist_id text PRIMARY KEY,
    name text, 
    location text, 
    latitude float, 
    longitude float)
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS tbl_Time (
    start_time TIMESTAMP PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int)
""")

# STAGING TABLES

staging_events_copy = (f"""
    COPY tbl_Staging_Events 
    FROM {config.get('S3','LOG_DATA')}
    CREDENTIALS 'aws_iam_role={config.get('IAM_ROLE','ARN')}' 
    REGION 'us-west-2'
    TIMEFORMAT 'epochmillisecs'
    JSON {config.get('S3','LOG_JSONPATH')}
""") 
#     iam_role '{config.get('IAM_ROLE','ARN')}'

staging_songs_copy = (f"""
    COPY tbl_Staging_Songs 
    FROM {config.get('S3','SONG_DATA')}
    CREDENTIALS 'aws_iam_role={config.get('IAM_ROLE','ARN')}'
    REGION 'us-west-2'
    JSON 'auto' 
""")


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO tbl_Songplay 
    (session_id, location, user_agent, user_id, level, start_time, artist_id, song_id) 
        
    SELECT DISTINCT s_e.session_id, s_e.location, s_e.user_agent, s_e.user_id, s_e.level,
    TIMESTAMP 'epoch' + (s_e.start_time/1000)*INTERVAL '1 second' as start_time,  
    s_s.artist_id, s_s.song_id
    FROM tbl_Staging_Events as s_e
    LEFT JOIN tbl_Staging_Songs as s_s ON (s_e.song=s_s.title AND s_e.artist=s_s.name)
    WHERE s_e.page ='NextSong';
""")

user_table_insert = ("""
    INSERT INTO tbl_User 
    (user_id, first_name, last_name, gender, level) 
    
    SELECT DISTINCT user_id, first_name, last_name, gender, level
    FROM tbl_Staging_Events 
    WHERE page ='NextSong';
""")

song_table_insert = ("""
    INSERT INTO tbl_Song
    (song_id, title, artist_id, year, duration) 
    
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM tbl_Staging_Songs;
""")

artist_table_insert = ("""
    INSERT INTO tbl_Artist
    (artist_id, name, location, latitude, longitude) 
    
    SELECT DISTINCT artist_id, name, location, latitude, longitude
    FROM tbl_Staging_Songs;
""")

time_table_insert = ("""
    INSERT INTO tbl_Time 
    (start_time, hour, day, week, month, year, weekday)
    
    SELECT DISTINCT s_p.start_time,
    EXTRACT (HOUR FROM s_p.start_time), EXTRACT (DAY FROM s_p.start_time),
    EXTRACT (WEEK FROM s_p.start_time), EXTRACT (MONTH FROM s_p.start_time),
    EXTRACT (YEAR FROM s_p.start_time), EXTRACT (WEEKDAY FROM s_p.start_time) 
    FROM tbl_Songplay as s_p;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
