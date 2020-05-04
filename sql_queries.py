import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(artist varchar, auth varchar, firstName varchar, gender varchar, iteminSession numeric, lastName varchar, length numeric, level varchar, location varchar, method varchar, page varchar, registration numeric, sessionId numeric, song text, status numeric, ts numeric, userAgent text, userId numeric);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(num_songs numeric, artist_id varchar,artist_latitude numeric, artist_longitude numeric, artist_location varchar, artist_name varchar, song_id varchar, title varchar, duration numeric, year numeric);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(songplay_id int identity(0,1) PRIMARY KEY, start_time TIMESTAMP NOT NULL, user_id int NOT NULL, level varchar , song_id varchar , artist_id varchar, session_id int NOT NULL, location varchar, user_agent varchar);

""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id int UNIQUE NOT NULL PRIMARY KEY, first_name varchar, last_name varchar, gender varchar, level varchar);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id varchar UNIQUE NOT NULL PRIMARY KEY, title varchar, artist_id varchar NOT NULL, year int, duration numeric);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id varchar UNIQUE NOT NULL PRIMARY KEY, name varchar UNIQUE, location varchar, latitude numeric, longitude numeric);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time TIMESTAMP UNIQUE PRIMARY KEY, t_hour int, t_day int, t_week int, t_month int, t_year int, t_weekday varchar);""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events  from {} credentials 'aws_iam_role={}'compupdate off region 'us-west-2' JSON {} ;
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])
print(staging_events_copy)
staging_songs_copy = ("""copy staging_songs from {}
credentials 'aws_iam_role={}'compupdate off region 'us-west-2'JSON 'auto';
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])
#print(staging_events_copy)
# FINAL TABLES

user_table_insert = ("""INSERT INTO users(user_id,first_name,last_name,gender,level)(SELECT DISTINCT userId AS user_id,firstName AS first_name,lastName AS last_name,gender,level FROM staging_events WHERE user_id IS NOT NULL AND page = 'NextSong')""")

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration)SELECT DISTINCT song_id AS song_id, title AS title, artist_id AS artist_id, year AS year, duration AS duration FROM staging_songs""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude)SELECT DISTINCT artist_id AS artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude FROM staging_songs""")

time_table_insert = ("""INSERT INTO time(start_time,t_hour,t_day,t_week, t_month, t_year, t_weekday)Select distinct ts AS start_time,EXTRACT(HOUR FROM ts) As t_hour,EXTRACT(DAY FROM ts) As t_day,EXTRACT(WEEK FROM ts) As t_week,EXTRACT(MONTH FROM ts) As t_month,EXTRACT(YEAR FROM ts) As t_year,EXTRACT(DOW FROM ts) As t_weekday FROM (SELECT (TIMESTAMP 'epoch' + ts/1000 * interval '1 second') as ts FROM staging_events) """)

songplay_table_insert = ("""insert into songplays(start_time,user_id, level, song_id, artist_id, session_id, location, user_agent) SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1second' as start_time,se.userId,se.level,ss.song_id,ss.artist_id,se.sessionId,se.location,se.userAgent FROM staging_events se, staging_songs ss
WHERE se.page = 'NextSong' AND se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration AND se.userId is not null""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
