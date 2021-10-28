import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSON_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = """
  CREATE TABLE staging_events (
    artist VARCHAR,
    auth VARCHAR,
    first_name VARCHAR,
    gender VARCHAR(1),
    item_in_session INTEGER,
    last_name VARCHAR,
    length DECIMAL,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    session_id INTEGER,
    song VARCHAR,
    status INTEGER,
    ts TIMESTAMP,
    user_agent VARCHAR,
    user_id INTEGER
  )
"""

staging_songs_table_create = """
  CREATE TABLE staging_songs (
    artist_id VARCHAR NOT NULL PRIMARY KEY,
    artist_latitude DECIMAL,
    artist_location VARCHAR,
    artist_longitude DECIMAL,
    artist_name VARCHAR,
    duration DECIMAL,
    num_songs INTEGER,
    song_id VARCHAR,
    title VARCHAR,
    year INTEGER
  )
"""

songplay_table_create = """
  CREATE TABLE songplays (
    songplay_id INTEGER IDENTITY(0,1) NOT NULL PRIMARY KEY,
    start_time TIMESTAMP,
    user_id INTEGER DISTKEY NOT NULL,
    level VARCHAR,
    song_id VARCHAR(18) NOT NULL,
    artist_id VARCHAR(18) NOT NULL,
    session_id INTEGER NOT NULL,
    location VARCHAR,
    user_agent VARCHAR
  )
"""

user_table_create = ("""
  CREATE TABLE users (
    user_id INTEGER NOT NULL PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR(1),
    level VARCHAR
  )
""")

song_table_create = """
  CREATE TABLE songs (
    song_id VARCHAR(18) NOT NULL PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR(18) NOT NULL,
    year INTEGER,
    duration DECIMAL
  )
"""

artist_table_create = """
  CREATE TABLE artists (
    artist_id VARCHAR(18) NOT NULL PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude DECIMAL,
    longitude DECIMAL
  )
"""

time_table_create = """
  CREATE TABLE time (
    start_time TIMESTAMP NOT NULL PRIMARY KEY,
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL
  )
"""

# STAGING TABLES
staging_events_copy = f"""
  COPY staging_events FROM {LOG_DATA}
  CREDENTIALS 'aws_iam_role={ARN}'
  FORMAT AS JSON {LOG_JSON_PATH}
  TIMEFORMAT as 'epochmillisecs'
  TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
  COMPUPDATE OFF
  REGION 'us-west-2';
"""

staging_songs_copy = f"""
  COPY staging_songs FROM {SONG_DATA}
  CREDENTIALS 'aws_iam_role={ARN}'
  JSON 'auto'
  TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
  COMPUPDATE OFF
  REGION 'us-west-2';
"""


songplay_table_insert = """
  INSERT INTO songplays
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
  SELECT
    DISTINCT ts,
    e.user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.session_id,
    e.location,
    e.user_agent
  FROM staging_events AS e
  JOIN staging_songs AS s ON e.artist = s.artist_name
  WHERE
    e.page = 'NextSong'
"""

user_table_insert = """
  INSERT INTO users
    (user_id, first_name, last_name, gender, level)
  SELECT
    DISTINCT user_id,
    first_name,
    last_name,
    gender,
    level
  FROM staging_events
  WHERE
    page = 'NextSong'
    AND user_id IS NOT NULL
"""

song_table_insert = """
  INSERT INTO songs
    (song_id, title, artist_id, year, duration)
  SELECT
    DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
  FROM staging_songs
  WHERE
    song_id IS NOT NULL
"""

artist_table_insert = """
  INSERT INTO artists
    (artist_id, name, location, latitude, longitude)
  SELECT
    DISTINCT artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
  FROM staging_songs
  WHERE
    artist_id IS NOT NULL
"""

time_table_insert = """
  INSERT INTO time
    (start_time, hour, day, week, month, year, weekday)
  SELECT
    DISTINCT ts,
    EXTRACT(hour FROM ts),
    EXTRACT(day FROM ts),
    EXTRACT(week FROM ts),
    EXTRACT(month FROM ts),
    EXTRACT(year FROM ts),
    EXTRACT(weekday FROM ts)
  FROM staging_events
  WHERE
    page = 'NextSong'
    AND ts IS NOT NULL
"""


create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
