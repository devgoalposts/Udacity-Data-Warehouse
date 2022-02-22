import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN               = config.get("IAM_ROLE", "ARN")
# DROP TABLES

staging_events_table_drop = "DROP TABLE  IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE  IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE  IF EXISTS songplay;"
user_table_drop = "DROP TABLE  IF EXISTS users;"
song_table_drop = "DROP TABLE  IF EXISTS songs;"
artist_table_drop = "DROP TABLE  IF EXISTS artists;"
time_table_drop = "DROP TABLE  IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession int,
        lastName varchar,
        length float8,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration float8,
        sessionId int,
        song varchar,
        status int,
        ts varchar, 
        userAgent varchar,
        userId varchar
);
""")

user_table_insert_try = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT(userId) as user_id, firstName, lastName, gender, level
    FROM staging_events
    WHERE user_id != ' '
    ORDER BY ts desc;
""")


#         UNIQUE (userId, sessionId, itemInSession)

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
        song_id varchar,
        num_song int,
        title varchar,
        artist_name varchar,
        artist_latitude float8,
        year int,
        duration float8,
        artist_id varchar,
        artist_longitude float8,
        artist_location varchar,
        UNIQUE (song_id)
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
        songplay_id  int identity(1,1),
        start_time timestamp NOT NULL,
        user_id int NOT NULL,
        level varchar, 
        song_id varchar, 
        artist_id varchar, 
        session_id int NOT NULL, 
        location varchar,
        user_agent varchar NOT NULL,
        UNIQUE (songplay_id)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
        user_id varchar PRIMARY KEY,
        first_name varchar NOT NULL,
        last_name varchar NOT NULL,
        gender varchar NOT NULL,
        level varchar NOT NULL,
        UNIQUE (user_id)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
        song_id varchar PRIMARY KEY,
        title varchar NOT NULL,
        artist_id varchar,
        year varchar,
        duration float8,
        UNIQUE (song_id)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar PRIMARY KEY,
        name varchar NOT NULL,
        location varchar,
        latitude float8,
        longitude float8,
        UNIQUE (artist_id)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
        start_time timestamp PRIMARY KEY,
        hour int,
        day int,
        month int,
        year int,
        weekday boolean
);
""")

# STAGING TABLES

staging_events_copy = """
COPY staging_events FROM 's3://udacity-dend/log_data' 
iam_role '{}'
format as json 'auto ignorecase';""".format(ARN)


staging_songs_copy = ("""
COPY staging_songs FROM 's3://udacity-dend/song_data' 
iam_role '{}'
format as json 'auto';""").format(ARN)

# FINAL TABLES

songplay_table_insert_try = ("""
INSERT INTO songplays ( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        date_add('ms',CAST(ts as BIGINT),'1970-01-01'),
        CAST(userId as INT), 
        level,
        (SELECT song_id FROM songs     WHERE songs.title   = 'A Heart Without A Home' LIMIT 1),
        (SELECT artist_id FROM artists WHERE artists.name  = 'The Hellacopters' LIMIT 1),
        sessionId, 
        staging_events.location, 
        userAgent
            FROM staging_events
            WHERE userId != ' ';
""")

songplay_table_insert = ("""
INSERT INTO songplays ( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch'+(st.ts/1000)*INTERVAL '1 second',
    CAST(st.userId as INT),
    st.level,
    s.song_id,
    s.artist_id,
    st.sessionId,
    st.location,
    st.userAgent
FROM staging_events st 
INNER JOIN staging_songs s ON s.title=st.song AND st.artist = s.artist_name
WHERE st.page = 'NextSong';
""")

                      
user_table_insert_try = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT(userId) as user_id, firstName, lastName, gender, level
    FROM staging_events
    WHERE userId != ' '
    ORDER BY ts desc;
""")


user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT(userId) as user_id, firstName, lastName, gender, level
    FROM staging_events
    WHERE page ='NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT(song_id), title, s.artist_id, year, duration 
    FROM staging_songs s
    JOIN artists a     ON (s.artist_name=a.name);
""")

artist_table_insert_try = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT(song_id), title, a.artist_id, year, duration 
    FROM staging_songs
""")


artist_table_insert_try = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT(artist_id), artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs;
""")

other_artist_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT(artist_id), artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT(artist_id), artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, month, year, weekday)
SELECT date_add('ms',CAST(ts as BIGINT),'1970-01-01') as staged_date,
    EXTRACT(hour FROM staged_date),
    EXTRACT(day FROM staged_date),
    EXTRACT(month FROM staged_date),
    EXTRACT(year FROM staged_date),
    CASE WHEN EXTRACT(WEEKDAY FROM staged_date) IN (6,7) THEN true ELSE false END
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, artist_table_insert, time_table_insert, song_table_insert]
drop_not_staging_tables = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]