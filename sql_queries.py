import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS factSongplay"
user_table_drop = "DROP TABLE IF EXISTS dimUser"
song_table_drop = "DROP TABLE IF EXISTS dimSong"
artist_table_drop = "DROP TABLE IF EXISTS dimArtist"
time_table_drop = "DROP TABLE IF EXISTS dimTime"

# CREATE TABLES

staging_events_table_create = (""" CREATE TABLE IF NOT EXISTS staging_events (
                                        artist        VARCHAR,
                                        auth          VARCHAR,
                                        firstName     VARCHAR,
                                        gender        CHAR(1),
                                        itemInSession INTEGER,
                                        lastName      VARCHAR,
                                        length        DECIMAL,
                                        level         CHAR(4),
                                        location      VARCHAR,
                                        method        VARCHAR,
                                        page          VARCHAR,
                                        registration  DECIMAL,
                                        sessionId     INTEGER,
                                        song          VARCHAR,
                                        status        INTEGER,
                                        ts            BIGINT,
                                        userAgent     VARCHAR,
                                        userId        INTEGER
                                        );
""")

staging_songs_table_create = ("""  CREATE TABLE IF NOT EXISTS staging_songs (
                                        num_songs            INTEGER,
                                        artist_id            CHAR(18),
                                        artist_latitude      DECIMAL,
                                        artist_longitude     DECIMAL,
                                        artist_location      VARCHAR,
                                        artist_name          VARCHAR,
                                        song_id              CHAR(18),
                                        title                VARCHAR,
                                        duration             DECIMAL,
                                        year                 INTEGER
                                        );
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS factSongplay (
                                   songplay_id INTEGER GENERATED BY DEFAULT AS IDENTITY(0,1) PRIMARY KEY,
                                   start_time  TIMESTAMP NOT NULL sortkey,
                                   user_id     INTEGER NOT NULL,
                                   level       CHAR(4) NOT NULL,
                                   song_id     CHAR(18) NOT NULL distkey,
                                   artist_id   CHAR(18) NOT NULL,
                                   session_id  INTEGER NOT NULL,
                                   location    VARCHAR,
                                   user_agent  VARCHAR
                                   );
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS dimUser (
                                   user_id     INTEGER NOT NULL PRIMARY KEY,
                                   first_name  CHAR(25) NOT NULL,
                                   last_name   CHAR(25) NOT NULL,
                                   gender      CHAR(1),
                                   level       CHAR(4) NOT NULL
                                   );
""")

song_table_create = ("""  CREATE TABLE IF NOT EXISTS dimSong (
                                   song_id     CHAR(18) NOT NULL PRIMARY KEY distkey,
                                   title       VARCHAR NOT NULL,
                                   artist_id   CHAR(18) NOT NULL,
                                   year        INTEGER,
                                   duration    DECIMAL
                                   );
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS dimArtist (
                                   artist_id   CHAR(18) NOT NULL PRIMARY KEY,
                                   name        VARCHAR NOT NULL,
                                   location    VARCHAR,
                                   latitude    DECIMAL,
                                   longitude   DECIMAL
                                   );
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS dimTime (
                                   start_time  TIMESTAMP NOT NULL PRIMARY KEY sortkey,
                                   hour        INTEGER NOT NULL,
                                   day         INTEGER NOT NULL,
                                   week        INTEGER NOT NULL,
                                   month       INTEGER NOT NULL,
                                   year        INTEGER NOT NULL,
                                   weekday     INTEGER NOT NULL
                                   );
""")

# STAGING TABLES

staging_events_copy = (""" COPY staging_events
                           FROM {}
                           IAM_ROLE {}
                           FORMAT JSON AS {}
                           REGION 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = (""" COPY staging_songs
                          FROM {}
                          IAM_ROLE {}
                          FORMAT JSON AS 'auto'
                          REGION 'us-west-2'
                          COMPUPDATE OFF;
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO factSongplay (start_time, user_id, level,
                             song_id, artist_id, session_id, location, user_agent)
                             SELECT TIMESTAMP 'epoch' + staging_events.ts/1000 * interval '1 second',
                                    staging_events.userId,
                                    staging_events.level,
                                    staging_songs.song_id,
                                    staging_songs.artist_id,
                                    staging_events.sessionId,
                                    staging_events.location,
                                    staging_events.userAgent
                              FROM staging_events
                              JOIN staging_songs ON staging_events.song=staging_songs.title
                              AND staging_events.artist=staging_songs.artist_name
                              WHERE staging_events.page = 'NextSong';
""")

user_table_insert = (""" INSERT INTO dimUser (user_id, first_name, last_name,
                                              gender, level)
                         SELECT DISTINCT userID,
                                firstName,
                                lastName,
                                gender,
                                level
                         FROM staging_events
                         WHERE page='NextSong';
""")

song_table_insert = (""" INSERT INTO dimSong (song_id, title, artist_id, year,
                                              duration)
                         SELECT DISTINCT song_id,
                                title,
                                artist_id,
                                year,
                                duration
                         FROM staging_songs
                         WHERE song_id IS NOT NULL;
""")

artist_table_insert = (""" INSERT INTO dimArtist (artist_id, name, location,
                                                  latitude, longitude)
                           SELECT DISTINCT artist_id,
                                  artist_name,
                                  artist_location,
                                  artist_latitude,
                                  artist_longitude
                           FROM staging_songs
                           WHERE artist_id IS NOT NULL;
""")

time_table_insert = (""" INSERT INTO dimTime (start_time, hour, day, week,
                                               month, year, weekday)
                         WITH tsConversion AS (
                              SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS tStamp
                              FROM staging_events
                              WHERE page='NextSong'
                         )
                         SELECT tsConversion.tStamp,
                                EXTRACT(hour FROM tsConversion.tStamp),
                                EXTRACT(day FROM tsConversion.tStamp),
                                EXTRACT(week FROM tsConversion.tStamp),
                                EXTRACT(month FROM tsConversion.tStamp),
                                EXTRACT(year FROM tsConversion.tStamp),
                                EXTRACT(dow FROM tsConversion.tStamp)
                         FROM tsConversion;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = {"staging_events": staging_events_copy, "staging_songs": staging_songs_copy}
insert_table_queries = {"factSongplay": songplay_table_insert, "dimUser": user_table_insert, "dimSong": song_table_insert, "dimArtist": artist_table_insert, "dimTime": time_table_insert}
