# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

# songplays
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_create = ("""
    create table if not exists songplays(
        songplay_id serial PRIMARY KEY,
        start_time bigint NOT NULL,
        user_id varchar,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id int NOT NULL,
        location varchar,
        user_agent varchar
    )
""")

# users
# user_id, first_name, last_name, gender, level
user_table_create = ("""
    create table if not exists users(
        user_id varchar PRIMARY KEY,
        first_name varchar,
        last_name varchar,
        gender varchar(1),
        level varchar
    )
""")

# songs
# song_id, title, artist_id, year, duration
song_table_create = ("""
    create table if not exists songs(
        song_id varchar PRIMARY KEY,
        title varchar NOT NULL,
        artist_id varchar,
        year int,
        duration float
    )
""")

# artists
# artist_id, name, location, latitude, longitude
artist_table_create = ("""
    create table if not exists artists(
        artist_id varchar PRIMARY KEY,
        name varchar NOT NULL,
        location varchar,
        latitude decimal,
        longitude decimal
    )
""")

# time
# start_time, hour, day, week, month, year, weekday
time_table_create = ("""
    create table if not exists time(
        start_time bigint PRIMARY KEY,
        hour int,
        day int,
        week int,    
        month int,    
        year int,
        weekday int
    )
""")

# INSERT RECORDS

songplay_table_insert = ("""
    insert into songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
    insert into users(user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) on conflict (user_id) do nothing;
""")

song_table_insert = ("""
    insert into songs(song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s) on conflict (song_id) do nothing;
""")

artist_table_insert = ("""
    insert into artists(artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s) on conflict (artist_id) do nothing;
""")


time_table_insert = ("""
    insert into time(start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s) on conflict (start_time) do nothing;
""")

# FIND SONGS
song_select = ("""
    select s.song_id, s.artist_id
    from songs s join artists a on s.artist_id = a.artist_id
    where title = %s and name = %s and duration = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]