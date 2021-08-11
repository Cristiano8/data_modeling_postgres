import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Read a song_file as a dataframe using pandas.
    Upload user songs taking the necessary field from the log.
    Upload user artists taking the necessary field from the log.
    
    params:
    - cur cursors object to execute query
    - filepath path of the log to process
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Read a log_file as a dataframe using pandas.
    Upload time table processing the ts from the log and calculating all the necessary fields.
    Upload user table taking the necessary field from the log.
    Uplead songplay table retrieving song_id and artist_id from songs and artists tables using the song_select query joining data
    using song's title, artist's name and song's length from the log.
    
    params:
    - cur cursors object to execute query
    - filepath path of the log to process
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == "NextSong"]["ts"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df, unit='ms')
    
    # insert time data records
    time_data = (pd.Series(df), t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.dayofweek)
    column_labels = ("timestamp", "hour", "day", "week_of_year", "month", "year", "day_of_week")
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    df = pd.read_json(filepath, lines=True)
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    df = pd.read_json(filepath, lines=True)
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
        conn.commit()


def process_data(cur, conn, filepath, func):
    """
    Create a list 'all_files' that contains every log file retrieved from the filepath parameter.
    Iterate over every file and process and ingest every single json using func method.
    params:
    - cur cursors object to execute query
    - conn connection object to commit database changes
    - filepath root path for the logs to process
    - func methos to process the logs with
    
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Connect to the sparkifydb database.
    Create a cursor object to execute the queries.
    Iterate over all the song_data json files using the process_data method and process and ingest every single json using process_song_file method.
    Iterate over all the log_data json files using the process_data method and process and ingest every single json using process_log_file method.
    
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()