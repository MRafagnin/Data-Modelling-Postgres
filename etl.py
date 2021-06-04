import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Process song files by:
        - Reading and turning `.json` files into dataframe;
        - Retrives data from dataframe and inserts into database tables(`song_data` and `artist_data`);
    
    Arguments:
        cur: the cursor object. 
        filepath: song data file path. 

    Returns:
        None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Process log files by:
        - Reading and turning `.json` files into dataframe;
        - Filters data on dataframe(*page column*);
        - Converts data in dataframe(*timestamp to datetime*);
        - Creates new dataframe(*time_df*) and populates with 'df' dataframe data via for loop;
        - Inserts appropriate data into 'time', 'users' and 'songplays' tables in database;
    
    Arguments:
        cur: the cursor object.
        filepath: log data file path.

    Returns:
        None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime and remove microseconds
    df['ts'] = pd.to_datetime(df['ts'], unit='ms').dt.ceil(freq='s')
    
    # insert time data records
    time_df = pd.DataFrame(columns=['start_time','hour', 'day', 'week', 'month', 'year', 'weekday'])

    series_list = [df['ts'].dt.time,
                   df['ts'].dt.hour, 
                   df['ts'].dt.day, 
                   df['ts'].dt.week, 
                   df['ts'].dt.month, 
                   df['ts'].dt.year, 
                   df['ts'].dt.day_name()]

    i = 0

    for serie in series_list:
        col = time_df.columns[i]
        time_df[col] = [var for var in serie]
        i += 1

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Process and gather data by:
        - Gathers all files in directory that match extension '.json';
        - Retrieves, create list of files and print total numbers;
        - Iterates over list of files and print message to confirm processing;
    
    Arguments:
        cur: the cursor object.
        conn: the connect object
        filepath: file path.
        func: the above functions object

    Returns:
        None
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
    - Creates and define 'conn' and 'cur' objects;
    - Defines 'filepath's';
    - Process all 'song' and 'log' data; 
    - Closes database the connection;
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()