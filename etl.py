import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Process a single song file and insert data into the songs and artists tables.

    Parameters:
    cur (psycopg2.cursor): The database cursor.
    filepath (str): The file path of the song file to be processed.
    """
    # Open the song file
    df = pd.read_json(filepath, lines=True)

    # Insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    cur.execute(song_table_insert, song_data)
    
    # Insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Process a single log file and insert data into the time, users, and songplays tables.

    Parameters:
    cur (psycopg2.cursor): The database cursor.
    filepath (str): The file path of the log file to be processed.
    """
    # Open the log file
    df = pd.read_json(filepath, lines=True)

    # Filter records by NextSong action
    df = df[df['page'] == 'NextSong']

    # Convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    # Insert time data records
    time_data = []
    for i, row in df.iterrows():
        time_data.append([row['ts'], row['ts'].hour, row['ts'].day, row['ts'].weekofyear, row['ts'].month, row['ts'].year, row['ts'].weekday()])
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(time_data, columns=column_labels)
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # Insert user records
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # Insert songplay records
    for index, row in df.iterrows():
        # Get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # Insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Process all files in a given directory using a specified function.

    Parameters:
    cur (psycopg2.cursor): The database cursor.
    conn (psycopg2.connection): The database connection.
    filepath (str): The directory path containing the files to be processed.
    func (function): The function to be applied to each file.
    """
    # Get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # Get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # Iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        print('{}/{} files processed.'.format(i, num_files))
        func(cur, datafile)
        conn.commit()


def main():
    """
    Main function to execute ETL process.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == '__main__':
    main()
