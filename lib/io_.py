import os
import json
import pandas as pd
import pickle
from . import mb

def get_config():
    """Function to load the config file into environment variable"""	
    with open('config.json', 'r') as f:
        ENV_VAR = json.load(f)
    
    return ENV_VAR

ENV = get_config()

def generate_paths(MLHD_ROOT):
    """Function to generate a list of paths to MLHD files"""	
    MLHD_FILES = []
    
    for root, dirs, files in os.walk(MLHD_ROOT):
        for file in files:
            if file.endswith(".gz"):
                MLHD_FILES.append(os.path.join(root, file))

    return MLHD_FILES

def load_path(file_path):
    """Function to load a file and return a dataframe"""

    df = pd.read_csv(
        file_path, sep='\t',
        header=None,
        names = ['timestamp', 'artist_MBID', 'release_MBID', 'recording_MBID'],
        dtype={'artist_MBID': str, 'release_MBID': str, 'recording_MBID': str}
        )
    
    # df.drop(df[df['recording_MBID'].isna()].index, inplace=True)

    return df

def write_frame(df_input, original_path):
    """
    Function to write a dataframe to a csv file
    """
    # Replace MLHD_ROOT with path to new MLHD folder.
    write_path = original_path.replace(ENV["MLHD_ROOT"]+'/', ENV["WRITE_ROOT"])
    write_path = write_path.replace('txt.gz', 'csv.zst')
    
    # print(write_path)
    # Make directory inside WRITE_ROOT if it doesn't exist
    os.makedirs(os.path.dirname(write_path), exist_ok=True)

    df_input.to_csv(
        write_path,
        index=False, 
        sep='\t',
        header=False, 
        compression={'method': 'zstd', 'level': 10},
        )

    return write_path


def log_output(Series_to_log, path, time_taken, timestamp, master_log_dict):
    """
    Function to log output series and path
    """
    # If keys not in dict, generate empty list with dict
    key_set = {'path', 'logs', 'time_taken', 'timestamp'}
    
    for key in key_set:
        if key not in master_log_dict.keys():
            master_log_dict[key]=[] 
    
    master_log_dict['path'].append(path)
    master_log_dict['logs'].append(Series_to_log)
    master_log_dict['time_taken'].append(time_taken)
    master_log_dict['timestamp'].append(timestamp)
        
    return master_log_dict

def write_log(log_dict, log_path = ENV['LOG_WRITE_PATH']):
    
    """Function to update log (json) file"""
    # Make directory inside WRITE_ROOT if it doesn't exist
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    with open(log_path, 'w') as f:
        json.dump(log_dict, f)
    return log_path


def get_track_sets():
    MB_track = mb.get_tracks()
    MB_track_redir = mb.get_track_redirects_old()

    # MB_track_set = set(MB_track.gid)
    # MB_track_redir_set = set(MB_track_redir.gid)
    
    return (set(MB_track.gid), set(MB_track_redir.gid))

def get_track_sets_pickled():
    """
    Function to load pickled track sets
    """
    with open(ENV['PICKLE_PATH'], 'rb') as f:
        MB_track_set, MB_track_redir_set = pickle.load(f)
    
    return (MB_track_set, MB_track_redir_set)

def check_rec(df, MB_track_set, MB_track_redir_set):
    """
    Function to check if a track is in the recording table
    INPUT: dataframe with recording_MBID column. 
    OUTPUT: Returns a series of "recording-MBIDs" that are in recording table.
    """    
    
    ret = [mbid for mbid in df.recording_MBID.tolist() if (mbid in MB_track_set) or (mbid in MB_track_redir_set)]
    if ret == []:
        return None
    else: 
        return ret

