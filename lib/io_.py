import os
import json
import pandas as pd
import pickle
import lib.mb as mb
from pyarrow import _csv as csv
from pyarrow import dataset as dataset


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


def load_path_pandas(path):
    """
    Function to load a file and return a dataframe
    """
    df = pd.read_csv(
        path, 
        sep='\t', 
        header=None, 
        names=['timestamp', 'artist_MBID', 'release_MBID', 'recording_MBID']
        )

    return df

arrow_csv_read_options  = csv.ReadOptions(
    column_names=['timestamp', 'artist_MBID', 'release_MBID', 'recording_MBID']
    )
arrow_csv_parse_options = csv.ParseOptions(
    delimiter='\t',
    )

def load_path(file_path, to_pandas = True):
    """Function to load a file and return a dataframe"""

    df = csv.read_csv(
        file_path,
        read_options = arrow_csv_read_options,
        parse_options = arrow_csv_parse_options,
        )
    
    if to_pandas:
        return df.to_pandas()
    else:
        return df


def load_path_file_pandas(paths, how_many = None, drop_subset = None):
    """
    Input: List of paths to files or text file with paths on each line.
    """
    if not isinstance(paths, list):
        # Open a file with MLHD file paths to process
        with open(paths, 'r') as f:
            file_paths = f.readlines()
            file_paths= [item.strip() for item in file_paths]
    
    elif isinstance(paths, list):
        #paths is list of paths
        file_paths = paths
    else:
        raise ValueError("Paths must be either a list of paths or a text file with paths")

    if how_many is None:
        how_many = len(file_paths)
    elif how_many <= len(file_paths):
        pass
    else:
        raise ValueError("how_many must be <= len(paths)")
    
    df = pd.DataFrame(columns = ['timestamp', 'artist_MBID', 'release_MBID', 'recording_MBID'])
    
    ls = [load_path_pandas(path) for path in file_paths[:how_many]]
    
    df = pd.concat(ls, ignore_index=True)
    del ls
    if drop_subset is None:
        return df
    else:
        return df.dropna(subset = drop_subset)

def load_path_file(paths, how_many = None, drop_subset = None):
    """
    Input: List of paths to files or text file with paths on each line.
    """
    if not isinstance(paths, list):
        # Open a file with MLHD file paths to process
        with open(paths, 'r') as f:
            file_paths = f.readlines()
            file_paths= [item.strip() for item in file_paths]
    
    elif isinstance(paths, list):
        #paths is list of paths
        file_paths = paths
    else:
        raise ValueError("Paths must be either a list of paths or a text file with paths")

    if how_many is None:
        how_many = len(file_paths)
    elif how_many <= len(file_paths):
        pass
    else:
        raise ValueError("how_many must be <= len(paths)")
    
    df = pd.DataFrame(columns = ['timestamp', 'artist_MBID', 'release_MBID', 'recording_MBID'])
    
    ls = [load_path(path) for path in file_paths[:how_many]]
    
    df = pd.concat(ls, ignore_index=True)
    del ls
    if drop_subset is None:
        return df
    else:
        return df.dropna(subset = drop_subset)

### Complete this function if MLHD is converted to pickle, or if pyarrow starts supporting CSV+zst
#  def load_folder(folder_path):
#     """
#     Function to load a folder of MLHD files using arrow
#     """

#     df = dataset.dataset(folder_path)
#     df = df.to_pandas()
#     return df

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

def replace(value, lookup_df, col_name):
    """Apply or map this function on a pandas series to fetch values from a lookup_df column with input value as index key
    
    Keyword arguments:
    value       : Value to search in lookup_df's index
    lookup_df   : DataFrame to lookup values. Return value from col_name column
    col_name    : Column name in lookup_df to lookup values
    """
    
    try:
        return lookup_df.at[value, col_name]
    except KeyError:
        return None

def replace_multi(value, lookup_df):
    """lookup a value in the index of a lookup_df and return the row as a tuple

    Args:
        value (str): value to lookup in the DataFrame index.
        lookup_df (Pandas.DataFrame): DataFrame to lookup values. Index contains the values to lookup.

    Returns:
        tuple: A tuple of values representing rows of lookup_df with index = value.
                Equivalent to pandas.DataFrame.loc[value, :], but faster.
    """
    try:
        return tuple(replace(value, lookup_df, col_name) for col_name in lookup_df.columns)
    except KeyError:
        return tuple(None for col_name in lookup_df.columns)