import os
import pickle

import config
import pandas as pd
from numpy import nan
from pyarrow import CompressedOutputStream as COS
from pyarrow import Table as Table
from pyarrow import _csv as csv

import lib.mb as mb


def generate_paths(MLHD_ROOT):
    """Function to generate a list of paths to MLHD files"""
    MLHD_FILES = []

    for root, _, files in os.walk(MLHD_ROOT):
        for file in files:
            if file.endswith(".gz") or file.endswith(".zst"):
                MLHD_FILES.append(os.path.join(root, file))
    MLHD_FILES.sort()
    return MLHD_FILES


def load_path_pandas(path):
    """
    Function to load a file and return a dataframe
    """
    df = pd.read_csv(
        path,
        sep="\t",
        header=None,
        names=["timestamp", "artist_MBID", "release_MBID", "recording_MBID"],
    )

    return df


arrow_csv_read_options = csv.ReadOptions(
    column_names=["timestamp", "artist_MBID", "release_MBID", "recording_MBID"]
)
arrow_csv_parse_options = csv.ParseOptions(
    delimiter="\t",
)


def load_path(file_path, to_pandas=True, mlhd=True):
    """Function to load a file and return a dataframe
    file_path: path to file
    to_pandas: boolean to return dataframe or arrow table
    mlhd: boolean to specify if reading MLHD tsv or normal csv file.
    """
    if mlhd:
        df = csv.read_csv(
            file_path,
            read_options=arrow_csv_read_options,
            parse_options=arrow_csv_parse_options,
        )
    else:
        df = csv.read_csv(
            file_path,
        )

    if to_pandas:
        df = df.to_pandas()
        df.replace("", nan, inplace=True)

        return df
    else:
        return df


def load_path_file_pandas(paths, how_many=None, drop_subset=None):
    """
    Input: List of paths to files or text file with paths on each line.
    """
    if not isinstance(paths, list):
        # Open a file with MLHD file paths to process
        with open(paths, "r") as f:
            file_paths = f.readlines()
            file_paths = [item.strip() for item in file_paths]

    elif isinstance(paths, list):
        # paths is list of paths
        file_paths = paths
    else:
        raise ValueError(
            "Paths must be either a list of paths or a text file with paths"
        )

    if how_many is None:
        how_many = len(file_paths)
    elif how_many <= len(file_paths):
        pass
    else:
        raise ValueError("how_many must be <= len(paths)")

    df = pd.DataFrame(
        columns=["timestamp", "artist_MBID", "release_MBID", "recording_MBID"]
    )

    ls = [load_path_pandas(path) for path in file_paths[:how_many]]

    df = pd.concat(ls, ignore_index=True)
    del ls
    if drop_subset is None:
        return df
    else:
        return df.dropna(subset=drop_subset)


def load_path_file(paths, how_many=None, drop_subset=None):
    """
    Input: List of paths to files or text file with paths on each line.
    """
    if not isinstance(paths, list):
        # Open a file with MLHD file paths to process
        with open(paths, "r") as f:
            file_paths = f.readlines()
            file_paths = [item.strip() for item in file_paths]

    elif isinstance(paths, list):
        # paths is list of paths
        file_paths = paths
    else:
        raise ValueError(
            "Paths must be either a list of paths or a text file with paths"
        )

    if how_many is None:
        how_many = len(file_paths)
    elif how_many <= len(file_paths):
        pass
    else:
        raise ValueError("how_many must be <= len(paths)")

    df = pd.DataFrame(
        columns=["timestamp", "artist_MBID", "release_MBID", "recording_MBID"]
    )

    ls = [load_path(path) for path in file_paths[:how_many]]

    df = pd.concat(ls, ignore_index=True)
    del ls
    if drop_subset is None:
        return df
    else:
        return df.dropna(subset=drop_subset)


### Complete this function if MLHD is converted to pickle, or if pyarrow starts supporting CSV+zst
#  def load_folder(folder_path):
#     """
#     Function to load a folder of MLHD files using arrow
#     """

#     df = dataset.dataset(folder_path)
#     df = df.to_pandas()
#     return df

write_options = csv.WriteOptions(
    include_header=False,
    delimiter="\t",
)


def write_frame(df_input, original_path):
    """
    Function to write a dataframe to a csv file using pyarrow
    """
    # Replace MLHD_ROOT with path to new MLHD folder.

    write_path = original_path.replace("csv.zst", "txt.zst")
    write_path = write_path.replace(config.MLHD_ROOT, "")
    write_path = os.path.join(config.WRITE_ROOT, write_path)

    # print(write_path)
    # Make directory inside WRITE_ROOT if it doesn't exist
    os.makedirs(os.path.dirname(write_path), exist_ok=True)

    df_input = Table.from_pandas(df_input)
    # csv.write_csv(df_input, output_file = write_path, write_options = )
    with COS(write_path, "zstd") as out:
        csv.write_csv(df_input, out, write_options=write_options)


def write_frame_pandas(df_input, original_path):
    """
    Function to write a dataframe to a csv file
    """
    # Replace MLHD_ROOT with path to new MLHD folder.
    write_path = original_path.replace(config.MLHD_ROOT, config.WRITE_ROOT)
    write_path = write_path.replace("txt.gz", "csv.zst")

    # print(write_path)
    # Make directory inside WRITE_ROOT if it doesn't exist
    os.makedirs(os.path.dirname(write_path), exist_ok=True)

    df_input.to_csv(
        write_path,
        index=False,
        sep="\t",
        header=False,
        compression={"method": "zstd", "level": 10},
    )

    return write_path


def log_output(values_to_log: list, master_log_dict: dict) -> str:
    """
    Function to log output series and path
    values_to_log (list): list of values to log <path, log_value, time_taken, timestamp>
    """
    # If keys not in dict, generate empty list with dict
    key_set = ["path", "log_value", "time_taken", "timestamp"]

    for key in key_set:
        if key not in master_log_dict:
            master_log_dict[key] = []

    for value in values_to_log:
        for key, value in zip(key_set, values_to_log):
            master_log_dict[key].append(value)

    # master_log_dict["path"].append(path)
    # master_log_dict["log_value"].append(log_value)
    # master_log_dict["time_taken"].append(time_taken)
    # master_log_dict["timestamp"].append(timestamp)
    log_str = ",".join([str(item) for item in values_to_log]) + "\n"

    return log_str


def write_log(log_str: str, log_path: str) -> str:

    """Function to update log (json) file"""
    # Make directory inside WRITE_ROOT if it doesn't exist
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    with open(log_path, "a") as f:
        f.write(log_str)

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
    with open(config.PICKLE_PATH, "rb") as f:
        MB_track_set, MB_track_redir_set = pickle.load(f)

    return (MB_track_set, MB_track_redir_set)


def check_rec(df, MB_track_set, MB_track_redir_set):
    """
    Function to check if a track is in the recording table
    INPUT: dataframe with recording_MBID column.
    OUTPUT: Returns a series of "recording-MBIDs" that are in recording table.
    """

    ret = [
        mbid
        for mbid in df.recording_MBID.tolist()
        if (mbid in MB_track_set) or (mbid in MB_track_redir_set)
    ]
    if ret == []:
        return None
    else:
        return ret


def replace(value, lookup_df, col_name):
    """Apply or map this function on a pandas series to fetch values from a lookup_df column
    with input value as index key

    Keyword arguments:
    value       : Value to search in lookup_df's index
    lookup_df   : DataFrame to lookup values. Return value from col_name column
    col_name    : Column name in lookup_df to lookup values
    """

    try:
        return lookup_df.at[value, col_name]
    except KeyError:
        return nan


def replace_multi(value, lookup_df):
    """lookup a value in the index of a lookup_df and return the row as a tuple

    Args:
        value (str): value to lookup in the DataFrame index.
        lookup_df (Pandas.DataFrame): DataFrame to lookup values in. Index contains the values to lookup.

    Returns:
        tuple: A tuple of values representing columns of lookup_df with index = value.
                Equivalent to pandas.DataFrame.loc[value, :], but faster.
    """
    try:
        return tuple(
            replace(value, lookup_df, col_name) for col_name in lookup_df.columns
        )
    except KeyError:
        return tuple(None for col_name in lookup_df.columns)


def generate_folders():

    os.makedirs(config.MB_ROOT, exist_ok=True)
    os.makedirs(config.SAMPLE_ROOT, exist_ok=True)
    os.makedirs(config.LOG_WRITE_ROOT, exist_ok=True)
    os.makedirs(config.HTML_ROOT, exist_ok=True)
    os.makedirs(config.MAPPER_OUTPUT_ROOT, exist_ok=True)

    if config.WRITE_ROOT != "":
        os.makedirs(config.WRITE_ROOT, exist_ok=True)
