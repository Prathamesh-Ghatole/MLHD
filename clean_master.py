# A master script to clean and export MLHD data!

from os.path import join
from time import monotonic
import pandas as pd
from numpy import nan

# For pretty CLI
from rich import print
from rich.console import Console
from rich.progress import track

import clean_master_config
import config

# Essential Imports
from lib import io_ as io

console = Console()
console.clear()

####
### Getting started ###
####

io.generate_folders()  # Generate folders to write all outputs (as specified in config.py)
master_start = monotonic()  # Start the master timer

OUTPUT_LOG = {}  # Log Progress
TIME_LOGS = {}  # Logs time taken for each step

# Loading ENV variables
console.log("Loading config...")

# Fetching a 1 Dimensional list of MLHD file paths
console.log("Generating MLHD Paths...")
MLHD_PATHS = io.generate_paths(config.MLHD_ROOT)
LOG_WRITE_PATH = join(config.LOG_WRITE_ROOT, clean_master_config.LOG_FILE_NAME)

####
### LOADING MB TABLES ###
####


def load_data():
    DATA = {}

    TIME_LOGS["MB_start"] = monotonic()

    console.log("loading recording gids...")
    MB_rec_gid = pd.read_parquet("warehouse/MB_tables/recording_gid.parquet")
    MB_rec_gid.set_index("gid", inplace=True)
    DATA["MB_rec_gid"] = MB_rec_gid

    console.log("loading recording redirects...")
    MB_rec_redirects = pd.read_parquet(
        "warehouse/MB_tables/recording_redirects.parquet"
    )
    MB_rec_redirects.set_index("old", inplace=True)
    DATA["MB_rec_redirects"] = MB_rec_redirects

    console.log("loading recording canonical MBIDs...")
    MB_rec_canonical = pd.read_parquet(
        "warehouse/MB_tables/recording_canonical.parquet"
    )
    MB_rec_canonical.set_index("old", inplace=True)
    DATA["MB_rec_canonical"] = MB_rec_canonical

    console.log("loading artist credit gids...")
    MB_artist_credit_list = pd.read_parquet(
        "warehouse/MB_tables/artist_credit_release_gid.parquet"
    )
    MB_artist_credit_list.set_index("recording_mbid", inplace=True)
    MB_artist_credit_list["artist_mbids"] = MB_artist_credit_list.artist_mbids.map(
        lambda x: x.strip("{}")
    )
    DATA["MB_artist_credit_list"] = MB_artist_credit_list

    # Converting MB_rec_gid to set for faster lookup
    rec_gid_set = set(MB_rec_gid.index)
    DATA["rec_gid_set"] = rec_gid_set

    TIME_LOGS["MB_end"] = monotonic()
    console.log(
        "loaded MB tables. Took {} seconds".format(
            round(TIME_LOGS["MB_end"] - TIME_LOGS["MB_start"], 2)
        )
    )

    return DATA


DATA = load_data()

####
### Defining Functions ###
####

# Main function to process dataframe
def process_df(
    df_input,
    keep_missing=clean_master_config.KEEP_MISSING,
    turn_blank=clean_master_config.TURN_BLANK,
):
    """Take an input df and process it into a cleaned df

    Args:
        df_input (pandas.DataFrame): input dataframe with columns: <timestamp, artist_MBID, release_MBID, recording_MBID>
        keep_missing (bool, optional): If True, keep rows with missing, unknown MBIDs to maintain the structure of the original data.
        turn_blank (bool, optional): If True, replace blank MBIDs with None

    Returns:
        pandas.DataFrame: Cleaned dataframe with columns: <timestamp, artist_MBID, release_MBID, recording_MBID>
    """

    rec_gid_set = DATA["rec_gid_set"]
    MB_rec_redirects = DATA["MB_rec_redirects"]
    MB_rec_canonical = DATA["MB_rec_canonical"]
    MB_artist_credit_list = DATA["MB_artist_credit_list"]

    # 1. Get redirects for MBIDs that aren't present in rec_gid_set using MB_rec_redirects.
    df_input["recording_MBID"] = df_input.recording_MBID.map(
        lambda x: io.replace(x, MB_rec_redirects, "new") if x not in rec_gid_set else x
    )

    # 2. Find canonical recordings for all cleaned/uncleaned recording_MBIDs
    df_input["recording_MBID"] = df_input["recording_MBID"].map(
        lambda x: io.replace(x, MB_rec_canonical, "new")
        if io.replace(x, MB_rec_canonical, "new") is not nan
        else x
    )

    # 3. Fetch artist, release_MBIDs for all recording_MBIDs
    artist_release_mbids = df_input["recording_MBID"].map(
        lambda x: io.replace_multi(x, MB_artist_credit_list)
    )

    df_input[["artist_MBID", "release_MBID"]] = pd.DataFrame(
        artist_release_mbids.tolist(),
        columns=["artist_MBID", "release_MBID"],
        index=df_input.index,
    )

    return df_input


# Driver function to read, clean, and write all the file_paths in the path_list, while logging their details
def driver(
    path_list,
    keep_missing=clean_master_config.KEEP_MISSING,
    turn_blank=clean_master_config.TURN_BLANK,
    write_root=config.WRITE_ROOT,
):

    """Driver function to read, clean, and write all the file_paths in the path_list, while logging their details

    Args:
        path_list (list): List of paths to the tables to be cleaned
        keep_missing (bool, optional): If True, keep rows with missing, unknown MBIDs to maintain the structure of the original data. Defaults to cmc.KEEP_MISSING.
        turn_blank (bool, optional): If True, replace blank MBIDs with None. Defaults to cmc.TURN_BLANK
        write_root (str, optional): Root directory to write the cleaned tables to. Defaults to config.WRITE_ROOT.
    Returns:
        list: List of cleaned dataframes
    """
    console.log("Looping through MLHD files...")
    file_counter = 0
    start_loop = monotonic()
    for path in track(path_list):

        # Start timer
        start_process = monotonic()

        df = io.load_path(path)  # Reading the table
        df = process_df(df, keep_missing, turn_blank)  # Reading the table
        io.write_frame(df, path)  # Writing the table

        # End timer
        end_process = monotonic()
        time_taken = round(end_process - start_process, 2)

        # Logging the table
        file_counter += 1
        io.log_output(df.shape[0], path, time_taken, monotonic(), OUTPUT_LOG)

        if file_counter % config.LOG_EPOCH == 0:
            _ = io.write_log(OUTPUT_LOG, LOG_WRITE_PATH)

    end_loop = monotonic()
    loop_time = round(end_loop - start_loop, 2)

    console.log(f"Looped through {len(path_list)} files in {loop_time} seconds")
    return None


####
### Running the driver function ###
####


def main():
    start_process = monotonic()

    driver(MLHD_PATHS[:30])

    end_process = monotonic()
    ####
    ### Outro ###
    ####

    master_end = monotonic()  # End the master timer
    process_time = round(
        end_process - start_process, 2
    )  # Calculate the time taken to process the data
    master_time = round(master_end - master_start, 2)  # Calculate the master time

    master_log = {
        "Master time": master_time,
        "Process time": process_time,
        "Log_Path": LOG_WRITE_PATH,
    }

    io.write_log(
        master_log, LOG_WRITE_PATH.replace(".json", "_master.json")
    )  # Write the master log

    console.log(f"Finished Process in {master_time} seconds")
    console.log(f"Output log written to {LOG_WRITE_PATH}")


if __name__ == "__main__":
    main()
