# A master script to clean and export MLHD data!

import os
from os.path import join
from os.path import splitext
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
MASTER_LOG_WRITE_PATH = join(
    config.LOG_WRITE_ROOT,
    splitext(clean_master_config.LOG_FILE_NAME)[0] + "_MASTER.csv",
)

####
### LOADING MB TABLES ###
####


def load_data() -> dict:
    DATA = {}

    TIME_LOGS["MB_start"] = monotonic()

    console.log("loading recording gids...")
    MB_rec_gid = pd.read_parquet(join(config.MB_ROOT, "recording_gid.parquet"))
    MB_rec_gid.set_index("gid", inplace=True)
    DATA["MB_rec_gid"] = MB_rec_gid

    console.log("loading recording redirects...")
    MB_rec_redirects = pd.read_parquet(
        join(config.MB_ROOT, "recording_gid_redirect.parquet")
    )
    MB_rec_redirects.set_index("old", inplace=True)
    DATA["MB_rec_redirect"] = MB_rec_redirects

    console.log("loading recording canonical MBIDs...")
    MB_rec_canonical = pd.read_parquet(
        join(config.MB_ROOT, "recording_canonical.parquet")
    )
    MB_rec_canonical.set_index("old", inplace=True)
    DATA["MB_rec_canonical"] = MB_rec_canonical

    console.log("loading artist credit gids...")
    MB_artist_credit_list = pd.read_parquet(
        join(config.MB_ROOT, "artist_credit_release_gid.parquet")
    )
    MB_artist_credit_list.set_index("recording_mbid", inplace=True)
    DATA["MB_artist_credit_list"] = MB_artist_credit_list

    console.log("loading artist redirect...")
    MB_artist_redirect = pd.read_parquet(
        join(config.MB_ROOT, "artist_gid_redirect.parquet")
    )
    MB_artist_redirect.set_index("old", inplace=True)
    DATA["MB_artist_redirect"] = MB_artist_redirect

    console.log("loading release redirect...")
    MB_release_redirect = pd.read_parquet(
        join(config.MB_ROOT, "release_gid_redirect.parquet")
    )
    MB_release_redirect.set_index("old", inplace=True)
    DATA["MB_rel_redirect"] = MB_release_redirect

    console.log("loading release canonical...")
    MB_release_canonical = pd.read_parquet(
        join(config.MB_ROOT, "release_canonical.parquet")
    )
    MB_release_canonical.set_index("release_mbid", inplace=True)
    DATA["MB_release_canonical"] = MB_release_canonical
    
    console.log("loading release metadata...")
    MB_release = pd.read_parquet(
        join(config.MB_ROOT, "release.parquet")
    )
    MB_release.set_index("release_mbid", inplace=True)
    DATA["MB_release"] = MB_release

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

def process_df_new(df):
    df = df.fillna('')

    rec_gid_set = DATA["rec_gid_set"]
    MB_rec_redirect = DATA["MB_rec_redirect"]
    MB_rel_redirect = DATA["MB_rel_redirect"]
    MB_rec_canonical = DATA["MB_rec_canonical"]
    MB_artist_credit_list = DATA["MB_artist_credit_list"]
    MB_artist_redirect = DATA["MB_artist_redirect"]
    MB_release = DATA["MB_release"]
    MB_release_canonical = DATA["MB_release_canonical"]

    # The output of processing this df, only for rows which are complete (artist, release, recording)
    complete_output = []
    # Any other rows that are missing one or more data values
    missing_output = []

    for row in df.values.tolist():
        timestamp, artist, release, recording = row

        if recording:
            # If the row has a recording, we throw away the release and artist and recompute it using our dataset
            # 89f6f6e8-892d-4270-be69-a3007db87940 is a valid recording which needs to be redirected
            orig_recording = recording
            if recording not in rec_gid_set:
                recording = io.get(MB_rec_redirect, "new", recording)
            if recording:
                canonical_recording_id = io.get(MB_rec_canonical, "new", recording)
                if canonical_recording_id:
                    recording = canonical_recording_id
                release_id = io.get(MB_artist_credit_list, "release_mbid", recording)
                artist_ids = io.get(MB_artist_credit_list, "artist_mbids", recording)
                if not release_id:
                    print(f"When getting a recording's release, got no value: {orig_recording=}, {recording=}, {canonical_recording_id=}")

                # TODO: If the recording id _is_ in rec_gid_set but is _not in_ MB_artist_credit_list, then it's likely that 
                #       this is a non-album track, which isn't present in the canonical mapping.
                #       In that case, we need to actually do a mapper lookup to convert the musicbrainz name+artist credit
                #       to our canonical recording id, without going through the MB_rec_canonical table
                if recording in rec_gid_set and not release_id:
                    print(f"{recording=}: Unexpectedly found no recording metadata even though it's a valid id. non-album track?")
                    # TODO: For now just skipping this
                    continue
                else:
                    assert release_id
                    assert artist_ids.tolist()

                complete_output.append({   
                    "timestamp": timestamp, 
                    "artist_mbids": artist_ids.tolist(), 
                    "release_mbid": release_id,
                    "recording_mbid": recording
                })
                continue
            else:
                recording = ''
                # If the id isn't in recording or the redirect table then it's an invalid recording id,
                # blank it out and continue, using the release or artist

        if release:
            # In case that we have no recording value, but have a release, use that to find a canonical release
            # compute artist from the release
            # Save this to the "missing data" file for this user
            orig_release = release
            release_redirect = io.get(MB_rel_redirect, "new", release)
            if release_redirect:
                #print(f"release {release} redirect to {release_redirect}")
                release = release_redirect

            # TODO: Does it make sense to use the canonical release ID?
            # TODO: Would it be better to do a releasegroup?
            canonical_release_id = io.get(MB_release_canonical, "release_mbid", release)
            if canonical_release_id:
                release = canonical_release_id
            artist_ids = io.get(MB_release, "artist_mbids", release)
            if artist_ids is None:
                print(f"{orig_release=}: Unexpectedly found no release metadata")
                continue

            artist_ids_list = artist_ids.tolist()
            if artist_ids_list:
                missing_output.append(
                    {
                        "timestamp": timestamp, 
                        "artist_mbids": artist_ids_list, 
                        "release_mbid": release,
                        "recording_mbid": ""
                    }
                )
                # TODO: If the release id is invalid (missing from the release table), blank it out (and continue to artist)
                continue
            else:
                print(f"Cannot find a release id: {orig_release=}, {release=}, {canonical_release_id=}")
                # If artist_ids is blank, it means that there's no such release with this ID (because we find this value
                # by looking up the id in the full release table)
                # This means that the id isn't a real release, blank out the release and fall down to the artist check
                release = ''

        if artist:
            # No release or recording, only an artist. Not a lot we can do, look up the redirect if it exists
            # Note that lots of the MLHD data for an artist is wrong, especially if there are many artists
            # with the same name
            # Save this to the "missing data" file for this user
            artist_redirect = io.get(MB_artist_redirect, "new", artist)
            if artist_redirect:
                artist = artist_redirect
            missing_output.append({
                "timestamp": timestamp, 
                "artist_mbids": [artist], 
                "release_mbid": "",
                "recording_mbid": ""
            })
            # TODO: If the artist is an invalid MBID
        else:
            # only a timestamp
            # Save this to the "missing data" file for this user (might be useful for session tracking)
            missing_output.append({
                "timestamp": timestamp, 
                "artist_mbids": "", 
                "release_mbid": "",
                "recording_mbid": ""
            })

    return complete_output, missing_output


# Main function to process dataframe
def process_df(
    df_input: pd.DataFrame,
) -> pd.DataFrame:
    """Take an input df and process it into a cleaned df

    Args:
        df_input (pandas.DataFrame): input dataframe with columns: <timestamp, artist_MBID, release_MBID, recording_MBID>

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
    path_list: list,
    reset_log_before_run: bool = clean_master_config.RESET_LOG_BEFORE_RUN,
) -> None:

    """Driver function to read, clean, and write all the file_paths in the path_list, while logging their details

    Args:
        path_list (list): List of paths to the tables to be cleaned
        write_root (str, optional): Root directory to write the cleaned tables to. Defaults to config.WRITE_ROOT.
    Returns:
        list: List of cleaned dataframes
    """
    console.log("Looping through MLHD files...")

    if reset_log_before_run:
        with open(LOG_WRITE_PATH, "w") as f:
            f.write("path,log_value,time_taken,timestamp\n")

    file_counter = 0
    start_loop = monotonic()
    for i, path in enumerate(path_list, 1):
        console.log(f"Processing {path}")

        # Start timer
        start_process = monotonic()

        subpath = os.path.relpath(path, config.MLHD_ROOT)
        write_path = subpath.replace("csv.zst", "txt.zst")
        df = io.load_path(path)

        complete_output, missing_output = process_df_new(df)
        complete_root = os.path.join(config.WRITE_ROOT, "complete")
        missing_root = os.path.join(config.WRITE_ROOT, "missing")
        io.write_frame_pandas(complete_output, complete_root, write_path)
        io.write_frame_pandas(missing_output, missing_root, write_path)

        # End timer
        end_process = monotonic()
        time_taken = round(end_process - start_process, 2)

        # Logging the table
        file_counter += 1
        log_str = io.log_output(
            [path, df.shape[0], time_taken, monotonic()], OUTPUT_LOG
        )

        if file_counter % config.LOG_EPOCH == 0:
            # Write log as a CSV file with columns <path, log_value, time_taken, timestamp>
            _ = io.write_log(log_str, LOG_WRITE_PATH)

        if i % 10 == 0:
            console.log(f"{i}/{len(path_list)}")

    end_loop = monotonic()
    loop_time = round(end_loop - start_loop, 2)

    console.log(f"Looped through {len(path_list)} files in {loop_time} seconds")


def write_master_log(
    stuff_to_log: list,
    write_path: str,
    reset_log_before_run: bool = clean_master_config.RESET_LOG_BEFORE_RUN,
) -> None:

    if clean_master_config.RESET_LOG_BEFORE_RUN:
        with open(MASTER_LOG_WRITE_PATH, "w") as f:
            f.write("master_time,process_time\n")

    log_str = ",".join([str(item) for item in stuff_to_log]) + "\n"

    io.write_log(log_str, MASTER_LOG_WRITE_PATH)


####
### Running the driver function ###
####


def main() -> None:
    start_process = monotonic()

    if type(clean_master_config.HOW_MANY) is int:
        driver(MLHD_PATHS[: clean_master_config.HOW_MANY])
    else:
        driver(MLHD_PATHS)

    end_process = monotonic()
    ####
    ### Outro ###
    ####

    master_end = monotonic()  # End the master timer
    process_time = round(
        end_process - start_process, 2
    )  # Calculate the time taken to process the data
    master_time = round(master_end - master_start, 2)  # Calculate the master time

    write_master_log([master_time, process_time], MASTER_LOG_WRITE_PATH)

    console.log(f"Finished Process in {master_time} seconds")
    console.log(f"Output log written to {LOG_WRITE_PATH}")


if __name__ == "__main__":
    main()
