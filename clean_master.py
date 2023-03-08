# A master script to clean and export MLHD data!

import os
from os.path import join
from os.path import splitext
import pickle
from time import monotonic
import logging

# For pretty CLI
from rich import print
from rich.console import Console
from rich.progress import track

import clean_master_config
import config

# Essential Imports
from lib import io_ as io

console = Console()

logging.basicConfig(level=logging.INFO)

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


def load_pkl(path):
    with open(path, "rb") as fp:
        return pickle.load(fp)
        

def load_data() -> dict:
    DATA = {}

    TIME_LOGS["MB_start"] = monotonic()

    console.log("loading recording gids...")
    DATA["MB_rec_gid"] = load_pkl(join(config.MB_ROOT, "recording_gid.pkl"))

    console.log("loading recording redirects...")
    DATA["MB_rec_redirect"] = load_pkl(join(config.MB_ROOT, "recording_gid_redirect.pkl"))

    console.log("loading recording canonical MBIDs...")
    DATA["MB_rec_canonical"] = load_pkl(join(config.MB_ROOT, "recording_canonical.pkl"))

    console.log("loading artist credit gids...")
    DATA["MB_artist_credit_list"] = load_pkl(join(config.MB_ROOT, "artist_credit_release_gid.pkl"))

    console.log("loading artist redirect...")
    DATA["MB_artist_redirect"] = load_pkl(join(config.MB_ROOT, "artist_gid_redirect.pkl"))

    console.log("loading release redirect...")
    DATA["MB_rel_redirect"] = load_pkl(join(config.MB_ROOT, "release_gid_redirect.pkl"))

    console.log("loading release canonical...")
    DATA["MB_release_canonical"] = load_pkl(join(config.MB_ROOT, "release_canonical.pkl"))
    
    console.log("loading release metadata...")
    DATA["MB_release"] = load_pkl(join(config.MB_ROOT, "release.pkl"))

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

    MB_rec_gid = DATA["MB_rec_gid"]
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
            if recording not in MB_rec_gid:
                recording = io.get(MB_rec_redirect, "new", recording)
            if recording:
                canonical_recording_id = io.get(MB_rec_canonical, "new", recording)
                if canonical_recording_id:
                    recording = canonical_recording_id
                release_id = io.get(MB_artist_credit_list, "release_mbid", recording)
                artist_ids = io.get(MB_artist_credit_list, "artist_mbids", recording)
                if not release_id:
                    logging.debug(f"When getting a recording's release, got no value: {orig_recording=}, {recording=}, {canonical_recording_id=}")

                # TODO: If the recording id _is_ in rec_gid_set but is _not in_ MB_artist_credit_list, then it's likely that 
                #       this is a non-album track, which isn't present in the canonical mapping.
                #       In that case, we need to actually do a mapper lookup to convert the musicbrainz name+artist credit
                #       to our canonical recording id, without going through the MB_rec_canonical table
                if recording in MB_rec_gid and not release_id:
                    # recording_name, artist_credit = get_metadata_for_recording_mbid_db(recording)
                    # print(f"https://musicbrainz.org/recording/{recording}: Unexpectedly found no recording metadata even though it's a valid id. non-album track?")
                    # print(get_url_mapper(artist_credit, recording_name))
                    # TODO: For now just skipping this
                    continue
                else:
                    assert release_id
                    assert len(artist_ids)

                complete_output.append({   
                    "timestamp": timestamp, 
                    "artist_mbids": ",".join(artist_ids), 
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
                logging.debug(f"{orig_release=}: Unexpectedly found no release metadata")
                continue

            if artist_ids:
                missing_output.append(
                    {
                        "timestamp": timestamp, 
                        "artist_mbids": ",".join(artist_ids),
                        "release_mbid": release,
                        "recording_mbid": ""
                    }
                )
                # TODO: If the release id is invalid (missing from the release table), blank it out (and continue to artist)
                continue
            else:
                logging.debug(f"Cannot find a release id: {orig_release=}, {release=}, {canonical_release_id=}")
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
                "artist_mbids": artist, 
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

# Driver function to read, clean, and write all the file_paths in the path_list, while logging their details
def driver(
    path_list: list,
) -> None:

    """Driver function to read, clean, and write all the file_paths in the path_list, while logging their details

    Args:
        path_list (list): List of paths to the tables to be cleaned
        write_root (str, optional): Root directory to write the cleaned tables to. Defaults to config.WRITE_ROOT.
    Returns:
        list: List of cleaned dataframes
    """
    console.log("Looping through MLHD files...")

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
) -> None:

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

    master_end = monotonic()  # End the master timer
    process_time = round(
        end_process - start_process, 2
    )  # Calculate the time taken to process the data
    master_time = round(master_end - master_start, 2)  # Calculate the master time

    write_master_log([master_time, process_time])

    console.log(f"Finished Process in {master_time} seconds")
    console.log(f"Output log written to {LOG_WRITE_PATH}")


if __name__ == "__main__":
    main()
