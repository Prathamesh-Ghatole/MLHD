from lib import io_ as io
from time import monotonic
from rich.progress import track
import config

from rich import print
from rich.console import Console

io.generate_folders()
console = Console()
console.clear()

master_start = monotonic()
###
# Loading ENV variables
console.log("Loading ENV variables...")
# ENV = io.get_config()

MLHD_ROOT = config.MLHD_ROOT
WRITE_ROOT = config.WRITE_ROOT
LOG_WRITE_PATH = config.LOG_WRITE_ROOT

# LOG_EPOCH = ENV['LOG_EPOCH']
LOG_EPOCH = 1

console.log("Generating MLHD Paths...")
MLHD_FILES = io.generate_paths(config.MLHD_ROOT)

with console.status("Loading MB Track Tables...") as stat:
    # MB_track_set, MB_track_redir_set = io.get_track_sets()
    MB_track_set, MB_track_redir_set = io.get_track_sets_pickled()

console.log("MB Track Tables Loaded")

OUTPUT_LOG = {}

# Looping for each MLHD file

console.log("Looping through MLHD files...")
file_counter = 0
for path in track(MLHD_FILES):

    # Start Timer
    start = monotonic()

    # Start Processing
    df_loop = io.load_path(path)
    output = io.check_rec(df_loop, MB_track_set, MB_track_redir_set)
    _ = io.write_frame(df_loop, path)

    # End Processing
    end = monotonic()

    # Logging
    file_counter += 1

    io.log_output(output, path, round(end - start, 3), monotonic(), OUTPUT_LOG)
    if file_counter % LOG_EPOCH == 0:
        _ = io.write_log(OUTPUT_LOG, LOG_WRITE_PATH)

master_end = monotonic()

console.log("Total Time Taken: ", round(master_end - master_start, 3))
console.log("Done!")
# print(output_log)
