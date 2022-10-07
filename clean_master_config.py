# Settings for the clean_master.py script

# Maximum number of workers for concurrent.futures.ThreadPoolExecutor
MAX_WORKERS = 5

# When TRUE -> Keep missing values in the data. (maintains the original structure/shape of MLHD)
KEEP_MISSING = True

# When TRUE -> Turn unknown values to NaN
TURN_BLANK = True

## Export options
WRITE_TSV_ZSTD = True
WRITE_PARQUET = False
LOG_FILE_NAME = "clean_master_log.json" # Name of the log file