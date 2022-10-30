# Settings for the clean_master.py script

# Total mumber of files to clean (use for debugging). Set to None to process all.
HOW_MANY = 5

# Maximum number of workers for concurrent.futures.ThreadPoolExecutor
MAX_WORKERS = 5

# Batch size for multiprocessing
CHUNK_SIZE = 5

# When TRUE -> Keep missing values in the data. (maintains the original structure/shape of MLHD)
KEEP_MISSING = True

# When TRUE -> Turn unknown values to NaN
TURN_BLANK = True

## Export options
WRITE_TSV_ZSTD = True
WRITE_PARQUET = False
LOG_FILE_NAME = "clean_master_log.csv"  # Name of the log file
RESET_LOG_BEFORE_RUN = True  # When TRUE -> Reset the log file before running the script
