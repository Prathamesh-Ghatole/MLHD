# Settings for the clean_master.py script

# When TRUE -> Keep missing values in the data. (maintains the original structure/shape of MLHD)
KEEP_MISSING = True

# When TRUE -> Turn unknown values to NaN
TURN_BLANK = True

## Export options
WRITE_TSV_ZSTD = True
WRITE_PARQUET = False
WRITE_EPOCHS = 10 # Process 10, store in memory, and write 10 of them at once.