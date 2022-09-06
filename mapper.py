### START OF MAIN ###
import lib.io_ as io
import lib.mapper_helper as mapper_helper
import pandas as pd
from time import monotonic

from rich import print
from rich.console import Console

console = Console()
console.clear()

time_logs = {}

### Load MLHD files ###

flag_cache = console.input("Use cached data? [Y/n]: ")

time_logs['load_start'] = monotonic()
console.log('loading MLHD files...')

if flag_cache.lower() == 'n':
    ### LOADING MB TABLES ###

    time_logs['MB_start'] = monotonic()

    console.log('loading recording gids...')
    MB_rec_gid = pd.read_parquet('warehouse/MB_tables/recording_gid.parquet')
    MB_rec_gid.set_index('gid', inplace=True)

    console.log('loading recording redirects...')
    MB_rec_redirects = pd.read_parquet('warehouse/MB_tables/recording_redirects.parquet')
    MB_rec_redirects.set_index('old', inplace=True)

    console.log('loading recording canonical MBIDs...')
    MB_rec_canonical = pd.read_parquet('warehouse/MB_tables/recording_canonical.parquet')
    MB_rec_canonical.set_index('old', inplace=True)

    console.log('loading artist credit list...')
    MB_artist_credit_list = pd.read_parquet('warehouse/MB_tables/artist_credit.parquet')
    MB_artist_credit_list.set_index('rec_gid', inplace=True)

    # Converting MB_rec_gid to set for faster lookup
    rec_gid_set = set(MB_rec_gid.index)

    time_logs['MB_end'] = monotonic()
    console.log("loaded MB tables. Took {} seconds".format(round(time_logs['MB_end'] - time_logs['MB_start'], 2)))

    ### Loading MLHD ###

    df = io.load_path_file('warehouse/samples/random_file_paths.txt', drop_subset = ['recording_MBID', 'artist_MBID'])
    df.drop(['release_MBID'], axis = 1, inplace = True)
    df.drop_duplicates(['artist_MBID', 'recording_MBID'], inplace = True)
    df.rename({'artist_MBID': 'mlhd_artist_mbid', 'recording_MBID': 'mlhd_recording_mbid'}, inplace=True, axis=1)
    df.reset_index(inplace=True, drop=True)

    time_logs['load_end'] = monotonic()
    console.log("loaded MLHD files with {} rows. Took {} seconds".format(df.shape[0], round(time_logs['load_end'] - time_logs['load_start'], 2)))

    ### Clean Up ###
    console.log('Starting Cleanup...')
    time_logs['clean_start'] = monotonic()

    shape_before = df.shape[0]
    df1 = mapper_helper.clean_rec(df, rec_gid_set, MB_rec_redirects, MB_rec_canonical, MB_artist_credit_list)
    shape_after = df.shape[0]

    df.to_csv('warehouse/mapper_outputs/mlhd_artist_credits.csv', index=False)
    console.log(f"Dropped {shape_before - shape_after} rows")


    time_logs['clean_end'] = monotonic()

    console.log("Cleaned {} rows. Took {} seconds".format(
        df.shape[0], 
        round(time_logs['clean_end'] - time_logs['clean_start'], 2))
        )

else:
    time_logs['load_start'] = monotonic()
    
    df = pd.read_csv('warehouse/mapper_outputs/mlhd_artist_credits.csv')
    
    time_logs['load_end'] = monotonic()
    console.log("loaded cleaned MLHD files. Took {} seconds".format(round(time_logs['load_end'] - time_logs['load_start']), 2))

### Mapper ###

try:
    num_rows = int(console.input("\nHow many rows do you want to process? [ALL]: "))
except ValueError:
    num_rows = df.shape[0]

if (num_rows < 0) or (num_rows > df.shape[0]):
    num_rows = df.shape[0]

mbc_flag = console.input("\nUse mbc? [Y/n]: ")
if mbc_flag.lower() == 'n':
    mbc_flag = False
else:
    mbc_flag = True

console.print()

time_logs['mapper_start'] = monotonic()

console.log("Mapping MBIDs...")

if mbc_flag:
    console.log('loading mbc_table...')
    mbc_table = pd.read_parquet('warehouse/MB_tables/mbc_combined.parquet')
    mbc_table.set_index('combined_lookup', inplace=True)

    mapped_output = mapper_helper.mapper_mbc(df.iloc[:num_rows, :], mbc_table)

else:
    mapped_output = mapper_helper.mapper(df.iloc[:num_rows, :], mbc = mbc_flag)


time_logs['mapper_end'] = monotonic()
console.log("Mapping Complete for {} rows. Took {}s".format(mapped_output.shape[0], round(time_logs['mapper_end'] - time_logs['mapper_start'], 2)))

# console.print(mapped_output.head())
console.print("\nRows with different mlhd_canonical_mbids and received_recording_mbids:")
console.print(mapped_output.received_rec_mbid.compare(mapped_output.mlhd_canonical_mbid))
print()

suffix_str = ('mbc' if mbc_flag else 'mbid-mapper')
url = mapper_helper.write_html(mapped_output, suffix = suffix_str)
mapped_output.to_csv(f'warehouse/mapper_outputs/mlhd_artist_credits_mapped_{suffix_str}_{num_rows}.csv', index=False)

# console.print(f"\nWriting HTML Output to: '{output_path}'")
console.print(f"Writing CSV Output to: 'warehouse/mapper_outputs/mlhd_artist_credits_mapped_{suffix_str}_{num_rows}.csv'")
console.print("URL: '{}'".format(url))