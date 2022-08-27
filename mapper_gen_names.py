# Cleans mlhd_recording_mbid,
# Fetches rec_name, artist_credit for given set of files.

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

time_logs['load_start'] = monotonic()

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

# df['mlhd_canonical_mbid'] = df['mlhd_recording_mbid'].map(lambda x: io.replace(x, MB_rec_redirects, 'new'))
# df['mlhd_canonical_mbid'] = df['mlhd_canonical_mbid'].map(lambda x: io.replace(x, MB_rec_canonical, 'new'))
# df['recording_name'] = df['mlhd_canonical_mbid'].map(lambda x: replace(x, MB_rec_names, 'name'))

# rec_name_artist_credit = df['mlhd_canonical_mbid'].map(lambda x: io.replace_multi(x, MB_artist_credit_list))
# df['rec_name'], df['artist_credit'] = zip(*rec_name_artist_credit)

# console.log("Dropping {} rows with missing artist_credit/rec_mbid".format(df.shape[0] - len(df.dropna(subset = ['rec_name', 'artist_credit']))))
# df.to_csv('warehouse/mapper_outputs/mlhd_artist_credits_test_nodrop.csv', index=False)

# df.dropna(subset = ['rec_name', 'artist_credit'], inplace=True)

shape_before = df.shape[0]
df1 = mapper_helper.clean_rec(df, rec_gid_set, MB_rec_redirects, MB_rec_canonical, MB_artist_credit_list)
shape_after = df.shape[0]

df.to_csv('warehouse/mapper_outputs/mlhd_artist_credits_test.csv', index=False)
console.log(f"Dropped {shape_before - shape_after} rows")


time_logs['clean_end'] = monotonic()

console.log("Cleaned {} rows. Took {} seconds".format(
    df.shape[0], 
    round(time_logs['clean_end'] - time_logs['clean_start'], 2))
    )