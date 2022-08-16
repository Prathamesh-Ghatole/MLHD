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

# MB_rec_names = mb.get_recording_name()
# console.log('loading recording recording names...')
# MB_rec_names = pd.read_parquet('warehouse/MB_tables/recording_names.parquet')
# MB_rec_names.set_index('gid', inplace=True)

# MB_rec_redir = mb.get_recording_redirects()
console.log('loading recording redirects...')
MB_rec_redirects = pd.read_parquet('warehouse/MB_tables/recording_redirects.parquet')
MB_rec_redirects.set_index('old', inplace=True)

# MB_rec_canonical = mb.get_recording_canonical()
console.log('loading recording canonical MBIDs...')
MB_rec_canonical = pd.read_parquet('warehouse/MB_tables/recording_canonical.parquet')
MB_rec_canonical.set_index('old', inplace=True)

console.log('loading artist credit list...')
MB_artist_credit_list = pd.read_parquet('warehouse/MB_tables/artist_credit.parquet')
MB_artist_credit_list.set_index('rec_gid', inplace=True)

# MB_track_name = mb.get_track_name()
# MB_artist_redir = mb.get_artist_redirects()
# MB_artist_name = mb.get_artist_name()

time_logs['MB_end'] = monotonic()
console.log("loaded MB tables. Took {} seconds".format(round(time_logs['MB_end'] - time_logs['MB_start'], 2)))

time_logs['load_start'] = monotonic()

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

df['mlhd_canonical_mbid'] = df['mlhd_recording_mbid'].map(lambda x: io.replace(x, MB_rec_redirects, 'new'))
df['mlhd_canonical_mbid'] = df['mlhd_canonical_mbid'].map(lambda x: io.replace(x, MB_rec_canonical, 'new'))
# df['recording_name'] = df['mlhd_canonical_mbid'].map(lambda x: replace(x, MB_rec_names, 'name'))

rec_name_artist_credit = df['mlhd_canonical_mbid'].map(lambda x: io.replace_multi(x, MB_artist_credit_list))
df['rec_name'], df['artist_credit'] = zip(*rec_name_artist_credit)

console.log("Dropping {} rows with missing artist_credit/rec_mbid".format(df.shape[0] - len(df.dropna(subset = ['rec_name', 'artist_credit']))))
df.dropna(subset = ['rec_name', 'artist_credit'], inplace=True)
df.to_csv('warehouse/mapper_outputs/mlhd_artist_credits_test.csv', index=False)

time_logs['clean_end'] = monotonic()

console.log("Cleaned {} rows. Took {} seconds".format(
    df.shape[0], 
    round(time_logs['clean_end'] - time_logs['clean_start'], 2))
    )