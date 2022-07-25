# To-Do:
"""

1. Load MB tables: 
    [-] recording: gid, name
    - track: gid, name
    [-] recording_gid_redirect: new, old
    [-] mapping.canonical_recording_redirect: new, old
    [-] artist_gid_redirect: new, old

[2.] Load MLHD files:
    [-] drop NaN, duplicates from artist_mbid and recording_mbid
    [-] pick <timestamp, mlhd_artist_mbid, mlhd_recording_mbid> rows

3. Clean mlhd_recording_mbid:
    [-] Replace redirecting MBIDs (In a new column)
    [-] Replace non-canonical MBIDs (In previous column)
    [-] Fetch recording names from recording table
    [-] Fetch artist_credit_list, artist_credit_name w.r.t mlhd_canonical_mbid

# 4. Clean mlhd_artist_mbid:
#     - Replace redirecting MBIDs
#     - Get mlhd_artist_names

[5.] Drop NaN rows w.r.t mlhd_recording_names and mlhd_artist_names

6. map <mlhd_recording_names, mlhd_artist_names> with mbid_mapper.
    - Write received_rec_mbid to new column in data.

7. Write data to CSV
"""

### START OF MAIN ###
import lib.io_ as io
import pandas as pd
from time import monotonic

from rich import print
from rich.console import Console

console = Console()
console.clear()

time_logs = {}

cache_flag = console.input("Load Cached? [Y/n]: ")


if cache_flag.lower() == 'n':

    ### LOADING MB TABLES ###
    """Maybe use Lazy Loading?"""

    time_logs['MB_start'] = monotonic()

    # MB_rec_names = mb.get_recording_name()
    # console.log('loading recording recording names...')
    # MB_rec_names = pd.read_parquet('warehouse/MB_tables/MB_rec_names.parquet')
    # MB_rec_names.set_index('gid', inplace=True)

    # MB_rec_redir = mb.get_recording_redirects()
    console.log('loading recording redirects...')
    MB_rec_redirects = pd.read_parquet('warehouse/MB_tables/MB_rec_redirects.parquet')
    MB_rec_redirects.set_index('old', inplace=True)

    # MB_rec_canonical = mb.get_recording_canonical()
    console.log('loading recording canonical MBIDs...')
    MB_rec_canonical = pd.read_parquet('warehouse/MB_tables/MB_rec_canonical.parquet')
    MB_rec_canonical.set_index('old', inplace=True)

    console.log('loading recording artist credit list...')
    MB_artist_credit_list = pd.read_parquet('warehouse/MB_tables/MB_artist_credit.parquet')
    MB_artist_credit_list.set_index('rec_gid', inplace=True)

    # MB_track_name = mb.get_track_name()
    # MB_artist_redir = mb.get_artist_redirects()
    # MB_artist_name = mb.get_artist_name()

    time_logs['MB_end'] = monotonic()
    console.log("loaded tables. Took {} seconds".format(round(time_logs['MB_end'] - time_logs['MB_start'], 2)))

    ### Load MLHD files ###

    time_logs['load_start'] = monotonic()
    console.log('Loading MLHD files...')

    df = io.load_path_file('random_file_paths.txt', drop_subset = ['recording_MBID', 'artist_MBID'])
    df.drop(['release_MBID'], axis = 1, inplace = True)
    df.drop_duplicates(['artist_MBID', 'recording_MBID'], inplace = True)
    df.rename({'artist_MBID': 'mlhd_artist_mbid', 'recording_MBID': 'mlhd_recording_mbid'}, inplace=True, axis=1)
    df.reset_index(inplace=True, drop=True)

    time_logs['load_end'] = monotonic()
    console.log("loaded MLHD files. Took {} seconds".format(round(time_logs['load_end'] - time_logs['load_start'], 2)))

    ### Clean Up ###
    console.log('Starting Cleanup...')
    time_logs['clean_start'] = monotonic()

    df['mlhd_canonical_mbid'] = df['mlhd_recording_mbid'].map(lambda x: io.replace(x, MB_rec_redirects, 'new'))
    df['mlhd_canonical_mbid'] = df['mlhd_canonical_mbid'].map(lambda x: io.replace(x, MB_rec_canonical, 'new'))
    # df['recording_name'] = df['mlhd_canonical_mbid'].map(lambda x: replace(x, MB_rec_names, 'name'))

    rec_name_artist_credit = df['mlhd_canonical_mbid'].map(lambda x: io.replace_multi(x, MB_artist_credit_list))
    df['rec_name'], df['artist_credit'] = zip(*rec_name_artist_credit)

    df.dropna(subset = ['rec_name', 'artist_credit'], inplace=True)


    time_logs['clean_end'] = monotonic()
    console.log("loaded cleaned and fetched values. Took {} seconds".format(round(time_logs['clean_end'] - time_logs['clean_start'], 2)))
    
    console.log("Writing table to warehouse/mlhd_artist_credits.csv...")
    df.to_csv('warehouse/mlhd_artist_credits.csv', index=False)

else:
    df = pd.read_csv('warehouse/mlhd_artist_credits.csv')

### MBID Mapping ###

print(df.head(5))
# print(df.shape)