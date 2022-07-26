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
    console.log("loaded tables. Took {} seconds".format(round(time_logs['MB_end'] - time_logs['MB_start'], 2)))
    
    df = io.load_path_file('warehouse/samples/random_file_paths.txt', drop_subset = ['recording_MBID', 'artist_MBID'])
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
    df.to_csv('warehouse/mapper_outputs/mlhd_artist_credits.csv', index=False)

    time_logs['clean_end'] = monotonic()
    console.log("loaded cleaned and fetched values. Took {} seconds".format(round(time_logs['clean_end'] - time_logs['clean_start'], 2)))

else:
    time_logs['load_start'] = monotonic()
    
    df = pd.read_csv('warehouse/mapper_outputs/mlhd_artist_credits.csv')
    
    time_logs['load_end'] = monotonic()
    console.log("loaded cleaned MLHD files. Took {} seconds".format(round(time_logs['load_end'] - time_logs['load_start']), 2))
### Mapper ###

try:
    num_rows = int(console.input("\nHow many rows do you want to process? [1000]: "))
except ValueError:
    num_rows = 1000

if (num_rows < 0) or (num_rows > df.shape[0]):
    num_rows = 1000

mbc_flag = console.input("\nUse mbc? [Y/n]: ")
if mbc_flag.lower() == 'n':
    mbc_flag = False
else:
    mbc_flag = True

console.print()

time_logs['mapper_start'] = monotonic()

console.log("Mapping MBIDs...")
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