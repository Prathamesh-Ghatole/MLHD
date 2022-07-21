# To-Do:
"""
1. Load MB tables: 
    - recording: gid, name
    - track: gid, name
    - recording_gid_redirect: new, old
    - mapping.canonical_recording_redirect: new, old
    - artist_gid_redirect: new, old

2. Load MLHD files:
    - drop NaN, duplicates from artist_mbid and recording_mbid
    - pick <timestamp, mlhd_artist_mbid, mlhd_recording_mbid> rows

3. Clean mlhd_recording_mbid:
    - Replace redirecting MBIDs
    - Replace non-canonical MBIDs (In a new column)
    - Get mlhd_recording_names
    - Fetch artist_credit_list w.r.t mlhd_canonical_mbid

4. Clean mlhd_artist_mbid:
    - Replace redirecting MBIDs
    - Get mlhd_artist_names

5. Drop NaN rows w.r.t mlhd_recording_names and mlhd_artist_names


6. map <mlhd_recording_names, mlhd_artist_names> with mbid_mapper.
    - Write received_rec_mbid to new column in data.

7. Write data to CSV
"""

### START OF MAIN ###
import lib.mb as mb
import lib.load as load
import lib.io_ as io
from time import monotonic


### LOADING MB TABLES ###
"""Maybe use Lazy Loading?"""
# MB_rec_name = mb.get_recording_name()
# MB_rec_redir = mb.get_recording_redirects()
# MB_rec_canon = mb.get_recording_canonical()
# MB_track_name = mb.get_track_name()
# MB_artist_redir = mb.get_artist_redirects()
# MB_artist_name = mb.get_artist_name()

### Load MLHD files ###

start_main = monotonic()
df = io.load_path_file('random_file_paths.txt', drop_subset = ['recording_MBID'])

end_main = monotonic()

print(df.head())
print(df.shape)

print(round(end_main - start_main, 3))