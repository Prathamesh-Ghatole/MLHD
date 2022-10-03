import lib.postgres_conn as pg
import pandas as pd
from warnings import simplefilter
simplefilter(action='ignore', category=UserWarning)

def get_table(query, limit=None):
    with pg.get_con() as conn:
        try:
            if limit is None:
                df = pd.read_sql(query, con=conn)
            else:
                df = pd.read_sql(query + " LIMIT {}".format(limit), con=conn)
        except:
            raise ValueError("Enter valid limit or recheck query")
        
    return df

### Recordings
def get_recording(limit=None):
    return get_table('SELECT gid, name FROM recording', limit)

def get_recording_name(limit=None):
    return get_table('SELECT gid, name FROM recording', limit)

def get_recording_gid(limit=None):
    return get_table('SELECT gid FROM recording', limit)

def get_recording_redirects(limit=None):
    return get_table("""
    select r.gid AS new, 
    rgr.gid AS old 
    from recording r 
    join recording_gid_redirect rgr 
    on rgr.new_id = r.id""", limit)

def get_recording_canonical(limit=None): 
    return get_table("""
    select recording_mbid as old, 
    canonical_recording_mbid as new 
    from mapping.canonical_recording_redirect""", limit)

def get_canonical_recording_redirect(limit=None): 
    return get_table("""
    select recording_mbid as old, 
    canonical_recording_mbid, canonical_release_mbid
    from mapping.canonical_recording_redirect""", limit)
### Artists

def get_artist(limit=None): 
    return get_table('SELECT gid FROM artist', limit)

def get_artist_name(limit=None): 
    return get_table('SELECT gid, name FROM artist', limit)

def get_artist_redirects(limit=None):
    return get_table("""
    select a.gid AS new, 
    agr.gid AS old from artist a 
    join artist_gid_redirect agr 
    on agr.new_id = a.id""", limit)

### Tracks
def get_track_redirects(limit=None): 
    return get_table("""
    select t.gid AS new, 
    tgr.gid AS old from track t 
    join track_gid_redirect tgr 
    on tgr.new_id = t.id""", limit)

def get_track_redirects_old(limit=None): 
    return get_table('select gid from track_gid_redirect', limit)

def get_tracks(limit=None): 
    return get_table('SELECT gid FROM track', limit)

def get_track_name(limit=None): 
    return get_table('SELECT gid, name FROM track', limit)

def get_artist_credit(limit=None): 
    return get_table("""
    select recording.gid as rec_gid,
    recording.name as rec_name,
    ac.name as artist_credit 
    from recording join artist_credit ac 
    on ac.id=artist_credit;""", limit)

def get_artist_credit_release_gid(limit=None): 
    return get_table("""
    SELECT artist_mbids, release_mbid, recording_mbid 
    FROM mapping.canonical_musicbrainz_data;""", limit)

def get_mbc_combined(limit=None):
    return get_table("""
    SELECT recording_mbid,
    combined_lookup
    FROM mapping.canonical_musicbrainz_data
    """, limit)

def get_recording_standalone(limit=None):
    return get_table("""
    SELECT r.gid
    FROM recording r
    left join track t
    ON r.id = t.recording
    WHERE t.recording IS NULL;
    """, limit)