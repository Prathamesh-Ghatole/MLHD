import lib.postgres_conn as pg
import pandas as pd

def get_table(query, limit=None):
    with pg.get_con() as conn:
        
        if limit is int and limit > 0:
            df = pd.read_sql(query + " LIMIT {}".format(limit), con=conn)
        else:
            df = pd.read_sql(query, con=conn)
        
    return df

### Recordings
def get_recording(limit=None):
    return get_table('SELECT gid, name FROM recording', limit)

def get_recording_name(limit=None):
    return get_table('SELECT gid, name FROM recording', limit)

def get_recording_redirects(limit=None):
    return get_table('select r.gid AS new, rgr.gid AS old from recording r join recording_gid_redirect rgr on rgr.new_id = r.id', limit)

### Artists

def get_artist(limit=None): 
    return get_table('SELECT gid FROM artist', limit)

def get_artist_redirects(limit=None):
    return get_table('select a.gid AS new, agr.gid AS old from artist a join artist_gid_redirect agr on agr.new_id = a.id', limit)

### Tracks
def get_track_redirects(limit=None): 
    return get_table('select t.gid AS new, tgr.gid AS old from track t join track_gid_redirect tgr on tgr.new_id = t.id', limit)

def get_tracks(limit=None): 
    return get_table('SELECT gid FROM track', limit)

def get_track_name(limit=None): 
    return get_table('SELECT gid, name FROM track', limit)