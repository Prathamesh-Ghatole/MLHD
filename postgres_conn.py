import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="musicbrainz_db",
    user="musicbrainz",
    password="musicbrainz",
    port=5432)

cursor = conn.cursor()

def check_in_recording(rec_mbid):
    cursor.execute("SELECT gid FROM recording WHERE gid=%(id)s;", {'id':rec_mbid})
    fetch = cursor.fetchone()
    
    if type(fetch) == tuple:
        return fetch[0]
    else:
        return None