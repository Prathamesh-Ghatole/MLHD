import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="musicbrainz_db",
    user="musicbrainz",
    password="musicbrainz",
    port=5432)

cursor = conn.cursor()

get_con = lambda : conn
get_cursror = lambda : cursor