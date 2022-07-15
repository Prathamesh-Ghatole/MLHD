# from sqlalchemy import create_engine

# engine = create_engine('postgresql://musicbrainz:musicbrainz@localhost/musicbrainz_db')

# # conn = engine.connect()
# def get_con(): 
#     return engine.connect()

from psycopg2 import connect
def get_con():
    conn = connect(
        host="localhost",
        database="musicbrainz_db",
        user="musicbrainz",
        port=5432)

    return conn