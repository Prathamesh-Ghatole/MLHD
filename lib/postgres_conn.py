# from sqlalchemy import create_engine

# engine = create_engine('postgresql://musicbrainz:musicbrainz@localhost/musicbrainz_db')

# # conn = engine.connect()
# def get_con(): 
#     return engine.connect()

from config import HOST, DATABASE, USER, PORT
from psycopg2 import connect

def get_con():
    conn = connect(
        host = HOST,
        database = DATABASE,
        user = USER,
        port = PORT
        )
    return conn