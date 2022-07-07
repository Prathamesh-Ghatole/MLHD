from sqlalchemy import create_engine

engine = create_engine('postgresql://musicbrainz:musicbrainz@localhost/musicbrainz_db')

# conn = engine.connect()
def get_con(): 
    return engine.connect()