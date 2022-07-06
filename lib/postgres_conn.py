from sqlalchemy import create_engine

engine = create_engine('postgresql://musicbrainz:musicbrainz@localhost/musicbrainz_db')

conn = engine.connect()
get_con = lambda : conn