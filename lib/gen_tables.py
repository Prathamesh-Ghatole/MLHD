import mb
import os
from time import monotonic
from rich.progress import track
from rich import print
from rich.console import Console

console = Console()
console.clear()

"""Script to fetch and dump essential MB_tables from MusicBrainz DB"""

BASE_PATH = "../warehouse/MB_tables/"
os.makedirs(os.path.dirname(BASE_PATH), exist_ok=True)

console.log("Fetching MB_recording_name...")
MB_recording_name = mb.get_recording_name()
MB_recording_name.to_parquet(BASE_PATH + "recording_name.parquet")

console.log("Fetching MB_recording_redirects...")
MB_recording_redirects = mb.get_recording_redirects()
MB_recording_redirects.to_parquet(BASE_PATH + "recording_redirects.parquet")

console.log("Fetching MB_recording_canonical...")
MB_recording_canonical = mb.get_recording_canonical()
MB_recording_canonical.to_parquet(BASE_PATH + "recording_canonical.parquet")

console.log("Fetching MB_artist_name...")
MB_artist_name = mb.get_artist_name()
MB_artist_name.to_parquet(BASE_PATH + "artist_name.parquet")

console.log("Fetching MB_artist_redirects...")
MB_artist_redirects = mb.get_artist_redirects()
MB_artist_redirects.to_parquet(BASE_PATH + "artist_redirects.parquet")

console.log("Fetching MB_track_name...")
MB_track_name = mb.get_track_name()
MB_track_name.to_parquet(BASE_PATH + "track_name.parquet")

console.log("Fetching MB_track_redirects...")
MB_track_redirects = mb.get_track_redirects()
MB_track_redirects.to_parquet(BASE_PATH + "track_redirects.parquet")

console.log("Fetching MB_track_canonical...")
MB_artist_credit = mb.get_artist_credit()
MB_artist_credit.to_parquet(BASE_PATH + "artist_credit.parquet")