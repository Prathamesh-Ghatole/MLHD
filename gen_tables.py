### Script to fetch and dump essential MB_tables from MusicBrainz DB ###

import lib.mb as mb
import lib.io_ as io
import os
from time import monotonic
from rich.progress import track
from rich import print
from rich.console import Console

import config

io.generate_folders()
console = Console()
# console.clear()


BASE_PATH = config.MB_ROOT
os.makedirs(BASE_PATH, exist_ok=True)


def gen_table(table_name, table_func, base_path=BASE_PATH):
    """Generate a table from a function and dump it to a file"""

    if (table_name) in os.listdir(base_path):
        console.log(f"{table_name} already exists, skipping")

    else:
        with console.status(f"Generating {table_name}"):
            table = table_func()
        console.log(f"Generated {table_name}")
        with console.status(
            f"Writing {table_name} to {os.path.join(base_path, table_name)}"
        ):
            table.to_parquet(os.path.join(base_path, table_name))
        console.log(f"Wrote {table_name} to {os.path.join(base_path, table_name)}")


tables_func_name = {
    "artist_credit.parquet": mb.get_artist_credit,
    "artist_credit_release_gid.parquet": mb.get_artist_credit_release_gid,
    "artist_name.parquet": mb.get_artist_name,
    "artist_gid_redirect.parquet": mb.get_artist_redirects,
    "recording_canonical.parquet": mb.get_recording_canonical,
    "recording_gid.parquet": mb.get_recording_gid,
    "recording_name.parquet": mb.get_recording_name,
    "recording_gid_redirect.parquet": mb.get_recording_redirects,
    "release_gid_redirect.parquet": mb.get_release_redirects,
    "release.parquet": mb.get_release,
    "release_canonical.parquet": mb.get_release_canonical,
    "recording_standalone.parquet": mb.get_recording_standalone,
    "canonical_recording_redirects.parquet": mb.get_canonical_recording_redirect,
    "track_name.parquet": mb.get_track_name,
    "track_gid_redirect.parquet": mb.get_track_redirects,
    "mbc_combined.parquet": mb.get_mbc_combined,
}


def generate_tables():
    start = monotonic()
    for table_name, table_func in tables_func_name.items():
        gen_table(table_name, table_func)

    end = monotonic()

    console.log(f"Tables generated in {round(end-start, 2)} seconds")


if __name__ == "__main__":
    generate_tables()