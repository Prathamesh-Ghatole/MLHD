### Script to fetch and dump essential MB_tables from MusicBrainz DB ###

import pickle
import lib.mb as mb
import lib.io_ as io
import os
from time import monotonic
from rich.console import Console

import config

io.generate_folders()
console = Console()


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
            with open(os.path.join(base_path, table_name), "wb") as fp:
                pickle.dump(table, fp)
        console.log(f"Wrote {table_name} to {os.path.join(base_path, table_name)}")


def get_artist_credit_release_gid():
    data = mb.get_artist_credit_release_gid()
    data.set_index("recording_mbid", inplace=True)
    return data.to_dict('index')

def get_artist_redirects():
    data = mb.get_artist_redirects()
    data.set_index("old", inplace=True)
    return data.to_dict('index')

def get_recording_canonical():
    data = mb.get_recording_canonical()
    data.set_index("old", inplace=True)
    return data.to_dict('index')

def get_recording_gid():
    data = mb.get_recording_gid()
    data.set_index("gid", inplace=True)
    return data.to_dict('index')

def get_recording_redirects():
    data = mb.get_recording_redirects()
    data.set_index("old", inplace=True)
    return data.to_dict('index')

def get_release_redirects():
    data = mb.get_release_redirects()
    data.set_index("old", inplace=True)
    return data.to_dict('index')

def get_release():
    data = mb.get_release()
    data.set_index("release_mbid", inplace=True)
    return data.to_dict('index')

def get_release_canonical():
    data = mb.get_release_canonical()
    data.set_index("release_mbid", inplace=True)
    return data.to_dict('index')


tables_func_name = {
    "artist_credit_release_gid.pkl": get_artist_credit_release_gid,
    "artist_gid_redirect.pkl": get_artist_redirects,
    "recording_canonical.pkl": get_recording_canonical,
    "recording_gid.pkl": get_recording_gid,
    "recording_gid_redirect.pkl": get_recording_redirects,
    "release_gid_redirect.pkl": get_release_redirects,
    "release.pkl": get_release,
    "release_canonical.pkl": get_release_canonical,
}


def generate_tables():
    start = monotonic()
    for table_name, table_func in tables_func_name.items():
        gen_table(table_name, table_func)

    end = monotonic()

    console.log(f"Tables generated in {round(end-start, 2)} seconds")


if __name__ == "__main__":
    generate_tables()