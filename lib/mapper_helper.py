import pandas as pd
import requests
import requests_cache
from jinja2 import Environment, BaseLoader
from datetime import datetime

jenv = Environment(loader=BaseLoader)
requests_cache.install_cache('../warehouse/mlhd_cache', expire_after=86400, allowable_methods=['GET', 'POST'])
# 86400 seconds = 24 hours. i.e. Cache will be cleared after 24 hours.

def gen_chunk(df, chunksize=15):
    """Breaks the dataframe into chunks of size "chunksize".

    Args:
        df (Pandas.DataFrame): input dataframe
        chunksize (int, optional): _description_. Defaults to 15.

    Yields:
        Pandas.DataFrame: A DataFrame of size <= "chunksize".
    """
    for i in range(0, len(df), chunksize):
        yield df[i:i+chunksize]

def make_payload(df):
    """Takes in a dataframe and returns a list of dictionaries with artist_credit_name and recording_name in required format.

    Args:
        df (Pandas.DataFrame): Input DataFrame containing columns artist_credit and rec_name

    Returns:
        list: A list of dictionaries with artist_credit_name and recording_name in required format.
    """
    payload = []
    try:
        series_artist_name = df.artist_credit.to_list()
        series_rec_name = df.rec_name.to_list()
    except AttributeError:
        print(df)
    
    for artist_name, rec_name in zip(series_artist_name, series_rec_name):
        json_inp = dict()
        json_inp["[artist_credit_name]"] = artist_name
        json_inp["[recording_name]"] = rec_name
        payload.append(json_inp)
    return payload

def get_mapping(payload, mbc = True):
    """Function to get post request reply from payload

    Args:
        payload list: a list of dictionaries with artist_credit_name and recording_name in required format

    Returns:
        list: list of dictionaries where each dictionary is a reply for a row.
    """
    if mbc==False:
        base_url = "https://labs.api.listenbrainz.org/mbid-mapping/json"
    else:
        base_url = "https://datasets.listenbrainz.org/mbc-lookup/json"
    r = requests.post(base_url, json = payload)
    try:
        return r.json()
    except:
        return "[{}]"

def extract_mapping(replies: list, features = ['artist_credit_arg', 'recording_arg', 'recording_mbid']):
    """takes replies received from get_mapping function and extracts the required features.

    Args:
        replies (list): replies received from get_mapping function
        features (list, optional): _description_. Defaults to ['artist_credit_arg', 'recording_arg', 'recording_mbid'].

    Returns:
        list: list of tuples where each tuple is a row of the extracted features.
    """
    return list(tuple(reply[feature] for feature in features) for reply in replies)

def mapper(df_input, mbc = True):
    """Master Function to fetch and map replies from mbid-mapper to input dataframe"""
    replies = []
    for chunk in gen_chunk(df_input, 15):
        # payload = make_payload((chunk))
        reply = extract_mapping(get_mapping(make_payload(chunk), mbc = mbc))
        replies.extend(reply)
    
    reply_df = pd.DataFrame(replies)
    reply_df.set_index([0, 1], inplace=True)
    reply_df.rename(columns={2: 'received_rec_mbid'}, inplace=True)
    
    return df_input.join(reply_df, on = ['artist_credit', 'rec_name'], how='left')


def write_html(df, base_path='/home/snaek/public_html/', suffix='mbc'):
    template = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
</head>
<body>

<table cellspacing="1" bgcolor="#000000" cellpadding="5">
<tr bgcolor="#ffffff"><th>Artist</th><th>Recording</th><th>Canonical MBID</th><th>Canonical Data Lookup</th></tr>
{% for key,value in df.iterrows() %}
<tr bgcolor="#ffffff">
<td>{{value['artist_credit']}}</td>
<td>{{value['rec_name']}}</td>
<td><a href="https://musicbrainz.org/recording/{{value['mlhd_canonical_mbid']}}">{{value['mlhd_canonical_mbid']}}</a></td>
<td><a href="https://musicbrainz.org/recording/{{value['received_rec_mbid']}}">{{value['received_rec_mbid']}}</a> (<a href="https://datasets.listenbrainz.org/mbc-lookup?%5Bartist_credit_name%5D={{value['artist_credit']}}&%5Brecording_name%5D={{value['rec_name']}}">lookup</a>)</td>
</tr>
{% endfor %}
</table>

</body>
</html>
"""
    f_name = "mlhd-lookup-{}-{}.html".format(datetime.now().strftime("%y-%m-%d"), suffix)
    jtemplate = jenv.from_string(template)
    different = df[df.mlhd_canonical_mbid!=df.received_rec_mbid]
    report_html = jtemplate.render(df=different)
    
    with open(base_path+f_name, "w", encoding="utf-8") as fp:
        fp.write(report_html)

    # path = base_path+f_name
    url = "https://wolf.metabrainz.org/~snaek/{}".format(f_name)
    
    return (url)