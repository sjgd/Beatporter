import json
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
from pandas import to_datetime

import pandas as pd

from config import genres, charts, labels, spotify_bkp
from config import overwrite_label
from spotify import find_playlist_chart_label, update_hist_pl_tracks


def get_top_100_playables(genre):
    r = requests.get("https://www.beatport.com/{}/{}/top-100".format("genre" if genres[genre] else "", genres[genre]))
    blob_start = r.text.find("window.Playables") + 19
    blob_end = r.text.find("};", blob_start) + 1
    blob = r.text[blob_start:blob_end].replace("\n", "")
    return json.loads(blob)


def parse_tracks(tracks_json):
    tracks = list()
    for track in tracks_json["tracks"]:
        tracks.append(
            {
                "title": track["title"],
                "name": track["name"],
                "mix": track["mix"],
                "artists": [artist["name"] for artist in track["artists"]],
                "remixers": [remixer["name"] for remixer in track["remixers"]],
                "release": track["release"]["name"],
                "label": track["label"]["name"],
                "published_date": track["date"]["published"],
                "released_date": track["date"]["released"],
                "duration": track["duration"]["minutes"],
                "duration_ms": track["duration"]["milliseconds"],
                "genres": [genre["name"] for genre in track["genres"]],
                "bpm": track["bpm"],
                "key": track["key"]
            }
        )
    return tracks


def get_top_100_tracks(genre):
    # print("[+] Fetching Top 100 {} Tracks".format(genre))
    raw_tracks_dict = get_top_100_playables(genre)
    return parse_tracks(raw_tracks_dict)

def find_chart(chart_bp_url_code):
    r = requests.get("https://www.beatport.com/search?q="+chart_bp_url_code)
    soup = BeautifulSoup(r.text, features="lxml")
    chart_urls = soup.find_all(class_="chart-url")
    chart_urls = ["https://www.beatport.com" + url.attrs['href'] for url in chart_urls]
    reg = re.compile(".*"+chart_bp_url_code+".*") #.replace("-", " ")
    chart_urls = list(filter(reg.match, chart_urls))

    if len(chart_urls) >= 1:
        return chart_urls[0]
    else:
        return None

def get_chart(url):
    """
    :param url: label full url, including beatport.com
    :return: dict of tracks
    """
    r = requests.get(url)
    blob_start = r.text.find("window.Playables") + 19
    blob_end = r.text.find("};", blob_start) + 1
    blob = r.text[blob_start:blob_end].replace("\n", "")
    return parse_tracks(json.loads(blob))

def parse_chart_url_datetime(str):
    return datetime.today().strftime(str)

def get_label_tracks(label, label_bp_url_code, df_hist_pl_tracks, overwrite = overwrite_label):
    """
    :param label: label name
    :param label_bp_url_code: label url code
    :param df_hist_pl_tracks: dataframe of historic track
    :param overwrite: if set to true will reload all tracks anyway, otherwise stops once the date of the last playlist refresh is reached
    :return: dict of tracks
    """

    r = requests.get("https://www.beatport.com/label/{}/tracks?per-page=150".format(label_bp_url_code))
    soup = BeautifulSoup(r.text, features="lxml")
    page_numbers = soup.find_all(class_="pag-number")
    page_numbers = [page.text for page in page_numbers]
    max_page_number = page_numbers[len(page_numbers) - 1]
    label_tracks = []

    playlist = find_playlist_chart_label(label)
    if playlist["id"]:
        df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)
        df_loc_hist = df_hist_pl_tracks.loc[df_hist_pl_tracks.playlist_id == playlist["id"]]
        if len(df_loc_hist.index) > 0:
            last_update = max(df_loc_hist.loc[:, "datetime_added"]).tz_localize(None)
            print("Label {} has {} pages. Last playlist update found {}(UTC): ".format(label, max_page_number,
                                                                                       last_update))
        else:
            last_update = datetime.min
            print("Label {} has {} pages".format(label, max_page_number))
    else:
        last_update = datetime.min
        print("Label {} has {} pages".format(label, max_page_number))

    for i in range(1, int(max_page_number) + 1):
        print("\t[+] Getting label {}, page {}".format(label_bp_url_code, i))
        r = requests.get("https://www.beatport.com/label/{}/tracks?page={}&per-page=150".format(label_bp_url_code, i))
        blob_start = r.text.find("window.Playables") + 19
        blob_end = r.text.find("};", blob_start) + 1
        blob = r.text[blob_start:blob_end].replace("\n", "")
        output = json.loads(blob)
        output = parse_tracks(output)

        label_tracks.extend(output)

        # Check if release date reached last update
        reached_last_update = sum([to_datetime(track['released_date']).tz_localize(None) < last_update for track in output])
        if reached_last_update > 0 and not overwrite:
            print("\t[+] Reached last updated date, stopping")
            break

    label_tracks.reverse()

    return label_tracks

