import sys
from os import path
import spotify
import beatport
from datetime import datetime
import pandas as pd
from config import username, shuffle_label, folder_path
from config import charts, spotify_bkp, genres, labels
from time import sleep
import random

# import argparse
import logging
from logging.handlers import RotatingFileHandler

file_name_hist = "hist_playlists_tracks.pkl"
curr_date = datetime.today().strftime("%Y-%m-%d")
option_parse = ["backup", "chart", "genre", "label"]

logFile = "runtime-beatporter.log"
logging.basicConfig(format="%(message)s", level=logging.DEBUG)
logger = logging.getLogger()
logger.handlers.clear()  # Avoid duplicated handlers

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# fileh = RotatingFileHandler(logFile, mode="a", maxBytes=50 * 1024 * 1024, backupCount=5, encoding=None, delay=False)
fileh = RotatingFileHandler(logFile, "w", maxBytes=1000)
formatter = logging.Formatter("%(asctime)s - %(message)s")
fileh.setFormatter(formatter)
fileh.setLevel(logging.INFO)  # Added
logger.addHandler(fileh)

fileh = RotatingFileHandler("runtime-beatporter-debug.log", "w", maxBytes=50 * 1024 * 1024)
formatter = logging.Formatter("%(asctime)s - %(message)s")
fileh.setFormatter(formatter)
fileh.setLevel(logging.DEBUG)  # Added
logger.addHandler(fileh)


def dump_tracks(tracks):
    i = 1
    for track in tracks:
        logging.info(
            "{}: {} ({}) - {} ({})".format(
                i, track["name"], track["mix"], ", ".join(track["artists"]), track["duration"]
            )
        )
        i += 1


def load_hist_file():
    """
    :return: Returns existing history file of track ID per playlist
    """
    # TODO arguments for file path / type ?
    if path.exists(folder_path + "hist_playlists_tracks.xlsx"):
        df_hist_pl_tracks = pd.read_excel(folder_path + "hist_playlists_tracks.xlsx")
    else:
        df_hist_pl_tracks = pd.DataFrame(
            columns=["playlist_id", "playlist_name", "track_id", "datetime_added", "artist_name"]
        )

    return df_hist_pl_tracks


def update_hist(master_refresh=False):
    # TODO: testing, to refine usage, include in first init ?

    df_hist_pl_tracks = load_hist_file()

    charts = {beatport.parse_chart_url_datetime(k): beatport.parse_chart_url_datetime(v) for k, v in charts.items()}

    for chart, chart_bp_url_code in charts.items():
        df_hist_pl_tracks = spotify.update_hist_from_playlist(chart, df_hist_pl_tracks)

    for label, label_bp_url_code in labels.items():
        df_hist_pl_tracks = spotify.update_hist_from_playlist(label, df_hist_pl_tracks)

    if master_refresh:
        # Get track ids from all playlists from username from config
        all_playlists = spotify.get_all_playlists()
        for playlist in all_playlists:
            # logging.info(playlist['name'])
            if playlist["owner"]["id"] == username:
                logging.info(playlist["name"])
                playlist = {"name": playlist["name"], "id": playlist["id"]}
                df_hist_pl_tracks = spotify.update_hist_pl_tracks(df_hist_pl_tracks, playlist)

    df_hist_pl_tracks = df_hist_pl_tracks.loc[
        :, ["playlist_id", "playlist_name", "track_id", "datetime_added", "artist_name"]
    ]
    df_hist_pl_tracks.to_pickle(folder_path + file_name_hist)
    df_hist_pl_tracks.to_excel(folder_path + "hist_playlists_tracks.xlsx", index=False)


def main(spotify_bkp=spotify_bkp, charts=charts, genres=genres, labels=labels):
    # Init
    start_time = datetime.now()
    logging.info("\n[!] Starting @ {}".format(start_time))
    df_hist_pl_tracks = load_hist_file()
    charts = {beatport.parse_chart_url_datetime(k): beatport.parse_chart_url_datetime(v) for k, v in charts.items()}

    # Load arguments
    args = sys.argv[1:]
    args = [arg.replace("-", "") for arg in args]
    if len(args) == 0:
        # If not argument passed then parse all
        args = option_parse

    # if path.exists(folder_path + file_name_hist):
    #     df_hist_pl_tracks = pd.read_pickle(folder_path + file_name_hist)
    # else:
    #     df_hist_pl_tracks = pd.DataFrame(
    #         columns=["playlist_id", "playlist_name", "track_id", "datetime_added", "artist_name"]
    #     )

    if "backup" in args:
        for playlist_name, org_playlist_id in spotify_bkp.items():
            logging.info("\n-Backing up playlist : ***** {} : {} *****".format(playlist_name, org_playlist_id))
            df_hist_pl_tracks = spotify.back_up_spotify_playlist(playlist_name, org_playlist_id, df_hist_pl_tracks)

    # Parse lists
    if "chart" in args:
        for chart, chart_bp_url_code in charts.items():
            # TODO handle return None, handle chart_bp_url_code has ID already or not
            logging.info("\n-Getting chart : ***** {} : {} *****".format(chart, chart_bp_url_code))
            chart_url = beatport.find_chart(chart_bp_url_code)

            if chart_url:
                tracks_dict = beatport.get_chart(beatport.find_chart(chart_bp_url_code))
                logging.debug(chart_bp_url_code + ":" + str(tracks_dict))
                logging.info("\t[+] Found {} tracks for {}".format(len(tracks_dict), chart))
                df_hist_pl_tracks = spotify.add_new_tracks_to_playlist_chart_label(
                    chart, tracks_dict, df_hist_pl_tracks
                )
            else:
                logging.info("\t[+] Chart not found")

    if "genre" in args:
        for genre, genre_bp_url_code in genres.items():
            logging.info("\n-Getting genre : ***** {} *****".format(genre))
            top_100_chart = beatport.get_top_100_tracks(genre)
            logging.debug(genre + ":" + str(top_100_chart))
            df_hist_pl_tracks = spotify.add_new_tracks_to_playlist_genre(genre, top_100_chart, df_hist_pl_tracks)

    if "label" in args:
        for label, label_bp_url_code in labels.items():
            # TODO avoid looping through all pages if already parsed before ?
            # TODO Add tracks per EP rather than track by track ?
            logging.info("\n-Getting label : ***** {} : {} *****".format(label, label_bp_url_code))
            tracks_dict = beatport.get_label_tracks(label, label_bp_url_code, df_hist_pl_tracks)
            logging.info("Found {} tracks for {}".format(len(tracks_dict), label))
            if shuffle_label:
                random.shuffle(tracks_dict)
            df_hist_pl_tracks = spotify.add_new_tracks_to_playlist_chart_label(label, tracks_dict, df_hist_pl_tracks)

    # Output
    logging.info("\n-Saving file")
    sleep(5)  # try to avoid read-write errors if running too quickly
    df_hist_pl_tracks = df_hist_pl_tracks.loc[
        :, ["playlist_id", "playlist_name", "track_id", "datetime_added", "artist_name"]
    ]
    df_hist_pl_tracks.to_pickle(file_name_hist)
    df_hist_pl_tracks.to_excel(folder_path + "hist_playlists_tracks.xlsx", index=False)
    # Save bkp
    df_hist_pl_tracks.to_excel("hist_playlists_tracks_{}.xlsx".format(curr_date), index=False)
    end_time = datetime.now()
    logging.info("[!] Done @ {}\n (Ran for: {})".format(end_time, end_time - start_time))


if __name__ == "__main__":
    main()

# TODO fix match artist name, remove original
# Log could not find track
# Regex out feat. artist2 remove brackets on (extended mix)
# Check to include original mix then remove
# TODO check error on pickle
# TODO review imports
# TODO modify read me
# TODO shuffle playlist option
# TODO add config to create playlist private per default
# TODO improve search with parsing names contains brackets, commas or special characters :
#  (feat. Aliz Smith) and feat. Griz-O and Feat. Denzel Curry, Pell
