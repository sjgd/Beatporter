"""Main module to run Beatporter."""
import random
import sys
import traceback
from datetime import datetime
from time import sleep

import beatport
import spotify
from config import charts, genres, labels, root_path, shuffle_label, spotify_bkp, username
from spotify import logger
from utils import load_hist_file, save_hist_dataframe

# import argparse

curr_date = datetime.today().strftime("%Y-%m-%d")
option_parse = ["backup", "chart", "genre", "label"]


def dump_tracks(tracks: dict) -> None:
    """Util function to print all tracks in list."""
    i = 1
    for track in tracks:
        logger.info(
            "{}: {} ({}) - {} ({})".format(
                i,
                track["name"],
                track["mix"],
                ", ".join(track["artists"]),
                track["duration"],
            )
        )
        i += 1


def update_hist(master_refresh: bool = False) -> None:
    """Update hist file with configs.

    Args:
        master_refresh: Refresh hist for all playlist of user?
    """
    # TODO: testing, to refine usage, include in first init ?

    df_hist_pl_tracks = load_hist_file()

    charts = {
        beatport.parse_chart_url_datetime(k): beatport.parse_chart_url_datetime(v)
        for k, v in charts.items()
    }

    for chart, chart_bp_url_code in charts.items():
        df_hist_pl_tracks = spotify.update_hist_from_playlist(chart, df_hist_pl_tracks)

    for label, label_bp_url_code in labels.items():
        df_hist_pl_tracks = spotify.update_hist_from_playlist(label, df_hist_pl_tracks)

    if master_refresh:
        # Get track ids from all playlists from username from config
        all_playlists = spotify.get_all_playlists()
        for playlist in all_playlists:
            # logger.info(playlist['name'])
            if playlist["owner"]["id"] == username:
                logger.info(playlist["name"])
                playlist = {"name": playlist["name"], "id": playlist["id"]}
                df_hist_pl_tracks = spotify.update_hist_pl_tracks(
                    df_hist_pl_tracks, playlist
                )

    save_hist_dataframe(df_hist_pl_tracks)


def main(
    spotify_bkp: dict[str, str] = spotify_bkp,
    charts: dict[str, str] = charts,
    genres: dict[str, str] = genres,
    labels: dict[str, str] = labels,
) -> None:
    """Main function to run Beatporter.

    Args:
        spotify_bkp: List of Spotify playlist to backup
        charts: List of Beatport charts to add to Spotify playlists
        genres: List of Beatport genres to add to Spotify playlists
        labels: List of Beatport labels to add to Spotify playlists
    """
    # Init
    start_time = datetime.now()
    logger.info(" ")
    logger.info("[!] Starting @ {}".format(start_time))
    df_hist_pl_tracks = load_hist_file()
    charts = {
        beatport.parse_chart_url_datetime(k): beatport.parse_chart_url_datetime(v)
        for k, v in charts.items()
    }

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
    #         columns=[
    #             "playlist_id",
    #             "playlist_name",
    #             "track_id",
    #             "datetime_added",
    #             "artist_name",
    #         ]
    #     )

    if "backup" in args:
        for playlist_name, org_playlist_id in spotify_bkp.items():
            logger.info(" ")
            logger.info(
                "-Backing up playlist : ***** {} : {} *****".format(
                    playlist_name, org_playlist_id
                )
            )
            try:
                df_hist_pl_tracks = spotify.back_up_spotify_playlist(
                    playlist_name, org_playlist_id, df_hist_pl_tracks
                )
            except Exception as e:
                traceback.print_exc()
                logger.warning(
                    "FAILED backing up playlist: "
                    f"***** {playlist_name} : {org_playlist_id} ***** "
                    f"with error: {e}"
                )

    # Parse lists
    if "chart" in args:
        for chart, chart_bp_url_code in charts.items():
            # TODO check if chart are working, otherwise do as genre and label
            # TODO handle return None, handle chart_bp_url_code has ID already or not
            logger.info(" ")
            logger.info(
                " Getting chart : ***** {} : {} *****".format(chart, chart_bp_url_code)
            )
            chart_url = beatport.find_chart(chart, chart_bp_url_code)

            if chart_url:
                try:
                    tracks_dicts = beatport.get_chart(chart_url)
                    logger.debug(chart_bp_url_code + ":" + str(tracks_dicts))
                    logger.info(
                        "\t[+] Found {} tracks for {}".format(len(tracks_dicts), chart)
                    )
                    df_hist_pl_tracks = spotify.add_new_tracks_to_playlist_chart_label(
                        chart, tracks_dicts, df_hist_pl_tracks
                    )
                except Exception as e:
                    traceback.print_exc()
                    logger.warning(
                        "FAILED getting chart: "
                        f"***** {chart} : {chart_bp_url_code} ***** "
                        f"with error: {e}"
                    )
            else:
                logger.info(f"\t[+] Chart {chart} not found")

    if "genre" in args:
        for genre, genre_bp_url_code in genres.items():
            logger.info(" ")
            logger.info(" Getting genre : ***** {} *****".format(genre))
            top_100_chart = beatport.get_top_100_tracks(genre)
            logger.debug(genre + ":" + str(top_100_chart))
            try:
                df_hist_pl_tracks = spotify.add_new_tracks_to_playlist_genre(
                    genre, top_100_chart, df_hist_pl_tracks
                )
            except Exception as e:
                traceback.print_exc()
                logger.warning(
                    f"FAILED getting genre: ***** {genre} ***** with error: {e}"
                )

    if "label" in args:
        for label, label_bp_url_code in labels.items():
            # TODO avoid looping through all pages if already parsed before ?
            # TODO Add tracks per EP rather than track by track ?
            logger.info(" ")
            logger.info(
                "Getting label : ***** {} : {} *****".format(label, label_bp_url_code)
            )
            try:
                tracks_dict = beatport.get_label_tracks(
                    label, label_bp_url_code, df_hist_pl_tracks
                )
                logger.info("Found {} tracks for {}".format(len(tracks_dict), label))
                if shuffle_label:
                    random.shuffle(tracks_dict)
                df_hist_pl_tracks = spotify.add_new_tracks_to_playlist_chart_label(
                    label, tracks_dict, df_hist_pl_tracks
                )
            except Exception as e:
                traceback.print_exc()
                logger.warning(
                    "FAILED getting label: "
                    f"***** {label} : {label_bp_url_code} ***** "
                    f"with error: {e}"
                )

    # Output
    sleep(5)
    save_hist_dataframe(df_hist_pl_tracks)
    # Save bkp
    df_hist_pl_tracks.to_excel(
        root_path + "data/hist_playlists_tracks_{}.xlsx".format(curr_date), index=False
    )
    end_time = datetime.now()
    logger.info("[!] Done @ {} (Ran for: {})".format(end_time, end_time - start_time))


if __name__ == "__main__":
    main()

    logger.handlers.clear()  # Avoid duplicated handlers

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
