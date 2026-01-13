"""Main module to run Beatporter."""

import gc
import logging
import os
import random
import sys
import traceback
from datetime import datetime
from time import sleep

import pandas as pd

from src.beatport import (
    find_chart,
    get_chart,
    get_label_tracks,
    get_top_100_tracks,
    parse_chart_url_datetime,
)
from src.config import (
    charts,
    daily_mode,
    genres,
    labels,
    shuffle_label,
    spotify_bkp,
    use_gcp,
    username,
)
from src.gcp import download_file_to_gcs, upload_file_to_gcs
from src.spotify_search import (
    add_new_tracks_to_playlist_chart_label,
    add_new_tracks_to_playlist_genre,
)
from src.spotify_utils import (
    back_up_spotify_playlist,
    dedup_playlists,
    get_all_playlists,
    update_hist_pl_tracks,
)
from src.utils import FILE_NAME_HIST, PATH_HIST_LOCAL, HistoryCache, deduplicate_hist_file

logger = logging.getLogger("beatporter")

curr_date = datetime.today().strftime("%Y-%m-%d")
valid_arguments = [
    "backups",
    "charts",
    "genres",
    "labels",
    "refresh_hist",
    "dedup_playlists",
]


def refresh_all_playlists_history() -> None:
    """Refresh history for all playlists of user."""
    all_playlists = get_all_playlists()
    for playlist in all_playlists:
        if playlist["owner"]["id"] == username:
            logger.info(f"Refreshing history for playlist: {playlist['name']}")
            update_hist_pl_tracks(playlist)


def _transfer_excel_to_parquet_if_needed() -> None:
    """Transfer excel file to parquet if it not exists and the excel does."""
    excel_path = f"{PATH_HIST_LOCAL}hist_playlists_tracks.xlsx"
    parquet_path = f"{PATH_HIST_LOCAL}{FILE_NAME_HIST}"

    if not os.path.exists(parquet_path) and os.path.exists(excel_path):
        logger.info("Transferring excel file to parquet...")
        df = pd.read_excel(excel_path)
        df.to_parquet(parquet_path, compression="gzip", index=False)
        logger.info("Transfer complete.")


def _handle_backups(args: list[str], spotify_bkp: dict[str, str]) -> None:
    if "backups" in args:
        for playlist_name, org_playlist_id in spotify_bkp.items():
            logger.info(" ")
            logger.info(
                f"-Backing up playlist : ***** {playlist_name} : {org_playlist_id} *****"
            )
            try:
                back_up_spotify_playlist(playlist_name, org_playlist_id)
            except Exception as e:
                traceback.print_exc()
                logger.warning(
                    "FAILED backing up playlist: "
                    f"***** {playlist_name} : {org_playlist_id} ***** "
                    f"with error: {e}"
                )
            HistoryCache.clear()
            gc.collect()


def _handle_charts(
    args: list[str],
    parsed_charts: dict[str, str],
) -> None:
    if "charts" in args:
        for chart, chart_bp_url_code in parsed_charts.items():
            # TODO check if chart are working, otherwise do as genre and label
            # TODO handle return None, handle chart_bp_url_code has ID already or not
            logger.info(" ")
            logger.info(f" Getting chart : ***** {chart} : {chart_bp_url_code} *****")
            chart_url = find_chart(chart, chart_bp_url_code)

            if chart_url:
                try:
                    tracks_dicts = get_chart(chart_url)
                    logger.debug(chart_bp_url_code + ":" + str(tracks_dicts))
                    logger.info(f"\t[+] Found {len(tracks_dicts)} tracks for {chart}")
                    add_new_tracks_to_playlist_chart_label(chart, tracks_dicts)
                except Exception as e:
                    traceback.print_exc()
                    logger.warning(
                        "FAILED getting chart: "
                        f"***** {chart} : {chart_bp_url_code} ***** "
                        f"with error: {e}"
                    )
            else:
                logger.info(f"\t[+] Chart {chart} not found")

            chart_url = None
            tracks_dicts = None
            HistoryCache.clear()
            gc.collect()


def _handle_genres(args: list[str], genres: dict[str, str]) -> None:
    if "genres" in args:
        for genre, genre_bp_url_code in genres.items():
            logger.info(" ")
            logger.info(f" Getting genre : ***** {genre} *****")
            top_100_chart = get_top_100_tracks(genre)
            logger.debug(genre + ":" + str(top_100_chart))
            try:
                add_new_tracks_to_playlist_genre(genre, top_100_chart)
            except Exception as e:
                traceback.print_exc()
                logger.warning(
                    f"FAILED getting genre: ***** {genre} ***** with error: {e}"
                )
            top_100_chart = None
            HistoryCache.clear()
            gc.collect()


def _handle_labels(
    args: list[str],
    labels: dict[str, str],
    shuffle_label: bool,
) -> None:
    if "labels" in args:
        for label, label_bp_url_code in labels.items():
            # TODO avoid looping through all pages if already parsed before ?
            # TODO Add tracks per EP rather than track by track ?
            logger.info(" ")
            logger.info(f"Getting label : ***** {label} : {label_bp_url_code} *****")
            try:
                tracks_dict = get_label_tracks(label, label_bp_url_code)
                logger.info(f"Found {len(tracks_dict)} tracks for {label}")
                if shuffle_label:
                    random.shuffle(tracks_dict)
                add_new_tracks_to_playlist_chart_label(label, tracks_dict)
            except Exception as e:
                traceback.print_exc()
                logger.warning(
                    "FAILED getting label: "
                    f"***** {label} : {label_bp_url_code} ***** "
                    f"with error: {e}"
                )
            tracks_dict = None
            HistoryCache.clear()
            gc.collect()


def main(
    spotify_bkp: dict[str, str] = spotify_bkp,
    charts: dict[str, str] = charts,
    genres: dict[str, str] = genres,
    labels: dict[str, str] = labels,
) -> None:
    """Run Beatporter.

    Args:
        spotify_bkp: List of Spotify playlist to backup
        charts: List of Beatport charts to add to Spotify playlists
        genres: List of Beatport genres to add to Spotify playlists
        labels: List of Beatport labels to add to Spotify playlists

    """
    # Init
    start_time = datetime.now()
    logger.info(" ")
    logger.info(f"[!] Starting @ {start_time}")

    _transfer_excel_to_parquet_if_needed()

    if use_gcp:
        download_file_to_gcs(file_name=FILE_NAME_HIST, local_folder=PATH_HIST_LOCAL)

    parsed_charts = {
        parse_chart_url_datetime(k): parse_chart_url_datetime(v)
        for k, v in charts.items()
    }

    # Load arguments
    args = sys.argv[1:]
    args = [arg.replace("-", "") for arg in args]

    if "refresh_hist" in args:
        refresh_all_playlists_history()

    if "dedup_playlists" in args:
        dedup_playlists(
            list(charts.keys())
            + list(labels.keys())
            + [f"{genre} - Top 100" for genre in genres]
            + [f"{genre} - Daily Top" for genre in genres if daily_mode]
        )

    if len(args) == 0:
        # If not argument passed then parse all
        args = valid_arguments
    logger.info(f"Using arguments: {args} of available {valid_arguments}")

    _handle_backups(args, spotify_bkp)
    _handle_charts(args, parsed_charts)
    _handle_genres(args, genres)
    _handle_labels(args, labels, shuffle_label)

    # Output
    sleep(5)
    deduplicate_hist_file()
    if use_gcp:
        upload_file_to_gcs(file_name=FILE_NAME_HIST, local_folder=PATH_HIST_LOCAL)
    # TODO add back option to save to excel for manual checking
    end_time = datetime.now()
    logger.info(f"[!] Done @ {end_time} (Ran for: {end_time - start_time})")


if __name__ == "__main__":
    main()

    logger.handlers.clear()  # Avoid duplicated handlers

# TODO fix match artist name, remove original
# Log could not find track
# Regex out feat. artist2 remove brackets on (extended mix)
# Check to include original mix then remove
# TODO review imports
# TODO modify read me
# TODO shuffle playlist option
# TODO add config to create playlist private per default
# TODO improve search with parsing names contains brackets, commas or special characters :
#  (feat. Aliz Smith) and feat. Griz-O and Feat. Denzel Curry, Pell
# TODO shuffle playlist option
# TODO add config to create playlist private per default
# TODO improve search with parsing names contains brackets, commas or special characters :
#  (feat. Aliz Smith) and feat. Griz-O and Feat. Denzel Curry, Pell
