"""Main module to run Beatporter."""

import gc
import logging
import os
import queue
import random
import sys
import threading
from datetime import datetime
from time import sleep

import pandas as pd

from src.beatport import (
    BeatportBrowser,
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
from src.utils import FILE_NAME_HIST, PATH_HIST_LOCAL, deduplicate_hist_file

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
            gc.collect()
            logger.info("")

    del all_playlists
    gc.collect()


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
                f"Backing up playlist : ***** {playlist_name} : {org_playlist_id} *****"
            )
            try:
                back_up_spotify_playlist(playlist_name, org_playlist_id)
            except Exception as e:
                logger.error(
                    "FAILED backing up playlist: "
                    f"***** {playlist_name} : {org_playlist_id} ***** "
                    f"with error: {e}"
                )
            gc.collect()


def _scrape_job(job: dict) -> dict | None:
    """Scrape data for a single job and return a result dict."""
    job_type = job["type"]
    job_data = job["data"]

    try:
        if job_type == "chart":
            chart, chart_bp_url_code = job_data
            logger.info(f"[Scraper] Scraping chart: {chart}")
            chart_url = find_chart(chart, chart_bp_url_code)
            if chart_url:
                chart_uri = chart_url.split("/chart/")[-1]
                logger.info(f"[Scraper] Found chart URI: {chart_uri}")
                tracks_dicts = get_chart(chart_url)
                return {
                    "type": "chart",
                    "name": chart,
                    "tracks": tracks_dicts,
                    "code": chart_bp_url_code,
                    "uri": chart_uri,
                }
            else:
                logger.warning(f"[Scraper] Chart {chart} not found")

        elif job_type == "genre":
            genre = job_data
            logger.info(f"[Scraper] Scraping genre: {genre}")
            top_100_chart = get_top_100_tracks(genre)
            return {"type": "genre", "name": genre, "tracks": top_100_chart}

        elif job_type == "label":
            label, label_bp_url_code, shuffle_label = job_data
            logger.info(f"[Scraper] Scraping label: {label}")
            tracks_dict = get_label_tracks(label, label_bp_url_code)
            return {
                "type": "label",
                "name": label,
                "tracks": tracks_dict,
                "code": label_bp_url_code,
                "shuffle": shuffle_label,
            }
    except Exception as e:
        logger.error(f"[Scraper] FAILED scraping {job_type}: {job_data} - error: {e}")

    return None


def _sync_result(result: dict) -> None:
    """Sync a single scraping result to Spotify."""
    res_type = result["type"]
    name = result["name"]
    tracks = result["tracks"]

    try:
        if res_type == "chart":
            logger.info(" ")
            logger.info(f"Syncing chart : ***** {name} *****")
            add_new_tracks_to_playlist_chart_label(name, tracks, uri=result.get("uri"))

        elif res_type == "genre":
            logger.info(" ")
            logger.info(f"Syncing genre : ***** {name} *****")
            add_new_tracks_to_playlist_genre(name, tracks)

        elif res_type == "label":
            logger.info(" ")
            logger.info(f"Syncing label : ***** {name} *****")
            if result["shuffle"]:
                random.shuffle(tracks)
            add_new_tracks_to_playlist_chart_label(name, tracks, uri=result["code"])

    except Exception as e:
        logger.error(f"[Sync] FAILED syncing {res_type} {name} to Spotify: {e}")
    finally:
        gc.collect()


def _scraper_producer(job_queue: list[dict], result_queue: queue.Queue) -> None:
    """Thread function to process the job queue and put results into the result queue."""
    logger.info("[Scraper] Thread started.")
    try:
        for job in job_queue:
            result = _scrape_job(job)
            if result:
                result_queue.put(result)
            # Small sleep between jobs to be nice to Beatport
            sleep(2)
    finally:
        # Close browser and send sentinel
        BeatportBrowser.quit()
        result_queue.put(None)
        logger.info("[Scraper] Thread finished.")


def main(
    spotify_bkp: dict[str, str] = spotify_bkp,
    charts: dict[str, str] = charts,
    genres: dict[str, str] = genres,
    labels: dict[str, str] = labels,
) -> None:
    """Run Beatporter with optimized Scrape-Sync pipeline.

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

    # 1. Build Job Queue for scraping tasks
    job_queue = []
    if "charts" in args:
        for chart, code in parsed_charts.items():
            job_queue.append({"type": "chart", "data": (chart, code)})
    if "genres" in args:
        for genre in genres.keys():
            job_queue.append({"type": "genre", "data": genre})
    if "labels" in args:
        for label, code in labels.items():
            job_queue.append({"type": "label", "data": (label, code, shuffle_label)})

    # 2. Start Scraper Producer Thread
    result_queue = queue.Queue()
    scraper_thread = None
    if job_queue:
        scraper_thread = threading.Thread(
            target=_scraper_producer, args=(job_queue, result_queue), name="ScraperThread"
        )
        scraper_thread.start()

    # 3. Handle backups in Main Thread while scraping happens in background
    _handle_backups(args, spotify_bkp)

    # 4. Process Scraping Results (Consumer) in Main Thread
    if scraper_thread:
        logger.info("[Main] Waiting for scraping results...")
        while True:
            result = result_queue.get()
            if result is None:  # Sentinel value
                break
            _sync_result(result)
            result_queue.task_done()

    # Output
    sleep(5)
    deduplicate_hist_file()
    if use_gcp:
        upload_file_to_gcs(file_name=FILE_NAME_HIST, local_folder=PATH_HIST_LOCAL)

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
