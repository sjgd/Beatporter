"""Utils module."""

import gc
import logging
import os
from datetime import datetime
from os import path
from time import sleep

import pandas as pd
import psutil

from src.config import ROOT_PATH, folder_path, use_gcp, use_local
from src.configure_logging import configure_logging
from src.gcp import download_file_to_gcs, upload_file_to_gcs

PATH_HIST_LOCAL = ROOT_PATH + "data/"
FILE_NAME_HIST = "hist_playlists_tracks.parquet.gz"
curr_date = datetime.today().strftime("%Y-%m-%d")

logger = logging.getLogger("utils")


def load_hist_file(allow_empty: bool = False) -> pd.DataFrame:
    """Load the hist file according to folder path in configs.

    Returns:
        Returns existing history file of track ID per playlist

    """
    try:
        if use_gcp:
            # TODO arguments for file path / type ?
            download_file_to_gcs(file_name=FILE_NAME_HIST, local_folder=PATH_HIST_LOCAL)
            df_hist_pl_tracks = pd.read_parquet(PATH_HIST_LOCAL + FILE_NAME_HIST)
            logger.info(" ")
            logger.info(
                f"Successfully loaded hist file with {df_hist_pl_tracks.shape[0]} records"
            )
            gc.collect()
            for col in df_hist_pl_tracks.columns:
                try:
                    df_hist_pl_tracks[col] = df_hist_pl_tracks[col].astype(
                        pd.StringDtype()
                    )
                except Exception as e:
                    logger.warning(e)
            return df_hist_pl_tracks
    except Exception as e:
        logger.warning(
            "Loading from GCP failed while the option is selected. "
            "Trying loading from local saved file."
            f"Error {e}"
        )
    if use_local and path.exists(folder_path + "hist_playlists_tracks.xlsx"):
        df_hist_pl_tracks = pd.read_excel(
            folder_path + "hist_playlists_tracks.xlsx", dtype=pd.StringDtype()
        )
        logger.info(" ")
        logger.info(
            f"Successfully loaded hist file with {df_hist_pl_tracks.shape[0]} records"
        )
        return df_hist_pl_tracks
    if allow_empty:
        df_hist_pl_tracks = pd.DataFrame(
            columns=[
                "playlist_id",
                "playlist_name",
                "track_id",
                "datetime_added",
                "artist_name",
            ]
        )
    else:
        raise ValueError("File does not exist and create empty is not allowed")

    # Alternative method
    # for col in [
    #     "playlist_id",
    #     "playlist_name",
    #     "track_id",
    #     "artist_name",
    # ]:
    #     df_hist_pl_tracks[col] = df_hist_pl_tracks[col].astype(pd.StringDtype())

    # for col in [
    #     "playlist_id",
    #     "playlist_name",
    #     "track_id",
    #     "artist_name",
    # ]:
    #     max_length = int(max(df_hist_pl_tracks[col].apply(lambda x: len(x))) * 1.2)
    #     print(col, f"|S{max_length}")
    #     if max_length < 128:
    #         try:
    #             df_hist_pl_tracks[col] = (
    #                   df_hist_pl_tracks[col].astype(f"|S{max_length}"))
    #         except Exception as e:
    #             print(e)

    return df_hist_pl_tracks


def save_hist_dataframe(df_hist_pl_tracks: pd.DataFrame) -> None:
    """Save the history dataframe according to configs."""
    logger = logging.getLogger()
    print_memory_usage_readable()
    sleep(1)  # try to avoid read-write errors if running too quickly
    logger.debug("Saving file of length: " + str(len(df_hist_pl_tracks)))
    df_hist_pl_tracks = df_hist_pl_tracks.loc[
        :, ["playlist_id", "playlist_name", "track_id", "datetime_added", "artist_name"]
    ]
    df_hist_pl_tracks.to_parquet(
        PATH_HIST_LOCAL + FILE_NAME_HIST, compression="gzip", index=False
    )
    if use_gcp:
        upload_file_to_gcs(file_name=FILE_NAME_HIST, local_folder=PATH_HIST_LOCAL)
    if use_local:
        df_hist_pl_tracks.to_excel(
            folder_path + "hist_playlists_tracks.xlsx", index=False
        )
    logger.info(
        f"Successfully saved hist file with {df_hist_pl_tracks.shape[0]:,} records"
    )

    _ = gc.collect()
    print_memory_usage_readable()


def print_memory_usage_readable() -> None:
    """Print the total memory size used by the current process.

    Print it in a human-readable format.

    """
    # Get the current process
    process = psutil.Process(os.getpid())

    # Get memory usage in bytes
    memory_usage_bytes = process.memory_info().rss  # Resident Set Size

    # Convert bytes to a readable format
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if memory_usage_bytes < 1024.0:
            logging.getLogger(__file__).info(
                f"Memory usage: {memory_usage_bytes:.2f} {unit}"
            )
            return None
        memory_usage_bytes /= 1024.0


configure_logging()
