"""Utils module."""

import gc
import logging
import multiprocessing
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

    Args:
        allow_empty (bool): If True, returns an empty DataFrame
         when the file does not exist; otherwise, raises an error.

    Returns:
        pd.DataFrame: Existing history file of track ID per playlist.

    Raises:
        ValueError: If the file does not exist and allow_empty is False.

    """
    try:
        if use_gcp:
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
            print_memory_usage_readable()
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
        expected_paths = []
        if use_gcp:
            expected_paths.append(PATH_HIST_LOCAL + FILE_NAME_HIST)
        if use_local:
            expected_paths.append(folder_path + "hist_playlists_tracks.xlsx")
        raise ValueError(
            f"File does not exist at the expected path(s): {', '.join(expected_paths)}. "
            "Creating an empty DataFrame is not allowed (allow_empty=False)."
        )

    return df_hist_pl_tracks


def _save_hist_file_proc(df_hist_pl_tracks: pd.DataFrame) -> None:
    """Save the history dataframe in a separate process."""
    # NOTE: This function runs in a separate process.
    # Logging from here might not be captured by the main process's handlers.
    proc_logger = logging.getLogger("proc_saver")

    try:
        df_hist_pl_tracks.to_parquet(
            PATH_HIST_LOCAL + FILE_NAME_HIST, compression="gzip", index=False
        )
        if use_gcp:
            upload_file_to_gcs(file_name=FILE_NAME_HIST, local_folder=PATH_HIST_LOCAL)
        if use_local:
            df_hist_pl_tracks.to_excel(
                folder_path + "hist_playlists_tracks.xlsx", index=False
            )
        proc_logger.info(
            f"Successfully saved hist file with {df_hist_pl_tracks.shape[0]:,} records"
        )
    except Exception as e:
        proc_logger.error(f"Failed to save hist file in subprocess: {e}", exc_info=True)


def save_hist_dataframe(df_hist_pl_tracks: pd.DataFrame) -> None:
    """Save the history dataframe according to configs in a separate process."""
    logger.info("Saving history file in a separate process to manage memory.")
    print_memory_usage_readable()
    sleep(1)

    ctx = multiprocessing.get_context("spawn")
    p = ctx.Process(target=_save_hist_file_proc, args=(df_hist_pl_tracks,))
    p.start()
    p.join()

    if p.exitcode != 0:
        logger.error("Subprocess for saving history file failed.")

    gc.collect()

    logger.info("Subprocess finished. Memory usage in main process:")
    print_memory_usage_readable()


def print_memory_usage_readable() -> None:
    """Print the total memory size used by the current process."""
    process = psutil.Process(os.getpid())
    memory_usage_bytes = process.memory_info().rss
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if memory_usage_bytes < 1024.0:
            logging.getLogger(__file__).info(
                f"Memory usage: {memory_usage_bytes:.2f} {unit}"
            )
            return
        memory_usage_bytes /= 1024.0


configure_logging()
