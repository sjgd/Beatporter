"""Utils module."""

import gc
import logging
import multiprocessing
import os
from datetime import datetime
from time import sleep

import pandas as pd
import psutil

from src.config import ROOT_PATH, use_gcp
from src.configure_logging import configure_logging
from src.gcp import download_file_to_gcs, upload_file_to_gcs

PATH_HIST_LOCAL = ROOT_PATH + "data/"
FILE_NAME_HIST = "hist_playlists_tracks.parquet.gz"
curr_date = datetime.today().strftime("%Y-%m-%d")

logger = logging.getLogger("utils")


def load_hist_file(
    file_path: str = PATH_HIST_LOCAL + FILE_NAME_HIST,
    playlist_id: str | None = None,
    allow_empty: bool = False,
) -> pd.DataFrame:
    """Load the hist file according to folder path in configs.

    Args:
        file_path (str): Path to the history file.
        playlist_id (str, optional): If provided, only load data for this playlist.
        allow_empty (bool): If True, returns an empty DataFrame
         when the file does not exist; otherwise, raises an error.

    Returns:
        pd.DataFrame: Existing history file of track ID per playlist.

    Raises:
        ValueError: If the file does not exist and allow_empty is False.
    """
    if use_gcp and file_path == PATH_HIST_LOCAL + FILE_NAME_HIST:
        try:
            download_file_to_gcs(file_name=FILE_NAME_HIST, local_folder=PATH_HIST_LOCAL)
        except Exception as e:
            logger.warning(
                "Loading from GCP failed while the option is selected. "
                "Trying loading from local saved file. "
                f"Error {e}"
            )

    if not os.path.exists(file_path):
        if allow_empty:
            return pd.DataFrame(
                columns=[
                    "playlist_id",
                    "playlist_name",
                    "track_id",
                    "datetime_added",
                    "artist_name",
                ]
            )
        # Try to load excel file if it exists
        excel_path = file_path.replace(".parquet.gz", ".xlsx")
        if os.path.exists(excel_path):
            logger.info("Found excel history file, loading it.")
            return pd.read_excel(excel_path)

        raise ValueError(
            f"File does not exist at the expected path: {file_path}. "
            "Creating an empty DataFrame is not allowed (allow_empty=False)."
        )

    try:
        filters = [("playlist_id", "==", playlist_id)] if playlist_id else None
        df_hist_pl_tracks = pd.read_parquet(file_path, filters=filters)
        logger.info(
            f"Successfully loaded hist file with {df_hist_pl_tracks.shape[0]} records"
        )
        gc.collect()
        for col in df_hist_pl_tracks.columns:
            try:
                df_hist_pl_tracks[col] = df_hist_pl_tracks[col].astype(pd.StringDtype())
            except Exception as e:
                logger.warning(e)
        print_memory_usage_readable()
        return df_hist_pl_tracks
    except Exception as e:
        logger.error(f"Failed to load Parquet file: {e}", exc_info=True)
        if allow_empty:
            return pd.DataFrame(
                columns=[
                    "playlist_id",
                    "playlist_name",
                    "track_id",
                    "datetime_added",
                    "artist_name",
                ]
            )
        raise


def _save_hist_file_proc(df_hist_pl_tracks: pd.DataFrame) -> None:
    """Save the history dataframe in a separate process."""
    proc_logger = logging.getLogger("proc_saver")

    try:
        df_hist_pl_tracks.to_parquet(
            PATH_HIST_LOCAL + FILE_NAME_HIST, compression="gzip", index=False
        )
        if use_gcp:
            upload_file_to_gcs(file_name=FILE_NAME_HIST, local_folder=PATH_HIST_LOCAL)
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


def append_to_hist_file(
    df_new_tracks: pd.DataFrame,
    file_path: str = PATH_HIST_LOCAL + FILE_NAME_HIST,
) -> None:
    """Append new tracks to the history file.

    Args:
        df_new_tracks (pd.DataFrame): DataFrame with new tracks to add.
        file_path (str): Path to the history file.
    """
    if df_new_tracks.empty:
        return

    try:
        df_history = load_hist_file(file_path=file_path, allow_empty=True)
        df_updated = pd.concat([df_history, df_new_tracks], ignore_index=True)
        save_hist_dataframe(df_updated)
    except Exception as e:
        logger.error(f"Failed to append to hist file: {e}", exc_info=True)


def deduplicate_hist_file(
    file_path: str = PATH_HIST_LOCAL + FILE_NAME_HIST,
) -> None:
    """De-duplicate the history file based on playlist_id and track_id.

    Args:
        file_path (str): Path to the history file.
    """
    logger.info("De-duplicating history file...")
    try:
        df_history = load_hist_file(file_path=file_path, allow_empty=True)
        if df_history.empty:
            logger.info("History file is empty, nothing to de-duplicate.")
            return

        n_rows_before = len(df_history)
        df_history.drop_duplicates(
            subset=["playlist_id", "track_id"], keep="first", inplace=True
        )
        n_rows_after = len(df_history)

        if n_rows_before > n_rows_after:
            logger.info(f"Removed {n_rows_before - n_rows_after} duplicate tracks.")
            save_hist_dataframe(df_history)
        else:
            logger.info("No duplicate tracks found.")
    except Exception as e:
        logger.error(f"Failed to de-duplicate hist file: {e}", exc_info=True)


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


def transfer_to_excel(
    file_path: str = PATH_HIST_LOCAL + FILE_NAME_HIST,
    excel_path: str = PATH_HIST_LOCAL + "hist_playlists_tracks.xlsx",
) -> None:
    """Transfer the history file to an Excel file.

    Args:
        file_path (str): Path to the history file.
        excel_path (str): Path to the Excel file.
    """
    logger.info("Transferring history file to Excel...")
    try:
        df_history = load_hist_file(file_path=file_path, allow_empty=True)
        if df_history.empty:
            logger.info("History file is empty, nothing to transfer.")
            return

        df_history.to_excel(excel_path, index=False)
        logger.info("Transfer complete.")
    except Exception as e:
        logger.error(f"Failed to transfer hist file to Excel: {e}", exc_info=True)


configure_logging()
