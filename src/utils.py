"""Utils module."""

import gc
import logging
import multiprocessing
import os
from datetime import datetime
from time import sleep
from typing import ClassVar

import pandas as pd
import psutil

from src.config import ROOT_PATH, use_gcp
from src.configure_logging import configure_logging
from src.gcp import upload_file_to_gcs

PATH_HIST_LOCAL = ROOT_PATH + "data/"
FILE_NAME_HIST = "hist_playlists_tracks.parquet.gz"
curr_date = datetime.today().strftime("%Y-%m-%d")

logger = logging.getLogger("utils")


class HistoryCache:
    """Cache for history files to avoid reloading them from disk."""

    _cache: ClassVar[dict[str, pd.DataFrame]] = {}

    @classmethod
    def get(cls, file_path: str) -> pd.DataFrame | None:
        """Get dataframe from cache."""
        return cls._cache.get(file_path)

    @classmethod
    def set(cls, file_path: str, df: pd.DataFrame) -> None:
        """Set dataframe in cache."""
        cls._cache[file_path] = df

    @classmethod
    def clear(cls) -> None:
        """Clear the cache."""
        cls._cache.clear()


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
    # If filtering by playlist_id, try to use pyarrow filters to load only needed rows
    # This significantly reduces memory usage by not loading the entire file
    if playlist_id and os.path.exists(file_path):
        try:
            # Use pyarrow filters to load only the needed playlist data
            df_hist_pl_tracks = pd.read_parquet(
                file_path,
                filters=[("playlist_id", "=", playlist_id)],
            )
            logger.info(
                f"Loaded {df_hist_pl_tracks.shape[0]} records for "
                f"playlist_id={playlist_id} using pyarrow filters"
            )
            print_memory_usage_readable()
            # Don't cache filtered results - they're playlist-specific
            return df_hist_pl_tracks
        except Exception as e:
            # Fallback to normal loading if pyarrow filtering fails
            logger.warning(f"PyArrow filtering failed ({e}), falling back to full load")

    df_hist_pl_tracks = HistoryCache.get(file_path)

    if df_hist_pl_tracks is None:
        if not os.path.exists(file_path):
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
                HistoryCache.set(file_path, df_hist_pl_tracks)
            else:
                # Try to load excel file if it exists
                excel_path = file_path.replace(".parquet.gz", ".xlsx")
                if os.path.exists(excel_path):
                    logger.info("Found excel history file, loading it.")
                    df_hist_pl_tracks = pd.read_excel(excel_path)
                    HistoryCache.set(file_path, df_hist_pl_tracks)
                else:
                    raise ValueError(
                        f"File does not exist at the expected path: {file_path}. "
                        "Creating an empty DataFrame is not allowed (allow_empty=False)."
                    )
        else:
            try:
                # We load the full file to cache it
                df_hist_pl_tracks = pd.read_parquet(file_path)
                logger.info(
                    f"Successfully loaded hist file with "
                    f"{df_hist_pl_tracks.shape[0]} records"
                )
                gc.collect()
                # Optimize memory usage by using categorical types for repeated values
                # This can reduce memory by 50-80% for columns with many repeated values
                for col in df_hist_pl_tracks.columns:
                    if col == "datetime_added":
                        continue
                    try:
                        # Use category type for playlist_id, playlist_name which have
                        # limited unique values but many repetitions
                        if col in ["playlist_id", "playlist_name"]:
                            df_hist_pl_tracks[col] = df_hist_pl_tracks[col].astype(
                                "category"
                            )
                        else:
                            df_hist_pl_tracks[col] = df_hist_pl_tracks[col].astype(
                                pd.StringDtype()
                            )
                    except Exception as e:
                        logger.warning(e)
                HistoryCache.set(file_path, df_hist_pl_tracks)
                print_memory_usage_readable()
            except Exception as e:
                logger.error(f"Failed to load Parquet file: {e}", exc_info=True)
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
                    HistoryCache.set(file_path, df_hist_pl_tracks)
                else:
                    raise

    if playlist_id:
        # Return filtered view without copying to save memory
        return df_hist_pl_tracks[df_hist_pl_tracks["playlist_id"] == playlist_id]

    return df_hist_pl_tracks


def _save_hist_file_proc(df_hist_pl_tracks: pd.DataFrame) -> None:
    """Save the history dataframe in a separate process."""
    proc_logger = logging.getLogger("proc_saver")

    try:
        if "datetime_added" in df_hist_pl_tracks.columns:
            df_hist_pl_tracks["datetime_added"] = pd.to_datetime(
                df_hist_pl_tracks["datetime_added"], format="ISO8601", utc=True
            )
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

    if os.name == "nt":
        ctx = multiprocessing.get_context("spawn")
    else:
        ctx = multiprocessing.get_context()
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
        # Load full history without caching (since we're updating it)
        # Temporarily clear cache to avoid holding duplicate data
        HistoryCache.clear()
        gc.collect()

        df_history = load_hist_file(file_path=file_path, allow_empty=True)
        df_updated = pd.concat([df_history, df_new_tracks], ignore_index=True)
        # Delete old references before saving to prevent memory leak
        del df_history
        gc.collect()
        save_hist_dataframe(df_updated)
        # Don't cache the full file - it will be loaded with pyarrow filters as needed
        # This prevents holding 162K+ records in memory
        del df_updated
        gc.collect()
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
            HistoryCache.clear()
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
