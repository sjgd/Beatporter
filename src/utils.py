"""Utils module."""

import logging
from datetime import datetime
from os import path
from time import sleep

import pandas as pd

from src.config import ROOT_PATH, folder_path, use_gcp
from src.configure_logging import configure_logging
from src.gcp import download_file_to_gcs, upload_file_to_gcs

PATH_HIST_LOCAL = ROOT_PATH + "data/"
FILE_NAME_HIST = "hist_playlists_tracks.pkl.gz"
curr_date = datetime.today().strftime("%Y-%m-%d")


def load_hist_file(allow_empty: bool = False) -> pd.DataFrame:
    """Load the hist file according to folder path in configs.

    Returns:
        Returns existing history file of track ID per playlist

    """
    logger = logging.getLogger(__file__)
    try:
        if use_gcp:
            # TODO arguments for file path / type ?
            download_file_to_gcs(file_name=FILE_NAME_HIST, local_folder=PATH_HIST_LOCAL)
            df_hist_pl_tracks = pd.read_pickle(PATH_HIST_LOCAL + FILE_NAME_HIST)
            logger.info(" ")
            logger.info(
                f"Successfully loaded hist file with {df_hist_pl_tracks.shape[0]} records"
            )
            return df_hist_pl_tracks
    except Exception as e:
        logger.warning(
            "Loading from GCP failed while the option is selected. "
            "Trying loading from local saved file."
            f"Error {e}"
        )
    if path.exists(folder_path + "hist_playlists_tracks.xlsx"):
        df_hist_pl_tracks = pd.read_excel(folder_path + "hist_playlists_tracks.xlsx")
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

    return df_hist_pl_tracks


def save_hist_dataframe(df_hist_pl_tracks: pd.DataFrame) -> None:
    """Save the history dataframe according to configs."""
    logger = logging.getLogger()
    sleep(1)  # try to avoid read-write errors if running too quickly
    logger.debug("Saving file")
    df_hist_pl_tracks = df_hist_pl_tracks.loc[
        :, ["playlist_id", "playlist_name", "track_id", "datetime_added", "artist_name"]
    ]
    df_hist_pl_tracks.to_pickle(PATH_HIST_LOCAL + FILE_NAME_HIST)
    if use_gcp:
        upload_file_to_gcs(file_name=FILE_NAME_HIST, local_folder=PATH_HIST_LOCAL)
    df_hist_pl_tracks.to_excel(folder_path + "hist_playlists_tracks.xlsx", index=False)
    logger.info(f"Successfully saved hist file with {df_hist_pl_tracks.shape[0]} records")


configure_logging()
