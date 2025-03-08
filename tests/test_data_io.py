"""Test data IO module."""

from datetime import datetime

from src.spotify import logger
from src.utils import load_hist_file, save_hist_dataframe

file_name_hist = "hist_playlists_tracks.pkl"
curr_date = datetime.today().strftime("%Y-%m-%d")
option_parse = ["backup", "chart", "genre", "label"]


def test_load_and_save_hist_file() -> None:
    """Load and save hist file."""
    df_hist_pl_tracks = load_hist_file()
    logger.info(f"{len(df_hist_pl_tracks)=}")
    save_hist_dataframe(df_hist_pl_tracks)
    assert "df_hist_pl_tracks" in locals()
