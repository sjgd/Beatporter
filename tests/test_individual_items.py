"""Test individual items"""
from datetime import datetime

import pytest

import beatport
import spotify
from config import genres, spotify_bkp
from spotify import logger
from src.utils import load_hist_file

# RUN in debug for logs output in debug console
# Or logs are in the Output / Python Test Log


@pytest.mark.parametrize("genre", genres)
def test_genre(genre: dict[str, str]) -> None:
    """Test each genre.

    Args:
        genre: The genre to test.

    Returns:
        None.

    """
    # Init
    start_time = datetime.now()
    logger.info(f"\n[!] Starting @ {start_time}")
    df_hist_pl_tracks = load_hist_file()

    logger.info(f"\n-Getting genre : ***** {genre} *****")
    top_100_chart = beatport.get_top_100_tracks(genre)
    logger.debug(genre + ":" + str(top_100_chart))
    df_hist_pl_tracks = spotify.add_new_tracks_to_playlist_genre(
        genre, top_100_chart, df_hist_pl_tracks
    )

    assert len(df_hist_pl_tracks) > 0


@pytest.mark.parametrize("playlist_name", spotify_bkp)
def test_backup(playlist_name: dict[str, str]) -> None:
    """Test each backup playlist.

    Args:
        playlist_name: The name of the playlist to test.

    Returns:
        None.

    """
    # Init
    start_time = datetime.now()
    logger.info(f"\n[!] Starting @ {start_time}")
    df_hist_pl_tracks = load_hist_file()

    org_playlist_id = spotify_bkp[playlist_name]
    logger.info(f"-Backing up playlist : ***** {playlist_name} : {org_playlist_id} *****")
    df_hist_pl_tracks = spotify.back_up_spotify_playlist(
        playlist_name, org_playlist_id, df_hist_pl_tracks
    )

    assert len(df_hist_pl_tracks) > 0
