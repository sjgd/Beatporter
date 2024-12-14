"""Test individual items"""
from datetime import datetime

import pytest

import beatport
import spotify
from config import genres, spotify_bkp
from spotify import logger
from src.beatporter import load_hist_file

# RUN in debug for logs output in debug console


@pytest.mark.parametrize("genre", genres)
def test_genre(genre):
    # Init
    start_time = datetime.now()
    logger.info("\n[!] Starting @ {}".format(start_time))
    # charts = {
    #     beatport.parse_chart_url_datetime(k): beatport.parse_chart_url_datetime(v)
    #     for k, v in charts.items()
    # }
    df_hist_pl_tracks = load_hist_file()

    logger.info("\n-Getting genre : ***** {} *****".format(genre))
    top_100_chart = beatport.get_top_100_tracks(genre)
    logger.debug(genre + ":" + str(top_100_chart))
    df_hist_pl_tracks = spotify.add_new_tracks_to_playlist_genre(
        genre, top_100_chart, df_hist_pl_tracks
    )

    assert len(df_hist_pl_tracks) > 0


@pytest.mark.parametrize("playlist_name", spotify_bkp)
def test_backup(playlist_name):
    # Init
    start_time = datetime.now()
    logger.info("\n[!] Starting @ {}".format(start_time))
    df_hist_pl_tracks = load_hist_file()

    org_playlist_id = spotify_bkp[playlist_name]
    logger.info(
        "-Backing up playlist : ***** {} : {} *****".format(
            playlist_name, org_playlist_id
        )
    )
    df_hist_pl_tracks = spotify.back_up_spotify_playlist(
        playlist_name, org_playlist_id, df_hist_pl_tracks
    )

    assert len(df_hist_pl_tracks) > 0
