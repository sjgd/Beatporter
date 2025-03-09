"""Test individual items."""

import logging
import random
import traceback
from datetime import datetime

import pytest

import beatport
from beatport import find_chart, get_chart, get_label_tracks
from config import charts, genres, labels, shuffle_label, spotify_bkp
from spotify_utils import (
    add_new_tracks_to_playlist_chart_label,
    add_new_tracks_to_playlist_genre,
    back_up_spotify_playlist,
)
from src.utils import load_hist_file

# RUN in debug for logs output in debug console
# Or logs are in the Output / Python Test Log

logger = logging.getLogger(__name__)


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
    df_hist_pl_tracks = add_new_tracks_to_playlist_genre(
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
    df_hist_pl_tracks = back_up_spotify_playlist(
        playlist_name, org_playlist_id, df_hist_pl_tracks
    )

    assert len(df_hist_pl_tracks) > 0


@pytest.mark.parametrize("chart", charts)
def test_chart(chart: dict[str, str]) -> None:
    """Test each chart.

    Args:
        chart: The name of the chart to test.

    Returns:
        None.

    """
    # Init
    start_time = datetime.now()
    logger.info(f"\n[!] Starting @ {start_time}")
    df_hist_pl_tracks = load_hist_file()

    chart_bp_url_code = charts[chart]
    logger.info(f" Getting chart : ***** {chart} : {chart_bp_url_code} *****")
    chart_url = find_chart(chart, chart_bp_url_code)

    if chart_url:
        try:
            tracks_dicts = get_chart(chart_url)
            logger.debug(chart_bp_url_code + ":" + str(tracks_dicts))
            logger.info(f"\t[+] Found {len(tracks_dicts)} tracks for {chart}")
            df_hist_pl_tracks = add_new_tracks_to_playlist_chart_label(
                chart, tracks_dicts, df_hist_pl_tracks
            )
        except Exception as e:
            traceback.print_exc()
            logger.warning(
                "FAILED getting chart: "
                f"***** {chart} : {chart_bp_url_code} ***** "
                f"with error: {e}"
            )
    else:
        logger.info(f"\t[+] Chart {chart} not found")

    assert len(df_hist_pl_tracks) > 0


@pytest.mark.parametrize("label", labels)
def test_label(label: dict[str, str]) -> None:
    """Test each label.

    Args:
        label: The name of the label to test.

    Returns:
        None.

    """
    # Init
    start_time = datetime.now()
    logger.info(f"\n[!] Starting @ {start_time}")
    df_hist_pl_tracks = load_hist_file()

    label_bp_url_code = labels[label]
    logger.info(f"Getting label : ***** {label} : {label_bp_url_code} *****")
    tracks_dict = get_label_tracks(label, label_bp_url_code, df_hist_pl_tracks)
    logger.info(f"Found {len(tracks_dict)} tracks for {label}")
    if shuffle_label:
        random.shuffle(tracks_dict)
    df_hist_pl_tracks = add_new_tracks_to_playlist_chart_label(
        label, tracks_dict, df_hist_pl_tracks
    )

    assert len(df_hist_pl_tracks) > 0
