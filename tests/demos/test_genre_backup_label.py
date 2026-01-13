"""Test individual items."""

import logging
import random
import traceback
from datetime import datetime

import pytest

from src.beatport import (
    find_chart,
    get_chart,
    get_label_tracks,
    get_top_100_tracks,
    parse_chart_url_datetime,
)
from src.config import charts, genres, labels, shuffle_label, silent_search, spotify_bkp
from src.spotify_search import (
    add_new_tracks_to_playlist_chart_label,
    add_new_tracks_to_playlist_genre,
)
from src.spotify_utils import back_up_spotify_playlist

# RUN in debug for logs output in debug console
# Or logs are in the Output / Python Test Log

logger = logging.getLogger(__name__)

parsed_charts = {
    parse_chart_url_datetime(k): parse_chart_url_datetime(v) for k, v in charts.items()
}


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
    logger.info(f"[!] Starting @ {start_time}")

    logger.info(f"-Getting genre : ***** {genre} *****")
    top_100_chart = get_top_100_tracks(genre)
    logger.debug(f"{genre}:{top_100_chart}")
    add_new_tracks_to_playlist_genre(
        genre=genre,
        top_100_chart=top_100_chart,
        silent=silent_search,
    )

    assert True


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

    org_playlist_id = spotify_bkp[playlist_name]
    logger.info(f"-Backing up playlist : ***** {playlist_name} : {org_playlist_id} *****")
    back_up_spotify_playlist(
        playlist_name=playlist_name,
        org_playlist_id=org_playlist_id,
    )

    assert True


@pytest.mark.parametrize("chart", parsed_charts)
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

    chart_bp_url_code = parsed_charts[chart]
    logger.info(f" Getting chart : ***** {chart} : {chart_bp_url_code} *****")
    chart_url = find_chart(chart, chart_bp_url_code)

    if chart_url:
        try:
            tracks_dicts = get_chart(chart_url)
            logger.debug(chart_bp_url_code + ":" + str(tracks_dicts))
            logger.info(f"\t[+] Found {len(tracks_dicts)} tracks for {chart}")
            add_new_tracks_to_playlist_chart_label(
                title=chart,
                tracks_dicts=tracks_dicts,
                use_prefix=True,
                silent=silent_search,
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

    assert True


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

    label_bp_url_code = labels[label]
    logger.info(f"Getting label : ***** {label} : {label_bp_url_code} *****")
    tracks_dict = get_label_tracks(label=label, label_bp_url_code=label_bp_url_code)
    logger.info(f"Found {len(tracks_dict)} tracks for {label}")
    if shuffle_label:
        random.shuffle(tracks_dict)
    add_new_tracks_to_playlist_chart_label(
        label=label, tracks_dict=tracks_dict, silent=silent_search
    )

    assert True
