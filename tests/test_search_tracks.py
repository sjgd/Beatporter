"""Test single tracks search module."""

import json
from datetime import datetime

from src.spotify import logger, search_track_function

file_name_hist = "hist_playlists_tracks.pkl"
curr_date = datetime.today().strftime("%Y-%m-%d")
option_parse = ["backup", "chart", "genre", "label"]

# Use https://open.spotify.com/track/4zC9MjbIIHJoBpny7Sh35s to explore a track


def test_track_blondish() -> None:
    """Test trackblondish."""
    # Tests start in /src/
    tracks = json.load(open("../tests/test_tracks.json"))
    track_search = tracks[1]
    track_id = search_track_function(track_search)
    logger.info(track_search)
    assert track_id == "4u3XiAwJ2U9Kxgy57gcAPB"


def test_track_toma() -> None:
    """Test track Toma."""
    tracks = json.load(open("../tests/test_tracks.json"))
    track_search = tracks[3]
    track_id = search_track_function(track_search)
    logger.info(track_search)
    assert track_id == "3plaSBlILmcUoVBAHDca5c"


def test_track_10() -> None:
    """Test track 10."""
    tracks = json.load(open("../tests/chart_tracks.json"))
    track_search = tracks[1]
    track_id = search_track_function(track_search)
    logger.info(track_search)
    assert track_id == "3jqEqTgf4gxvJfDsvRcOlC"


def test_track_mumble() -> None:
    """Test track Mumble."""
    tracks = json.load(open("../tests/chart_tracks.json"))
    track_search = tracks[2]
    track_id = search_track_function(track_search)
    logger.info(track_search)
    # Or could be 7gznOBAlfJYgOBGdMM3Pas also
    # Org 5I3iJRM1eSpg2QNg4kc35c
    assert track_id == "7gznOBAlfJYgOBGdMM3Pas"


def test_track_so_bad() -> None:
    """Test track So Bad."""
    tracks = json.load(open("../tests/chart_tracks.json"))
    track_search = tracks[4]
    track_id = search_track_function(track_search)
    logger.info(track_search)
    assert track_id == "490QKNw0N4EItGZJPt0tqm"


def test_track_eelke() -> None:
    """Test track Eelke."""
    tracks = json.load(open("../tests/chart_tracks.json"))
    track_search = tracks[16]
    track_id = search_track_function(track_search)
    logger.info(track_search)
    # Could also be "46nC3sh5ujmckoYZPUVmDc" or "4zC9MjbIIHJoBpny7Sh35s"
    # Org 4zC9MjbIIHJoBpny7Sh35s
    assert track_id == "4zC9MjbIIHJoBpny7Sh35s"


def test_track_paul() -> None:
    """Test track Paul."""
    tracks = json.load(open("../tests/chart_tracks.json"))
    track_search = tracks[17]
    track_id = search_track_function(track_search)
    logger.info(track_search)
    assert track_id is None


def test_track_glances() -> None:
    """Test track Glances."""
    tracks = json.load(open("../tests/chart_tracks.json"))
    track_search = tracks[25]
    track_id = search_track_function(track_search)
    logger.info(track_search)
    assert track_id == "1rlmw9jPyDnYv9lnYKI1IO"


def test_track_skantia() -> None:
    """Test track Skantia."""
    tracks = json.load(open("../tests/chart_tracks.json"))
    track_search = tracks[29]
    track_id = search_track_function(track_search)
    logger.info(track_search)
    assert track_id == "3FXk5VM9d9ix0Xem3KSywt"


def test_track_kolter() -> None:
    """Test track Kolter."""
    track_search = {
        "name": "15 Seconds of Fame",
        "mix": "Original Mix",
        "artists": ["Kolter"],
        "remixers": [],
        "release": "15 Seconds of Fame",
        "label": "Koltrax",
        "published_date": "2024-10-11",
        "duration": "6:14",
        "duration_ms": 374769,
        "genres": "Minimal / Deep Tech",
        "bpm": 130,
        "key": "Db Major",
    }
    track_id = search_track_function(track_search)
    logger.info(track_search)

    assert track_id == "1Q8WZ2aN87ld2vcb9UmrTB"
