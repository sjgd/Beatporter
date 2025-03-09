"""Test single tracks search module."""

import json
import logging
from datetime import datetime

from config import ROOT_PATH
from models import BeatportTrack
from spotify_search import search_for_track_v4, search_track_function

logger = logging.getLogger("test_search_tracks")


file_name_hist = "hist_playlists_tracks.pkl"
curr_date = datetime.today().strftime("%Y-%m-%d")
option_parse = ["backup", "chart", "genre", "label"]

# Use https://open.spotify.com/track/4zC9MjbIIHJoBpny7Sh35s to explore a track


def load_test_tracks(file: str) -> dict:
    """Load test tracks."""
    with open(ROOT_PATH + f"tests/core/{file}") as json_file:
        tracks = json.load(json_file)
        return tracks


def test_track_blondish() -> None:
    """Test trackblondish."""
    # Tests start in /src/
    track_search = {
        "title": "",
        "name": "Sete",
        "mix": "Original Mix",
        "artists": ["BLOND:ISH", "Amadou & Mariam", "Francis Mercier"],
        "remixers": [],
        "release": "Sete",
        "label": "Insomniac Records",
        "published_date": "2022-03-25",
        "released_date": "2022-03-25",
        "duration": "6:35",
        "duration_ms": 395040,
    }
    track_search = BeatportTrack(**track_search)
    track_id = search_track_function(track_search)
    logger.info(track_search)
    assert track_id == "4u3XiAwJ2U9Kxgy57gcAPB"


def test_track_toma() -> None:
    """Test track Toma."""
    track_search = {
        "title": "",
        "name": "Toma Dale",
        "mix": "Original Mix",
        "artists": ["Classmatic"],
        "remixers": [],
        "release": "Toma Dale",
        "label": "Hot Creations",
        "published_date": "2022-04-22",
        "released_date": "2022-04-22",
        "duration": "5:23",
        "duration_ms": 323720,
    }
    track_search = BeatportTrack(**track_search)
    track_id = search_track_function(track_search)
    logger.info(track_search)
    assert track_id == "3plaSBlILmcUoVBAHDca5c"


def test_track_10() -> None:
    """Test track 10."""
    track_search = {
        "title": "",
        "name": "God Made Me Phunky",
        "mix": "10 Years Of Eats Everything Extended Remix",
        "artists": ["MD X-Spress"],
        "remixers": ["Eats Everything"],
        "release": "God Made Me Phunky - Remixes",
        "label": "Defected",
        "published_date": "2021-05-14",
        "released_date": "2021-05-14",
        "duration": "6:37",
        "duration_ms": 397755,
    }
    track_search = BeatportTrack(**track_search)
    track_id = search_track_function(track_search)
    logger.info(track_search)
    assert track_id == "3jqEqTgf4gxvJfDsvRcOlC"


def test_track_mumble() -> None:
    """Test track Mumble."""
    track_search = {
        "title": "",
        "name": "Mumble",
        "mix": "Extended Mix",
        "artists": ["Kormak"],
        "remixers": [],
        "release": "Mumble (Extended Mix)",
        "label": "REALM Records",
        "published_date": "2021-05-14",
        "released_date": "2021-05-14",
        "duration": "6:37",
        "duration_ms": 397500,
    }
    track_search = BeatportTrack(**track_search)
    track_id = search_track_function(track_search)
    logger.info(track_search)
    # Or could be 7gznOBAlfJYgOBGdMM3Pas also
    # Org 5I3iJRM1eSpg2QNg4kc35c
    assert track_id == "7gznOBAlfJYgOBGdMM3Pas"


def test_track_so_bad() -> None:
    """Test track So Bad."""
    track_search = {
        "title": "",
        "name": "So Bad",
        "mix": "Original Mix",
        "artists": ["MADVILLA"],
        "remixers": [],
        "release": "Old Flame EP",
        "label": "Locus",
        "published_date": "2021-05-14",
        "released_date": "2021-05-14",
        "duration": "6:04",
        "duration_ms": 364651,
    }
    track_search = BeatportTrack(**track_search)
    track_id = search_track_function(track_search)
    logger.info(track_search)
    assert track_id == "490QKNw0N4EItGZJPt0tqm"


def test_track_eelke() -> None:
    """Test track Eelke."""
    track_search = {
        "title": "",
        "name": "Taking Flight feat. Nathan Nicholson",
        "mix": "Colyn Extended Remix",
        "artists": ["Eelke Kleijn", "Nathan Nicholson"],
        "remixers": ["Colyn"],
        "release": "Taking Flight - Colyn Remix",
        "label": "DAYS like NIGHTS",
        "published_date": "2021-05-14",
        "released_date": "2021-05-14",
        "duration": "6:56",
        "duration_ms": 416553,
    }
    track_search = BeatportTrack(**track_search)
    track_id = search_track_function(track_search)
    logger.info(track_search)
    # Could also be "46nC3sh5ujmckoYZPUVmDc" or "4zC9MjbIIHJoBpny7Sh35s"
    # Org 4zC9MjbIIHJoBpny7Sh35s
    assert track_id == "4zC9MjbIIHJoBpny7Sh35s"


def test_track_paul() -> None:
    """Test track Paul."""
    track_search = {
        "title": "",
        "name": "Filthy Music",
        "mix": "Original Mix",
        "artists": ["Paul Najera", "Junior Quidija"],
        "remixers": [],
        "release": "Filthy Music",
        "label": "Purveyor Underground",
        "published_date": "2021-05-14",
        "released_date": "2021-05-14",
        "duration": "6:14",
        "duration_ms": 374452,
    }
    track_search = BeatportTrack(**track_search)
    track_id = search_track_function(track_search)
    logger.info(track_search)
    assert track_id is None


def test_track_glances() -> None:
    """Test track Glances."""
    track_search = {
        "title": "",
        "name": "Parting Glances",
        "mix": "Original Mix",
        "artists": ["S-file"],
        "remixers": [],
        "release": "Work it",
        "label": "UNCAGE",
        "published_date": "2021-05-14",
        "released_date": "2021-05-14",
        "duration": "5:55",
        "duration_ms": 355555,
    }
    track_search = BeatportTrack(**track_search)
    track_id = search_track_function(track_search)
    logger.info(track_search)
    assert track_id == "1rlmw9jPyDnYv9lnYKI1IO"


def test_track_skantia() -> None:
    """Test track Skantia."""
    track_search = {
        "title": "",
        "name": "Providence",
        "mix": "Original Mix",
        "artists": ["Skantia", "Nectax"],
        "remixers": [],
        "release": "Providence",
        "label": "RAM Records",
        "published_date": "2021-05-14",
        "released_date": "2021-05-14",
        "duration": "4:12",
        "duration_ms": 252558,
    }
    track_search = BeatportTrack(**track_search)
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
    track_search = BeatportTrack(**track_search)
    track_id = search_track_function(track_search)
    logger.info(track_search)

    assert track_id == "1Q8WZ2aN87ld2vcb9UmrTB"


def test_9oases_extended_remix() -> None:
    """Test track 9OASES Extended Remix."""
    track_search = {
        "name": "Tremble (ft. Kim English)",
        "mix": "9OASES Extended Remix",
        "artists": ["Yolanda Be Cool", "9OASES"],
        "remixers": [],
        "release": "Tremble (ft. Kim English)",
        "label": "Black Book Records",
        "published_date": "2025-02-21",
        "duration": "5:17",
        "duration_ms": 317492,
        "genres": "Tech House",
        "bpm": 128,
        "key": "Bb Minor",
    }
    track_search = BeatportTrack(**track_search)
    track_id = search_for_track_v4(track_search)
    logger.info(track_search)

    assert track_id == "74HzDkCaSgCIeAPi06uxAv"
