# Use https://open.spotify.com/track/4zC9MjbIIHJoBpny7Sh35s to explore a track
import json
from datetime import datetime

from src.spotify import logger, search_for_track_v2
from src.utils import load_hist_file, save_hist_dataframe

file_name_hist = "hist_playlists_tracks.pkl"
curr_date = datetime.today().strftime("%Y-%m-%d")
option_parse = ["backup", "chart", "genre", "label"]

function_search = search_for_track_v2

logger.info("[START] Testing")


def test_load_and_save_hist_file():
    """Load and save hist file."""
    df_hist_pl_tracks = load_hist_file()
    logger.info(f"{len(df_hist_pl_tracks)=}")
    save_hist_dataframe(df_hist_pl_tracks)
    assert "df_hist_pl_tracks" in locals()


def test_track_matching():
    assert True


def test_track_blondish():
    # Tests start in /src/
    tracks = json.load(open("../tests/test_tracks.json"))
    track_search = tracks[1]
    track_id = function_search(track_search)
    logger.info(track_search)
    assert track_id == "4u3XiAwJ2U9Kxgy57gcAPB"


def test_track_toma():
    tracks = json.load(open("../tests/test_tracks.json"))
    track_search = tracks[3]
    track_id = function_search(track_search)
    logger.info(track_search)
    assert track_id == "3plaSBlILmcUoVBAHDca5c"


def test_track_10():
    tracks = json.load(open("../tests/chart_tracks.json"))
    track_search = tracks[1]
    track_id = function_search(track_search)
    logger.info(track_search)
    assert track_id == "3jqEqTgf4gxvJfDsvRcOlC"


def test_track_mumble():
    tracks = json.load(open("../tests/chart_tracks.json"))
    track_search = tracks[2]
    track_id = function_search(track_search)
    logger.info(track_search)
    # Or could be 7gznOBAlfJYgOBGdMM3Pas also
    # Org 5I3iJRM1eSpg2QNg4kc35c
    assert track_id == "7gznOBAlfJYgOBGdMM3Pas"


def test_track_so_bad():
    tracks = json.load(open("../tests/chart_tracks.json"))
    track_search = tracks[4]
    track_id = function_search(track_search)
    logger.info(track_search)
    assert track_id == "490QKNw0N4EItGZJPt0tqm"


def test_track_eelke():
    tracks = json.load(open("../tests/chart_tracks.json"))
    track_search = tracks[16]
    track_id = function_search(track_search)
    logger.info(track_search)
    # Could also be "46nC3sh5ujmckoYZPUVmDc" or "4zC9MjbIIHJoBpny7Sh35s"
    # Org 4zC9MjbIIHJoBpny7Sh35s
    assert track_id == "4zC9MjbIIHJoBpny7Sh35s"


def test_track_paul():
    tracks = json.load(open("../tests/chart_tracks.json"))
    track_search = tracks[17]
    track_id = function_search(track_search)
    logger.info(track_search)
    assert track_id is None


def test_track_glances():
    tracks = json.load(open("../tests/chart_tracks.json"))
    track_search = tracks[25]
    track_id = function_search(track_search)
    logger.info(track_search)
    assert track_id == "1rlmw9jPyDnYv9lnYKI1IO"


def test_track_skantia():
    tracks = json.load(open("../tests/chart_tracks.json"))
    track_search = tracks[29]
    track_id = function_search(track_search)
    logger.info(track_search)
    assert track_id == "3FXk5VM9d9ix0Xem3KSywt"
