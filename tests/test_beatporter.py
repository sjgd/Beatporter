import unittest
# import os
# import numpy as np
# import pandas as pd
# from dotenv import load_dotenv
# import spotify
# import beatport
# from datetime import datetime
from beatporter import load_hist_file
from spotify import search_for_track_v3, logger
import json
from datetime import datetime

file_name_hist = "hist_playlists_tracks.pkl"
curr_date = datetime.today().strftime("%Y-%m-%d")
option_parse = ["backup", "chart", "genre", "label"]

function_search = search_for_track_v3

logger.info("[START] Testing")


class TestBeatporter(unittest.TestCase):
    """
    Track might not be available depending on the user location
    """

    def test_load_hist_file(self):
        df_hist_pl_tracks = load_hist_file()
        # print(len(df_hist_pl_tracks))
        self.assertTrue('df_hist_pl_tracks' in locals())

    def test_track_matching(self):
        self.assertTrue(True)

    def test_track_blondish(self):
        tracks = json.load(open("tests/test_tracks.json"))
        track_search = tracks[1]
        track_id = function_search(track_search)
        self.assertEqual(track_id, "5B4gUqNKYgU38ULSWP5Bzj")

    def test_track_toma(self):
        tracks = json.load(open("tests/test_tracks.json"))
        track_search = tracks[3]
        track_id = function_search(track_search)
        self.assertEqual(track_id, "4duvXkbpQVht8mv7K8cx3i")

    def test_track_10(self):
        tracks = json.load(open("tests/chart_tracks.json"))
        track_search = tracks[1]
        track_id = function_search(track_search)
        self.assertEqual(track_id, '1blDpAjlQwkc7xHc9dJn8q')

    def test_track_mumble(self):
        tracks = json.load(open("tests/chart_tracks.json"))
        track_search = tracks[2]
        track_id = function_search(track_search)
        self.assertEqual(track_id, "5I3iJRM1eSpg2QNg4kc35c")

    def test_track_so_bad(self):
        tracks = json.load(open("tests/chart_tracks.json"))
        track_search = tracks[4]
        track_id = function_search(track_search)
        self.assertEqual(track_id, '3NYSAj8EmsvR1JzwtjDJkf')

    def test_track_eelke(self):
        tracks = json.load(open("tests/chart_tracks.json"))
        track_search = tracks[16]
        track_id = function_search(track_search)
        # Could also be "46nC3sh5ujmckoYZPUVmDc"
        self.assertEqual(track_id, "4zC9MjbIIHJoBpny7Sh35s")

    def test_track_paul(self):
        tracks = json.load(open("tests/chart_tracks.json"))
        track_search = tracks[17]
        track_id = function_search(track_search)
        self.assertEqual(track_id, None)

    def test_track_glances(self):
        tracks = json.load(open("tests/chart_tracks.json"))
        track_search = tracks[25]
        track_id = function_search(track_search)
        self.assertEqual(track_id, "3JIvZvxp11S4tBCWMlubby")

    def test_track_skantia(self):
        tracks = json.load(open("tests/chart_tracks.json"))
        track_search = tracks[29]
        track_id = function_search(track_search)
        self.assertEqual(track_id, "3FXk5VM9d9ix0Xem3KSywt")
