"""Module to manage Beatport."""

import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pandas import to_datetime

from src.config import genres, overwrite_label, silent_search
from src.models import BeatportTrack
from src.spotify_utils import find_playlist_chart_label, update_hist_pl_tracks

logger = logging.getLogger("beatport")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36",
    "accept": "application/json",
}


def get_beatport_page_script_queries(url: str) -> dict:
    """Extract script queries results from the Beatport URL.

    Args:
        url: URL to query.

    Returns:
        JSON of the script queries.

    """
    r = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(r.text, features="html.parser")
    all_scripts = soup.find_all("script", type="application/json")
    assert len(all_scripts) == 1, "Found too many scripts in the result page"
    script = all_scripts[0]

    results_data = json.loads(script.text)
    results_data_queries = results_data["props"]["pageProps"]["dehydratedState"][
        "queries"
    ]

    return results_data_queries


def get_top_100_playables(genre: str) -> list[dict]:
    """Get top 100 tracks for a given genre.

    Args:
        genre: Genre name

    Returns:
        raw_tracks_dicts: Beatport list of tracks as dict

    """
    url = "https://www.beatport.com/{}/{}/top-100".format(
        "genre" if genres[genre] else "", genres[genre]
    )

    results_data = get_beatport_page_script_queries(url)
    raw_tracks_dicts = results_data[0]["state"]["data"]["results"]
    assert len(raw_tracks_dicts) > 0, f"No tracks found on the genre page: {url}"

    return raw_tracks_dicts


def parse_tracks(raw_tracks_dicts: list[dict]) -> list:
    """Parse tracks from Beatport JSON response.

    Args:
        raw_tracks_dicts: Beatport list of tracks as dict.

    Returns:
        List of tracks.

    """
    tracks = list()
    for track in raw_tracks_dicts:
        tracks.append(
            BeatportTrack.model_validate(
                {
                    # "title": track["title"],
                    "name": track["name"],
                    "mix": track["mix_name"],
                    "artists": [artist["name"] for artist in track["artists"]],
                    "remixers": [remixer["name"] for remixer in track["remixers"]],
                    "release": track["release"]["name"],
                    "label": track["release"]["label"]["name"],
                    "published_date": track["publish_date"],
                    # "released_date": track["date"]["released"],
                    "duration": track[
                        "length"
                    ],  # TODO was ["duration"]["minutes"] before, to check if the same
                    "duration_ms": track["length_ms"],
                    "genres": track["genre"][
                        "name"
                    ],  # Used to be track["genres"] as list
                    "bpm": track["bpm"],
                    "key": track["key"]["name"],  # Was only track["key"] before, but dict
                }
            )
        )
    return tracks


def get_top_100_tracks(genre: str) -> list[BeatportTrack]:
    """Get top 100 tracks from Beatport for a given genre.

    Args:
        genre: Beatport genre.

    Returns:
        List of tracks.

    """
    # logger.info("[+] Fetching Top 100 {} Tracks".format(genre))
    raw_tracks_dicts = get_top_100_playables(genre)
    return parse_tracks(raw_tracks_dicts)


def find_chart(chart: str, chart_bp_url_code: str) -> Any | str | None:
    """Find Beatport chart URL from chart name or URL code.

    Finds Beatport chart URL from Beatport chart name or URL code.
    If chart 6 digits number is given, will return the URL directly.
    If chart contains a year,
      will only return the URL of the chart if the publication year matches.

    Args:
        chart: Beatport chart name or URL code.
        chart_bp_url_code: Beatport chart URL code (optional).

    Returns: Beatport chart URL.

    """
    # Check if have chart number in name already
    # Otherwise need to find the chart ID
    if re.match(r".*(\/[0-9]{6})", chart_bp_url_code) is None:
        # If not, search for chart code
        url = (
            "https://www.beatport.com/search/charts"
            f"?q={chart_bp_url_code}&page=1&per_page=150"
        )
        results_data = get_beatport_page_script_queries(url)

        charts = results_data = results_data[0]["state"]["data"]["data"]

        for i in range(len(charts)):
            charts[i]["url_tentative"] = (
                "https://www.beatport.com/chart/"
                + re.sub(
                    "[^a-zA-Z0-9 \n\\.]", "", charts[i]["chart_name"].lower()
                ).replace(" ", "-")
                + "/"
                + str(charts[i]["chart_id"])
            )
            charts[i]["url_tentative"] = charts[i]["url_tentative"].replace("--", "-")

        chart_urls = [chart["url_tentative"] for chart in charts]
        chart_urls.sort(reverse=True)  # That way larger ID is on top = newest chart
        # TODO reverse above not necessary charts have release date now
    else:
        chart_urls = ["https://www.beatport.com/chart/" + chart_bp_url_code]

    if len(chart_urls) >= 1:
        # TODO export as function ?
        # Checking if '(2XXX)' year is present
        # in chart name and matching chart release year
        match_year_name = re.match(r".*(\(2[0-9]{3}\))", chart)
        if match_year_name is not None:
            match_year_name = match_year_name.group(1)
            logger.info(
                f"Found year {match_year_name} in chart name,"
                " checking if release is matching"
            )
            results_data = get_beatport_page_script_queries(chart_urls[0])

            change_date_chart = results_data[0]["state"]["data"]["change_date"]

            # TODO: better match release year
            is_year = bool(re.search(r"2[0-9]{3}-[0-9]{2}-[0-9]{2}", change_date_chart))
            if not is_year:
                logger.warning(
                    f"ERROR - Release date: {change_date_chart},"
                    " does not seem to be a date, aborting"
                )
            else:
                release_year = re.match(r"2[0-9]{3}", change_date_chart).group(0)
                if f"({release_year})" == match_year_name:
                    logger.info(
                        f"Years match ({release_year}), returning chart {chart_urls[0]}"
                    )
                    return chart_urls[0]
                else:
                    logger.warning(
                        f"ERROR - Release date: {change_date_chart}, "
                        f"does not match requeried date: {match_year_name},"
                        f" aborting chart: {chart_urls[0]}"
                    )
                    return None
        else:
            logger.info(f"No year found in chart name, returning {chart_urls[0]}")
            return chart_urls[0]

    return None


def get_chart(url: str) -> list[BeatportTrack]:
    """Get chart tracks from a given URL.

    Args:
        url: Chart full url, including beatport.com, chart name and chart ID.

    Returns:
        tracks_dicts: List of dicts of tracks.

    """
    results_data = get_beatport_page_script_queries(url)
    raw_tracks_dicts = results_data[1]["state"]["data"]["results"]
    assert len(raw_tracks_dicts) > 0, f"No tracks found on the genre page: {url}"

    tracks_dicts = parse_tracks(raw_tracks_dicts)

    return tracks_dicts


def parse_chart_url_datetime(str: str) -> str:
    """Format date string; if Sunday, return previous week.

    Args:
        str: string to format.

    Returns:
        datetime object.

    """
    if datetime.today().weekday() > 5:
        return (datetime.today() - timedelta(days=6)).strftime(str)
    else:
        return datetime.today().strftime(str)


def get_label_tracks(
    label: str,
    label_bp_url_code: str,
    df_hist_pl_tracks: pd.DataFrame,
    overwrite: bool = overwrite_label,
    silent: bool = silent_search,
) -> list[BeatportTrack]:
    """Get all tracks from a given label.

    Args:
        label: label name.
        label_bp_url_code: label url code.
        df_hist_pl_tracks: dataframe of historic track.
        overwrite: If True, reload all tracks; otherwise, stop once the date
                     of the last playlist refresh is reached.
        silent: If True, suppress logging messages.

    Returns:
        List of dict of tracks from oldest (first) to newest (last).

    """
    # Get max number of pages for label
    url = f"https://www.beatport.com/label/{label_bp_url_code}/tracks?per-page=50"
    results_data = get_beatport_page_script_queries(url)
    page_numbers_string = results_data[1]["state"]["data"]["page"]
    max_page_number = page_numbers_string.split("/")[1]
    assert len(max_page_number) > 0, "No pages found in label page"

    # Get tracks for label
    label_tracks = []

    # Load history
    playlist = find_playlist_chart_label(label)
    if playlist["id"]:
        df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)
        df_loc_hist = df_hist_pl_tracks.loc[
            df_hist_pl_tracks.playlist_id == playlist["id"]
        ]
        if len(df_loc_hist.index) > 0:
            last_update = max(df_loc_hist.loc[:, "datetime_added"])
            if isinstance(last_update, str):
                last_update = datetime.strptime(last_update, "%Y-%m-%dT%H:%M:%SZ")
            else:
                last_update = last_update.tz_localize(None)
            logger.info(
                f"Label {label} has {max_page_number} pages. "
                f"Last playlist update found {last_update}(UTC): "
            )
        else:
            last_update = datetime.min
            logger.info(f"Label {label} has {max_page_number} pages")
    else:
        last_update = datetime.min
        logger.info(f"Label {label} has {max_page_number} pages")

    # Parse label pages
    for i in range(1, int(max_page_number)):
        if not silent:
            logger.info(f"\t[+] Getting label {label_bp_url_code}, page {i}")
        url = (
            "https://www.beatport.com/label"
            f"/{label_bp_url_code}/tracks?page={i}&per-page=50"
        )
        results_data = get_beatport_page_script_queries(url)
        raw_tracks_dicts = results_data[1]["state"]["data"]["results"]
        assert len(raw_tracks_dicts) > 0, f"No tracks found on the label page: {url}"
        raw_tracks = parse_tracks(raw_tracks_dicts)

        label_tracks.extend(raw_tracks)

        # Check if release date reached last update
        reached_last_update = sum(
            [
                to_datetime(track.published_date).tz_localize(None) < last_update
                for track in raw_tracks
            ]
        )
        if reached_last_update > 0 and not overwrite:
            logger.info("\t[+] Reached last updated date, stopping")
            break

    label_tracks.reverse()

    return label_tracks
