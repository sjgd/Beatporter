import json
import re
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
from pandas import to_datetime

from config import genres, overwrite_label, silent_search
from spotify import find_playlist_chart_label, logger, update_hist_pl_tracks


def get_top_100_playables(genre):
    """
    Get top 100 tracks for a genre
    :param genre: Genre name
    :return raw_tracks_dicts: Beatport list of tracks as dict
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36",
        "accept": "application/json",
    }
    url = "https://www.beatport.com/{}/{}/top-100".format(
        "genre" if genres[genre] else "", genres[genre]
    )
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, features="html.parser")
    all_scripts = soup.find_all("script", type="application/json")
    assert len(all_scripts) == 1
    script = all_scripts[0]

    results_data = json.loads(script.text)
    raw_tracks_dicts = results_data["props"]["pageProps"]["dehydratedState"]["queries"][
        0
    ]["state"]["data"]["results"]

    return raw_tracks_dicts


def parse_tracks(raw_tracks_dicts):
    """
    Parse tracks from Beatport JSON response
    :param raw_tracks_dicts: Beatport list of tracks as dict
    :return: List of tracks
    """
    tracks = list()
    for track in raw_tracks_dicts:
        tracks.append(
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
                "genres": track["genre"]["name"],  # Used to be track["genres"] as list
                "bpm": track["bpm"],
                "key": track["key"]["name"],  # Was only track["key"] before, but dict
            }
        )
    return tracks


def get_top_100_tracks(genre):
    """
    Get top 100 tracks from Beatport
    :param genre: Beatport genre
    :return: List of tracks
    """
    # logger.info("[+] Fetching Top 100 {} Tracks".format(genre))
    raw_tracks_dicts = get_top_100_playables(genre)
    return parse_tracks(raw_tracks_dicts)


def find_chart(chart, chart_bp_url_code):
    """ "
    Find chart URL from Beatport chart name or URL code.
    If chart 6 digits number is given, will return the URL directly.
    If chart contains a year,
      will only return the URL of the chart if the publication year matches.
    :param chart: Beatport chart name
    :param chart_bp_url_code: Beatport chart URL code
    :return: Beatport chart URL
    """

    # Check if got chart number in name already:
    if re.match(r".*(\/[0-9]{6})", chart_bp_url_code) is None:
        # If not, search for chart code
        r = requests.get("https://www.beatport.com/search?q=" + chart_bp_url_code)
        soup = BeautifulSoup(r.text, features="lxml")
        chart_urls = soup.find_all(class_="chart-url")
        chart_urls = [
            "https://www.beatport.com" + url.attrs["href"] for url in chart_urls
        ]
        reg = re.compile(".*" + chart_bp_url_code + ".*")  # .replace("-", " ")
        chart_urls = list(filter(reg.match, chart_urls))
        chart_urls.sort(reverse=True)  # That way larger ID is on top = newest chart
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
                "Found year {} in chart name, checking if release is matching".format(
                    match_year_name
                )
            )
            r = requests.get(chart_urls[0])
            soup = BeautifulSoup(r.text, features="lxml")
            # TODO: better match release year
            release_date = soup.find("span", {"class": "value"}).text
            is_year = bool(re.search(r"2[0-9]{3}-[0-9]{2}-[0-9]{2}", release_date))
            if not is_year:
                logger.warn(
                    "ERROR - Release date: {},"
                    " does not seem to be a date, aborting".format(release_date)
                )
            else:
                release_year = re.match(r"2[0-9]{3}", release_date).group(0)
                if f"({release_year})" == match_year_name:
                    logger.info(
                        "Years match ({}), returning chart {}".format(
                            release_year, chart_urls[0]
                        )
                    )
                    return chart_urls[0]
                else:
                    logger.warn(
                        f"ERROR - Release date: {release_date}, does not match,"
                        f" aborting chart: {chart_urls[0]}"
                    )
                    return None
        else:
            logger.info("No year found in chart name, returning {}".format(chart_urls[0]))
            return chart_urls[0]
    else:
        return None


def get_chart(url):
    """
    :param url: label full url, including beatport.com
    :return: dict of tracks
    """
    r = requests.get(url)
    blob_start = r.text.find("window.Playables") + 19
    blob_end = r.text.find("};", blob_start) + 1
    blob = r.text[blob_start:blob_end].replace("\n", "")
    return parse_tracks(json.loads(blob))


def parse_chart_url_datetime(str):
    """
    Format string, if Sunday returns previous week.
    :param str: string to format
    :return: datetime object
    """
    if datetime.today().weekday() > 5:
        return (datetime.today() - timedelta(days=6)).strftime(str)
    else:
        return datetime.today().strftime(str)


def get_label_tracks(
    label,
    label_bp_url_code,
    df_hist_pl_tracks,
    overwrite=overwrite_label,
    silent=silent_search,
):
    """
    Get all tracks from a label
    :param label: label name
    :param label_bp_url_code: label url code
    :param df_hist_pl_tracks: dataframe of historic track
    :param overwrite: if set to true will reload all tracks anyway,
    otherwise stops once the date of the last playlist refresh is reached
    :return: dict of tracks from oldest (first) to newest (last)
    """

    r = requests.get(
        "https://www.beatport.com/label/{}/tracks?per-page=50".format(label_bp_url_code)
    )
    soup = BeautifulSoup(r.text, features="lxml")
    page_numbers = soup.find_all(class_="pag-number")
    page_numbers = [page.text for page in page_numbers]
    max_page_number = page_numbers[len(page_numbers) - 1]
    label_tracks = []

    playlist = find_playlist_chart_label(label)
    if playlist["id"]:
        df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)
        df_loc_hist = df_hist_pl_tracks.loc[
            df_hist_pl_tracks.playlist_id == playlist["id"]
        ]
        if len(df_loc_hist.index) > 0:
            last_update = max(df_loc_hist.loc[:, "datetime_added"])
            if type(last_update) == str:
                last_update = datetime.strptime(last_update, "%Y-%m-%dT%H:%M:%SZ")
            else:
                last_update = last_update.tz_localize(None)
            logger.info(
                "Label {} has {} pages. Last playlist update found {}(UTC): ".format(
                    label, max_page_number, last_update
                )
            )
        else:
            last_update = datetime.min
            logger.info("Label {} has {} pages".format(label, max_page_number))
    else:
        last_update = datetime.min
        logger.info("Label {} has {} pages".format(label, max_page_number))

    for i in range(1, int(max_page_number) + 1):
        if not silent:
            logger.info("\t[+] Getting label {}, page {}".format(label_bp_url_code, i))
        r = requests.get(
            "https://www.beatport.com/label/{}/tracks?page={}&per-page=50".format(
                label_bp_url_code, i
            )
        )
        blob_start = r.text.find("window.Playables") + 19
        blob_end = r.text.find("};", blob_start) + 1
        blob = r.text[blob_start:blob_end].replace("\n", "")
        output = json.loads(blob)
        output = parse_tracks(output)

        label_tracks.extend(output)

        # Check if release date reached last update
        reached_last_update = sum(
            [
                to_datetime(track["released_date"]).tz_localize(None) < last_update
                for track in output
            ]
        )
        if reached_last_update > 0 and not overwrite:
            logger.info("\t[+] Reached last updated date, stopping")
            break

    label_tracks.reverse()

    return label_tracks
