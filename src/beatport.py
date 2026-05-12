"""Module to manage Beatport."""

import gc
import json
import logging
import re
from contextlib import suppress
from datetime import datetime, timedelta
from functools import lru_cache
from time import sleep
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup
from pandas import to_datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from src.config import genres, overwrite_label, silent_search
from src.models import BeatportTrack
from src.spotify_utils import find_playlist_chart_label, update_hist_pl_tracks
from src.utils import load_hist_file

logger = logging.getLogger("beatport")

SLEEP_LOAD_PAGE = 7


def _accept_cookies(driver: Any) -> None:
    """Attempt to accept cookies if a banner is present."""
    try:
        # Beatport typically uses OneTrust
        cookie_button_selector = "#onetrust-accept-btn-handler"
        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, cookie_button_selector))
        ).click()
        logger.debug("Accepted cookies.")
    except Exception:
        # Banner might not be present or different selector
        pass


# Reduce noise from third-party libraries
logging.getLogger("undetected_chromedriver").setLevel(logging.ERROR)
logging.getLogger("undetected_chromedriver.patcher").setLevel(logging.ERROR)
logging.getLogger("uc").setLevel(logging.ERROR)
logging.getLogger("selenium").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    ),
    "accept": "application/json",
}


def _get_driver(max_retries: int = 3) -> Any:
    """Create a new undetected_chromedriver instance with retries."""
    import undetected_chromedriver as uc

    for i in range(max_retries):
        try:
            driver = uc.Chrome(headless=True, use_subprocess=True)
            return driver
        except Exception as e:
            logger.warning(
                f"Failed to create driver (attempt {i + 1}/{max_retries}): {e}"
            )
            if i == max_retries - 1:
                raise
            sleep(2)


@lru_cache(maxsize=16)
def get_beatport_page_script_queries(url: str) -> dict:
    """Extract script queries results from the Beatport URL using undetected-chromedriver.

    Args:
        url: URL to query.

    Returns:
        JSON of the script queries.

    """
    driver = _get_driver()
    try:
        driver.get(url)
        sleep(SLEEP_LOAD_PAGE)  # Wait for page to load
        _accept_cookies(driver)
        # Wait for dehydratedState to appear
        page_source = ""
        for _ in range(60):
            try:
                page_source = driver.page_source
                if "dehydratedState" in page_source:
                    break
            except Exception as e:
                logger.warning(f"Error getting page source: {e}")
            sleep(1)

        if not page_source:
            raise ValueError(f"Failed to get page source from {url}")

        soup = BeautifulSoup(page_source, features="html.parser")
    finally:
        with suppress(Exception):
            driver.quit()

    all_scripts = soup.find_all("script", type="application/json")
    script = None
    for s in all_scripts:
        if "dehydratedState" in s.text:
            script = s
            break

    if script is None:
        raise ValueError(f"Could not find script with dehydratedState in {url}")

    results_data = json.loads(script.text)
    results_data_queries = results_data["props"]["pageProps"]["dehydratedState"][
        "queries"
    ]

    return results_data_queries


def scrape_beatport_charts(
    url: str, max_wait: int = 20, chart_bp_url_code: str = ""
) -> list[str]:
    """Scrape Beatport artist page for chart links using undetected-chromedriver.

    Args:
        url (str): The Beatport artist page URL to scrape for charts.
        max_wait (int, optional): Maximum number of seconds to wait for
        the page to load.
        chart_bp_url_code (str, optional): If provided, only chart links
        containing this code will be returned.

    Returns:
        list[str]: A list of chart URLs (as strings) found on the artist
        page.

    """
    driver = _get_driver()
    charts: list[str] = []
    try:
        logger.info(f"Loading URL: {url}")
        driver.get(url)
        sleep(SLEEP_LOAD_PAGE)  # Wait for page to load
        _accept_cookies(driver)

        # Wait for chart links or dehydratedState
        for _ in range(max_wait):
            try:
                if (
                    driver.find_elements(By.CSS_SELECTOR, 'a[href*="/chart/"]')
                    or "dehydratedState" in driver.page_source
                ):
                    break
            except Exception as e:
                logger.warning(f"Error during wait in scrape_beatport_charts: {e}")
            sleep(1)

        try:
            links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/chart/"]')
        except Exception as e:
            logger.error(f"Failed to find elements: {e}")
            links = []

        for link in links:
            try:
                href = link.get_attribute("href")
                if not href:
                    continue
                full_url = (
                    href if href.startswith("http") else f"https://www.beatport.com{href}"
                )
                if (
                    chart_bp_url_code and chart_bp_url_code in full_url
                ) or not chart_bp_url_code:
                    charts.append(full_url)
            except Exception as e:
                logger.warning(f"Error extracting link attribute: {e}")
    finally:
        with suppress(Exception):
            driver.quit()

    return list(set(charts))


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
        try:
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
                        ],  # TODO was ["duration"]["minutes"] before,
                        # to check if the same
                        "duration_ms": track["length_ms"],
                        "genres": track["genre"][
                            "name"
                        ],  # Used to be track["genres"] as list
                        "bpm": track["bpm"],
                        "key": track["key"][
                            "name"
                        ],  # Was only track["key"] before, but dict
                    }
                )
            )
        except Exception as e:
            logger.warning(f"Failed to parse track {track}: {e}")
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


def find_chart(chart: str, chart_bp_url_code: str) -> str | None:
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
    # Check if have chart number is missing
    # And if format with year-week pattern
    if (re.match(r".*(\/[0-9]{6})", chart_bp_url_code) is None) and (
        # Regex to match patterns like weekend-picks-2025-week-7
        re.match(r"^weekend-picks-(\d{4})-week-(\d{1,2})$", chart_bp_url_code) is not None
    ):
        url = "https://www.beatport.com/artist/beatport/45/charts?page=1&per_page=150"
        chart_urls = scrape_beatport_charts(url, chart_bp_url_code=chart_bp_url_code)
    # Otherwise need to find the chart ID
    elif re.match(r".*(\/[0-9]{6})", chart_bp_url_code) is None:
        # If not, search for chart code
        url = (
            "https://www.beatport.com/search/charts"
            f"?q={chart_bp_url_code}&page=1&per_page=150"
        )
        results_data = get_beatport_page_script_queries(url)

        charts = results_data[0]["state"]["data"]["data"]

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

        charts.sort(
            key=lambda x: x["chart_id"], reverse=True
        )  # That way larger ID is on top = newest chart
        chart_urls = [chart["url_tentative"] for chart in charts]
        # TODO reverse above not necessary charts have release date now
    elif re.match(r".*(\/[0-9]{6})", chart_bp_url_code) is not None:
        # Direct chart ID
        chart_urls = ["https://www.beatport.com/chart/" + chart_bp_url_code]
    else:
        raise ValueError("Chart code format not recognized")

    if len(chart_urls) >= 1:
        # TODO export as function ?
        # Checking if '(2XXX)' year is present
        # in chart name and matching chart release year
        match_year_name = re.match(r".*(\(2[0-9]{3}\))", chart)
        if match_year_name:
            match_year_name_str = match_year_name.group(1)
            logger.info(
                f"Found year {match_year_name_str} in chart name,"
                " checking if release is matching"
            )
            try:
                results_data = get_beatport_page_script_queries(chart_urls[0])

                change_date_chart = results_data[0]["state"]["data"]["change_date"]

                # TODO: better match release year
                is_year = bool(
                    re.search(r"2[0-9]{3}-[0-9]{2}-[0-9]{2}", change_date_chart)
                )
                if not is_year:
                    logger.warning(
                        f"ERROR - Release date: {change_date_chart},"
                        " does not seem to be a date, aborting"
                    )
                else:
                    release_year_match = re.match(r"2[0-9]{3}", change_date_chart)
                    if release_year_match:
                        release_year = release_year_match.group(0)
                        if (
                            f"({release_year})" == match_year_name_str
                            and chart_bp_url_code in chart_urls[0]
                        ):
                            logger.info(
                                f"Years match ({release_year}), returning chart "
                                f"{chart_urls[0]}"
                            )
                            return chart_urls[0]
                        else:
                            logger.warning(
                                f"ERROR - Release date: {change_date_chart}, "
                                f"does not match requeried date: {match_year_name_str},"
                                f" aborting chart: {chart_urls[0]}"
                            )
                            return None
            except Exception as e:
                logger.error(f"Error during year validation for {chart_urls[0]}: {e}")
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

    # Clean up large JSON data
    del results_data, raw_tracks_dicts
    gc.collect()

    return tracks_dicts


def parse_chart_url_datetime(date_str: str) -> str:
    """Format date string; if Sunday, return previous week.

    Args:
        date_str: string to format.

    Returns:
        datetime object.

    """
    if datetime.today().weekday() > 5:
        return (datetime.today() - timedelta(days=6)).strftime(date_str)
    else:
        return datetime.today().strftime(date_str)


def get_label_tracks(
    label: str,
    label_bp_url_code: str,
    overwrite: bool = overwrite_label,
    silent: bool = silent_search,
) -> list[BeatportTrack]:
    """Get all tracks from a given label.

    Args:
        label: label name.
        label_bp_url_code: label url code.
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
    df_loc_hist = pd.DataFrame()
    if playlist["id"]:
        update_hist_pl_tracks(playlist)
        df_loc_hist = load_hist_file(playlist_id=playlist["id"], allow_empty=True)
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

        # Clean up per page
        del results_data, raw_tracks_dicts, raw_tracks
        gc.collect()

        if reached_last_update > 0 and not overwrite:
            logger.info("\t[+] Reached last updated date, stopping")
            break

    label_tracks.reverse()

    # Final cleanup
    del df_loc_hist
    gc.collect()

    return label_tracks
