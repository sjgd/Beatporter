"""Module to manage Beatport."""

import gc
import json
import logging
import math
import re
import subprocess
import sys
from contextlib import suppress
from datetime import datetime, timedelta
from functools import lru_cache
from time import sleep
from typing import Any

from bs4 import BeautifulSoup
from pandas import to_datetime
from selenium.common.exceptions import NoSuchWindowException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from src.config import genres, overwrite_label, silent_search
from src.models import BeatportTrack
from src.spotify_utils import find_playlist_chart_label, update_hist_pl_tracks
from src.utils import load_hist_file

# Reduce noise from third-party libraries - move to the very top
logging.getLogger("undetected_chromedriver").setLevel(logging.ERROR)
logging.getLogger("undetected_chromedriver.patcher").setLevel(logging.ERROR)
logging.getLogger("uc").setLevel(logging.ERROR)
logging.getLogger("selenium").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

logger = logging.getLogger("beatport")

SLEEP_LOAD_PAGE = 20


def _accept_cookies(driver: Any) -> None:
    """Attempt to accept cookies if a banner is present."""
    try:
        # Check if window still exists
        if not driver.window_handles:
            return

        # Beatport typically uses OneTrust
        cookie_button_selector = "#onetrust-accept-btn-handler"
        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, cookie_button_selector))
        ).click()
        logger.debug("Accepted cookies.")
    except (NoSuchWindowException, WebDriverException):
        pass
    except Exception:
        # Banner might not be present or different selector
        pass


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    ),
    "accept": "application/json",
}


class BeatportBrowser:
    """Manages a persistent browser instance for Beatport scraping."""

    _driver: Any = None

    @classmethod
    def get_driver(cls) -> Any:
        """Get or create the persistent driver instance."""
        is_dead = False
        if cls._driver is not None:
            try:
                # Rigorous check: must have handles and be able to access them
                handles = cls._driver.window_handles
                if not handles:
                    is_dead = True
                else:
                    # Try to access current window to be sure
                    _ = cls._driver.current_window_handle
            except Exception:
                is_dead = True

        if cls._driver is None or is_dead:
            if is_dead:
                logger.warning("Persistent driver session invalid, recreating...")
                with suppress(Exception):
                    cls._driver.quit()
            cls._driver = _get_driver()

        return cls._driver

    @classmethod
    def quit(cls) -> None:
        """Safely quit the persistent driver."""
        if cls._driver:
            with suppress(Exception):
                cls._driver.quit()
            cls._driver = None


def _get_chrome_major_version() -> int | None:
    """Detect the major version of Google Chrome installed on the system."""
    try:
        if sys.platform == "darwin":
            path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
        elif sys.platform == "win32":
            path = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
        else:
            path = "google-chrome"

        output = subprocess.check_output([path, "--version"]).decode()
        version_match = re.search(r"(\d+)\.", output)
        if version_match:
            return int(version_match.group(1))
    except Exception as e:
        logger.debug(f"Failed to detect Chrome version: {e}")
    return None


def _get_driver(max_retries: int = 3) -> Any:
    """Create a new undetected_chromedriver instance with retries."""
    import undetected_chromedriver as uc

    # Detect Chrome version to avoid driver mismatch
    chrome_version = _get_chrome_major_version()

    # On Mac, we want to restore focus to the previous app after Chrome "pops"
    active_app = None
    if sys.platform == "darwin":
        try:
            active_app = (
                subprocess.check_output(
                    [
                        "osascript",
                        "-e",
                        'tell application "System Events" to get name of first process whose frontmost is true',
                    ]
                )
                .decode()
                .strip()
            )
        except Exception:
            pass

    for i in range(max_retries):
        try:
            # Create fresh options for each attempt to avoid 'cannot reuse' error
            options = uc.ChromeOptions()
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--window-position=0,5000")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-popup-blocking")
            options.add_argument("--disable-blink-features=AutomationControlled")

            # Anti-throttling flags for when screen is locked or window is hidden
            options.add_argument("--disable-background-timer-throttling")
            options.add_argument("--disable-backgrounding-occluded-windows")
            options.add_argument("--disable-breakpad")
            options.add_argument("--disable-component-update")
            options.add_argument("--disable-domain-reliability")
            options.add_argument("--disable-renderer-backgrounding")

            # On Mac, proactively start a background task to restore focus
            # This handles the "pop" that happens during the blocking uc.Chrome() call
            if active_app:
                try:
                    subprocess.Popen(
                        [
                            "osascript",
                            "-e",
                            "delay 2",
                            "-e",
                            f'tell application "{active_app}" to activate',
                        ],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                except Exception:
                    pass

            # Use standard headless=False to better bypass Cloudflare
            # This is a blocking call that can take several seconds
            driver = uc.Chrome(
                options=options, headless=False, version_main=chrome_version
            )

            # Second attempt to restore focus after driver is created, just in case
            if active_app:
                try:
                    subprocess.run(
                        [
                            "osascript",
                            "-e",
                            f'tell application "{active_app}" to activate',
                        ],
                        capture_output=True,
                    )
                except Exception:
                    pass

            try:
                # Move window far below the screen to avoid disturbing the user
                driver.set_window_position(0, 5000)
            except Exception:
                pass
            # Give it more time to settle
            sleep(2)
            return driver
        except Exception as e:
            err_msg = str(e).split("\n")[0] if str(e) else type(e).__name__
            logger.warning(
                f"Failed to create driver (attempt {i + 1}/{max_retries}): {err_msg}"
            )
            if i == max_retries - 1:
                raise
            sleep(5)


def _wait_for_dehydrated_state(driver: Any, url: str) -> str:
    """Wait for dehydratedState to appear in page source and return it."""
    page_source = ""
    for i in range(60):
        try:
            # Check if window still exists before accessing
            if not driver.window_handles:
                raise ValueError("Browser window closed unexpectedly")

            page_source = driver.page_source
            if "dehydratedState" in page_source:
                break

            if i % 10 == 0:
                logger.debug(
                    f"Still waiting for dehydratedState on {url} (attempt {i}/60)..."
                )
        except (NoSuchWindowException, WebDriverException) as e:
            err_msg = str(e).split("\n")[0] if str(e) else type(e).__name__
            logger.warning(f"Browser window lost during wait: {err_msg}")
            raise
        except Exception as e:
            logger.debug(f"Transient error getting page source: {e}")
        sleep(1)

    if not page_source or "dehydratedState" not in page_source:
        try:
            logger.warning(f"Failed to find dehydratedState. Page title: {driver.title}")
        except Exception:
            pass
        raise ValueError(f"Failed to find dehydratedState in {url}")
    return page_source


@lru_cache(maxsize=16)
def get_beatport_page_script_queries(url: str) -> dict:
    """Extract script queries results from the Beatport URL using undetected-chromedriver.

    Args:
        url: URL to query.

    Returns:
        JSON of the script queries.

    """
    max_load_retries = 3
    last_error = None

    for attempt in range(max_load_retries):
        try:
            logger.debug(
                f"Scraping attempt {attempt + 1}/{max_load_retries} for {url}..."
            )
            driver = BeatportBrowser.get_driver()
            logger.debug(f"Using persistent driver for {url}. Getting URL...")
            driver.get(url)
            logger.debug(f"URL loaded for {url}. Sleeping {SLEEP_LOAD_PAGE}s...")
            sleep(SLEEP_LOAD_PAGE)  # Wait for page to load
            _accept_cookies(driver)

            logger.debug(f"Waiting for dehydratedState on {url}...")
            page_source = _wait_for_dehydrated_state(driver, url)
            logger.debug(f"dehydratedState found on {url}.")

            soup = BeautifulSoup(page_source, features="html.parser")
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

        except Exception as e:
            last_error = e
            err_msg = str(e).split("\n")[0] if str(e) else type(e).__name__
            logger.warning(
                f"Attempt {attempt + 1}/{max_load_retries} failed for {url}: {err_msg}"
            )
            # If window is lost, force a driver reset for the next attempt
            if "no such window" in str(e).lower() or "session" in str(e).lower():
                logger.info("Resetting driver due to lost window...")
                BeatportBrowser.quit()

            # Extra sleep on failure
            sleep(5)

    raise last_error or ValueError(
        f"Failed to scrape {url} after {max_load_retries} attempts"
    )


def _wait_for_charts(driver: Any, max_wait: int) -> None:
    """Wait for chart links or dehydratedState to appear."""
    for _ in range(max_wait):
        try:
            if not driver.window_handles:
                raise ValueError("Browser window closed unexpectedly")

            if (
                driver.find_elements(By.CSS_SELECTOR, 'a[href*="/chart/"]')
                or "dehydratedState" in driver.page_source
            ):
                break
        except (NoSuchWindowException, WebDriverException) as e:
            err_msg = str(e).split("\n")[0] if str(e) else type(e).__name__
            logger.warning(f"Browser window lost during charts wait: {err_msg}")
            raise
        except Exception as e:
            logger.debug(f"Transient error in scrape_beatport_charts wait: {e}")
        sleep(1)


def _extract_links(driver: Any) -> list:
    """Extract chart links from the page."""
    try:
        if not driver.window_handles:
            raise ValueError("Browser window closed unexpectedly before finding elements")
        return driver.find_elements(By.CSS_SELECTOR, 'a[href*="/chart/"]')
    except (NoSuchWindowException, WebDriverException) as e:
        err_msg = str(e).split("\n")[0] if str(e) else type(e).__name__
        logger.warning(f"Browser window lost before element find: {err_msg}")
        raise
    except Exception as e:
        err_msg = str(e).split("\n")[0] if str(e) else type(e).__name__
        logger.error(f"Failed to find elements: {err_msg}")
        return []


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
    max_load_retries = 3
    last_error = None

    for attempt in range(max_load_retries):
        charts: list[str] = []
        try:
            driver = BeatportBrowser.get_driver()
            logger.info(f"Loading URL: {url}")
            driver.get(url)
            sleep(SLEEP_LOAD_PAGE)  # Wait for page to load
            _accept_cookies(driver)

            _wait_for_charts(driver, max_wait)
            links = _extract_links(driver)

            for link in links:
                try:
                    href = link.get_attribute("href")
                    if not href:
                        continue
                    full_url = (
                        href
                        if href.startswith("http")
                        else f"https://www.beatport.com{href}"
                    )
                    if (
                        chart_bp_url_code and chart_bp_url_code in full_url
                    ) or not chart_bp_url_code:
                        charts.append(full_url)
                except Exception as e:
                    logger.debug(f"Error extracting link attribute: {e}")

            return list(set(charts))

        except Exception as e:
            last_error = e
            err_msg = str(e).split("\n")[0] if str(e) else type(e).__name__
            logger.warning(
                f"Attempt {attempt + 1}/{max_load_retries} failed for {url}: {err_msg}"
            )
            # If window is lost, force a driver reset for the next attempt
            if "no such window" in str(e).lower() or "session" in str(e).lower():
                logger.info("Resetting driver due to lost window...")
                BeatportBrowser.quit()
            sleep(5)

    logger.error(f"Failed to scrape charts from {url}: {last_error}")
    return []


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


def _format_duration_ms(duration_ms: int) -> str:
    """Format duration in milliseconds into M:SS string."""
    total_seconds = max(duration_ms, 0) // 1000
    minutes, seconds = divmod(total_seconds, 60)
    return f"{minutes}:{seconds:02d}"


def parse_tracks(raw_tracks_dicts: list[dict]) -> list:
    """Parse tracks from Beatport JSON response.

    Args:
        raw_tracks_dicts: Beatport list of tracks as dict.

    Returns:
        List of tracks.

    """
    tracks = []
    for track in raw_tracks_dicts:
        try:
            duration_ms = int(track.get("length_ms") or 0)
            duration = str(track.get("length") or _format_duration_ms(duration_ms))
            published_date = (
                track.get("publish_date") or track.get("new_release_date") or ""
            )
            release_obj = track.get("release") or {}
            label_obj = release_obj.get("label") or {}
            genre_obj = track.get("genre") or {}
            key_obj = track.get("key") or {}

            tracks.append(
                BeatportTrack.model_validate(
                    {
                        "name": track["name"],
                        "mix": track.get("mix_name") or "",
                        "artists": [
                            artist["name"] for artist in (track.get("artists") or [])
                        ],
                        "remixers": [
                            remixer["name"] for remixer in (track.get("remixers") or [])
                        ],
                        "release": release_obj.get("name", ""),
                        "label": label_obj.get("name", ""),
                        "published_date": published_date,
                        "released_date": track.get("new_release_date") or "",
                        "duration": duration,
                        "duration_ms": duration_ms,
                        "genres": genre_obj.get("name", ""),
                        "bpm": track.get("bpm"),
                        "key": key_obj.get("name", ""),
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


def _validate_chart_year(
    chart_url: str, match_year_name_str: str, chart_bp_url_code: str
) -> str | None:
    """Validate if the chart's release year matches the requested year."""
    try:
        results_data = get_beatport_page_script_queries(chart_url)
        change_date_chart = results_data[0]["state"]["data"]["change_date"]

        # TODO: better match release year
        is_year = bool(re.search(r"2[0-9]{3}-[0-9]{2}-[0-9]{2}", change_date_chart))
        if not is_year:
            logger.warning(
                f"ERROR - Release date: {change_date_chart},"
                " does not seem to be a date, aborting"
            )
            return None

        release_year_match = re.match(r"2[0-9]{3}", change_date_chart)
        if release_year_match:
            release_year = release_year_match.group(0)
            if (
                f"({release_year})" == match_year_name_str
                and chart_bp_url_code in chart_url
            ):
                logger.info(f"Years match ({release_year}), returning chart {chart_url}")
                return chart_url

            logger.warning(
                f"ERROR - Release date: {change_date_chart}, "
                f"does not match requeried date: {match_year_name_str},"
                f" aborting chart: {chart_url}"
            )
        return None
    except Exception as e:
        logger.error(f"Error during year validation for {chart_url}: {e}")
        return None


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
            return _validate_chart_year(
                chart_urls[0], match_year_name_str, chart_bp_url_code
            )

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


def _extract_label_tracks_data(results_data: Any) -> dict:
    """Extract the tracks query state data from Beatport dehydrated queries."""
    for query in results_data:
        if not isinstance(query, dict):
            continue
        query_key = query.get("queryKey")
        state_data = query.get("state", {}).get("data")
        if (
            isinstance(query_key, list)
            and query_key
            and query_key[0] == "tracks"
            and isinstance(state_data, dict)
        ):
            return state_data

    if len(results_data) > 1 and isinstance(results_data[1], dict):
        fallback_data = results_data[1].get("state", {}).get("data")
        if isinstance(fallback_data, dict):
            return fallback_data

    raise ValueError("Could not find tracks query data in label page response")


def _get_label_max_pages(tracks_data: dict, per_page: int = 50) -> int:
    """Determine total number of pages from label tracks query data."""
    page_str = tracks_data.get("page")
    if isinstance(page_str, str) and "/" in page_str:
        max_page_str = page_str.split("/")[1]
        if max_page_str.isdigit():
            return int(max_page_str)

    count = tracks_data.get("count")
    if isinstance(count, int):
        if count <= 0:
            return 0
        results = tracks_data.get("results")
        results_len = len(results) if isinstance(results, list) else 0
        effective_per_page = results_len if 0 < results_len < count else per_page
        return math.ceil(count / effective_per_page)

    raise KeyError("Neither 'page' nor 'count' found in label tracks data")


def _get_label_last_update(label: str, max_page_number: int) -> datetime:
    """Load playlist history for a label and return its last update timestamp."""
    playlist = find_playlist_chart_label(label)
    if playlist["id"]:
        update_hist_pl_tracks(playlist)
        df_loc_hist = load_hist_file(playlist_id=playlist["id"], allow_empty=True)
        try:
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
                return last_update
        finally:
            del df_loc_hist
            gc.collect()

    logger.info(f"Label {label} has {max_page_number} pages")
    return datetime.min


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
    per_page = 50
    first_page_url = (
        f"https://www.beatport.com/label/{label_bp_url_code}/tracks"
        f"?page=1&per_page={per_page}"
    )
    results_data = get_beatport_page_script_queries(first_page_url)
    tracks_data = _extract_label_tracks_data(results_data)
    max_page_number = _get_label_max_pages(tracks_data, per_page=per_page)
    assert max_page_number > 0, "No pages found in label page"

    label_tracks: list[BeatportTrack] = []
    last_update = _get_label_last_update(label, max_page_number)

    # Parse label pages
    for i in range(1, max_page_number + 1):
        if not silent:
            logger.info(f"\t[+] Getting label {label_bp_url_code}, page {i}")
        url = (
            f"https://www.beatport.com/label/{label_bp_url_code}/tracks"
            f"?page={i}&per_page={per_page}"
        )
        results_data = get_beatport_page_script_queries(url)
        page_data = _extract_label_tracks_data(results_data)
        raw_tracks_dicts = page_data.get("results") or []
        if i == 1:
            assert len(raw_tracks_dicts) > 0, f"No tracks found on the label page: {url}"
        elif not raw_tracks_dicts:
            break

        raw_tracks = parse_tracks(raw_tracks_dicts)
        label_tracks.extend(raw_tracks)

        # Check if release date reached last update
        reached_last_update = sum(
            [
                bool(track.published_date)
                and to_datetime(track.published_date, utc=True).tz_localize(None)
                < last_update
                for track in raw_tracks
            ]
        )

        # Clean up per page
        del results_data, page_data, raw_tracks_dicts, raw_tracks
        gc.collect()

        if reached_last_update > 0 and not overwrite:
            logger.info("\t[+] Reached last updated date, stopping")
            break

    label_tracks.reverse()
    gc.collect()

    return label_tracks
