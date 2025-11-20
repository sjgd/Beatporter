"""Module to manage Spotify queries."""

import asyncio
import gc
import logging
import re
import socket
import webbrowser
from datetime import datetime
from difflib import SequenceMatcher
from typing import TypedDict, cast

import numpy as np
import pandas as pd
import spotipy
from spotipy import SpotifyException, oauth2
from spotipy.oauth2 import CacheFileHandler

from src.config import (
    add_at_top_playlist,
    client_id,
    client_secret,
    digging_mode,
    playlist_description,
    playlist_prefix,
    redirect_uri,
    refresh_token_n_tracks,
    scope,
    silent_search,
    username,
)
from src.configure_logging import configure_logging
from src.models import BeatportTrack
from src.search_utils import clean_track_name
from src.utils import save_hist_dataframe


class PlaylistDescription(TypedDict):
    """TypedDict for Spotify playlist description."""

    description: str


# import json
# import sys
# from time import sleep, time

configure_logging()
logger = logging.getLogger("spotify_utils")

TRACKS_DICT_NAMES = ["id", "duration_ms", "href", "name", "popularity", "uri", "artists"]
handler = CacheFileHandler(username=username)
sp_oauth = oauth2.SpotifyOAuth(
    client_id, client_secret, redirect_uri, cache_handler=handler, scope=scope
)

# def do_spotify_oauth() -> dict:
#     """Authenticate to Spotify.

#     Returns:
#         dict: The Spotify token information.

#     """
#     TOKEN_PATH = ROOT_PATH + "data/token.json"
#     try:
#         with open(TOKEN_PATH) as fh:
#             token = fh.read()
#         token = json.loads(token)
#     except Exception:
#         token = None
#     if token:
#         if int(time()) > (
#             token["expires_at"] - 60
#         ):  # Take 60s margin to avoid timeout while searching
#             logger.info("[+][+] Refreshing Spotify token")
#             token = sp_oauth.refresh_access_token(token["refresh_token"])
#     else:
#         authorization_code = asyncio.run(async_get_auth_code())
#         logger.info(authorization_code)
#         if not authorization_code:
#             logger.info(
#                 "[!] Unable to authenticate to Spotify."
#                 "  Couldn't get authorization code"
#             )
#             sys.exit(-1)
#         token = sp_oauth.get_access_token(authorization_code)
#     if not token:
#         logger.info("[!] Unable to authenticate to Spotify."
#                     "  Couldn't get access token.")
#         sys.exit(-1)
#     try:
#         with open(TOKEN_PATH, "w+") as fh:
#             fh.write(json.dumps(token))
#     except Exception:
#         logger.info("[!] Unable to to write token object to disk.  This is non-fatal.")
#     return token


# token_info = do_spotify_oauth()
# spotify_ins = spotipy.Spotify(
#     auth=token_info["access_token"], requests_timeout=15, retries=3, backoff_factor=15
# )
def spotify_auth(verbose_aut: bool = False) -> spotipy.Spotify:
    """Authenticate to Spotify.

    Args:
        verbose_aut (bool): Whether to enable verbose logging.

    Returns:
        spotipy.Spotify: The Spotify instance.
    """
    return spotipy.Spotify(
        auth_manager=sp_oauth, requests_timeout=15, retries=3, backoff_factor=15
    )


def similar(a: str, b: str) -> float:
    """Compute similarity between two strings.

    Args:
        a (str): The first string.
        b (str): The second string.

    Returns:
        float: A similarity ratio between 0 and 1.

    """
    return SequenceMatcher(None, str(a), str(b)).ratio()


def listen_for_callback_code() -> str:
    """Listen for the Spotify callback code.

    Returns:
        str: The Spotify authorization code.

    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("localhost", int(redirect_uri.split(":")[-1])))
    s.listen(1)
    while True:
        connection, address = s.accept()
        buf = str(connection.recv(1024))
        if len(buf) > 0:
            break
    start_code = buf.find("?code=") + 6
    end_code = buf.find(" ", start_code)
    if "&" in buf[start_code:end_code]:
        end_code = buf.find("&")
    return buf[start_code:end_code]


async def get_spotify_auth_code() -> None:
    """Get Spotify authorization code."""
    auth_url = sp_oauth.get_authorize_url()
    webbrowser.open(auth_url)


async def async_get_auth_code() -> str:
    """Get Spotify authorization code asynchronously.

    Returns:
        str: The Spotify authorization code.

    """
    task = asyncio.create_task(get_spotify_auth_code())
    await task
    return listen_for_callback_code()


def get_all_playlists() -> list:
    """Get all playlists.

    Returns:
        list: List of playlists.

    """
    spotify_ins = spotify_auth()
    playlists_pager = spotify_ins.current_user_playlists()
    playlists = playlists_pager["items"]
    while playlists_pager["next"]:
        playlists_pager = spotify_ins.next(playlists_pager)
        playlists.extend(playlists_pager["items"])
    return playlists


def create_playlist(playlist_name: str) -> str:
    """Create a new playlist.

    Args:
        playlist_name (str): The name of the playlist.

    Returns:
        str: The ID of the created playlist.

    """
    # TODO export parameter description
    spotify_ins = spotify_auth()
    playlist = spotify_ins.user_playlist_create(
        username, playlist_name, description=playlist_description
    )
    return playlist["id"]


def get_playlist_id(playlist_name: str) -> str | None:
    """Get playlist ID by name.

    Args:
        playlist_name (str): The name of the playlist.

    Returns:
        str: The ID of the playlist or None if not found.

    """
    spotify_ins = spotify_auth()
    playlists = spotify_ins.current_user_playlists()

    while playlists:
        for playlist in playlists["items"]:
            if (playlist["owner"]["id"] == username) and (
                playlist["name"] == playlist_name
            ):
                return playlist["id"]
        # check if more pages exist
        playlists = spotify_ins.next(playlists) if playlists["next"] else None

    return None


def do_durations_match(
    source_track_duration: int,
    found_track_duration: int,
    silent: bool = silent_search,
    debug_duration: bool = False,
) -> bool:
    """Check if durations match.

    Args:
        source_track_duration (int): Duration of the source track.
        found_track_duration (int): Duration of the found track.
        silent (bool): Whether to suppress logging output.
        debug_duration (bool): Whether to enable debug logging.

    Returns:
        bool: True if durations match, False otherwise.

    """
    if source_track_duration == found_track_duration:
        if not silent:
            logger.info("\t\t\t\t[+] Durations match")
        return True
    else:
        if not silent and debug_duration:
            logger.info("\t\t\t\t[!] Durations do not match")
        return False


def most_popular_track(tracks: list) -> str | None:
    """Find the most popular track.

    Args:
        tracks (list): List of tracks.

    Returns:
        str: The ID of the most popular track.

    """
    # Popularity does not always yield the correct result
    high_score = -1
    winner = None
    for track in tracks:
        if track["popularity"] > high_score:
            winner = track["id"]
            high_score = track["popularity"]
    return winner


def tracks_similarity(
    source_track: BeatportTrack,
    found_tracks: list[dict],
    debug_comp: bool = False,
) -> list:
    """Compute similarity between tracks.

    Args:
        source_track (dict): Source track.
        found_tracks (list): List of found tracks.
        debug_comp (bool): Whether to enable debug logging.

    Returns:
        list: List of similarity scores.

    """
    artist_similar = []
    track_n_similar = []
    duration_similar = []

    for track in found_tracks:
        # artist_r = track.artists[0][
        #     "name"
        # ]  # ", ".join([artist["name"] for artist in track.artists])
        artist_match = []
        for artist_s in source_track.artists:  # ", ".join(source_track.artists)
            for artist_r in track["artists"]:
                artist_match.append(similar(artist_s.lower(), artist_r["name"].lower()))
                if debug_comp:
                    logger.info(
                        "\t\t\t[+] {} vs {}: {}".format(
                            artist_s, artist_r["name"], artist_match[-1]
                        )
                    )
        sim_artists = max(artist_match)

        artist_similar.append(sim_artists)

        track_n_s = source_track.name + (
            "" if not source_track.mix else f" - {source_track.mix}"
        )
        track_n_r = track["name"]
        sim_name = similar(track_n_s, track_n_r)
        if debug_comp:
            logger.info(
                f"\t\t\t[+] {track_n_s.lower()} vs {track_n_r.lower()}: {sim_name}"
            )
        track_n_similar.append(sim_name)

        duration_s = source_track.duration_ms
        duration_r = track["duration_ms"]
        try:
            sim_duration = duration_r / duration_s
        except Exception as e:
            logger.warning(f"track {source_track!s} has duration error {e}")
        sim_duration = 1  # TODO: remove this
        # if debug_comp:
        #     logger.info(
        #         "\t\t\t[+] {} vs {}: {}".format(duration_s, duration_r, sim_duration)
        #     )
        duration_similar.append(sim_duration)
        if debug_comp:
            logger.info("-")

        tracks_sim = [
            a * n * d
            for a, n, d in zip(artist_similar, track_n_similar, duration_similar)
        ]

    return tracks_sim


def _get_duration_matches(
    source_track: BeatportTrack,
    found_tracks: list[dict],
    silent: bool,
    debug_duration: bool,
) -> list[dict]:
    duration_matches = []
    for track in found_tracks:
        if do_durations_match(
            source_track.duration_ms,
            track["duration_ms"],
            silent=silent,
            debug_duration=debug_duration,
        ):
            duration_matches.append(track)
    return duration_matches


def _get_best_similarity_match(
    source_track: BeatportTrack,
    found_tracks: list[dict],
    silent: bool,
    match_threshold: float,
    debug_comp: bool,
) -> str | None:
    tracks_sim = tracks_similarity(source_track, found_tracks, debug_comp)
    tracks_sim_a = np.array(tracks_sim)
    if any(tracks_sim_a >= match_threshold):
        max_value = max(tracks_sim)
        max_index = tracks_sim.index(max_value)
        best_sim_id = found_tracks[max_index]["id"]
        if not silent:
            logger.info(
                "\t\t\t[+] Multiple matches with more than 85%:"
                f" {sum(tracks_sim_a >= match_threshold)}, "
                f"max:{max_value}, ID: {best_sim_id}"
            )
        return best_sim_id
    else:
        if not silent:
            logger.info("\t\t\t[+] No good match found, skipping")
        return None


def best_of_multiple_matches(
    source_track: BeatportTrack,
    found_tracks: list[dict],
    silent: bool = silent_search,
) -> str | None:
    """Find the best match among multiple tracks.

    Args:
        source_track (dict): Source track.
        found_tracks (list): List of found tracks.
        silent (bool): Whether to suppress logging output.

    Returns:
        str: ID of the best match.

    """
    # Only one diff in letter case is only 85% similarity
    match_threshold = 0.85
    debug_duration = False
    debug_comp = False  # Will show the comparison score between the tracks

    duration_matches = _get_duration_matches(
        source_track, found_tracks, silent, debug_duration
    )

    if len(duration_matches) == 1:
        best_track = duration_matches[0]
        tracks_sim = tracks_similarity(source_track, [best_track])
        if tracks_sim[0] >= match_threshold:
            if not silent:
                logger.info(
                    "\t\t\t[+] Only one exact match with matching duration, "
                    f"going with that one: {best_track['id']}"
                )
            return best_track["id"]
        else:
            if not silent:
                logger.info(
                    "\t\t\t[+] Only one exact match with matching duration, "
                    f"but similarity is too low {tracks_sim[0]}:"
                    f" {get_track_detail(best_track['id'])}"
                )
    # TODO: Popularity does not always yield the correct result
    best_sim_id = _get_best_similarity_match(
        source_track, found_tracks, silent, match_threshold, debug_comp
    )
    if best_sim_id:
        return best_sim_id

    # Keep alternative, could use if multiple matches with same score
    best_track_id = most_popular_track(found_tracks)

    if not silent:
        logger.info(
            "\t\t\t[+] Multiple exact matches with matching durations,"
            f" going with the most popular one: {best_track_id}"
        )
    return best_track_id


def search_wrapper(query: str, logger: logging.Logger = logger) -> dict:
    """Search for a track on Spotify.

    Args:
        query (str): Search query.
        logger (logging.Logger): Logger instance.

    Returns:
        dict: Search results.

    """
    spotify_ins = spotify_auth()
    logger.setLevel(logging.FATAL)
    try:
        result = spotify_ins.search(query)
    except SpotifyException as e:
        logger.setLevel(logging.INFO)
        if e.http_status == 404 or (e.http_status == 400 and e.code == -1):
            # Return empty result
            return {"tracks": {"items": []}}
        else:
            pass
    except Exception as e:
        logger.setLevel(logging.INFO)
        logger.warning(f"NEW exception: {e!s}")
    logger.setLevel(logging.INFO)
    return result


def _perform_track_search(
    query_description: str,
    query: str,
    track: BeatportTrack,
    silent: bool,
) -> str | None:
    if not silent:
        logger.info(query_description)
        logger.info(f"\t[+] Search Query: {query}")
    search_results = search_wrapper(query)
    if len(search_results["tracks"]["items"]) == 1:
        track_id = search_results["tracks"]["items"][0]["id"]
        if not silent:
            logger.info(f"\t\t[+] Found an exact match: {track_id}")
        do_durations_match(
            track.duration_ms, search_results["tracks"]["items"][0]["duration_ms"]
        )
        return track_id

    if len(search_results["tracks"]["items"]) > 1:
        if not silent:
            logger.info(
                "\t\t\t[+] Found multiple exact matches ({})".format(
                    len(search_results["tracks"]["items"])
                )
                + "."
            )
        return best_of_multiple_matches(track, search_results["tracks"]["items"])
    return None


def search_for_track(track: BeatportTrack, silent: bool = silent_search) -> str | None:
    """Search for a track on Spotify.

    Args:
        track (dict): Track to search for.
        silent (bool): Whether to suppress logging output.

    Returns:
        str: Track ID if found, otherwise None.

    """
    # TODO: This is repetitive, can probably refactor but works for now
    if not silent:
        logger.info(
            "[+] Searching for track: {}{}by {} on {}".format(
                track.name,
                " " if not track.mix else f" ({track.mix}) ",
                ", ".join(track.artists),
                track.release,
            )
        )

    # Search with Title, Mix, Artists, and Release / Album
    query = "{}{}{} {}".format(
        track.name,
        " " if not track.mix else f" {track.mix} ",
        " ".join(track.artists),
        track.release,
    )
    track_id = _perform_track_search(
        "\t\t[+] No exact matches on name, mix, artists, and release.",
        query,
        track,
        silent,
    )
    if track_id:
        return track_id

    # Not enough results, search w/o release
    if not silent:
        logger.info(
            "\t\t[+] No exact matches on name, mix, artists, and release."
            "  Trying without release."
        )  # Search with Title, Mix, and Artists
    query = "{}{}{}".format(
        track.name,
        " " if not track.mix else f" {track.mix} ",
        " ".join(track.artists),
    )
    track_id = _perform_track_search(
        "\t\t[+] No exact matches on name, mix, and artists."
        " Trying without mix, but with release.",
        query,
        track,
        silent,
    )
    if track_id:
        return track_id

    # Not enough results, search w/o mix, but with release
    if not silent:
        logger.info(
            "\t\t[+] No exact matches on name, mix, and artists."
            " Trying without mix, but with release."
        )
    query = "{} {} {}".format(track.name, " ".join(track.artists), track.release)
    track_id = _perform_track_search(
        "\t\t[+] No exact matches on name, artists, and release."
        " Trying with just name and artists.",
        query,
        track,
        silent,
    )
    if track_id:
        return track_id

    # Not enough results, search w/o mix or release
    if not silent:
        logger.info(
            "\t\t[+] No exact matches on name, artists, and release."
            " Trying with just name and artists."
        )
    query = "{} {}".format(track.name, " ".join(track.artists))
    track_id = _perform_track_search(
        "\t\t[+] No exact matches on name and artists v1.",
        query,
        track,
        silent,
    )
    if track_id:
        return track_id

    logger.info(
        "\t\t[+] No exact matches on name and artists v1 : {} - {}{}".format(
            track.artists[0],
            track.name,
            "" if not track.mix else f" - {track.mix}",
        )
    )
    logger.info("\t[!] Could not find this song on Spotify!")
    return None


def parse_search_results_spotify(
    search_results: dict, track: BeatportTrack, silent: bool = silent_search
) -> str | None:
    """Parse Spotify search results.

    Args:
        search_results (dict): Spotify API search results.
        track (dict): Track to search for.
        silent (bool): Whether to suppress logging output.

    Returns:
        str: Track ID if found, otherwise None.

    """
    track_id = None

    if len(search_results["tracks"]["items"]) == 1:
        best_track = search_results["tracks"]["items"][0]
        tracks_sim = tracks_similarity(track, [best_track])
        if tracks_sim[0] > 0.9:
            if not silent:
                logger.info(
                    "\t\t\t[+] Only one exact match from search: {} - {}".format(
                        get_track_detail(best_track["id"]), best_track["id"]
                    )
                )
            return best_track["id"]
        else:
            if not silent:
                logger.info(
                    "\t\t\t[+] Only one exact match with matching duration,"
                    " but similarity is too low {}: {}".format(
                        tracks_sim[0], get_track_detail(best_track["id"])
                    )
                )

    if len(search_results["tracks"]["items"]) > 1:
        if not silent:
            logger.info(
                "\t\t[+] Found multiple exact matches ({}).".format(
                    len(search_results["tracks"]["items"])
                )
            )
        return best_of_multiple_matches(track, search_results["tracks"]["items"])

    return track_id


def parse_track_regex_beatport(track: BeatportTrack) -> list:
    """Parse track name and mix using regular expressions.

    Args:
        track (dict): Track dictionary.

    Returns:
        list: List of modified track dictionaries.

    """
    tracks_out = []

    # Method 1
    track_out = track.model_copy()  # Otherwise modifies the dict
    track_out.name = re.sub(
        r"(\s*(Feat|feat|Ft|ft)\. [\w\s]*$)", "", track_out.name
    )  # Remove feat info, mostly not present in spotify
    track_out.name = re.sub(
        r"\W", " ", track_out.name
    )  # Remove special characters as they are not handled by Spotify API

    tracks_out.append(track_out)

    # Method 2
    # Remove feat, special char and mixes
    track_out = track.model_copy()  # Otherwise modifies the dict
    track_out.name = re.sub(
        r"(\s*(Feat|feat|Ft|ft)\. [\w\s]*$)", "", track_out.name
    )  # Remove feat info, mostly not present in spotify
    # track_out.name = re.sub(
    #     r"[^\w\s]", "", track_out.name
    # )  # Remove special characters as they are not handled by Spotify API
    if re.search("[O|o]riginal [M|m]ix", track_out.mix):
        # Remove original mix as not used in Spotify
        # TODO add track duration check in similarity
        track_out.mix = None
    if track_out.mix == "Extended Mix":
        # Remove Extended Mix as not used in Spotify
        # TODO add track duration check in similarity
        track_out.mix = None

    tracks_out.append(track_out)

    # Method 3
    track_out = track.model_copy()  # Otherwise modifies the dict
    track_out.name = re.sub(
        r"(\s*(Feat|feat|Ft|ft)\. [\w\s]*$)", "", track_out.name
    )  # Remove feat info, mostly not present in spotify
    track_out.name = re.sub(
        r"[^\w\s]", "", track_out.name
    )  # Remove special characters as they are not handled by Spotify API

    tracks_out.append(track_out)

    # Method 4
    track_out = track.model_copy()  # Otherwise modifies the dict
    track_out.name = re.sub(
        r"(\s*(Feat|feat|Ft|ft)\. [\w\s]*$)", "", track_out.name
    )  # Remove feat info, mostly not present in spotify
    track_out.name = re.sub(
        r"[^\w\s]", "", track_out.name
    )  # Remove special characters as they are not handled by Spotify API
    track_out.mix = re.sub("[R|r]emix", "mix", track_out.mix)  # Change remix
    track_out.mix = re.sub("[M|m]ix", "Remix", track_out.mix)  # Change to remix

    tracks_out.append(track_out)

    # Method 5
    track_out = track.model_copy()  # Otherwise modifies the dict
    track_out.name = re.sub(
        r"(\s*(Feat|feat|Ft|ft)\. [\w\s]*$)", "", track_out.name
    )  # Remove feat info, mostly not present in spotify
    track_out.name = re.sub(
        r"[^\w\s]", "", track_out.name
    )  # Remove special characters as they are not handled by Spotify API
    track_out.mix = re.sub(
        "[M|m]ix", "", track_out.mix
    )  # Remove special characters as they are not handled by Spotify API

    tracks_out.append(track_out)

    # Method 6
    # Remove feat, special char and replace mixes with radio edit
    # as often exists on Spotify only
    track_out = track.model_copy()  # Otherwise modifies the dict
    track_out.name = re.sub(
        r"(\s*(Feat|feat|Ft|ft)\. [\w\s]*$)", "", track_out.name
    )  # Remove feat info, mostly not present in spotify
    # track_out.name = re.sub(
    #     r"[^\w\s]", "", track_out.name
    # )  # Remove special characters as they are not handled by Spotify API
    if re.search("[O|o]riginal [M|m]ix", track_out.mix):
        # Remove original mix as not used in Spotify
        # TODO add track duration check in similarity
        track_out.mix = "Radio Edit"
    if track_out.mix == "Extended Mix":
        # Remove Extended Mix as not used in Spotify
        # TODO add track duration check in similarity
        track_out.mix = "Radio Edit"

    # Method 7
    # Remove feat, special char and replace mixes with radio edit
    # as often exists on Spotify only
    track_out = track.model_copy()  # Otherwise modifies the dict
    track_out.name = re.sub(
        r"(\s*(Feat|feat|Ft|ft)\. [\w\s]*$)", "", track_out.name
    )  # Remove feat info, mostly not present in spotify
    track_out.mix = "Edit"
    tracks_out.append(track_out)

    # Method 8
    # Remove feat, special char and replace mixes with radio edit
    # as often exists on Spotify only
    track_out = track.model_copy()  # Otherwise modifies the dict
    track_out.name = re.sub(
        r"(\s*(Feat|feat|Ft|ft)\. [\w\s]*$)", "", track_out.name
    )  # Remove feat info, mostly not present in spotify
    track_out.mix = "Radio-Edit"
    tracks_out.append(track_out)

    # Method 9
    # Remove feat, special char and replace mixes with radio edit
    # as often exists on Spotify only
    track_out = track.model_copy()  # Otherwise modifies the dict
    track_out.name = clean_track_name(track_out.name)
    track_out.mix = ""
    # tracks_out.append(track_out)

    return tracks_out


def add_space(match: re.Match) -> str:
    """Add a space before the matched string.

    Args:
        match (re.Match): The matched string.

    Returns:
        str: The modified string with a space added.

    """
    return " " + match.group()


def query_track_album_label(
    track_name: str, artist: str, track_: BeatportTrack, silent: bool = silent_search
) -> str:
    """Generate a Spotify search query with track name, artist, album, and label.

    Args:
        track_name (str): Track name.
        artist (str): Artist name.
        track_ (dict): Track dictionary.
        silent (bool): Whether to suppress logging output.

    Returns:
        str: Search query string.

    """
    # Search with Title, Mix, Artist, Release / Album and Label
    if not silent:
        logger.info(
            f"\t[+] Searching for track: {track_name} by {artist}"
            f" on {track_.release} on {track_.label} label"
        )
    return (
        f'track:"{track_name}" artist:"{artist}" '
        f'album:"{track_.release}" label:"{track_.label}"'
    )


def query_track_label(
    track_name: str, artist: str, track_: BeatportTrack, silent: bool = silent_search
) -> str:
    """Generate a Spotify search query with track name, artist, and label.

    Args:
        track_name (str): Track name.
        artist (str): Artist name.
        track_ (dict): Track dictionary.
        silent (bool): Whether to suppress logging output.

    Returns:
        str: Search query string.

    """
    # Search with Title, Mix, Artist and Label, w/o Release / Album
    if not silent:
        logger.info(
            f"[+]\tSearching for track: {track_name} by {artist} on {track_.label} label"
        )
    return f'track:"{track_name}" artist:"{artist}" label:"{track_.label}"'


def query_track_album(
    track_name: str, artist: str, track_: BeatportTrack, silent: bool = silent_search
) -> str:
    """Generate a Spotify search query with track name, artist, and album.

    Args:
        track_name (str): Track name.
        artist (str): Artist name.
        track_ (BeatportTrack): Track dictionary.
        silent (bool): Whether to suppress logging output.

    Returns:
        str: Search query string.

    """
    # Search with Title, Mix, Artist, Release / Album, w/o  Label
    if not silent:
        logger.info(
            f"[+]\tSearching for track: {track_name}"
            f" by {artist} on {track_.release} album"
        )
    return f'track:"{track_name}" artist:"{artist}" album:"{track_.release}"'


def query_track(
    track_name: str, artist: str, track_: BeatportTrack, silent: bool = silent_search
) -> str:
    """Generate a Spotify search query with track name and artist.

    Args:
        track_name (str): Track name.
        artist (str): Artist name.
        track_ (BeatportTrack): Track dictionary.
        silent (bool): Whether to suppress logging output.

    Returns:
        str: Search query string.

    """
    # Search with Title, Artist, w/o Release or Label
    if not silent:
        logger.info(f"\t\t[+] Searching for track: {track_name} by {artist}")
    return f'track:"{track_name}" artist:"{artist}"'


def track_in_playlist(playlist_id: str, track_id: str) -> bool:
    """Check if a track is in a playlist.

    Args:
        playlist_id (str): Playlist ID.
        track_id (str): Track ID.

    Returns:
        bool: True if track is in playlist, otherwise False.

    """
    for track in get_all_tracks_in_playlist(playlist_id):
        if track["track"]["id"] == track_id:
            return True
    return False


def add_tracks_to_playlist(playlist_id: str, track_ids: list) -> None:
    """Add tracks to a playlist.

    Args:
        playlist_id (str): Playlist ID.
        track_ids (list): List of track IDs.

    """
    if track_ids:
        spotify_ins = spotify_auth()
        position = 0 if add_at_top_playlist else None
        spotify_ins.user_playlist_add_tracks(
            user=username, playlist_id=playlist_id, tracks=track_ids, position=position
        )


def get_all_tracks_in_playlist(playlist_id: str) -> list:
    """Get all tracks in a playlist.

    Args:
        playlist_id (str): Playlist ID.

    Returns:
        list: List of tracks.

    """
    spotify_ins = spotify_auth()
    playlist_tracks_pager = spotify_ins.playlist_items(
        playlist_id=playlist_id, additional_types=("track",)
    )
    playlist_tracks = playlist_tracks_pager["items"]
    while playlist_tracks_pager.get("next"):
        playlist_tracks_pager = spotify_ins.next(playlist_tracks_pager)
        if playlist_tracks_pager is None:
            break
        playlist_tracks.extend(playlist_tracks_pager["items"])
    return playlist_tracks


def clear_playlist(playlist_id: str) -> None:
    """Clear a playlist.

    Args:
        playlist_id (str): Playlist ID.

    """
    spotify_ins = spotify_auth()
    for track in get_all_tracks_in_playlist(playlist_id):
        spotify_ins.user_playlist_remove_all_occurrences_of_tracks(
            username,
            playlist_id,
            [
                track["track"]["id"],
            ],
        )


def parse_tracks_spotify(tracks_json: dict) -> list[BeatportTrack]:
    """Parse tracks from Spotify JSON.

    Args:
        tracks_json (dict): JSON data containing tracks.

    Returns:
        list: List of parsed tracks.

    """
    tracks = list()
    for track_item in tracks_json["tracks"]["items"]:
        track_data = {
            "title": track_item.get("name"),
            "name": track_item.get("name", ""),
            "mix": "",
            "artists": [artist["name"] for artist in track_item.get("artists", [])],
            "remixers": [],
            "release": track_item.get("album", {}).get("name", ""),
            "label": track_item.get("album", {}).get("label", ""),
            "published_date": track_item.get("album", {}).get("release_date", ""),
            "released_date": track_item.get("album", {}).get("release_date", ""),
            "duration_ms": track_item.get("duration_ms", 0),
            "genres": [],
            "bpm": 0,
            "key": "",
        }

        duration_ms = track_data["duration_ms"]
        minutes = duration_ms // 60000
        seconds = (duration_ms % 60000) // 1000
        track_data["duration"] = f"{minutes}:{seconds:02d}"

        try:
            tracks.append(BeatportTrack.model_validate(track_data))
        except Exception as e:
            logger.warning(f"Failed to parse Spotify track {track_item.get('name')}: {e}")
    return tracks


def update_hist_pl_tracks(
    df_hist_pl_tracks: pd.DataFrame, playlist: dict
) -> pd.DataFrame:
    """Update the history DataFrame with a playlist.

    Args:
        df_hist_pl_tracks (pd.DataFrame): DataFrame of
          history of track id and playlist id.
        playlist (dict): Playlist dictionary.

    Returns:
        pd.DataFrame: Updated DataFrame.

    """
    track_list = get_all_tracks_in_playlist(playlist["id"])
    if not track_list:
        return df_hist_pl_tracks

    df_playlist_tracks = pd.DataFrame.from_records(track_list)

    # A track can be None if it has been removed from Spotify
    df_playlist_tracks.dropna(subset=["track"], inplace=True)
    df_playlist_tracks.reset_index(drop=True, inplace=True)  # Reset index for alignment

    if df_playlist_tracks.empty:
        return df_hist_pl_tracks

    # Expand the 'track' column (which contains dicts) into a DataFrame
    track_details = pd.json_normalize(df_playlist_tracks["track"])

    # Create a temporary DataFrame with the new tracks to be added to history
    df_temp = pd.DataFrame(
        {
            "track_id": track_details["id"],
            "datetime_added": df_playlist_tracks["added_at"],
            "artist_name": (
                track_details["artists"].apply(
                    lambda a: a[0]["name"] if a else "Unknown Artist"
                )
                + " - "
                + track_details["name"]
            ),
            "playlist_id": playlist["id"],
            "playlist_name": playlist["name"],
        }
    )

    # Combine with the main history DataFrame, remove duplicates, and reset index
    df_hist_pl_tracks = pd.concat([df_hist_pl_tracks, df_temp], ignore_index=True)
    df_hist_pl_tracks = df_hist_pl_tracks.drop_duplicates(inplace=False)
    df_hist_pl_tracks = df_hist_pl_tracks.reset_index(drop=True, inplace=False)
    df_hist_pl_tracks = df_hist_pl_tracks.copy(deep=True)

    _ = gc.collect()

    return df_hist_pl_tracks


def find_playlist_chart_label(title: str) -> dict:
    """Find playlist chart label.

    Args:
        title (str): Chart or label title.

    Returns:
        dict: Dictionary of playlist name and playlist ID.

    """
    persistent_playlist_name = f"{playlist_prefix}{title}"
    playlist = {
        "name": persistent_playlist_name,
        "id": get_playlist_id(persistent_playlist_name),
    }

    return playlist


def _get_local_history_for_playlist_id(
    df_hist_pl_tracks: pd.DataFrame, playlist_id: str, digging_mode: str
) -> pd.DataFrame:
    if digging_mode == "playlist":
        df_local_hist = df_hist_pl_tracks.loc[
            df_hist_pl_tracks["playlist_id"] == playlist_id
        ]
    elif digging_mode == "all":
        df_local_hist = df_hist_pl_tracks
    else:
        df_local_hist = pd.DataFrame(
            columns=[
                "playlist_id",
                "playlist_name",
                "track_id",
                "datetime_added",
                "artist_name",
            ]
        )
    return df_local_hist


def _process_track_in_playlist_id(
    track: dict, df_local_hist: pd.DataFrame, playlist_track_ids: pd.Series, silent: bool
) -> tuple[str | None, bool]:
    track_id = None
    added = False
    if track["track"] is not None:  # Prevent error of empty track
        track_id = track["track"]["id"]
        if track_id not in df_local_hist.values:
            if track_id not in playlist_track_ids.values:
                if not silent:
                    logger.info(f"\t[+] Adding track id : {track_id}")
                if track_id is not None:
                    added = True
                else:
                    logger.warn(
                        "\t[+]! Trying to add track_id None : {} - {}".format(
                            track["track"]["artists"][0]["name"],
                            track["track"]["name"],
                        )
                    )
            else:
                if not silent:
                    logger.info("\tTrack already found in playlist or history")
    return track_id, added


def add_new_tracks_to_playlist_id(
    playlist_name: str,
    track_ids: list,
    df_hist_pl_tracks: pd.DataFrame,
    silent: bool = silent_search,
) -> pd.DataFrame:
    """Add new tracks to a playlist by ID.

    Args:
        playlist_name (str): Playlist name to be used.
        track_ids (list): List of track IDs.
        df_hist_pl_tracks (pd.DataFrame): DataFrame of history of track.
        silent (bool): If true do not display searching details except errors.

    Returns:
        pd.DataFrame: Updated DataFrame.

    """
    # TODO unify all add_new_track in one function

    # TODO export playlist prefix name to config
    persistent_playlist_name = playlist_name
    logger.info(f'[+] Identifying new tracks for playlist: "{persistent_playlist_name}"')

    playlist = {
        "name": persistent_playlist_name,
        "id": get_playlist_id(persistent_playlist_name),
    }

    if playlist["id"] is None:  # Explicitly check for None
        logger.warning(
            '\t[!] Playlist "{}" does not exist, creating it.'.format(playlist["name"])
        )
        new_playlist_id: str = create_playlist(cast(str, playlist["name"]))
        playlist["id"] = new_playlist_id

    assert playlist["id"] is not None

    df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)
    playlist_track_ids = df_hist_pl_tracks.loc[
        df_hist_pl_tracks["playlist_id"] == playlist["id"], "track_id"
    ]

    df_local_hist = _get_local_history_for_playlist_id(
        df_hist_pl_tracks, playlist["id"], digging_mode
    )

    persistent_track_ids = list()
    track_count = 0

    for track_count_tot, track in enumerate(track_ids):
        track_id, added = _process_track_in_playlist_id(
            track, df_local_hist, playlist_track_ids, silent
        )
        if added:
            persistent_track_ids.append(track_id)
            track_count += 1
        if track_count >= 99:  # Have limit of 100 trakcks per import
            logger.warning(
                f"[+] Adding {len(persistent_track_ids)} new tracks "
                f'to the playlist: "{persistent_playlist_name}"'
            )
            add_tracks_to_playlist(playlist["id"], persistent_track_ids)
            # TODO consider only adding new ID to avoid reloading large playlist
            df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)
            playlist_track_ids = df_hist_pl_tracks.loc[
                df_hist_pl_tracks["playlist_id"] == playlist["id"], "track_id"
            ]
            track_count = 0
            persistent_track_ids = list()
            update_playlist_description_with_date(playlist)

        if track_count_tot % refresh_token_n_tracks == 0:  # Avoid time out
            _ = spotify_auth()

    if len(persistent_track_ids) > 0:
        logger.warning(
            f"[+] Adding {len(persistent_track_ids)} new tracks to the "
            f'playlist: "{persistent_playlist_name}"'
        )
        add_tracks_to_playlist(playlist["id"], persistent_track_ids)
        update_playlist_description_with_date(playlist)
    else:
        logger.info(
            f'[+] No new tracks to add to the playlist: "{persistent_playlist_name}"'
        )

    df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)

    if len(persistent_track_ids) > 0:
        save_hist_dataframe(df_hist_pl_tracks)

    return df_hist_pl_tracks


def update_playlist_description_with_date(playlist: dict) -> None:
    """Update playlist description with current date.

    Args:
        playlist (dict): Playlist dictionary.

    """
    spotify_ins = spotify_auth()
    # Cast the entire playlist_desc to help mypy understand its structure
    # We assume 'description' key will always be a string for a playlist object
    playlist_info: PlaylistDescription = cast(
        PlaylistDescription, spotify_ins.playlist(playlist_id=playlist["id"])
    )

    current_description: str = playlist_info["description"]
    current_description = re.sub(
        r"\s*Updated on \d{4}-\d{2}-\d{2}\.*", "", current_description
    )
    current_description = re.sub(r"&#x2F;", "/", current_description)

    spotify_ins.playlist_change_details(
        playlist_id=playlist["id"],
        description=current_description
        + " Updated on {}.".format(datetime.today().strftime("%Y-%m-%d")),
    )


def update_hist_from_playlist(
    title: str, df_hist_pl_tracks: pd.DataFrame
) -> pd.DataFrame:
    """Update history from playlist.

    Args:
        title (str): Playlist title.
        df_hist_pl_tracks (pd.DataFrame): DataFrame of history of track.

    Returns:
        pd.DataFrame: Updated DataFrame.

    """
    # TODO test, to remove
    persistent_playlist_name = f"{playlist_prefix}{title}"
    logger.info(f'[+] Getting hist of tracks for playlist: "{persistent_playlist_name}"')

    playlist = {
        "name": persistent_playlist_name,
        "id": get_playlist_id(persistent_playlist_name),
    }

    if playlist["id"]:
        assert isinstance(playlist["id"], str)
        df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)
        # TODO else pass ?

    return df_hist_pl_tracks


def back_up_spotify_playlist(
    playlist_name: str, org_playlist_id: str, df_hist_pl_tracks: pd.DataFrame
) -> pd.DataFrame:
    """Back up tracks in Spotify playlist.

    Args:
        playlist_name (str): Playlist name.
        org_playlist_id (str): Original playlist ID.
        df_hist_pl_tracks (pd.DataFrame): DataFrame of history of track.

    Returns:
        pd.DataFrame: Updated DataFrame.

    """
    track_ids = get_all_tracks_in_playlist(org_playlist_id)
    df_hist_pl_tracks = add_new_tracks_to_playlist_id(
        playlist_name, track_ids, df_hist_pl_tracks
    )

    return df_hist_pl_tracks


def get_track_detail(track_id: str) -> str:
    """Get track details.

    Args:
        track_id (str): Track ID.

    Returns:
        str: Track details string.

    """
    spotify_ins = spotify_auth()
    track_result = spotify_ins.track(f"spotify:track:{track_id}")
    artists_list: list[str] = [artist["name"] for artist in track_result["artists"]]
    artists_str: str = ", ".join(artists_list)

    return "{} by {}".format(track_result["name"], artists_str)


# Annex testing tracks with known issues

track_working_mix: BeatportTrack = BeatportTrack(
    title="",
    name="The Shake",
    mix="Extended Mix",
    artists=["Ellis Moss"],
    remixers=[],
    release="The Shake",
    label="Toolroom Trax",
    published_date="2021-01-29",
    released_date="2021-01-29",
    duration="6:14",
    duration_ms=374032,
    genres=["Tech House"],
    bpm=124,
    key="G min",
)

track_not_working_mix: BeatportTrack = BeatportTrack(
    title="",
    name="Jumpin'",
    mix="Extended",
    artists=["CID", "Westend"],
    remixers=[],
    release="Jumpin'",
    label="Repopulate Mars",
    published_date="2021-02-12",
    released_date="2021-02-12",
    duration="5:04",
    duration_ms=304761,
    genres=["Tech House"],
    bpm=126,
    key="A min",
)

track_not_working_artist: BeatportTrack = BeatportTrack(
    title="",
    name="Set U Free",
    mix="Extended Mix",
    artists=["GUZ (NL)"],
    remixers=[],
    release="Set U Free (Extended Mix)",
    label="Sink or Swim",
    published_date="2021-01-29",
    released_date="2021-01-29",
    duration="4:23",
    duration_ms=263040,
    genres=["Tech House"],
    bpm=125,
    key="B maj",
)

track_special_characters: BeatportTrack = BeatportTrack(
    title="",
    name="Don't Touch The Pool",
    mix="Original Mix",
    artists=["FOVOS"],
    remixers=[],
    release="Hot Mess",
    label="Country Club Disco",
    published_date="2021-02-12",
    released_date="2021-02-12",
    duration="3:47",
    duration_ms=227302,
    genres=["Tech House"],
    bpm=128,
    key="A maj",
)

track_name_special_char_cant_space: BeatportTrack = BeatportTrack(
    title="",
    name="Don't Make Me",
    mix="Original Mix",
    artists=["Dillon Nathaniel"],
    remixers=[],
    release="Reason to Fly",
    label="Sola",
    published_date="2021-02-19",
    released_date="2021-02-19",
    duration="5:46",
    duration_ms=346666,
    genres=["Tech House"],
    bpm=126,
    key="G\u266d min",
)
