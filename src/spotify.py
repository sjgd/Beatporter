"""Module to manage Spotify queries."""

import asyncio
import json
import logging
import re
import socket
import sys
import webbrowser
from datetime import datetime
from difflib import SequenceMatcher
from time import sleep, time
from typing import Any

import numpy as np
import pandas as pd
import spotipy
from requests.exceptions import ReadTimeout
from spotipy import SpotifyException, oauth2
from spotipy.oauth2 import CacheFileHandler

from config import (
    add_at_top_playlist,
    client_id,
    client_secret,
    daily_mode,
    daily_n_track,
    digging_mode,
    parse_track,
    playlist_description,
    playlist_prefix,
    redirect_uri,
    refresh_token_n_tracks,
    root_path,
    scope,
    silent_search,
    username,
)
from src.search_utils import clean_track_name
from utils import configure_logging, save_hist_dataframe

configure_logging()
logger = logging.getLogger("spotify")

tracks_dict_names = ["id", "duration_ms", "href", "name", "popularity", "uri", "artists"]


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


def do_spotify_oauth() -> dict:
    """Authenticate to Spotify.

    Returns:
        dict: The Spotify token information.

    """
    TOKEN_PATH = root_path + "data/token.json"
    try:
        with open(TOKEN_PATH) as fh:
            token = fh.read()
        token = json.loads(token)
    except Exception:
        token = None
    if token:
        if int(time()) > (
            token["expires_at"] - 50
        ):  # Take 50s margin to avoid timeout while searching
            logger.info("[+][+] Refreshing Spotify token")
            token = sp_oauth.refresh_access_token(token["refresh_token"])
    else:
        authorization_code = asyncio.run(async_get_auth_code())
        logger.info(authorization_code)
        if not authorization_code:
            logger.info(
                "[!] Unable to authenticate to Spotify.  Couldn't get authorization code"
            )
            sys.exit(-1)
        token = sp_oauth.get_access_token(authorization_code)
    if not token:
        logger.info("[!] Unable to authenticate to Spotify.  Couldn't get access token.")
        sys.exit(-1)
    try:
        with open(TOKEN_PATH, "w+") as fh:
            fh.write(json.dumps(token))
    except Exception:
        logger.info("[!] Unable to to write token object to disk.  This is non-fatal.")
    return token


def get_all_playlists() -> list:
    """Get all playlists.

    Returns:
        list: List of playlists.

    """
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
    playlist = spotify_ins.user_playlist_create(
        username, playlist_name, description=playlist_description
    )
    return playlist["id"]


def get_playlist_id(playlist_name: str) -> str:
    """Get playlist ID.

    Args:
        playlist_name (str): The name of the playlist.

    Returns:
        str: The ID of the playlist.

    """
    playlists = get_all_playlists()
    for playlist in playlists:
        if (
            playlist["owner"]["id"] == username
        ):  # Can only modify playlist that the user owns
            if playlist["name"] == playlist_name:
                return playlist["id"]
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


def most_popular_track(tracks: list) -> str:
    """Find the most popular track.

    Args:
        tracks (list): List of tracks.

    Returns:
        str: The ID of the most popular track.

    """
    # Popularity does not always yield the correct result
    high_score = 0
    winner = None
    for track in tracks:
        if track["popularity"] > high_score:
            winner = track["id"]
            high_score = track["popularity"]
    return winner


def tracks_similarity(
    source_track: dict, found_tracks: list, debug_comp: bool = False
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
        # artist_r = track["artists"][0][
        #     "name"
        # ]  # ", ".join([artist["name"] for artist in track["artists"]])
        artist_match = []
        for artist_s in source_track["artists"]:  # ", ".join(source_track["artists"])
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

        track_n_s = source_track["name"] + (
            "" if not source_track["mix"] else " - {}".format(source_track["mix"])
        )
        track_n_r = track["name"]
        sim_name = similar(track_n_s, track_n_r)
        if debug_comp:
            logger.info(
                f"\t\t\t[+] {track_n_s.lower()} vs {track_n_r.lower()}: {sim_name}"
            )
        track_n_similar.append(sim_name)

        duration_s = source_track["duration_ms"]
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


def best_of_multiple_matches(
    source_track: dict, found_tracks: list, silent: bool = silent_search
) -> str:
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

    counter = 1
    duration_matches = [
        0,
    ]
    for track in found_tracks:
        if not silent and debug_duration:
            logger.info("\t\t\t[+] Match {}: {}".format(counter, track["id"]))
        if do_durations_match(
            source_track["duration_ms"],
            track["duration_ms"],
            debug_duration=debug_duration,
        ):
            duration_matches[0] += 1
            duration_matches.append(track)
        counter += 1
    if duration_matches[0] == 1:
        best_track = duration_matches.pop()
        tracks_sim = tracks_similarity(source_track, [best_track])
        if tracks_sim[0] >= match_threshold:
            if not silent:
                logger.info(
                    "\t\t\t[+] Only one exact match with matching duration, "
                    "going with that one: {}".format(best_track["id"])
                )
            return best_track["id"]
        else:
            if not silent:
                logger.info(
                    "\t\t\t[+] Only one exact match with matching duration, "
                    "but similarity is too low {}: {}".format(
                        tracks_sim[0], get_track_detail(best_track["id"])
                    )
                )

    # TODO: Popularity does not always yield the correct result
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

    # Keep alternative, could use if multiple matches with same score
    best_track = most_popular_track(found_tracks)

    if not silent:
        logger.info(
            "\t\t\t[+] Multiple exact matches with matching durations,"
            f" going with the most popular one: {best_track}"
        )
    return best_track


def search_wrapper(query: str, logger: logging.Logger = logger) -> dict:
    """Search for a track on Spotify.

    Args:
        query (str): Search query.
        logger (logging.Logger): Logger instance.

    Returns:
        dict: Search results.

    """
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


def search_for_track(track: dict, silent: bool = silent_search) -> str:
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
                track["name"],
                " " if not track["mix"] else " ({}) ".format(track["mix"]),
                ", ".join(track["artists"]),
                track["release"],
            )
        )
    # Search with Title, Mix, Artists, and Release / Album
    query = "{}{}{} {}".format(
        track["name"],
        " " if not track["mix"] else " {} ".format(track["mix"]),
        " ".join(track["artists"]),
        track["release"],
    )
    if not silent:
        logger.info(f"\t[+] Search Query: {query}")
    search_results = search_wrapper(query)
    if len(search_results["tracks"]["items"]) == 1:
        track_id = search_results["tracks"]["items"][0]["id"]
        if not silent:
            logger.info(
                "\t\t[+] Found an exact match on name, mix, artists,"
                f" and release: {track_id}"
            )
        do_durations_match(
            track["duration_ms"], search_results["tracks"]["items"][0]["duration_ms"]
        )
        return track_id

    if len(search_results["tracks"]["items"]) > 1:
        if not silent:
            logger.info(
                "\t\t[+] Found multiple exact matches ({}) on name,"
                " mix, artists, and release.".format(
                    len(search_results["tracks"]["items"])
                )
            )
        return best_of_multiple_matches(track, search_results["tracks"]["items"])

    # Not enough results, search w/o release
    if not silent:
        logger.info(
            "\t\t[+] No exact matches on name, mix, artists, and release."
            "  Trying without release."
        )
    # Search with Title, Mix, and Artists
    query = "{}{}{}".format(
        track["name"],
        " " if not track["mix"] else " {} ".format(track["mix"]),
        " ".join(track["artists"]),
    )
    if not silent:
        logger.info(f"\t[+] Search Query: {query}")
    search_results = search_wrapper(query)
    if len(search_results["tracks"]["items"]) == 1:
        track_id = search_results["tracks"]["items"][0]["id"]
        if not silent:
            logger.info(
                f"\t\t[+] Found an exact match on name, mix, and artists: {track_id}"
            )
        do_durations_match(
            track["duration_ms"], search_results["tracks"]["items"][0]["duration_ms"]
        )
        return track_id

    if len(search_results["tracks"]["items"]) > 1:
        if not silent:
            logger.info(
                "\t\t[+] Found multiple exact matches ({}) on name,"
                " mix, and artists.".format(len(search_results["tracks"]["items"]))
            )
        return best_of_multiple_matches(track, search_results["tracks"]["items"])

    # Not enough results, search w/o mix, but with release
    if not silent:
        logger.info(
            "\t\t[+] No exact matches on name, mix, and artists."
            " Trying without mix, but with release."
        )
    query = "{} {} {}".format(track["name"], " ".join(track["artists"]), track["release"])
    if not silent:
        logger.info(f"\t[+] Search Query: {query}")
    search_results = search_wrapper(query)
    if len(search_results["tracks"]["items"]) == 1:
        track_id = search_results["tracks"]["items"][0]["id"]
        if not silent:
            logger.info(
                f"\t\t[+] Found an exact match on name, artists, and release: {track_id}"
            )
        do_durations_match(
            track["duration_ms"], search_results["tracks"]["items"][0]["duration_ms"]
        )
        return track_id

    if len(search_results["tracks"]["items"]) > 1:
        if not silent:
            logger.info(
                "\t\t[+] Found multiple exact matches ({}) on name,"
                " artists, and release.".format(len(search_results["tracks"]["items"]))
            )
        return best_of_multiple_matches(track, search_results["tracks"]["items"])

    # Not enough results, search w/o mix or release
    if not silent:
        logger.info(
            "\t\t[+] No exact matches on name, artists, and release."
            " Trying with just name and artists."
        )
    query = "{} {}".format(track["name"], " ".join(track["artists"]))
    if not silent:
        logger.info(f"\t[+] Search Query: {query}")
    search_results = search_wrapper(query)
    if len(search_results["tracks"]["items"]) == 1:
        track_id = search_results["tracks"]["items"][0]["id"]
        if not silent:
            logger.info(f"\t\t[+] Found an exact match on name and artists: {track_id}")
        do_durations_match(
            track["duration_ms"], search_results["tracks"]["items"][0]["duration_ms"]
        )
        return track_id

    if len(search_results["tracks"]["items"]) > 1:
        if not silent:
            logger.info(
                "\t\t[+] Found multiple exact matches ({}) on name and artists.".format(
                    len(search_results["tracks"]["items"])
                )
            )
        return best_of_multiple_matches(track, search_results["tracks"]["items"])

    logger.info(
        "\t\t[+] No exact matches on name and artists v1 : {} - {}{}".format(
            track["artists"][0],
            track["name"],
            "" if not track["mix"] else " - {}".format(track["mix"]),
        )
    )
    logger.info("\t[!] Could not find this song on Spotify!")
    return None


def parse_search_results_spotify(
    search_results: dict, track: dict, silent: bool = silent_search
) -> str:
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


def parse_track_regex_beatport(track: dict) -> list:
    """Parse track name and mix using regular expressions.

    Args:
        track (dict): Track dictionary.

    Returns:
        list: List of modified track dictionaries.

    """
    tracks_out = []

    # Method 1
    track_out = track.copy()  # Otherwise modifies the dict
    track_out["name"] = re.sub(
        r"(\s*(Feat|feat|Ft|ft)\. [\w\s]*$)", "", track_out["name"]
    )  # Remove feat info, mostly not present in spotify
    track_out["name"] = re.sub(
        r"\W", " ", track_out["name"]
    )  # Remove special characters as they are not handled by Spotify API

    tracks_out.append(track_out)

    # Method 2
    # Remove feat, special char and mixes
    track_out = track.copy()  # Otherwise modifies the dict
    track_out["name"] = re.sub(
        r"(\s*(Feat|feat|Ft|ft)\. [\w\s]*$)", "", track_out["name"]
    )  # Remove feat info, mostly not present in spotify
    # track_out["name"] = re.sub(
    #     r"[^\w\s]", "", track_out["name"]
    # )  # Remove special characters as they are not handled by Spotify API
    if re.search("[O|o]riginal [M|m]ix", track_out["mix"]):
        # Remove original mix as not used in Spotify
        # TODO add track duration check in similarity
        track_out["mix"] = None
    if track_out["mix"] == "Extended Mix":
        # Remove Extended Mix as not used in Spotify
        # TODO add track duration check in similarity
        track_out["mix"] = None

    tracks_out.append(track_out)

    # Method 3
    track_out = track.copy()  # Otherwise modifies the dict
    track_out["name"] = re.sub(
        r"(\s*(Feat|feat|Ft|ft)\. [\w\s]*$)", "", track_out["name"]
    )  # Remove feat info, mostly not present in spotify
    track_out["name"] = re.sub(
        r"[^\w\s]", "", track_out["name"]
    )  # Remove special characters as they are not handled by Spotify API

    tracks_out.append(track_out)

    # Method 4
    track_out = track.copy()  # Otherwise modifies the dict
    track_out["name"] = re.sub(
        r"(\s*(Feat|feat|Ft|ft)\. [\w\s]*$)", "", track_out["name"]
    )  # Remove feat info, mostly not present in spotify
    track_out["name"] = re.sub(
        r"[^\w\s]", "", track_out["name"]
    )  # Remove special characters as they are not handled by Spotify API
    track_out["mix"] = re.sub("[R|r]emix", "mix", track_out["mix"])  # Change remix
    track_out["mix"] = re.sub("[M|m]ix", "Remix", track_out["mix"])  # Change to remix

    tracks_out.append(track_out)

    # Method 5
    track_out = track.copy()  # Otherwise modifies the dict
    track_out["name"] = re.sub(
        r"(\s*(Feat|feat|Ft|ft)\. [\w\s]*$)", "", track_out["name"]
    )  # Remove feat info, mostly not present in spotify
    track_out["name"] = re.sub(
        r"[^\w\s]", "", track_out["name"]
    )  # Remove special characters as they are not handled by Spotify API
    track_out["mix"] = re.sub(
        "[M|m]ix", "", track_out["mix"]
    )  # Remove special characters as they are not handled by Spotify API

    tracks_out.append(track_out)

    # Method 6
    # Remove feat, special char and replace mixes with radio edit
    # as often exists on Spotify only
    track_out = track.copy()  # Otherwise modifies the dict
    track_out["name"] = re.sub(
        r"(\s*(Feat|feat|Ft|ft)\. [\w\s]*$)", "", track_out["name"]
    )  # Remove feat info, mostly not present in spotify
    # track_out["name"] = re.sub(
    #     r"[^\w\s]", "", track_out["name"]
    # )  # Remove special characters as they are not handled by Spotify API
    if re.search("[O|o]riginal [M|m]ix", track_out["mix"]):
        # Remove original mix as not used in Spotify
        # TODO add track duration check in similarity
        track_out["mix"] = "Radio Edit"
    if track_out["mix"] == "Extended Mix":
        # Remove Extended Mix as not used in Spotify
        # TODO add track duration check in similarity
        track_out["mix"] = "Radio Edit"

    # Method 7
    # Remove feat, special char and replace mixes with radio edit
    # as often exists on Spotify only
    track_out = track.copy()  # Otherwise modifies the dict
    track_out["name"] = re.sub(
        r"(\s*(Feat|feat|Ft|ft)\. [\w\s]*$)", "", track_out["name"]
    )  # Remove feat info, mostly not present in spotify
    track_out["mix"] = "Edit"
    tracks_out.append(track_out)

    # Method 8
    # Remove feat, special char and replace mixes with radio edit
    # as often exists on Spotify only
    track_out = track.copy()  # Otherwise modifies the dict
    track_out["name"] = re.sub(
        r"(\s*(Feat|feat|Ft|ft)\. [\w\s]*$)", "", track_out["name"]
    )  # Remove feat info, mostly not present in spotify
    track_out["mix"] = "Radio-Edit"
    tracks_out.append(track_out)

    # Method 9
    # Remove feat, special char and replace mixes with radio edit
    # as often exists on Spotify only
    track_out = track.copy()  # Otherwise modifies the dict
    track_out["name"] = clean_track_name(track_out["name"])
    track_out["mix"] = ""
    tracks_out.append(track_out)

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
    track_name: str, artist: str, track_: dict, silent: bool = silent_search
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
            "\t[+] Searching for track: {} by {} on {} on {} label".format(
                track_name, artist, track_["release"], track_["label"]
            )
        )
    return 'track:"{}" artist:"{}" album:"{}" label:"{}"'.format(
        track_name, artist, track_["release"], track_["label"]
    )


def query_track_label(
    track_name: str, artist: str, track_: dict, silent: bool = silent_search
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
            "[+]\tSearching for track: {} by {} on {} label".format(
                track_name, artist, track_["label"]
            )
        )
    return 'track:"{}" artist:"{}" label:"{}"'.format(track_name, artist, track_["label"])


def query_track_album(
    track_name: str, artist: str, track_: dict, silent: bool = silent_search
) -> str:
    """Generate a Spotify search query with track name, artist, and album.

    Args:
        track_name (str): Track name.
        artist (str): Artist name.
        track_ (dict): Track dictionary.
        silent (bool): Whether to suppress logging output.

    Returns:
        str: Search query string.

    """
    # Search with Title, Mix, Artist, Release / Album, w/o  Label
    if not silent:
        logger.info(
            "[+]\tSearching for track: {} by {} on {} album".format(
                track_name, artist, track_["release"]
            )
        )
    return 'track:"{}" artist:"{}" album:"{}"'.format(
        track_name, artist, track_["release"]
    )


def query_track(
    track_name: str, artist: str, track_: dict, silent: bool = silent_search
) -> str:
    """Generate a Spotify search query with track name and artist.

    Args:
        track_name (str): Track name.
        artist (str): Artist name.
        track_ (dict): Track dictionary.
        silent (bool): Whether to suppress logging output.

    Returns:
        str: Search query string.

    """
    # Search with Title, Artist, w/o Release or Label
    if not silent:
        logger.info(f"\t\t[+] Searching for track: {track_name} by {artist}")
    return f'track:"{track_name}" artist:"{artist}"'


def search_for_track_v2(
    track: dict, silent: bool = silent_search, parse_track: bool = parse_track
) -> str:
    """Search for a track on Spotify using various search strategies.

    Args:
        track (dict): Track dictionary.
        silent (bool): Whether to suppress logging output.
        parse_track (bool): Whether to parse the track name and mix.

    Returns:
        str: Spotify track ID if found, otherwise None.

    """
    if parse_track:
        track_parsed = [
            track.copy(),
            *parse_track_regex_beatport(track),
        ]
    else:
        track_parsed = [track]

    for track_ in track_parsed:
        # Create a field name mix according to Spotify formatting
        track_["name_mix"] = "{}{}".format(
            track_["name"], "" if not track_["mix"] else " - {}".format(track_["mix"])
        )

        # Create a parsed artist and try both
        artist_search = [*track_["artists"]]
        if parse_track:
            # Add parsed artist if not in list already
            artist_search.extend(
                x
                for x in [
                    re.sub(r"\s*\([^)]*\)", "", artist_) for artist_ in track_["artists"]
                ]
                if x not in artist_search
            )  # Remove (UK) for example
            artist_search.extend(
                x
                for x in [re.sub(r"\W+", " ", artist_) for artist_ in track_["artists"]]
                if x not in artist_search
            )  # Remove special characters, in case it is not handled by Spotify API
            artist_search.extend(
                x
                for x in [
                    re.sub(r"[^\w\s]", "", artist_) for artist_ in track_["artists"]
                ]
                if x not in artist_search
            )  # Remove special characters, in case it is not handled by Spotify API
            artist_search.extend(
                x
                for x in [
                    re.sub(r"(?<=\w)[A-Z]", add_space, artist_)
                    for artist_ in track_["artists"]
                ]
                if x not in artist_search
            )  # Splitting artist name with a space after a capital letter
            artist_search.extend(
                x
                for x in [re.sub(r"\s&.*$", "", artist_) for artist_ in track_["artists"]]
                if x not in artist_search
            )  # Removing second part after &

        # Search artist and artist parsed if parsed is on
        for artist in artist_search:
            # Search track name and track name without mix (even if parsed is off)
            for track_name in [track_["name_mix"]]:  # , track_["name"]]:
                # # Search with Title, Mix, Artist, Release / Album and Label
                # if not silent:
                #     logger.info(
                #         "\t[+] Searching for track: {} by {} on {} on {} label".format(
                #             track_name, artist, track_["release"], track_["label"]
                #         )
                #     )
                # query = 'track:"{}" artist:"{}" album:"{}" label:"{}"'.format(
                #     track_name, artist, track_["release"], track_["label"]
                # )
                # if not silent:
                #     logger.info("\t\t[+] Search Query: {}".format(query))
                # search_results = search_wrapper(query)
                # track_id = parse_search_results_spotify(search_results, track_)
                # if track_id:
                #     return track_id

                # # Search with Title, Mix, Artist and Label, w/o Release / Album
                # if not silent:
                #     logger.info(
                #         "[+]\tSearching for track: {} by {} on {} label".format(
                #             track_name, artist, track_["label"]
                #         )
                #     )
                # query = 'track:"{}" artist:"{}" label:"{}"'.format(
                #     track_name, artist, track_["label"]
                # )
                # if not silent:
                #     logger.info("\t\t[+] Search Query: {}".format(query))
                # search_results = search_wrapper(query)
                # track_id = parse_search_results_spotify(search_results, track_)
                # if track_id:
                #     return track_id

                # Search with Title, Mix, Artist, Release / Album, w/o  Label
                if not silent:
                    logger.info(
                        "[+]\tSearching for track: {} by {} on {} album".format(
                            track_name, artist, track_["release"]
                        )
                    )
                query = 'track:"{}" artist:"{}" album:"{}"'.format(
                    track_name, artist, track_["release"]
                )
                if not silent:
                    logger.info(f"\t\t[+] Search Query: {query}")
                search_results = search_wrapper(query)
                track_id = parse_search_results_spotify(search_results, track_)
                if track_id:
                    return track_id

                # Search with Title, Artist, Release / Album and Label,
                #  w/o Release and Label
                if not silent:
                    logger.info(f"\t\t[+] Searching for track: {track_name} by {artist}")
                query = f'track:"{track_name}" artist:"{artist}"'
                if not silent:
                    logger.info(f"\t\t[+] Search Query: {query}")
                search_results = search_wrapper(query)
                track_id = parse_search_results_spotify(search_results, track_)
                if track_id:
                    return track_id

    logger.info(
        " [Done] No exact matches on name and artists v2 : {} - {}{}".format(
            track["artists"][0],
            track["name"],
            "" if not track["mix"] else " - {}".format(track["mix"]),
        )
    )

    # Possible to use return search_for_track(track) but do not improve search results
    return None


def search_for_track_v3(
    track: dict, silent: bool = silent_search, parse_track: bool = parse_track
) -> str:
    """Search for a track on Spotify using various search strategies.

    Args:
        track (dict): Track dictionary.
        silent (bool): Whether to suppress logging output.
        parse_track (bool): Whether to parse the track name and mix.

    Returns:
        str: Spotify track ID if found, otherwise None.

    """
    if parse_track:
        track_parsed = [
            track.copy(),
            *parse_track_regex_beatport(track),
        ]
    else:
        track_parsed = [track]

    queries_functions = [
        query_track_album_label,
        query_track_label,
        query_track_album,
        query_track,
    ]

    # Create a parsed artist and try both
    # TODO: Export to function
    artist_search = [*track["artists"]]
    if parse_track:
        # Add parsed artist if not in list already
        artist_search.extend(
            x
            for x in [
                re.sub(r"\s*\([^)]*\)", "", artist_) for artist_ in track["artists"]
            ]
            if x not in artist_search
        )  # Remove (UK) for example
        artist_search.extend(
            x
            for x in [re.sub(r"\W+", " ", artist_) for artist_ in track["artists"]]
            if x not in artist_search
        )  # Remove special characters, in case it is not handled by Spotify API
        artist_search.extend(
            x
            for x in [re.sub(r"[^\w\s]", "", artist_) for artist_ in track["artists"]]
            if x not in artist_search
        )  # Remove special characters, in case it is not handled by Spotify API
        artist_search.extend(
            x
            for x in [
                re.sub(r"(?<=\w)[A-Z]", add_space, artist_)
                for artist_ in track["artists"]
            ]
            if x not in artist_search
        )  # Splitting artist name with a space after a capital letter
        artist_search.extend(
            x
            for x in [re.sub(r"\s&.*$", "", artist_) for artist_ in track["artists"]]
            if x not in artist_search
        )  # Removing second part after &

    # Search artist and artist parsed if parsed is on
    for artist in artist_search:
        # Search track name and track name without mix (even if parsed is off)

        for query_function in queries_functions:
            for track_ in track_parsed:
                # Create a field name mix according to Spotify formatting
                track_["name_mix"] = "{}{}".format(
                    track_["name"],
                    "" if not track_["mix"] else " - {}".format(track_["mix"]),
                )
                for track_name in [track_["name_mix"]]:  # , track_["name"]]:
                    query = query_function(track_name, artist, track_, silent)
                    if not silent:
                        logger.info(f"\t\t[+] Search Query: {query}")
                    search_results = search_wrapper(query)
                    track_id = parse_search_results_spotify(search_results, track_)
                    if track_id:
                        return track_id

    logger.info(
        " [Done] No exact matches on name and artists v2 : {} - {}{}".format(
            track["artists"][0],
            track["name"],
            "" if not track["mix"] else " - {}".format(track["mix"]),
        )
    )

    # Possible to use return search_for_track(track) but do not improve search results
    return None


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
        spotify_auth()
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
    spotify_auth()
    playlist_tracks_pager = spotify_ins.playlist_items(
        playlist_id=playlist_id, additional_types=("track",)
    )
    playlist_tracks = playlist_tracks_pager["items"]
    while playlist_tracks_pager["next"]:
        playlist_tracks_pager = spotify_ins.next(playlist_tracks_pager)
        playlist_tracks.extend(playlist_tracks_pager["items"])
    return playlist_tracks


def clear_playlist(playlist_id: str) -> None:
    """Clear a playlist.

    Args:
        playlist_id (str): Playlist ID.

    """
    for track in get_all_tracks_in_playlist(playlist_id):
        spotify_ins.user_playlist_remove_all_occurrences_of_tracks(
            username,
            playlist_id,
            [
                track["track"]["id"],
            ],
        )


search_track_function = search_for_track_v2


def add_new_tracks_to_playlist(genre: str, tracks_dict: list) -> None:
    """Add new tracks to a playlist.

    Args:
        genre (str): Genre name.
        tracks_dict (list): Dictionary of tracks.

    """
    # TODO export playlist anterior name to config
    # persistent_top_100_playlist_name = "{}{} - Top 100".format(playlist_prefix, genre)
    # daily_top_10_playlist_name = "{}{} - Daily Top".format(playlist_prefix, genre)
    persistent_top_100_playlist_name = f"Beatporter: {genre} - Top 100"
    daily_top_n_playlist_name = f"Beatporter: {genre} - Daily Top"
    logger.info(
        f'[+] Identifying new tracks for playlist: "{persistent_top_100_playlist_name}"'
    )

    if daily_mode:
        playlists = [
            {
                "name": persistent_top_100_playlist_name,
                "id": get_playlist_id(persistent_top_100_playlist_name),
            },
            {
                "name": daily_top_n_playlist_name,
                "id": get_playlist_id(daily_top_n_playlist_name),
            },
        ]
    else:
        playlists = [
            {
                "name": persistent_top_100_playlist_name,
                "id": get_playlist_id(persistent_top_100_playlist_name),
            }
        ]

    for playlist in playlists:
        if not playlist["id"]:
            logger.warning(
                '\t[!] Playlist "{}" does not exist, creating it.'.format(
                    playlist["name"]
                )
            )
            playlist["id"] = create_playlist(playlist["name"])

    if daily_mode:
        # Clear daily playlist
        clear_playlist(playlists[1]["id"])

    persistent_top_100_track_ids = list()
    daily_top_n_track_ids = list()
    track_count = 0
    for track in tracks_dict:
        try:
            track_id = search_track_function(track)
        except ReadTimeout:
            track_id = search_track_function(track)
        except spotipy.exceptions.SpotifyException:
            spotify_auth()
            track_id = search_track_function(track)
        if track_id and not track_in_playlist(playlists[0]["id"], track_id):
            persistent_top_100_track_ids.append(track_id)
        if track_id and track_count < daily_n_track:
            daily_top_n_track_ids.append(track_id)
        track_count += 1
    logger.info(
        f"[+] Adding {len(persistent_top_100_track_ids)} "
        f'new tracks to the playlist: "{persistent_top_100_playlist_name}"'
    )
    add_tracks_to_playlist(playlists[0]["id"], persistent_top_100_track_ids)
    if daily_mode:
        logger.info(
            f"[+] Adding {len(daily_top_n_track_ids)} new "
            f'tracks to the playlist: "{daily_top_n_playlist_name}"'
        )
        add_tracks_to_playlist(playlists[1]["id"], daily_top_n_track_ids)


def parse_tracks_spotify(tracks_json: dict) -> list:
    """Parse tracks from Spotify JSON.

    Args:
        tracks_json (dict): JSON data containing tracks.

    Returns:
        list: List of parsed tracks.

    """
    tracks = list()
    for track in tracks_json["tracks"]:
        tracks.append(
            {
                "title": track["title"],
                "name": track["name"],
                "mix": track["mix"],
                "artists": [artist["name"] for artist in track["artists"]],
                "remixers": [remixer["name"] for remixer in track["remixers"]],
                "release": track["release"]["name"],
                "label": track["label"]["name"],
                "published_date": track["date"]["published"],
                "released_date": track["date"]["released"],
                "duration": track["duration"]["minutes"],
                "duration_ms": track["duration"]["milliseconds"],
                "genres": [genre["name"] for genre in track["genres"]],
                "bpm": track["bpm"],
                "key": track["key"],
            }
        )
    return tracks


def parse_artist(value: Any, key: str) -> any:
    """Parse artist information.

    Args:
        value (any): Artist value.
        key (str): Key.

    Returns:
        any: Parsed artist information.

    """
    # TODO find better method
    if key == "artists":
        value = value[0]["name"]
    else:
        value

    return value


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
    spotify_auth()

    # TODO find better method
    track_list = get_all_tracks_in_playlist(playlist["id"])
    df_tracks = pd.DataFrame.from_dict(track_list)

    if len(df_tracks.index) > 0:
        df_tracks["track"] = [
            {key: value for key, value in track.items() if key in tracks_dict_names}
            for track in df_tracks["track"]
        ]
        df_tracks["track"] = [
            {key: parse_artist(value, key) for key, value in track.items()}
            for track in df_tracks["track"]
        ]

        df_tracks_o = pd.DataFrame()
        for row in df_tracks.iterrows():
            df_tracks_o = pd.concat(
                [df_tracks_o, pd.DataFrame(row[1]["track"], index=[0])]
            )
        df_tracks_o = df_tracks_o.loc[:, tracks_dict_names].reset_index(drop=True)
        df_tracks_o["artist_name"] = df_tracks_o["artists"] + " - " + df_tracks_o["name"]

        df_tracks = pd.concat([df_tracks_o, df_tracks.loc[:, "added_at"]], axis=1)

        df_temp = df_tracks.loc[:, ["id", "added_at", "artist_name"]]
        df_temp["playlist_id"] = playlist["id"]
        df_temp["playlist_name"] = playlist["name"]
        df_temp = df_temp.rename(columns={"id": "track_id", "added_at": "datetime_added"})

        df_hist_pl_tracks = pd.concat([df_hist_pl_tracks, df_temp])
        df_hist_pl_tracks = df_hist_pl_tracks.drop_duplicates().reset_index(drop=True)

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


def add_new_tracks_to_playlist_chart_label(
    title: str,
    tracks_dict: list,
    df_hist_pl_tracks: pd.DataFrame,
    use_prefix: bool = True,
    silent: bool = silent_search,
) -> pd.DataFrame:
    """Add tracks from Beatport to a Spotify playlist.

    Args:
        title (str): Chart or label playlist title.
        tracks_dict (list): Dictionary of tracks to add.
        df_hist_pl_tracks (pd.DataFrame): DataFrame of history of track.
        use_prefix (bool): Add a prefix to the playlist name as defined in config.
        silent (bool): If True, do not display searching details except errors.

    Returns:
        pd.DataFrame: Updated DataFrame.

    """
    spotify_auth()

    # TODO export playlist anterior name to config
    persistent_playlist_name = f"{playlist_prefix}{title}" if use_prefix else title
    logger.info(f'[+] Identifying new tracks for playlist: "{persistent_playlist_name}"')

    playlist = {
        "name": persistent_playlist_name,
        "id": get_playlist_id(persistent_playlist_name),
    }

    if not playlist["id"]:
        logger.warning(
            '\t[!] Playlist "{}" does not exist, creating it.'.format(playlist["name"])
        )
        playlist["id"] = create_playlist(playlist["name"])

    df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)
    playlist_track_ids = df_hist_pl_tracks.loc[
        df_hist_pl_tracks["playlist_id"] == playlist["id"], "track_id"
    ]

    if digging_mode == "playlist":
        df_local_hist = df_hist_pl_tracks.loc[
            df_hist_pl_tracks["playlist_id"] == playlist["id"]
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

    persistent_track_ids = list()
    track_count = 0
    track_count_tot = 0

    # TODO Refresh oauth to avoid time out
    spotify_auth()

    for track in tracks_dict:
        track_count_tot += 1
        track_artist_name = (
            track["artists"][0] + " - " + track["name"] + " - " + track["mix"]
        )
        if not silent:
            logger.info(
                f"  [Start] {round(track_count_tot / len(tracks_dict) * 100, 2)!s}% "
                f": {track_artist_name} : nb {track_count_tot} out of {len(tracks_dict)}"
            )
        if track_artist_name not in df_local_hist.values:
            try:
                track_id = search_track_function(track)
            except ReadTimeout:
                track_id = search_track_function(track)
            except spotipy.exceptions.SpotifyException:
                spotify_auth()
                track_id = search_track_function(track)
            if (
                track_id
                and track_id not in playlist_track_ids.values
                and track_id not in df_local_hist.values
            ):
                if not silent:
                    logger.info(
                        "  [Done] "
                        f"{round(track_count_tot / len(tracks_dict) * 100, 2)!s}%: "
                        f"Adding track: {get_track_detail(track_id)} - {track_id} "
                        f": nb {track_count}"
                    )
                persistent_track_ids.append(track_id)
                track_count += 1
            if track_count >= 99:  # Have limit of 100 trakcks per import
                logger.warning(
                    f"[+] Adding {len(persistent_track_ids)} new tracks to the playlist:"
                    f' "{persistent_playlist_name}"'
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
        else:
            if not silent:
                logger.info("  [Done] Similar track name already found")

        if track_count_tot % refresh_token_n_tracks == 0:  # Avoid time out
            spotify_auth()

    if len(persistent_track_ids) > 0:
        logger.warning(
            f"[+] Adding {len(persistent_track_ids)} new tracks to the playlist:"
            f' "{persistent_playlist_name}"'
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

    if not playlist["id"]:
        logger.warning(
            '\t[!] Playlist "{}" does not exist, creating it.'.format(playlist["name"])
        )
        playlist["id"] = create_playlist(playlist["name"])

    df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)
    playlist_track_ids = df_hist_pl_tracks.loc[
        df_hist_pl_tracks["playlist_id"] == playlist["id"], "track_id"
    ]

    if digging_mode == "playlist":
        df_local_hist = df_hist_pl_tracks.loc[
            df_hist_pl_tracks["playlist_id"] == playlist["id"]
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

    persistent_track_ids = list()
    track_count = 0
    track_count_tot = 0

    # TODO Refresh oauth to avoid time out
    spotify_auth()

    for track in track_ids:
        if track["track"] is not None:  # Prevent error of empty track
            track_id = track["track"]["id"]
            track_count_tot += 1
            if track_id not in df_local_hist.values:
                if track_id not in playlist_track_ids.values:
                    if not silent:
                        logger.info(
                            f"\t[+] Adding track id : {track_id} : nb {track_count}"
                        )
                    if track_id is not None:
                        persistent_track_ids.append(track_id)
                        track_count += 1
                    else:
                        logger.warn(
                            "\t[+]! Trying to add track_id None : {} - {}".format(
                                track["track"]["artists"][0]["name"],
                                track["track"]["name"],
                            )
                        )
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
            else:
                if not silent:
                    logger.info("\tTrack already found in playlist or history")

            if track_count_tot % refresh_token_n_tracks == 0:  # Avoid time out
                spotify_auth()

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


def add_new_tracks_to_playlist_genre(
    genre: str,
    top_100_chart: list,
    df_hist_pl_tracks: pd.DataFrame,
    silent: bool = silent_search,
) -> pd.DataFrame:
    """Add Beatport tracks from genre category to Spotify playlist.

    Args:
        genre (str): Genre name.
        top_100_chart (list): List of tracks to add.
        df_hist_pl_tracks (pd.DataFrame): DataFrame of history of track.
        silent (bool): If true do not display searching details except errors.

    Returns:
        pd.DataFrame: Updated DataFrame.

    """
    spotify_auth()

    # TODO export playlist anterior name to config
    # persistent_top_100_playlist_name = "{}{} - Top 100".format(playlist_prefix, genre)
    # daily_top_10_playlist_name = "{}{} - Daily Top".format(playlist_prefix, genre)
    persistent_top_100_playlist_name = f"Beatporter: {genre} - Top 100"
    daily_top_n_playlist_name = f"Beatporter: {genre} - Daily Top"
    logger.info(
        f'[+] Identifying new tracks for playlist: "{persistent_top_100_playlist_name}"'
    )

    if daily_mode:
        playlists = [
            {
                "name": persistent_top_100_playlist_name,
                "id": get_playlist_id(persistent_top_100_playlist_name),
            },
            {
                "name": daily_top_n_playlist_name,
                "id": get_playlist_id(daily_top_n_playlist_name),
            },
        ]
    else:
        playlists = [
            {
                "name": persistent_top_100_playlist_name,
                "id": get_playlist_id(persistent_top_100_playlist_name),
            }
        ]

    for playlist in playlists:
        if not playlist["id"]:
            logger.warning(
                '\t[!] Playlist "{}" does not exist, creating it.'.format(
                    playlist["name"]
                )
            )
            playlist["id"] = create_playlist(playlist["name"])
        df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)

    # Create local hist for top 100 playlist
    if digging_mode == "playlist":
        df_local_hist = df_hist_pl_tracks.loc[
            df_hist_pl_tracks["playlist_id"] == playlists[0]["id"]
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
    playlist_track_ids = df_hist_pl_tracks.loc[
        df_hist_pl_tracks["playlist_id"] == playlists[0]["id"], "track_id"
    ]

    if daily_mode:
        if digging_mode == "":
            # Clear daily playlist if digging mode is not using hist
            # otherwise will delete tracks not yet listened
            clear_playlist(playlists[1]["id"])
            df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlists[1])
            playlist_track_ids_daily = pd.Series([], name="track_id", dtype=object)
            df_local_hist_daily = pd.DataFrame(
                columns=[
                    "playlist_id",
                    "playlist_name",
                    "track_id",
                    "datetime_added",
                    "artist_name",
                ]
            )
        else:
            # Create local hist for daily playlist
            if digging_mode == "playlist":
                df_local_hist_daily = df_hist_pl_tracks.loc[
                    df_hist_pl_tracks["playlist_id"] == playlists[1]["id"]
                ]
            elif digging_mode == "all":
                df_local_hist_daily = df_hist_pl_tracks
            playlist_track_ids_daily = df_hist_pl_tracks.loc[
                df_hist_pl_tracks["playlist_id"] == playlists[1]["id"], "track_id"
            ]

    persistent_track_ids = list()
    daily_top_n_track_ids = list()
    track_count = 0
    track_count_tot = 0

    # Get the number of tracks in the daily playlist
    if daily_mode:
        daily_playlist = spotify_ins.playlist(playlist_id=playlists[1]["id"])
        n_daily_tracks = len(daily_playlist["tracks"]["items"])
    else:
        n_daily_tracks = 0

    # TODO Refresh oauth to avoid time out
    spotify_auth()

    for track in top_100_chart:
        track_count_tot += 1
        track_artist_name = (
            track["artists"][0] + " - " + track["name"] + " - " + track["mix"]
        )
        if not silent:
            logger.info(
                "  [Start] {}% : {} : nb {} out of {}".format(
                    str(round(track_count_tot / len(top_100_chart) * 100, 2)),
                    track["name"]
                    + " - "
                    + track["mix"]
                    + " by "
                    + track["artists"][0],  # track_artist_name,
                    track_count_tot,
                    len(top_100_chart),
                )
            )

        if track_artist_name not in df_local_hist.values:
            try:
                track_id = search_track_function(track)
            except ReadTimeout:
                try:
                    track_id = search_track_function(track)
                except Exception as e:
                    logger.warning(f"Track {track_artist_name} failed with error {e}")
                    track_id = None
            except spotipy.exceptions.SpotifyException:
                spotify_auth()
                try:
                    track_id = search_track_function(track)
                except Exception as e:
                    logger.warning(f"Track {track_artist_name} failed with error {e}")
                    track_id = None

            if track_id:
                if (
                    track_id not in playlist_track_ids.values
                    and track_id not in df_local_hist.values
                ):
                    if not silent:
                        logger.info(
                            "  [Done] "
                            f"{round(track_count_tot / len(top_100_chart) * 100, 2)!s}%:"
                            f" Adding track {get_track_detail(track_id)} - {track_id}"
                            f" : nb {track_count}"
                        )
                    persistent_track_ids.append(track_id)
                    track_count += 1
                else:
                    if not silent:
                        logger.info("  [Done] Similar track id already found")

                if (
                    n_daily_tracks < daily_n_track
                    and track_id not in playlist_track_ids_daily.values
                    and track_id not in df_local_hist_daily.values
                ):
                    daily_top_n_track_ids.append(track_id)
                    n_daily_tracks += 1

                if track_count >= 99:  # Have limit of 100 tracks per import
                    logger.warning(
                        f"[+] Adding {len(persistent_track_ids)} new tracks to the"
                        f' playlist: "{persistent_top_100_playlist_name}"'
                    )
                    add_tracks_to_playlist(playlists[0]["id"], persistent_track_ids)
                    # TODO consider only adding new ID to avoid reloading large playlist
                    df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)
                    playlist_track_ids = df_hist_pl_tracks.loc[
                        df_hist_pl_tracks["playlist_id"] == playlists[0]["id"], "track_id"
                    ]
                    track_count = 0
                    persistent_track_ids = list()
                    update_playlist_description_with_date(playlists[0])

        else:
            if not silent:
                logger.info("  [Done] Similar track name already found")

        if track_count_tot % refresh_token_n_tracks == 0:  # Avoid time out
            spotify_auth()

    if len(persistent_track_ids) > 0:
        logger.warning(
            f"[+] Adding {len(persistent_track_ids)} new tracks to the playlist:"
            f' "{persistent_top_100_playlist_name}"'
        )
        add_tracks_to_playlist(playlists[0]["id"], persistent_track_ids)
        update_playlist_description_with_date(playlists[0])
    else:
        logger.info(
            "[+] No new tracks to add to the playlist"
            f': "{persistent_top_100_playlist_name}"'
        )

    if len(daily_top_n_track_ids) > 0:
        logger.warning(
            f"[+] Adding {len(daily_top_n_track_ids)} new tracks to the playlist:"
            f' "{daily_top_n_playlist_name}"'
        )
        add_tracks_to_playlist(playlists[1]["id"], daily_top_n_track_ids)
        update_playlist_description_with_date(playlists[1])
    else:
        logger.info(
            "[+] No new tracks to add to the playlist: " f'"{daily_top_n_playlist_name}"'
        )

    # Add more to daily playlist if not full
    if daily_mode:
        playlist_track_ids = playlist_track_ids[
            ::-1
        ]  # Reverse order to get freshest first
        if n_daily_tracks < daily_n_track:
            extra_daily_top_n_track_ids = list()
            for track_id in playlist_track_ids:  # Full playlist tracks ID
                if (
                    n_daily_tracks < daily_n_track
                    and track_id not in playlist_track_ids_daily.values
                ) and (
                    track_id not in df_local_hist_daily.values
                    and track_id not in daily_top_n_track_ids
                ):
                    extra_daily_top_n_track_ids.append(track_id)
                    n_daily_tracks += 1

            logger.warning(
                f"[+] Adding {len(extra_daily_top_n_track_ids)} extra new "
                f'tracks to the playlist: "{daily_top_n_playlist_name}"'
            )
            add_tracks_to_playlist(playlists[1]["id"], extra_daily_top_n_track_ids)
            update_playlist_description_with_date(playlists[1])

    for playlist in playlists:
        df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)

    if (len(persistent_track_ids) > 0) or (len(daily_top_n_track_ids) > 0):
        save_hist_dataframe(df_hist_pl_tracks)

    return df_hist_pl_tracks


def update_playlist_description_with_date(playlist: dict) -> None:
    """Update playlist description with current date.

    Args:
        playlist (dict): Playlist dictionary.

    """
    playlist_desc = spotify_ins.playlist(playlist_id=playlist["id"])
    playlist_desc["description"] = re.sub(
        r"\s*Updated on \d{4}-\d{2}-\d{2}\.*", "", playlist_desc["description"]
    )
    playlist_desc["description"] = re.sub(r"&#x2F;", "/", playlist_desc["description"])
    spotify_ins.playlist_change_details(
        playlist_id=playlist["id"],
        description=playlist_desc["description"]
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
    spotify_auth()

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
    track_result = spotify_ins.track(f"spotify:track:{track_id}")
    artists = [artist["name"] for artist in track_result["artists"]]
    artists = ", ".join(artists)

    return "{} by {}".format(track_result["name"], artists)


def spotify_auth(verbose_aut: bool = False) -> None:
    """Authenticate to Spotify.

    Args:
        verbose_aut (bool): Whether to enable verbose logging.

    """
    # Get authenticated to Spotify
    if verbose_aut:
        logger.info("[+][+] Refreshing Spotify auth")
    global spotify_ins
    token_info = do_spotify_oauth()
    spotify_ins = spotipy.Spotify(
        auth=token_info["access_token"], requests_timeout=15, retries=3, backoff_factor=15
    )

    try:
        _ = spotify_ins.current_user_playlists()
    except Exception as e:
        logger.warning(
            f"Error during spotify Auth, testing of playlist fetch, with error {e}"
        )
        logger.warning("Going to sleep for 2 minutes")
        sleep(2 * 60)
        logger.warning("Sleep done")
        token_info = do_spotify_oauth()
        spotify_ins = spotipy.Spotify(
            auth=token_info["access_token"],
            requests_timeout=15,
            retries=3,
            backoff_factor=15,
        )


handler = CacheFileHandler(username=username)
sp_oauth = oauth2.SpotifyOAuth(
    client_id, client_secret, redirect_uri, cache_handler=handler, scope=scope
)
spotify_auth()

# Annex testing tracks with known issues

track_working_mix = {
    "title": "",
    "name": "The Shake",
    "mix": "Extended Mix",
    "artists": ["Ellis Moss"],
    "remixers": [],
    "release": "The Shake",
    "label": "Toolroom Trax",
    "published_date": "2021-01-29",
    "released_date": "2021-01-29",
    "duration": "6:14",
    "duration_ms": 374032,
    "genres": ["Tech House"],
    "bpm": 124,
    "key": "G min",
}

track_not_working_mix = {
    "title": "",
    "name": "Jumpin'",
    "mix": "Extended",
    "artists": ["CID", "Westend"],
    "remixers": [],
    "release": "Jumpin'",
    "label": "Repopulate Mars",
    "published_date": "2021-02-12",
    "released_date": "2021-02-12",
    "duration": "5:04",
    "duration_ms": 304761,
    "genres": ["Tech House"],
    "bpm": 126,
    "key": "A min",
}

track_not_working_artist = {
    "title": "",
    "name": "Set U Free",
    "mix": "Extended Mix",
    "artists": ["GUZ (NL)"],
    "remixers": [],
    "release": "Set U Free (Extended Mix)",
    "label": "Sink or Swim",
    "published_date": "2021-01-29",
    "released_date": "2021-01-29",
    "duration": "4:23",
    "duration_ms": 263040,
    "genres": ["Tech House"],
    "bpm": 125,
    "key": "B maj",
}

track_special_characters = {
    "title": "",
    "name": "Don't Touch The Pool",
    "mix": "Original Mix",
    "artists": ["FOVOS"],
    "remixers": [],
    "release": "Hot Mess",
    "label": "Country Club Disco",
    "published_date": "2021-02-12",
    "released_date": "2021-02-12",
    "duration": "3:47",
    "duration_ms": 227302,
    "genres": ["Tech House"],
    "bpm": 128,
    "key": "A maj",
}

track_name_special_char_cant_space = {
    "title": "",
    "name": "Don't Make Me",
    "mix": "Original Mix",
    "artists": ["Dillon Nathaniel"],
    "remixers": [],
    "release": "Reason to Fly",
    "label": "Sola",
    "published_date": "2021-02-19",
    "released_date": "2021-02-19",
    "duration": "5:46",
    "duration_ms": 346666,
    "genres": ["Tech House"],
    "bpm": 126,
    "key": "G\u266d min",
}
