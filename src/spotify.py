"""Module to manage Spotify queries."""

import logging
import re

from config import parse_track, silent_search
from spotify_utils import (
    add_space,
    parse_search_results_spotify,
    parse_track_regex_beatport,
    query_track,
    query_track_album,
    query_track_album_label,
    query_track_label,
    search_wrapper,
)
from utils import configure_logging

configure_logging()
logger = logging.getLogger("spotify")

tracks_dict_names = ["id", "duration_ms", "href", "name", "popularity", "uri", "artists"]


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
