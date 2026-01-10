"""Module to manage Spotify queries."""

import logging
import re

import pandas as pd
import spotipy
from requests.exceptions import ReadTimeout

from src.config import (
    daily_mode,
    daily_n_track,
    digging_mode,
    parse_track,
    playlist_prefix,
    silent_search,
)
from src.configure_logging import configure_logging
from src.models import BeatportTrack
from src.spotify_utils import (
    add_space,
    add_tracks_to_playlist,
    clear_playlist,
    create_playlist,
    get_playlist_id,
    get_track_detail,
    parse_search_results_spotify,
    parse_track_regex_beatport,
    query_track,
    query_track_album,
    query_track_album_label,
    query_track_label,
    search_wrapper,
    spotify_auth,
    track_in_playlist,
    update_hist_pl_tracks,
    update_playlist_description_with_date,
)
from src.utils import save_hist_dataframe

configure_logging()
logger = logging.getLogger("spotify_search")

TRACKS_DICT_NAMES = ["id", "duration_ms", "href", "name", "popularity", "uri", "artists"]


def _parse_artists(track: BeatportTrack, parse_track: bool) -> list[str]:
    # Create a parsed artist and try both
    artist_search = [*track.artists]
    if parse_track:
        # Add parsed artist if not in list already
        artist_search.extend(
            x
            for x in [re.sub(r"\s*\([^)]*\)", "", artist_) for artist_ in track.artists]
            if x not in artist_search
        )  # Remove (UK) for example
        artist_search.extend(
            x
            for x in [re.sub(r"\W+", " ", artist_) for artist_ in track.artists]
            if x not in artist_search
        )  # Remove special characters, in case it is not handled by Spotify API
        artist_search.extend(
            x
            for x in [re.sub(r"[^\w\s]", "", artist_) for artist_ in track.artists]
            if x not in artist_search
        )  # Remove special characters, in case it is not handled by Spotify API
        artist_search.extend(
            x
            for x in [
                re.sub(r"(?<=\w)[A-Z]", add_space, artist_) for artist_ in track.artists
            ]
            if x not in artist_search
        )  # Splitting artist name with a space after a capital letter
        artist_search.extend(
            x
            for x in [re.sub(r"\s&.*$", "", artist_) for artist_ in track.artists]
            if x not in artist_search
        )  # Removing second part after &
    return artist_search


def _perform_artist_track_search(
    track_: BeatportTrack, artist: str, silent: bool
) -> str | None:
    for track_name in [track_.name_mix]:
        # # Search with Title, Mix, Artist, Release / Album and Label
        # if not silent:
        #     logger.info(
        #         "\t[+] Searching for track: {} by {} on {} on {} label".format(
        #             track_name, artist, track_.release, track_.label
        #         )
        #     )
        # query = 'track:"{}" artist:"{}" album:"{}" label:"{}"'.format(
        #     track_name, artist, track_.release, track_.label
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
        #             track_name, artist, track_.label
        #         )
        #     )
        # query = 'track:"{}" artist:"{}" label:"{}"'.format(
        #     track_name, artist, track_.label
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
                f"[+]\tSearching for track: {track_name} by {artist} "
                f"on {track_.release} album"
            )
        query = f'track:"{track_name}" artist:"{artist}" album:"{track_.release}"'
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
    return None


def search_for_track_v2(
    track: BeatportTrack, silent: bool = silent_search, parse_track: bool = parse_track
) -> str | None:
    """Search for a track on Spotify using various search strategies.

    Args:
        track (BeatportTrack): Track dictionary.
        silent (bool): Whether to suppress logging output.
        parse_track (bool): Whether to parse the track name and mix.

    Returns:
        str: Spotify track ID if found, otherwise None.

    """
    if parse_track:
        track_parsed = [
            track.model_copy(),
            *parse_track_regex_beatport(track),
        ]
    else:
        track_parsed = [track]

    artist_search = _parse_artists(track, parse_track)

    for track_ in track_parsed:
        # Create a field name mix according to Spotify formatting
        track_.name_mix = "{}{}".format(
            track_.name, "" if not track_.mix else f" - {track_.mix}"
        )

        # Search artist and artist parsed if parsed is on
        for artist in artist_search:
            track_id = _perform_artist_track_search(track_, artist, silent)
            if track_id:
                return track_id

    logger.info(
        " [Done] No exact matches on name and artists v2 : {} - {}{}".format(
            track.artists[0],
            track.name,
            "" if not track.mix else f" - {track.mix}",
        )
    )

    # Possible to use return search_for_track(track) but do not improve search results
    return None


def search_for_track_v3(
    track: BeatportTrack, silent: bool = silent_search, parse_track: bool = parse_track
) -> str | None:
    """Search for a track on Spotify using various search strategies.

    Args:
        track (BeatportTrack): Track dictionary.
        silent (bool): Whether to suppress logging output.
        parse_track (bool): Whether to parse the track name and mix.

    Returns:
        str: Spotify track ID if found, otherwise None.

    """
    if parse_track:
        track_parsed = [
            track.model_copy(),
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
    artist_search = [*track.artists]
    if parse_track:
        # Add parsed artist if not in list already
        artist_search.extend(
            x
            for x in [re.sub(r"\s*\([^)]*\)", "", artist_) for artist_ in track.artists]
            if x not in artist_search
        )  # Remove (UK) for example
        artist_search.extend(
            x
            for x in [re.sub(r"\W+", " ", artist_) for artist_ in track.artists]
            if x not in artist_search
        )  # Remove special characters, in case it is not handled by Spotify API
        artist_search.extend(
            x
            for x in [re.sub(r"[^\w\s]", "", artist_) for artist_ in track.artists]
            if x not in artist_search
        )  # Remove special characters, in case it is not handled by Spotify API
        artist_search.extend(
            x
            for x in [
                re.sub(r"(?<=\w)[A-Z]", add_space, artist_) for artist_ in track.artists
            ]
            if x not in artist_search
        )  # Splitting artist name with a space after a capital letter
        artist_search.extend(
            x
            for x in [re.sub(r"\s&.*$", "", artist_) for artist_ in track.artists]
            if x not in artist_search
        )  # Removing second part after &

    # Search artist and artist parsed if parsed is on
    for artist in artist_search:
        # Search track name and track name without mix (even if parsed is off)

        for query_function in queries_functions:
            for track_ in track_parsed:
                # Create a field name mix according to Spotify formatting
                track_.name_mix = "{}{}".format(
                    track_.name,
                    "" if not track_.mix else f" - {track_.mix}",
                )
                for track_name in [track_.name_mix]:  # , track_. name]:
                    query = query_function(track_name, artist, track_, silent)
                    if not silent:
                        logger.info(f"\t\t[+] Search Query: {query}")
                    search_results = search_wrapper(query)
                    track_id = parse_search_results_spotify(search_results, track_)
                    if track_id:
                        return track_id

    logger.info(
        " [Done] No exact matches on name and artists v2 : {} - {}{}".format(
            track.artists[0],
            track.name,
            "" if not track.mix else f" - {track.mix}",
        )
    )

    # Possible to use return search_for_track(track) but do not improve search results
    return None


def search_for_track_v4(
    track: BeatportTrack, silent: bool = silent_search, parse_track: bool = parse_track
) -> str | None:
    """Search for a track on Spotify using various search strategies.

    Args:
        track (BeatportTrack): Track dictionary.
        silent (bool): Whether to suppress logging output.
        parse_track (bool): Whether to parse the track name and mix.

    Returns:
        str: Spotify track ID if found, otherwise None.

    """
    query = " ".join(
        [
            track.name,
            " ".join(track.artists),
            " ".join(track.remixers),
            # track.mix,
            # track.release,
            # track.label,
        ]
    )
    if not silent:
        logger.info(f"\t\t[+] Search Query: {query}")
    search_results = search_wrapper(query)
    track_id = parse_search_results_spotify(search_results, track)
    if track_id:
        return track_id

    query = " ".join(
        [
            track.name,
            " ".join(track.artists),
            " ".join(track.remixers),
            track.mix,
            # track.release,
            # track.label,
        ]
    )
    if not silent:
        logger.info(f"\t\t[+] Search Query: {query}")
    search_results = search_wrapper(query)
    track_id = parse_search_results_spotify(search_results, track)
    if track_id:
        return track_id

    logger.info(
        " [Done] No exact matches on name and artists v2 : {} - {}{}".format(
            track.artists[0],
            track.name,
            "" if not track.mix else f" - {track.mix}",
        )
    )

    # Possible to use return search_for_track(track) but do not improve search results
    return None


def search_track_function(
    track: BeatportTrack, silent: bool = silent_search, parse_track: bool = parse_track
) -> str | None:
    """Search for a track on Spotify using various search strategies.

    Args:
        track (BeatportTrack): Track dictionary.
        silent (bool): Whether to suppress logging output.
        parse_track (bool): Whether to parse the track name and mix.

    Returns:
        str: Spotify track ID if found, otherwise None.

    """
    return search_for_track_v2(track=track, silent=silent, parse_track=parse_track)


#


def _get_or_create_playlists(
    persistent_top_100_playlist_name: str,
    daily_top_n_playlist_name: str,
    daily_mode: bool,
) -> list[dict]:
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
    return playlists


def _clear_daily_playlist(playlists: list[dict], daily_mode: bool) -> None:
    if daily_mode:
        # Clear daily playlist
        clear_playlist(playlists[1]["id"])


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

    playlists = _get_or_create_playlists(
        persistent_top_100_playlist_name, daily_top_n_playlist_name, daily_mode
    )
    _clear_daily_playlist(playlists, daily_mode)

    persistent_top_100_track_ids = list()
    daily_top_n_track_ids = list()
    for track_count, track in enumerate(tracks_dict):
        try:
            track_id = search_track_function(track)
        except ReadTimeout:
            track_id = search_track_function(track)
        except spotipy.exceptions.SpotifyException:
            track_id = search_track_function(track)
        if track_id and not track_in_playlist(playlists[0]["id"], track_id):
            persistent_top_100_track_ids.append(track_id)
        if track_id and track_count < daily_n_track:
            daily_top_n_track_ids.append(track_id)
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


def _get_local_history_dataframe(
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


def _check_and_add_track(
    track: BeatportTrack,
    df_local_hist: pd.DataFrame,
    playlist_track_ids: pd.Series,
    silent: bool,
) -> tuple[str | None, bool]:
    track_artist_name = track.artists[0] + " - " + track.name + " - " + track.mix
    if track_artist_name not in df_local_hist.values:
        try:
            track_id = search_track_function(track)
        except ReadTimeout:
            track_id = search_track_function(track)
        except spotipy.exceptions.SpotifyException:
            track_id = search_track_function(track)
        if (
            track_id
            and track_id not in playlist_track_ids.values
            and track_id not in df_local_hist.values
        ):
            if not silent:
                logger.info(
                    f"  [Done] Adding track: {get_track_detail(track_id)} - {track_id}"
                )
            return track_id, True
    else:
        if not silent:
            logger.info("  [Done] Similar track name already found")
    return None, False


def add_new_tracks_to_playlist_chart_label(
    title: str,
    tracks_dict: list[BeatportTrack],
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

    df_local_hist = _get_local_history_dataframe(
        df_hist_pl_tracks, playlist["id"], digging_mode
    )

    persistent_track_ids = list()
    track_count = 0

    # TODO Refresh oauth to avoid time out
    spotify_auth()

    for track_count_tot, track in enumerate(tracks_dict):
        track_artist_name = track.artists[0] + " - " + track.name + " - " + track.mix
        if not silent:
            logger.info(
                f"  [Start] {round(track_count_tot / len(tracks_dict) * 100, 2)!s}% "
                f": {track_artist_name} : nb {track_count_tot} out of {len(tracks_dict)}"
            )

        track_id, added = _check_and_add_track(
            track, df_local_hist, playlist_track_ids, silent
        )
        if added:
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
                df_local_hist = _get_local_history_dataframe(
                    df_hist_pl_tracks, playlist["id"], digging_mode
                )
                track_count = 0
                persistent_track_ids = list()
                update_playlist_description_with_date(playlist)

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


def _get_or_create_genre_playlists(
    genre: str, playlist_prefix: str, daily_mode: bool
) -> list[dict]:
    persistent_top_100_playlist_name = f"{playlist_prefix}{genre} - Top 100"
    daily_top_n_playlist_name = f"{playlist_prefix}{genre} - Daily Top"

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
    return playlists


def _get_genre_local_history_dataframes(
    df_hist_pl_tracks: pd.DataFrame, playlists: list[dict], digging_mode: str
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series]:
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

    df_local_hist_daily = pd.DataFrame(
        columns=[
            "playlist_id",
            "playlist_name",
            "track_id",
            "datetime_added",
            "artist_name",
        ]
    )
    playlist_track_ids_daily = pd.Series([], name="track_id", dtype=object)

    if playlists[1]["id"] and digging_mode != "":
        if digging_mode == "playlist":
            df_local_hist_daily = df_hist_pl_tracks.loc[
                df_hist_pl_tracks["playlist_id"] == playlists[1]["id"]
            ]
        elif digging_mode == "all":
            df_local_hist_daily = df_hist_pl_tracks
        playlist_track_ids_daily = df_hist_pl_tracks.loc[
            df_hist_pl_tracks["playlist_id"] == playlists[1]["id"], "track_id"
        ]
    return (
        df_local_hist,
        playlist_track_ids,
        df_local_hist_daily,
        playlist_track_ids_daily,
    )


def _safe_track_search(track: BeatportTrack, track_artist_name: str) -> str | None:
    try:
        track_id = search_track_function(track)
    except ReadTimeout:
        try:
            track_id = search_track_function(track)
        except Exception as e:
            logger.warning(f"Track {track_artist_name} failed with error {e}")
            track_id = None
    except spotipy.exceptions.SpotifyException:
        try:
            track_id = search_track_function(track)
        except Exception as e:
            logger.warning(f"Track {track_artist_name} failed with error {e}")
            track_id = None
    return track_id


def _update_playlists_and_history(
    persistent_track_ids: list[str],
    daily_top_n_track_ids: list[str],
    persistent_top_100_playlist_name: str,
    daily_top_n_playlist_name: str,
    playlists: list[dict],
    df_hist_pl_tracks: pd.DataFrame,
) -> pd.DataFrame:
    """Update playlists and history with new tracks."""
    if persistent_track_ids:
        logger.warning(
            f"[+] Adding {len(persistent_track_ids)} new tracks to the"
            f' playlist: "{persistent_top_100_playlist_name}"'
        )
        add_tracks_to_playlist(playlists[0]["id"], persistent_track_ids)
        update_playlist_description_with_date(playlists[0])
        df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlists[0])
    else:
        logger.info(
            f"[+] No new tracks to add to the playlist: "
            f'"{persistent_top_100_playlist_name}"'
        )

    if daily_top_n_track_ids:
        if len(daily_top_n_track_ids) > daily_n_track:
            daily_top_n_track_ids = daily_top_n_track_ids[:daily_n_track]
        logger.warning(
            f"[+] Adding {len(daily_top_n_track_ids)} new tracks to the playlist:"
            f' "{daily_top_n_playlist_name}"'
        )
        add_tracks_to_playlist(playlists[1]["id"], daily_top_n_track_ids)
        update_playlist_description_with_date(playlists[1])
        df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlists[1])
    else:
        logger.info(
            f'[+] No new tracks to add to the playlist: "{daily_top_n_playlist_name}"'
        )

    return df_hist_pl_tracks


def _add_more_daily_genre(
    playlists: list[dict],
    df_local_hist_daily: pd.DataFrame,
    playlist_track_ids: pd.Series,
    daily_top_n_track_ids: list[str],
    playlist_track_ids_daily: pd.Series,
    n_daily_tracks: int,
    daily_top_n_playlist_name: str,
) -> None:
    """Add more tracks to daily genre playlist if not enough found."""
    # TODO check to add to _update_playlists_and_history
    playlist_track_ids = playlist_track_ids[::-1]  # Reverse order to get freshest first
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


def _process_genre_track(
    track: BeatportTrack,
    df_local_hist: pd.DataFrame,
    playlist_track_ids: pd.Series,
    df_local_hist_daily: pd.DataFrame,
    playlist_track_ids_daily: pd.Series,
    silent: bool,
    n_daily_tracks: int,
) -> tuple[str | None, bool, bool, int]:
    track_id = None
    added_to_persistent = False
    added_to_daily = False
    track_artist_name = track.artists[0] + " - " + track.name + " - " + track.mix

    if track_artist_name not in df_local_hist.values:
        track_id = _safe_track_search(track, track_artist_name)

        if track_id:
            if (
                track_id not in playlist_track_ids.values
                and track_id not in df_local_hist.values
            ):
                if not silent:
                    logger.info(
                        f"  [Done] Adding track {get_track_detail(track_id)} - {track_id}"
                    )
                added_to_persistent = True

            if (
                n_daily_tracks < daily_n_track
                and track_id not in playlist_track_ids_daily.values
                and track_id not in df_local_hist_daily.values
            ):
                added_to_daily = True
        else:
            if not silent:
                logger.info("  [Done] Similar track id already found")
    else:
        if not silent:
            logger.info("  [Done] Similar track name already found")
    return track_id, added_to_persistent, added_to_daily, n_daily_tracks


def _initialize_daily_history_dataframes(
    df_hist_pl_tracks: pd.DataFrame, playlists: list[dict], digging_mode: str
) -> tuple[pd.DataFrame, pd.Series]:
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
        # Create local hist for daily top n playlist
        if digging_mode == "playlist":
            df_local_hist_daily = df_hist_pl_tracks.loc[
                df_hist_pl_tracks["playlist_id"] == playlists[1]["id"]
            ]
        elif digging_mode == "all":
            df_local_hist_daily = df_hist_pl_tracks
        playlist_track_ids_daily = df_hist_pl_tracks.loc[
            df_hist_pl_tracks["playlist_id"] == playlists[1]["id"], "track_id"
        ]
    return df_local_hist_daily, playlist_track_ids_daily


def add_new_tracks_to_playlist_genre(
    genre: str,
    top_100_chart: list[BeatportTrack],
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
    persistent_top_100_playlist_name = f"{playlist_prefix}{genre} - Top 100"
    daily_top_n_playlist_name = f"{playlist_prefix}{genre} - Daily Top"

    logger.info(
        f'[+] Identifying new tracks for playlist: "{persistent_top_100_playlist_name}"'
    )

    playlists = _get_or_create_genre_playlists(genre, playlist_prefix, daily_mode)
    for playlist in playlists:
        df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)

    (
        df_local_hist,
        playlist_track_ids,
        df_local_hist_daily,
        playlist_track_ids_daily,
    ) = _get_genre_local_history_dataframes(df_hist_pl_tracks, playlists, digging_mode)

    if daily_mode:
        df_local_hist_daily, playlist_track_ids_daily = (
            _initialize_daily_history_dataframes(
                df_hist_pl_tracks, playlists, digging_mode
            )
        )

    persistent_track_ids = list()
    daily_top_n_track_ids = list()
    track_count = 0

    if daily_mode:
        # Get the number of tracks in the daily playlist
        spotify_ins = spotify_auth()
        daily_playlist = spotify_ins.playlist(playlist_id=playlists[1]["id"])
        n_daily_tracks = len(daily_playlist["tracks"]["items"])
    else:
        n_daily_tracks = 0

    for track_count_tot, track in enumerate(top_100_chart):
        if not silent:
            logger.info(
                f"  [Start] {round(track_count_tot / len(top_100_chart) * 100, 2)}% "
                f": {track.name} - {track.mix} by {track.artists[0]} "
                f": nb {track_count_tot} out of {len(top_100_chart)}"
            )

        (
            track_id,
            added_to_persistent,
            added_to_daily,
            n_daily_tracks,
        ) = _process_genre_track(
            track,
            df_local_hist,
            playlist_track_ids,
            df_local_hist_daily,
            playlist_track_ids_daily,
            silent,
            n_daily_tracks,
        )

        if added_to_persistent and track_id:
            persistent_track_ids.append(track_id)
            track_count += 1
        if added_to_daily and track_id:
            daily_top_n_track_ids.append(track_id)
            n_daily_tracks += 1

    df_hist_pl_tracks = _update_playlists_and_history(
        persistent_track_ids,
        daily_top_n_track_ids,
        persistent_top_100_playlist_name,
        daily_top_n_playlist_name,
        playlists,
        df_hist_pl_tracks,
    )

    if daily_mode and n_daily_tracks < daily_n_track:
        # Add more to daily playlist if not full
        _add_more_daily_genre(
            playlists,
            df_local_hist_daily,
            playlist_track_ids,
            daily_top_n_track_ids,
            playlist_track_ids_daily,
            n_daily_tracks,
            daily_top_n_playlist_name,
        )

    if (len(persistent_track_ids) > 0) or (len(daily_top_n_track_ids) > 0):
        save_hist_dataframe(df_hist_pl_tracks)

    return df_hist_pl_tracks


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
