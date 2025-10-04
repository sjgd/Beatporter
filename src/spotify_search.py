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
    refresh_token_n_tracks,
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


def search_for_track_v2(
    track: BeatportTrack, silent: bool = silent_search, parse_track: bool = parse_track
) -> str:
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

    for track_ in track_parsed:
        # Create a field name mix according to Spotify formatting
        track_.name_mix = "{}{}".format(
            track_.name, "" if not track_.mix else f" - {track_.mix}"
        )

        # Create a parsed artist and try both
        artist_search = [*track_.artists]
        if parse_track:
            # Add parsed artist if not in list already
            artist_search.extend(
                x
                for x in [
                    re.sub(r"\s*\([^)]*\)", "", artist_) for artist_ in track_.artists
                ]
                if x not in artist_search
            )  # Remove (UK) for example
            artist_search.extend(
                x
                for x in [re.sub(r"\W+", " ", artist_) for artist_ in track_.artists]
                if x not in artist_search
            )  # Remove special characters, in case it is not handled by Spotify API
            artist_search.extend(
                x
                for x in [re.sub(r"[^\w\s]", "", artist_) for artist_ in track_.artists]
                if x not in artist_search
            )  # Remove special characters, in case it is not handled by Spotify API
            artist_search.extend(
                x
                for x in [
                    re.sub(r"(?<=\w)[A-Z]", add_space, artist_)
                    for artist_ in track_.artists
                ]
                if x not in artist_search
            )  # Splitting artist name with a space after a capital letter
            artist_search.extend(
                x
                for x in [re.sub(r"\s&.*$", "", artist_) for artist_ in track_.artists]
                if x not in artist_search
            )  # Removing second part after &

        # Search artist and artist parsed if parsed is on
        for artist in artist_search:
            # Search track name and track name without mix (even if parsed is off)
            for track_name in [track_.name_mix]:  # , track_. name]:
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
) -> str:
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
) -> str:
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
) -> str:
    """Search for a track on Spotify using various search strategies.

    Args:
        track (BeatportTrack): Track dictionary.
        silent (bool): Whether to suppress logging output.
        parse_track (bool): Whether to parse the track name and mix.

    Returns:
        str: Spotify track ID if found, otherwise None.

    """
    spotify_auth()
    return search_for_track_v2(track=track, silent=silent, parse_track=parse_track)


#


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
        track_artist_name = track.artists[0] + " - " + track.name + " - " + track.mix
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
        spotify_ins = spotify_auth()
        daily_playlist = spotify_ins.playlist(playlist_id=playlists[1]["id"])
        n_daily_tracks = len(daily_playlist["tracks"]["items"])
    else:
        n_daily_tracks = 0

    # TODO Refresh oauth to avoid time out
    spotify_auth()

    for track in top_100_chart:
        track_count_tot += 1
        track_artist_name = track.artists[0] + " - " + track.name + " - " + track.mix
        if not silent:
            logger.info(
                "  [Start] {}% : {} : nb {} out of {}".format(
                    str(round(track_count_tot / len(top_100_chart) * 100, 2)),
                    track.name
                    + " - "
                    + track.mix
                    + " by "
                    + track.artists[0],  # track_artist_name,
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
            f'[+] No new tracks to add to the playlist: "{daily_top_n_playlist_name}"'
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
