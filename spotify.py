import sys
import json
import socket
import spotipy
import asyncio
import webbrowser
from time import time
from spotipy import oauth2
import pandas as pd
import re
from datetime import datetime
from time import sleep

from config import *

tracks_dict_names = ['id', 'duration_ms', 'href', 'name', 'popularity', 'uri', 'artists']

def save_hist_file(df_hist_pl_tracks, folder_path=folder_path):
    """
    Function to save the playlist history in a Excel file
    :param df_hist_pl_tracks: dataframe of the history of playlist tracks to save
    :folder_path: Path where to save the dataframe as Excel file
    """
    sleep(1) # try to avoid read-write errors if running too quickly
    df_hist_pl_tracks_out = df_hist_pl_tracks.loc[:, ['playlist_id', 'track_id', 'datetime_added', 'artist_name']]
    df_hist_pl_tracks_out.to_excel(folder_path+'hist_playlists_tracks.xlsx', index = False)
    print("\n Done saving file")

def listen_for_callback_code():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', int(redirect_uri.split(":")[-1])))
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


async def get_spotify_auth_code():
    auth_url = sp_oauth.get_authorize_url()
    webbrowser.open(auth_url)


async def async_get_auth_code():
    task = asyncio.create_task(get_spotify_auth_code())
    await task
    return listen_for_callback_code()


def do_spotify_oauth():
    try:
        with open("token.json", "r") as fh:
            token = fh.read()
        token = json.loads(token)
    except:
        token = None
    if token:
        if int(time()) > (token["expires_at"] - 50):  # Take 50s margin to avoid timeout while searching
            print("Refreshing Spotify token")
            token = sp_oauth.refresh_access_token(token["refresh_token"])
    else:
        authorization_code = asyncio.run(async_get_auth_code())
        print(authorization_code)
        if not authorization_code:
            print("\n[!] Unable to authenticate to Spotify.  Couldn't get authorization code")
            sys.exit(-1)
        token = sp_oauth.get_access_token(authorization_code)
    if not token:
        print("\n[!] Unable to authenticate to Spotify.  Couldn't get access token.")
        sys.exit(-1)
    try:
        with open("token.json", "w+") as fh:
            fh.write(json.dumps(token))
    except:
        print("\n[!] Unable to to write token object to disk.  This is non-fatal.")
    return token


def get_all_playlists():
    playlists_pager = spotify_ins.user_playlists(username)
    playlists = playlists_pager["items"]
    while playlists_pager["next"]:
        playlists_pager = spotify_ins.next(playlists_pager)
        playlists.extend(playlists_pager["items"])
    return playlists


def create_playlist(playlist_name):
    # TODO export parameter description
    playlist = spotify_ins.user_playlist_create(username, playlist_name, description=playlist_description)
    return playlist["id"]


def get_playlist_id(playlist_name):
    playlists = get_all_playlists()
    for playlist in playlists:
        if playlist['owner']['id'] == username:  # Can only modify playlist that the user owns
            if playlist["name"] == playlist_name:
                return playlist["id"]
    return None


def do_durations_match(source_track_duration, found_track_duration, silent=silent_search):
    if source_track_duration == found_track_duration:
        if not silent: print("\t\t\t\t[+] Durations match")
        return True
    else:
        if not silent: print("\t\t\t\t[!] Durations do not match")
        return False


def most_popular_track(tracks):
    # Popularity does not always yield the correct result
    high_score = 0
    winner = None
    for track in tracks:
        if track["popularity"] > high_score:
            winner = track["id"]
            high_score = track["popularity"]
    return winner


def best_of_multiple_matches(source_track, found_tracks, silent=silent_search):
    counter = 1
    duration_matches = [0, ]
    for track in found_tracks:
        if not silent: print("\t\t\t[+] Match {}: {}".format(counter, track["id"]))
        if do_durations_match(source_track["duration_ms"], track["duration_ms"]):
            duration_matches[0] += 1
            duration_matches.append(track)
        counter += 1
    if duration_matches[0] == 1:
        best_track = duration_matches.pop()["id"]
        if not silent: print(
            "\t\t\t[+] Only one exact match with matching duration, going with that one: {}".format(best_track))
        return best_track
    # TODO: Popularity does not always yield the correct result
    best_track = most_popular_track(found_tracks)
    if not silent: print(
        "\t\t\t[+] Multiple exact matches with matching durations, going with the most popular one: {}".format(
            best_track))
    return best_track


def search_for_track(track, silent=silent_search):
    # TODO: This is repetitive, can probably refactor but works for now
    if not silent: print("\n[+] Searching for track: {}{}by {} on {}".format(track["name"], " " if not track[
        "mix"] else " ({}) ".format(track["mix"]), ", ".join(track["artists"]), track["release"]))
    # Search with Title, Mix, Artists, and Release / Album
    query = "{}{}{} {}".format(track["name"], " " if not track["mix"] else " {} ".format(track["mix"]),
                               " ".join(track["artists"]), track["release"])
    if not silent: print("\t[+] Search Query: {}".format(query))
    search_results = spotify_ins.search(query)
    if len(search_results["tracks"]["items"]) == 1:
        track_id = search_results["tracks"]["items"][0]["id"]
        if not silent: print("\t\t[+] Found an exact match on name, mix, artists, and release: {}".format(track_id))
        do_durations_match(track["duration_ms"], search_results["tracks"]["items"][0]["duration_ms"])
        return track_id

    if len(search_results["tracks"]["items"]) > 1:
        if not silent: print("\t\t[+] Found multiple exact matches ({}) on name, mix, artists, and release.".format(
            len(search_results["tracks"]["items"])))
        return best_of_multiple_matches(track, search_results["tracks"]["items"])

    # Not enough results, search w/o release
    if not silent: print("\t\t[+] No exact matches on name, mix, artists, and release.  Trying without release.")
    # Search with Title, Mix, and Artists
    query = "{}{}{}".format(track["name"], " " if not track["mix"] else " {} ".format(track["mix"]),
                            " ".join(track["artists"]))
    if not silent: print("\t[+] Search Query: {}".format(query))
    search_results = spotify_ins.search(query)
    if len(search_results["tracks"]["items"]) == 1:
        track_id = search_results["tracks"]["items"][0]["id"]
        if not silent: print("\t\t[+] Found an exact match on name, mix, and artists: {}".format(track_id))
        do_durations_match(track["duration_ms"], search_results["tracks"]["items"][0]["duration_ms"])
        return track_id

    if len(search_results["tracks"]["items"]) > 1:
        if not silent: print("\t\t[+] Found multiple exact matches ({}) on name, mix, and artists.".format(
            len(search_results["tracks"]["items"])))
        return best_of_multiple_matches(track, search_results["tracks"]["items"])

    # Not enough results, search w/o mix, but with release
    if not silent: print("\t\t[+] No exact matches on name, mix, and artists.  Trying without mix, but with release.")
    query = "{} {} {}".format(track["name"], " ".join(track["artists"]), track["release"])
    if not silent: print("\t[+] Search Query: {}".format(query))
    search_results = spotify_ins.search(query)
    if len(search_results["tracks"]["items"]) == 1:
        track_id = search_results["tracks"]["items"][0]["id"]
        if not silent: print("\t\t[+] Found an exact match on name, artists, and release: {}".format(track_id))
        do_durations_match(track["duration_ms"], search_results["tracks"]["items"][0]["duration_ms"])
        return track_id

    if len(search_results["tracks"]["items"]) > 1:
        if not silent: print("\t\t[+] Found multiple exact matches ({}) on name, artists, and release.".format(
            len(search_results["tracks"]["items"])))
        return best_of_multiple_matches(track, search_results["tracks"]["items"])

    # Not enough results, search w/o mix or release
    if not silent: print("\t\t[+] No exact matches on name, artists, and release.  Trying with just name and artists.")
    query = "{} {}".format(track["name"], " ".join(track["artists"]))
    if not silent: print("\t[+] Search Query: {}".format(query))
    search_results = spotify_ins.search(query)
    if len(search_results["tracks"]["items"]) == 1:
        track_id = search_results["tracks"]["items"][0]["id"]
        if not silent: print("\t\t[+] Found an exact match on name and artists: {}".format(track_id))
        do_durations_match(track["duration_ms"], search_results["tracks"]["items"][0]["duration_ms"])
        return track_id

    if len(search_results["tracks"]["items"]) > 1:
        if not silent: print("\t\t[+] Found multiple exact matches ({}) on name and artists.".format(
            len(search_results["tracks"]["items"])))
        return best_of_multiple_matches(track, search_results["tracks"]["items"])

    print("\t\t[+] No exact matches on name and artists v1 : {} - {}{}".format(track["artists"][0], track["name"],
                                                                               "" if not track[
                                                                                   "mix"] else " - {}".format(
                                                                                   track["mix"])))
    print("\t[!] Could not find this song on Spotify!")
    return None


def parse_search_results_spotify(search_results, track, silent=silent_search):
    """
    :param search_results: Spotify API search result
    :param track: track dict to search
    :param silent: If false print detailed search results
    :return: track_id as string if found, else None
    """

    track_id = None

    if len(search_results["tracks"]["items"]) == 1:
        track_id = search_results["tracks"]["items"][0]["id"]
        if not silent: print("\t\t[+] Found an exact match on name, mix, artists, and release: {}".format(track_id))
        do_durations_match(track["duration_ms"], search_results["tracks"]["items"][0]["duration_ms"])
        return track_id

    if len(search_results["tracks"]["items"]) > 1:
        if not silent: print("\t\t[+] Found multiple exact matches ({}) on name, mix, artists, and release.".format(
            len(search_results["tracks"]["items"])))
        return best_of_multiple_matches(track, search_results["tracks"]["items"])

    return track_id


def parse_track_regex_beatport(track):
    track_out = track.copy()  # Otherwise modifies the dict
    track_out["name"] = re.sub(r'(\s*(Feat|feat|Ft|ft)\. [\w\s]*$)', '',
                               track_out["name"])  # Remove feat info, mostly not present in spotify
    track_out["name"] = re.sub(r'\W', ' ',
                               track_out["name"])  # Remove special characters as they are not handled by Spotify API

    return track_out


def parse_track_regex_beatport_v2(track):
    track_out = track.copy()  # Otherwise modifies the dict
    track_out["name"] = re.sub(r'(\s*(Feat|feat|Ft|ft)\. [\w\s]*$)', '',
                               track_out["name"])  # Remove feat info, mostly not present in spotify
    track_out["name"] = re.sub(r'[^\w\s]', '',
                               track_out["name"])  # Remove special characters as they are not handled by Spotify API

    return track_out


def parse_track_regex_beatport_v3(track):
    track_out = track.copy()  # Otherwise modifies the dict
    track_out["name"] = re.sub(r'(\s*(Feat|feat|Ft|ft)\. [\w\s]*$)', '',
                               track_out["name"])  # Remove feat info, mostly not present in spotify
    track_out["name"] = re.sub(r'[^\w\s]', '',
                               track_out["name"])  # Remove special characters as they are not handled by Spotify API
    track_out["mix"] = re.sub("[R|r]emix", 'mix',
                              track_out["mix"])  # Change remix
    track_out["mix"] = re.sub("[M|m]ix", 'Remix',
                              track_out["mix"])  # Change to remix

    return track_out


def parse_track_regex_beatport_v4(track):
    track_out = track.copy()  # Otherwise modifies the dict
    track_out["name"] = re.sub(r'(\s*(Feat|feat|Ft|ft)\. [\w\s]*$)', '',
                               track_out["name"])  # Remove feat info, mostly not present in spotify
    track_out["name"] = re.sub(r'[^\w\s]', '',
                               track_out["name"])  # Remove special characters as they are not handled by Spotify API
    track_out["mix"] = re.sub("[M|m]ix", '',
                              track_out["mix"])  # Remove special characters as they are not handled by Spotify API

    return track_out


def add_space(match):
    return " " + match.group()


def search_for_track_v2(track, silent=silent_search, parse_track=parse_track):
    """
    :param track: track dict
    :param silent: if true does not pring detailed, only if not found
    :param parse_track: if true try to remove (.*) and mix information
    :return: Spotify track_id
    """
    if parse_track:
        track_parsed = [track.copy(), parse_track_regex_beatport(track),
                        parse_track_regex_beatport_v2(track), parse_track_regex_beatport_v3(track),
                        parse_track_regex_beatport_v4(track)]
    else:
        track_parsed = [track]

    for track_ in track_parsed:
        # Create a field name mix according to Spotify formatting
        track_["name_mix"] = "{}{}".format(track_["name"], "" if not track_["mix"] else " - {}".format(track_["mix"]))

        # Create a parsed artist and try both
        artist_search = [*track_["artists"]]
        if parse_track:
            # Add parsed artist if not in list already
            artist_search.extend(x for x in [re.sub(r'\s*\([^)]*\)', '', artist_) for artist_ in track_["artists"]] if
                                 x not in artist_search)  # Remove (UK) for example
            artist_search.extend(x for x in [re.sub(r'\W+', ' ', artist_) for artist_ in track_["artists"]] if
                                 x not in artist_search)  # Remove special characters, in case it is not handled by Spotify API
            artist_search.extend(x for x in [re.sub(r'[^\w\s]', '', artist_) for artist_ in track_["artists"]] if
                                 x not in artist_search)  # Remove special characters, in case it is not handled by Spotify API
            artist_search.extend(
                x for x in [re.sub(r'(?<=\w)[A-Z]', add_space, artist_) for artist_ in track_["artists"]] if
                x not in artist_search)  # Splitting artist name with a space after a capital letter
            artist_search.extend(x for x in [re.sub(r'\s&.*$', "", artist_) for artist_ in track_["artists"]] if
                                 x not in artist_search)  # Removing second part after &

        # Search artist and artist parsed if parsed is on
        for artist in artist_search:
            # Search track name and track name without mix (even if parsed is off)
            for track_name in [track_["name_mix"], track_["name"]]:
                # Search with Title, Mix, Artist, Release / Album and Label
                if not silent:
                    print("\n[+] Searching for track: {} by {} on {} on {} label".format(track_name, artist,
                                                                                         track_["release"],
                                                                                         track_["label"]))
                query = 'track:"{}" artist:"{}" album:"{}" label:"{}"'.format(track_name, artist,
                                                                              track_["release"],
                                                                              track_["label"])
                if not silent:
                    print("\t[+] Search Query: {}".format(query))
                search_results = spotify_ins.search(query)
                track_id = parse_search_results_spotify(search_results, track_)
                if track_id:
                    return track_id

                # Search with Title, Mix, Artist and Label, w/o Release / Album
                if not silent:
                    print("\n[+] Searching for track: {} by {} on {} label".format(track_name, artist, track_["label"]))
                query = 'track:"{}" artist:"{}" label:"{}"'.format(track_name, artist, track_["label"])
                if not silent:
                    print("\t[+] Search Query: {}".format(query))
                search_results = spotify_ins.search(query)
                track_id = parse_search_results_spotify(search_results, track_)
                if track_id:
                    return track_id

                # Search with Title, Mix, Artist, Release / Album, w/o  Label
                if not silent:
                    print(
                        "\n[+] Searching for track: {} by {} on {} album".format(track_name, artist, track_["release"]))
                query = 'track:"{}" artist:"{}" album:"{}"'.format(track_name, artist, track_["release"])
                if not silent:
                    print("\t[+] Search Query: {}".format(query))
                search_results = spotify_ins.search(query)
                track_id = parse_search_results_spotify(search_results, track_)
                if track_id:
                    return track_id

                # Search with Title, Artist, Release / Album and Label, w/o Release and Label
                if not silent:
                    print("\n[+] Searching for track: {} by {}".format(track_name, artist))
                query = 'track:"{}" artist:"{}"'.format(track_name, artist)
                if not silent:
                    print("\t[+] Search Query: {}".format(query))
                search_results = spotify_ins.search(query)
                track_id = parse_search_results_spotify(search_results, track_)
                if track_id:
                    return track_id

    print("\t[+] No exact matches on name and artists v2 : {} - {}{}".format(track["artists"][0], track["name"],
                                                                               "" if not track[
                                                                                   "mix"] else " - {}".format(
                                                                                   track["mix"])))

    # Possible to use return search_for_track(track) but do not improve search results
    return None


def track_in_playlist(playlist_id, track_id):
    for track in get_all_tracks_in_playlist(playlist_id):
        if track["track"]["id"] == track_id:
            return True
    return False


def add_tracks_to_playlist(playlist_id, track_ids):
    if track_ids:
        spotify_auth()
        spotify_ins.user_playlist_add_tracks(username, playlist_id, track_ids)


def get_all_tracks_in_playlist(playlist_id):
    playlist_tracks_results = spotify_ins.user_playlist(username, playlist_id, fields="tracks")
    playlist_tracks_pager = playlist_tracks_results["tracks"]
    playlist_tracks = playlist_tracks_pager["items"]
    while playlist_tracks_pager["next"]:
        playlist_tracks_pager = spotify_ins.next(playlist_tracks_pager)
        playlist_tracks.extend(playlist_tracks_pager["items"])
    return playlist_tracks


def clear_playlist(playlist_id):
    for track in get_all_tracks_in_playlist(playlist_id):
        spotify_ins.user_playlist_remove_all_occurrences_of_tracks(username, playlist_id, [track["track"]["id"], ])


def add_new_tracks_to_playlist(genre, tracks_dict):
    # TODO export playlist anterior name to config
    # persistent_top_100_playlist_name = "{}{} - Top 100".format(playlist_prefix, genre)
    # daily_top_10_playlist_name = "{}{} - Daily Top".format(playlist_prefix, genre)
    persistent_top_100_playlist_name = "Beatporter: {} - Top 100".format(genre)
    daily_top_n_playlist_name = "Beatporter: {} - Daily Top".format(genre)
    print("[+] Identifying new tracks for playlist: \"{}\"".format(persistent_top_100_playlist_name))

    if daily_mode:
        playlists = [
            {"name": persistent_top_100_playlist_name, "id": get_playlist_id(persistent_top_100_playlist_name)},
            {"name": daily_top_n_playlist_name, "id": get_playlist_id(daily_top_n_playlist_name)}]
    else:
        playlists = [
            {"name": persistent_top_100_playlist_name, "id": get_playlist_id(persistent_top_100_playlist_name)}]

    for playlist in playlists:
        if not playlist["id"]:
            print("\t[!] Playlist \"{}\" does not exist, creating it.".format(playlist["name"]))
            playlist["id"] = create_playlist(playlist["name"])

    if daily_mode:
        # Clear daily playlist
        clear_playlist(playlists[1]["id"])

    persistent_top_100_track_ids = list()
    daily_top_n_track_ids = list()
    track_count = 0
    for track in tracks_dict:
        track_id = search_for_track_v2(track)
        if track_id and not track_in_playlist(playlists[0]["id"], track_id):
            persistent_top_100_track_ids.append(track_id)
        if track_id and track_count < daily_n_track:
            daily_top_n_track_ids.append(track_id)
        track_count += 1
    print("\n[+] Adding {} new tracks to the playlist: \"{}\"".format(len(persistent_top_100_track_ids),
                                                                      persistent_top_100_playlist_name))
    add_tracks_to_playlist(playlists[0]["id"], persistent_top_100_track_ids)
    if daily_mode:
        print("\n[+] Adding {} new tracks to the playlist: \"{}\"".format(len(daily_top_n_track_ids),
                                                                          daily_top_n_playlist_name))
        add_tracks_to_playlist(playlists[1]["id"], daily_top_n_track_ids)


def parse_tracks_spotify(tracks_json):
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
                "key": track["key"]
            }
        )
    return tracks


def parse_artist(value, key):
    # TODO find better method
    if key == 'artists':
        value = value[0]['name']
    else:
        value

    return value


def update_hist_pl_tracks(df_hist_pl_tracks, playlist):
    """
    :param df_hist_pl_tracks: dataframe of history of track id and playlist id
    :param playlist: dict typ playlist = {"name": playlist_name, "id": playlist_id}
    :return: updated df_hist_pl_tracks
    """
    # TODO find better method

    track_list = get_all_tracks_in_playlist(playlist["id"])
    df_tracks = pd.DataFrame.from_dict(track_list)

    if len(df_tracks.index) > 0:
        df_tracks['track'] = [{key: value for key, value in track.items() if key in tracks_dict_names} for track in
                              df_tracks['track']]
        df_tracks['track'] = [{key: parse_artist(value, key) for key, value in track.items()} for track in
                              df_tracks['track']]

        df_tracks_o = pd.DataFrame()
        for row in df_tracks.iterrows():
            df_tracks_o = pd.concat([df_tracks_o, pd.DataFrame(row[1]['track'], index=[0])])
        df_tracks_o = df_tracks_o.loc[:, tracks_dict_names].reset_index(drop=True)
        df_tracks_o['artist_name'] = df_tracks_o['artists'] + " - " + df_tracks_o['name']

        df_tracks = pd.concat([df_tracks_o, df_tracks.loc[:, 'added_at']], axis=1)

        df_temp = df_tracks.loc[:, ['id', 'added_at', 'artist_name']]
        df_temp['playlist_id'] = playlist["id"]
        df_temp = df_temp.rename(columns={'id': 'track_id', 'added_at': 'datetime_added'})

        df_hist_pl_tracks = pd.concat([df_hist_pl_tracks, df_temp])
        df_hist_pl_tracks = df_hist_pl_tracks.drop_duplicates().reset_index(drop=True)

    return (df_hist_pl_tracks)


def find_playlist_chart_label(title):
    """
    :param title: chart or label title
    :return: dict of playlist name and playlist ID, playlist ID is None if not found
    """
    persistent_playlist_name = "{}{}".format(playlist_prefix, title)
    playlist = {"name": persistent_playlist_name, "id": get_playlist_id(persistent_playlist_name)}

    return playlist


def add_new_tracks_to_playlist_chart_label(title, tracks_dict, df_hist_pl_tracks, use_prefix=True,
                                           silent=silent_search):
    """
    :param title: Chart or label playlist title
    :param tracks_dict: dict of tracks to add
    :param df_hist_pl_tracks: dataframe of history of track, will not add track_id already present
    :param use_prefix: add a prefix to the playlist name as defined in config
    :param silent: If true do not display searching details except errors
    :return: updated df_hist_pl_tracks
    """

    # TODO Refresh oauth to avoid time out
    spotify_auth()

    # TODO export playlist anterior name to config
    if use_prefix:
        persistent_playlist_name = "{}{}".format(playlist_prefix, title)
    else:
        persistent_playlist_name = title
    print("[+] Identifying new tracks for playlist: \"{}\"".format(persistent_playlist_name))

    playlist = {"name": persistent_playlist_name, "id": get_playlist_id(persistent_playlist_name)}

    if not playlist["id"]:
        print("\t[!] Playlist \"{}\" does not exist, creating it.".format(playlist["name"]))
        playlist["id"] = create_playlist(playlist["name"])

    df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)
    playlist_track_ids = df_hist_pl_tracks.loc[df_hist_pl_tracks["playlist_id"] == playlist["id"], "track_id"]

    if digging_mode == "playlist":
        df_local_hist = df_hist_pl_tracks.loc[df_hist_pl_tracks["playlist_id"] == playlist["id"]]
    elif digging_mode == "all":
        df_local_hist = df_hist_pl_tracks
    else:
        df_local_hist = pd.DataFrame(columns=['playlist_id', 'track_id', 'datetime_added', 'artist_name'])

    persistent_track_ids = list()
    track_count = 0
    track_count_tot = 0

    for track in tracks_dict:
        track_count_tot += 1
        track_artist_name = track['artists'][0] + " - " + track['name'] + " - " + track["mix"]
        if not silent:
            print("{}% : {} : nb {} out of {}".format(str(round(track_count_tot / len(tracks_dict) * 100, 2)),
                                                      track_artist_name, track_count_tot, len(tracks_dict)))
        if track_artist_name not in df_local_hist.values:
            track_id = search_for_track_v2(track)
            if track_id and track_id not in playlist_track_ids.values and track_id not in df_local_hist.values:
                if not silent:
                    print("\t[+] Adding track id : {} : nb {}".format(track_id, track_count))
                persistent_track_ids.append(track_id)
                track_count += 1
            if track_count >= 99:  # Have limit of 100 trakcks per import
                print("\n[+] Adding {} new tracks to the playlist: \"{}\"".format(len(persistent_track_ids),
                                                                                  persistent_playlist_name))
                add_tracks_to_playlist(playlist["id"], persistent_track_ids)
                # TODO consider only adding new ID to avoid reloading large playlist
                df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)
                playlist_track_ids = df_hist_pl_tracks.loc[
                    df_hist_pl_tracks["playlist_id"] == playlist["id"], "track_id"]
                track_count = 0
                persistent_track_ids = list()
                update_playlist_description_with_date(playlist)
        else:
            if not silent:
                print("\tSimilar track name already found")

        if track_count_tot % refresh_token_n_tracks == 0:  # Avoid time out
            spotify_auth()
            print("[+] Identifying new tracks for playlist: \"{}\"\n".format(persistent_playlist_name))

    print("\n[+] Adding {} new tracks to the playlist: \"{}\"".format(len(persistent_track_ids),
                                                                      persistent_playlist_name))
    if len(persistent_track_ids) > 0:
        add_tracks_to_playlist(playlist["id"], persistent_track_ids)
        update_playlist_description_with_date(playlist)

    df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)

    if len(persistent_track_ids) > 0:
        save_hist_file(df_hist_pl_tracks, folder_path=folder_path)

    return df_hist_pl_tracks


def add_new_tracks_to_playlist_id(playlist_name, track_ids, df_hist_pl_tracks, silent=silent_search):
    """
    :param playlist_name: Playlist name to be used, will not be modified
    :param track_ids: dict of tracks with their IDS
    :param df_hist_pl_tracks: dataframe of history of track, will not add track_id already present
    :param silent: If true do not display searching details except errors
    :return: updated df_hist_pl_tracks
    """
    # TODO unify all add_new_track in one function

    # TODO Refresh oauth to avoid time out
    spotify_auth()

    # TODO export playlist prefix name to config
    persistent_playlist_name = playlist_name
    print("[+] Identifying new tracks for playlist: \"{}\"".format(persistent_playlist_name))

    playlist = {"name": persistent_playlist_name, "id": get_playlist_id(persistent_playlist_name)}

    if not playlist["id"]:
        print("\t[!] Playlist \"{}\" does not exist, creating it.".format(playlist["name"]))
        playlist["id"] = create_playlist(playlist["name"])

    df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)
    playlist_track_ids = df_hist_pl_tracks.loc[df_hist_pl_tracks["playlist_id"] == playlist["id"], "track_id"]

    if digging_mode == "playlist":
        df_local_hist = df_hist_pl_tracks.loc[df_hist_pl_tracks["playlist_id"] == playlist["id"]]
    elif digging_mode == "all":
        df_local_hist = df_hist_pl_tracks
    else:
        df_local_hist = pd.DataFrame(columns=['playlist_id', 'track_id', 'datetime_added', 'artist_name'])

    persistent_track_ids = list()
    track_count = 0
    track_count_tot = 0

    for track in track_ids:
        if track['track'] is not None:  # Prevent error of empty track
            track_id = track['track']['id']
            track_count_tot += 1
            if track_id not in df_local_hist.values:
                if track_id not in playlist_track_ids.values:
                    if not silent:
                        print("\t[+] Adding track id : {} : nb {}".format(track_id, track_count))
                    persistent_track_ids.append(track_id)
                    track_count += 1
                if track_count >= 99:  # Have limit of 100 trakcks per import
                    print("\n[+] Adding {} new tracks to the playlist: \"{}\"".format(len(persistent_track_ids),
                                                                                      persistent_playlist_name))
                    add_tracks_to_playlist(playlist["id"], persistent_track_ids)
                    # TODO consider only adding new ID to avoid reloading large playlist
                    df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)
                    playlist_track_ids = df_hist_pl_tracks.loc[
                        df_hist_pl_tracks["playlist_id"] == playlist["id"], "track_id"]
                    track_count = 0
                    persistent_track_ids = list()
                    update_playlist_description_with_date(playlist)
            else:
                if not silent:
                    print("\tTrack already found in playlist or history")

            if track_count_tot % refresh_token_n_tracks == 0:  # Avoid time out
                spotify_auth()
                print("[+] Identifying new tracks for playlist: \"{}\"\n".format(persistent_playlist_name))

    print("\n[+] Adding {} new tracks to the playlist: \"{}\"".format(len(persistent_track_ids),
                                                                      persistent_playlist_name))
    if len(persistent_track_ids) > 0:
        add_tracks_to_playlist(playlist["id"], persistent_track_ids)
        update_playlist_description_with_date(playlist)

    df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)

    if len(persistent_track_ids) > 0:
        save_hist_file(df_hist_pl_tracks, folder_path=folder_path)

    return df_hist_pl_tracks


def add_new_tracks_to_playlist_genre(genre, top_100_chart, df_hist_pl_tracks, silent=silent_search):
    """
    :param genre: Genre name
    :param top_100_chart: dict of tracks to add
    :param df_hist_pl_tracks: dataframe of history of track, will not add track_id already present
    :param silent: If true do not display searching details except errors
    :return: updated df_hist_pl_tracks
    """

    # TODO export playlist anterior name to config
    # persistent_top_100_playlist_name = "{}{} - Top 100".format(playlist_prefix, genre)
    # daily_top_10_playlist_name = "{}{} - Daily Top".format(playlist_prefix, genre)
    persistent_top_100_playlist_name = "Beatporter: {} - Top 100".format(genre)
    daily_top_n_playlist_name = "Beatporter: {} - Daily Top".format(genre)
    print("[+] Identifying new tracks for playlist: \"{}\"".format(persistent_top_100_playlist_name))

    if daily_mode:
        playlists = [
            {"name": persistent_top_100_playlist_name, "id": get_playlist_id(persistent_top_100_playlist_name)},
            {"name": daily_top_n_playlist_name, "id": get_playlist_id(daily_top_n_playlist_name)}]
    else:
        playlists = [
            {"name": persistent_top_100_playlist_name, "id": get_playlist_id(persistent_top_100_playlist_name)}]

    for playlist in playlists:
        if not playlist["id"]:
            print("\t[!] Playlist \"{}\" does not exist, creating it.".format(playlist["name"]))
            playlist["id"] = create_playlist(playlist["name"])
        df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)

    # Create local hist for top 100 playlist
    if digging_mode == "playlist":
        df_local_hist = df_hist_pl_tracks.loc[df_hist_pl_tracks["playlist_id"] == playlists[0]["id"]]
    elif digging_mode == "all":
        df_local_hist = df_hist_pl_tracks
    else:
        df_local_hist = pd.DataFrame(columns=['playlist_id', 'track_id', 'datetime_added', 'artist_name'])
    playlist_track_ids = df_hist_pl_tracks.loc[df_hist_pl_tracks["playlist_id"] == playlists[0]["id"], "track_id"]

    if daily_mode:
        if digging_mode == "":
            # Clear daily playlist if digging mode is not using hist otherwise will delete tracks not yet listened
            clear_playlist(playlists[1]["id"])
            df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlists[1])
            playlist_track_ids_daily = pd.Series([], name="track_id", dtype=object)
            df_local_hist_daily = pd.DataFrame(columns=['playlist_id', 'track_id', 'datetime_added', 'artist_name'])
        else:
            # Create local hist for daily playlist
            if digging_mode == "playlist":
                df_local_hist_daily = df_hist_pl_tracks.loc[df_hist_pl_tracks["playlist_id"] == playlists[1]["id"]]
            elif digging_mode == "all":
                df_local_hist_daily = df_hist_pl_tracks
            playlist_track_ids_daily = df_hist_pl_tracks.loc[
                df_hist_pl_tracks["playlist_id"] == playlists[1]["id"], "track_id"]

    persistent_track_ids = list()
    daily_top_n_track_ids = list()
    track_count = 0
    track_count_tot = 0

    # Get the number of tracks in the daily playlist
    if daily_mode:
        daily_playlist = spotify_ins.playlist(playlist_id=playlists[1]["id"])
        n_daily_tracks = len(daily_playlist['tracks']['items'])
    else:
        n_daily_tracks = 0

    for track in top_100_chart:
        track_count_tot += 1
        track_artist_name = track['artists'][0] + " - " + track['name'] + " - " + track["mix"]
        if not silent:
            print("{}% : {} : nb {} out of {}".format(str(round(track_count_tot / len(top_100_chart) * 100, 2)),
                                                      track_artist_name, track_count_tot, len(top_100_chart)))

        track_id = search_for_track_v2(track)

        if track_id:
            if track_id not in playlist_track_ids.values and track_id not in df_local_hist.values:
                if not silent:
                    print("\t[+] Adding track id : {} : nb {}".format(track_id, track_count))
                persistent_track_ids.append(track_id)
                track_count += 1
            else:
                if not silent:
                    print("\tSimilar track name already found")

            if n_daily_tracks < daily_n_track and track_id not in playlist_track_ids_daily.values and track_id not in df_local_hist_daily.values:
                daily_top_n_track_ids.append(track_id)
                n_daily_tracks += 1
        if track_count >= 99:  # Have limit of 100 tracks per import
            print("\n[+] Adding {} new tracks to the playlist: \"{}\"".format(len(persistent_track_ids),
                                                                              persistent_top_100_playlist_name))
            add_tracks_to_playlist(playlists[0]["id"], persistent_track_ids)
            # TODO consider only adding new ID to avoid reloading large playlist
            df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)
            playlist_track_ids = df_hist_pl_tracks.loc[
                df_hist_pl_tracks["playlist_id"] == playlists[0]["id"], "track_id"]
            track_count = 0
            persistent_track_ids = list()
            update_playlist_description_with_date(playlists[0])

        if track_count_tot % refresh_token_n_tracks == 0:  # Avoid time out
            spotify_auth()
            print("[+] Identifying new tracks for playlist: \"{}\"\n".format(persistent_top_100_playlist_name))

    print("\n[+] Adding {} new tracks to the playlist: \"{}\"".format(len(persistent_track_ids),
                                                                      persistent_top_100_playlist_name))
    print("\n[+] Adding {} new tracks to the playlist: \"{}\"".format(len(daily_top_n_track_ids),
                                                                      daily_top_n_playlist_name))
    if len(persistent_track_ids) > 0:
        add_tracks_to_playlist(playlists[0]["id"], persistent_track_ids)
        update_playlist_description_with_date(playlists[0])

    if len(daily_top_n_track_ids) > 0 & daily_mode:
        add_tracks_to_playlist(playlists[1]["id"], daily_top_n_track_ids)
        update_playlist_description_with_date(playlists[1])

    # Add more to daily playlist if not full
    if daily_mode:
        playlist_track_ids = playlist_track_ids[::-1]  # Reverse order to get freshest first
        if n_daily_tracks < daily_n_track:
            extra_daily_top_n_track_ids = list()
            for track_id in playlist_track_ids:  # Full playlist tracks ID
                if n_daily_tracks < daily_n_track and track_id not in playlist_track_ids_daily.values:
                    if track_id not in df_local_hist_daily.values and track_id not in daily_top_n_track_ids:
                        extra_daily_top_n_track_ids.append(track_id)
                        n_daily_tracks += 1

            print("\n[+] Adding {} extra new tracks to the playlist: \"{}\"".format(len(extra_daily_top_n_track_ids),
                                                                                    daily_top_n_playlist_name))
            add_tracks_to_playlist(playlists[1]["id"], extra_daily_top_n_track_ids)
            update_playlist_description_with_date(playlists[1])

    for playlist in playlists:
        df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)

    if (len(persistent_track_ids) > 0) or (len(daily_top_n_track_ids) > 0):
        save_hist_file(df_hist_pl_tracks, folder_path=folder_path)

    return df_hist_pl_tracks


def update_playlist_description_with_date(playlist):
    """
    :param playlist: playlist dict
    :return: None
    """
    playlist_desc = spotify_ins.playlist(playlist_id=playlist["id"])
    playlist_desc['description'] = re.sub(r'\s*Updated on \d{4}-\d{2}-\d{2}\.*', '', playlist_desc['description'])
    playlist_desc['description'] = re.sub(r'&#x2F;', '/', playlist_desc['description'])
    spotify_ins.playlist_change_details(playlist_id=playlist["id"],
                                        description=playlist_desc['description'] + " Updated on {}.".format(
                                            datetime.today().strftime('%Y-%m-%d')))


def update_hist_from_playlist(title, df_hist_pl_tracks):
    # TODO test, to remove
    persistent_playlist_name = "{}{}".format(playlist_prefix, title)
    print("[+] Getting hist of tracks for playlist: \"{}\"".format(persistent_playlist_name))

    playlist = {"name": persistent_playlist_name, "id": get_playlist_id(persistent_playlist_name)}

    if playlist["id"]:
        df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)
        # TODO else pass ?

    return df_hist_pl_tracks


def back_up_spotify_playlist(playlist_name, org_playlist_id, df_hist_pl_tracks):
    track_ids = get_all_tracks_in_playlist(org_playlist_id)
    df_hist_pl_tracks = add_new_tracks_to_playlist_id(playlist_name, track_ids, df_hist_pl_tracks)

    return df_hist_pl_tracks


def spotify_auth():
    # Get authenticated to Spotify
    print("\nRefreshing Spotify auth")
    global spotify_ins
    token_info = do_spotify_oauth()
    spotify_ins = spotipy.Spotify(auth=token_info["access_token"])


sp_oauth = oauth2.SpotifyOAuth(client_id, client_secret, redirect_uri, username=username, scope=scope)
spotify_auth()

# Annex testing tracks with known issues

track_working_mix = {
    "title": "",
    "name": "The Shake",
    "mix": "Extended Mix",
    "artists": [
        "Ellis Moss"
    ],
    "remixers": [],
    "release": "The Shake",
    "label": "Toolroom Trax",
    "published_date": "2021-01-29",
    "released_date": "2021-01-29",
    "duration": "6:14",
    "duration_ms": 374032,
    "genres": [
        "Tech House"
    ],
    "bpm": 124,
    "key": "G min"
}

track_not_working_mix = {
    "title": "",
    "name": "Jumpin'",
    "mix": "Extended",
    "artists": [
        "CID",
        "Westend"
    ],
    "remixers": [],
    "release": "Jumpin'",
    "label": "Repopulate Mars",
    "published_date": "2021-02-12",
    "released_date": "2021-02-12",
    "duration": "5:04",
    "duration_ms": 304761,
    "genres": [
        "Tech House"
    ],
    "bpm": 126,
    "key": "A min"
}

track_not_working_artist = {
    "title": "",
    "name": "Set U Free",
    "mix": "Extended Mix",
    "artists": [
        "GUZ (NL)"
    ],
    "remixers": [],
    "release": "Set U Free (Extended Mix)",
    "label": "Sink or Swim",
    "published_date": "2021-01-29",
    "released_date": "2021-01-29",
    "duration": "4:23",
    "duration_ms": 263040,
    "genres": [
        "Tech House"
    ],
    "bpm": 125,
    "key": "B maj"
}

track_special_characters = {
    "title": "",
    "name": "Don't Touch The Pool",
    "mix": "Original Mix",
    "artists": [
        "FOVOS"
    ],
    "remixers": [],
    "release": "Hot Mess",
    "label": "Country Club Disco",
    "published_date": "2021-02-12",
    "released_date": "2021-02-12",
    "duration": "3:47",
    "duration_ms": 227302,
    "genres": [
        "Tech House"
    ],
    "bpm": 128,
    "key": "A maj"
}

track_name_special_char_cant_space = {
    "title": "",
    "name": "Don't Make Me",
    "mix": "Original Mix",
    "artists": [
        "Dillon Nathaniel"
    ],
    "remixers": [],
    "release": "Reason to Fly",
    "label": "Sola",
    "published_date": "2021-02-19",
    "released_date": "2021-02-19",
    "duration": "5:46",
    "duration_ms": 346666,
    "genres": [
        "Tech House"
    ],
    "bpm": 126,
    "key": "G\u266d min"
}
