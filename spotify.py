import sys
import json
import socket
import spotipy
import asyncio
import webbrowser
from time import time
from spotipy import oauth2
import pandas as pd
# from beatport import parse_tracks

from config import *

tracks_dict_names = ['id', 'duration_ms', 'href', 'name', 'popularity', 'uri', 'artists']

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
        if int(time()) > token["expires_at"]:
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
    playlists_pager = spotify.user_playlists(username)
    playlists = playlists_pager["items"]
    while playlists_pager["next"]:
        playlists_pager = spotify.next(playlists_pager)
        playlists.extend(playlists_pager["items"])
    return playlists


def create_playlist(playlist_name):
    playlist = spotify.user_playlist_create(username, playlist_name, description="Created using Beatporter.py")
    return playlist["id"]


def get_playlist_id(playlist_name):
    playlists = get_all_playlists()
    for playlist in playlists:
        if playlist["name"] == playlist_name:
            return playlist["id"]
    return None


def do_durations_match(source_track_duration, found_track_duration, silent = True):
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


def best_of_multiple_matches(source_track, found_tracks, silent = True):
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
        if not silent: print("\t\t\t[+] Only one exact match with matching duration, going with that one: {}".format(best_track))
        return best_track
    # TODO: Popularity does not always yield the correct result
    best_track = most_popular_track(found_tracks)
    if not silent: print("\t\t\t[+] Multiple exact matches with matching durations, going with the most popular one: {}".format(best_track))
    return best_track


def search_for_track(track, silent = True):
    # TODO: This is repetitive, can probably refactor but works for now
    print("\n[+] Searching for track: {}{}by {} on {}".format(track["name"], " " if not track["mix"] else " ({}) ".format(track["mix"]), ", ".join(track["artists"]), track["release"]))
    # Search with Title, Mix, Artists, and Release / Album
    query = "{}{}{} {}".format(track["name"], " " if not track["mix"] else " {} ".format(track["mix"]), " ".join(track["artists"]), track["release"])
    if not silent: print("\t[+] Search Query: {}".format(query))
    search_results = spotify.search(query)
    if len(search_results["tracks"]["items"]) == 1:
        track_id = search_results["tracks"]["items"][0]["id"]
        if not silent: print("\t\t[+] Found an exact match on name, mix, artists, and release: {}".format(track_id))
        do_durations_match(track["duration_ms"], search_results["tracks"]["items"][0]["duration_ms"])
        return track_id

    if len(search_results["tracks"]["items"]) > 1:
        if not silent: print("\t\t[+] Found multiple exact matches ({}) on name, mix, artists, and release.".format(len(search_results["tracks"]["items"])))
        return best_of_multiple_matches(track, search_results["tracks"]["items"])

    # Not enough results, search w/o release
    if not silent: print("\t\t[+] No exact matches on name, mix, artists, and release.  Trying without release.")
    # Search with Title, Mix, and Artists
    query = "{}{}{}".format(track["name"], " " if not track["mix"] else " {} ".format(track["mix"]), " ".join(track["artists"]))
    if not silent: print("\t[+] Search Query: {}".format(query))
    search_results = spotify.search(query)
    if len(search_results["tracks"]["items"]) == 1:
        track_id = search_results["tracks"]["items"][0]["id"]
        if not silent: print("\t\t[+] Found an exact match on name, mix, and artists: {}".format(track_id))
        do_durations_match(track["duration_ms"], search_results["tracks"]["items"][0]["duration_ms"])
        return track_id

    if len(search_results["tracks"]["items"]) > 1:
        if not silent: print("\t\t[+] Found multiple exact matches ({}) on name, mix, and artists.".format(len(search_results["tracks"]["items"])))
        return best_of_multiple_matches(track, search_results["tracks"]["items"])

    # Not enough results, search w/o mix, but with release
    if not silent: print("\t\t[+] No exact matches on name, mix, and artists.  Trying without mix, but with release.")
    query = "{} {} {}".format(track["name"], " ".join(track["artists"]), track["release"])
    if not silent: print("\t[+] Search Query: {}".format(query))
    search_results = spotify.search(query)
    if len(search_results["tracks"]["items"]) == 1:
        track_id = search_results["tracks"]["items"][0]["id"]
        if not silent: print("\t\t[+] Found an exact match on name, artists, and release: {}".format(track_id))
        do_durations_match(track["duration_ms"], search_results["tracks"]["items"][0]["duration_ms"])
        return track_id

    if len(search_results["tracks"]["items"]) > 1:
        if not silent: print("\t\t[+] Found multiple exact matches ({}) on name, artists, and release.".format(len(search_results["tracks"]["items"])))
        return best_of_multiple_matches(track, search_results["tracks"]["items"])

    # Not enough results, search w/o mix or release
    if not silent: print("\t\t[+] No exact matches on name, artists, and release.  Trying with just name and artists.")
    query = "{} {}".format(track["name"], " ".join(track["artists"]))
    if not silent: print("\t[+] Search Query: {}".format(query))
    search_results = spotify.search(query)
    if len(search_results["tracks"]["items"]) == 1:
        track_id = search_results["tracks"]["items"][0]["id"]
        if not silent: print("\t\t[+] Found an exact match on name and artists: {}".format(track_id))
        do_durations_match(track["duration_ms"], search_results["tracks"]["items"][0]["duration_ms"])
        return track_id

    if len(search_results["tracks"]["items"]) > 1:
        if not silent: print("\t\t[+] Found multiple exact matches ({}) on name and artists.".format(len(search_results["tracks"]["items"])))
        return best_of_multiple_matches(track, search_results["tracks"]["items"])

    print("\t\t[+] No exact matches on name and artists.")
    print("\t[!] Could not find this song on Spotify!")
    return None


def track_in_playlist(playlist_id, track_id):
    for track in get_all_tracks_in_playlist(playlist_id):
        if track["track"]["id"] == track_id:
            return True
    return False


def add_tracks_to_playlist(playlist_id, track_ids):
    if track_ids:
        spotify.user_playlist_add_tracks(username, playlist_id, track_ids)


def get_all_tracks_in_playlist(playlist_id):
    playlist_tracks_results = spotify.user_playlist(username, playlist_id, fields="tracks")
    playlist_tracks_pager = playlist_tracks_results["tracks"]
    playlist_tracks = playlist_tracks_pager["items"]
    while playlist_tracks_pager["next"]:
        playlist_tracks_pager = spotify.next(playlist_tracks_pager)
        playlist_tracks.extend(playlist_tracks_pager["items"])
    return playlist_tracks


def clear_playlist(playlist_id):
    for track in get_all_tracks_in_playlist(playlist_id):
        spotify.user_playlist_remove_all_occurrences_of_tracks(username, playlist_id, [track["track"]["id"],])


def add_new_tracks_to_playlist(genre, tracks_dict):
    persistent_top_100_playlist_name = "Beatporter: {} - Top 100".format(genre)
    daily_top_10_playlist_name = "Beatporter: {} - Daily Top 10".format(genre)
    print("[+] Identifying new tracks for playlist: \"{}\"".format(persistent_top_100_playlist_name))

    playlists = [{"name": persistent_top_100_playlist_name, "id": get_playlist_id(persistent_top_100_playlist_name)},
                 {"name": daily_top_10_playlist_name, "id": get_playlist_id(daily_top_10_playlist_name)}]

    for playlist in playlists:
        if not playlist["id"]:
            print("\t[!] Playlist \"{}\" does not exist, creating it.".format(playlist["name"]))
            playlist["id"] = create_playlist(playlist["name"])

    # Clear daily playlist
    clear_playlist(playlists[1]["id"])

    persistent_top_100_track_ids = list()
    daily_top_10_track_ids = list()
    track_count = 0
    for track in tracks_dict:
        track_id = search_for_track(track)
        if track_id and not track_in_playlist(playlists[0]["id"], track_id):
            persistent_top_100_track_ids.append(track_id)
        if track_id and track_count < 10:
            daily_top_10_track_ids.append(track_id)
        track_count += 1
    print("\n[+] Adding {} new tracks to the playlist: \"{}\"".format(len(persistent_top_100_track_ids), persistent_top_100_playlist_name))
    add_tracks_to_playlist(playlists[0]["id"], persistent_top_100_track_ids)
    print("\n[+] Adding {} new tracks to the playlist: \"{}\"".format(len(daily_top_10_track_ids), daily_top_10_playlist_name))
    add_tracks_to_playlist(playlists[1]["id"], daily_top_10_track_ids)

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
    df_tracks = pd.DataFrame.from_dict(spotify.playlist_items(playlist["id"])['items'])

    if len(df_tracks.index) > 0:
        df_tracks['track'] = [{key: value for key, value in track.items() if key in tracks_dict_names} for track in
                               df_tracks['track']]
        df_tracks['track'] = [{key: parse_artist(value, key) for key, value in track.items()} for track in
                               df_tracks['track']]

        df_tracks_o = pd.DataFrame()
        for row in df_tracks.iterrows():
            df_tracks_o = df_tracks_o.append(pd.DataFrame(row[1]['track'], index=[0]))
        df_tracks_o = df_tracks_o.loc[:, tracks_dict_names].reset_index(drop=True)
        df_tracks_o['artist_name'] = df_tracks_o['artists'] + " - " + df_tracks_o['name']

        df_tracks = pd.concat([df_tracks_o, df_tracks.loc[:, 'added_at']], axis=1)

        df_temp = df_tracks.loc[:, ['id', 'added_at', 'artist_name']]
        df_temp['playlist_id'] = playlist["id"]
        df_temp = df_temp.rename(columns={'id': 'track_id', 'added_at': 'datetime_added'})

        df_hist_pl_tracks = df_hist_pl_tracks.append(df_temp).drop_duplicates().reset_index(drop=True)

    return(df_hist_pl_tracks)

def find_playlist_chart_label(title):
    """
    :param title: chart or label title
    :return: dict of playlist name and playlist ID, playlist ID is None if not found
    """
    persistent_playlist_name = "Beatport: {}".format(title)
    playlist = {"name": persistent_playlist_name, "id": get_playlist_id(persistent_playlist_name)}

    return(playlist)


def add_new_tracks_to_playlist_chart_label(title, tracks_dict, df_hist_pl_tracks):
    """
    :param title: Chart or label playlist title
    :param tracks_dict: dict of tracks to add
    :param df_hist_pl_tracks: dataframe of history of track, will not add track_id already present
    :return: updated df_hist_pl_tracks
    """

    # # TODO Refersh oauth to avoid time out
    # sp_oauth = oauth2.SpotifyOAuth(client_id, client_secret, redirect_uri, username=username, scope=scope)
    # token_info = do_spotify_oauth()
    # spotify = spotipy.Spotify(auth=token_info["access_token"])

    persistent_playlist_name = "Beatport: {}".format(title)
    print("[+] Identifying new tracks for playlist: \"{}\"".format(persistent_playlist_name))

    playlist = {"name": persistent_playlist_name, "id": get_playlist_id(persistent_playlist_name)}

    if not playlist["id"]:
        print("\t[!] Playlist \"{}\" does not exist, creating it.".format(playlist["name"]))
        playlist["id"] = create_playlist(playlist["name"])

    df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)

    persistent_track_ids = list()
    track_count = 0
    track_count_tot = 0

    for track in tracks_dict:
        track_count_tot += 1
        track_artist_name = track['artists'][0] + " - " + track['name']
        # TODO reformat string
        print(str(round(track_count_tot / len(tracks_dict) * 100,2)) + "% : " + track_artist_name + " : ")
        if not track_artist_name in df_hist_pl_tracks.values:
            # print("Search")
            track_id = search_for_track(track)
            if track_id and not track_in_playlist(playlist["id"], track_id) and not track_id in df_hist_pl_tracks.values:
                persistent_track_ids.append(track_id)
            track_count += 1
            if track_count >= 99: # Have limit of 100 trakcks per import
                print("\n[+] Adding {} new tracks to the playlist: \"{}\"".format(len(persistent_track_ids), persistent_playlist_name))
                add_tracks_to_playlist(playlist["id"], persistent_track_ids)
                df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)
                track_count = 0
                persistent_track_ids = list()
        else: print("Similar track name already found")

    print("\n[+] Adding {} new tracks to the playlist: \"{}\"".format(len(persistent_track_ids), persistent_playlist_name))
    add_tracks_to_playlist(playlist["id"], persistent_track_ids)

    df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)

    return(df_hist_pl_tracks)

def update_hist_from_playlist(title, df_hist_pl_tracks):
    # TODO test, to remove
    persistent_playlist_name = "Beatport: {}".format(title)
    print("[+] Getting hist of tracks for playlist: \"{}\"".format(persistent_playlist_name))

    playlist = {"name": persistent_playlist_name, "id": get_playlist_id(persistent_playlist_name)}

    if playlist["id"]:
        df_hist_pl_tracks = update_hist_pl_tracks(df_hist_pl_tracks, playlist)
        # TODO else pass ?

    return(df_hist_pl_tracks)

# Get authenticated to Spotify on import
sp_oauth = oauth2.SpotifyOAuth(client_id, client_secret, redirect_uri, username=username, scope=scope)
token_info = do_spotify_oauth()
spotify = spotipy.Spotify(auth=token_info["access_token"])
