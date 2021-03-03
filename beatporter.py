import spotify
import beatport
from datetime import datetime
from os import path
import pandas as pd
from config import username, shuffle_playlist, daily_mode, daily_n_track
from time import sleep
import openpyxl

file_name_hist = 'hist_playlists_tracks.parquet'
curr_date = datetime.today().strftime('%Y-%m-%d')

def dump_tracks(tracks):
    i = 1
    for track in tracks:
        print("{}: {} ({}) - {} ({})".format(i, track["name"], track["mix"], ", ".join(track["artists"]), track["duration"]))
        i += 1

def load_hist_file():
    """
    :return: Returns existing history file of track ID per playlist
    """
    # TODO arguments for file path / type ?
    if path.exists('hist_playlists_tracks.xlsx'):
        df_hist_pl_tracks = pd.read_excel('hist_playlists_tracks.xlsx')
    else:
        df_hist_pl_tracks = pd.DataFrame(columns=['playlist_id', 'track_id', 'datetime_added', 'artist_name'])

    return(df_hist_pl_tracks)

def update_hist(master_refresh = False):
    # TODO: testing, to refine usage, include in first init ?

    df_hist_pl_tracks = load_hist_file()

    beatport.charts = {beatport.parse_chart_url_datetime(k): beatport.parse_chart_url_datetime(v) for k, v in beatport.charts.items()}

    for chart, chart_bp_url_code in beatport.charts.items():

        df_hist_pl_tracks = spotify.update_hist_from_playlist(chart, df_hist_pl_tracks)

    for label, label_bp_url_code in beatport.labels.items():
        df_hist_pl_tracks = spotify.update_hist_from_playlist(label, df_hist_pl_tracks)

    if master_refresh:
        # Get track ids from all playlists from username from config
        all_playlists = spotify.get_all_playlists()
        for playlist in all_playlists:
            # print(playlist['name'])
            if playlist['owner']['id'] == username:
                print(playlist['name'])
                playlist = {"name": playlist['name'], "id": playlist['id']}
                df_hist_pl_tracks = spotify.update_hist_pl_tracks(df_hist_pl_tracks, playlist)

    df_hist_pl_tracks = df_hist_pl_tracks.loc[:, ['playlist_id', 'track_id', 'datetime_added', 'artist_name']]
    df_hist_pl_tracks.to_pickle(file_name_hist)
    df_hist_pl_tracks.to_excel('hist_playlists_tracks.xlsx', index = False)

if __name__ == "__main__":

    # Init
    start_time = datetime.now()
    print("[!] Starting @ {}\n".format(start_time))
    df_hist_pl_tracks = load_hist_file()
    beatport.charts = {beatport.parse_chart_url_datetime(k): beatport.parse_chart_url_datetime(v) for k, v in beatport.charts.items()}

    # if path.exists(file_name_hist):
    #     df_hist_pl_tracks = pd.read_parquet(file_name_hist)
    # else:
    #     df_hist_pl_tracks = pd.DataFrame(columns=['playlist_id', 'track_id', 'datetime_added', 'artist_name'])

    # Parse lists
    for genre, genre_bp_url_code in beatport.genres.items():
        print("\n Getting genre : ***** {} *****".format(genre))
        top_100_chart = beatport.get_top_100_tracks(genre)
        df_hist_pl_tracks = spotify.add_new_tracks_to_playlist_genre(genre, top_100_chart, df_hist_pl_tracks)

    for chart, chart_bp_url_code in beatport.charts.items():
        #TODO handle return None, handle chart_bp_url_code has ID already or not
        print("\n Getting chart : ***** {} : {} *****".format(chart, chart_bp_url_code))
        chart_url = beatport.find_chart(chart_bp_url_code)

        if chart_url:
            tracks_dict = beatport.get_chart(beatport.find_chart(chart_bp_url_code))
            print("\t[+] Found {} tracks for {}".format(len(tracks_dict), chart))
        else:
            print("\t[+] Chart not found")

        df_hist_pl_tracks = spotify.add_new_tracks_to_playlist_chart_label(chart, tracks_dict, df_hist_pl_tracks)

    for label, label_bp_url_code in beatport.labels.items():
        # TODO avoid looping through all pages if already parsed before ?
        # TODO Add tracks per EP rather than track by track ?
        print("\n Getting label : ***** {} : {} *****".format(label, label_bp_url_code))
        tracks_dict = beatport.get_label_tracks(label, label_bp_url_code, df_hist_pl_tracks)
        print("Found {} tracks for {}".format(len(tracks_dict), label))
        df_hist_pl_tracks = spotify.add_new_tracks_to_playlist_chart_label(label, tracks_dict, df_hist_pl_tracks)

    for playlist_name, org_playlist_id in beatport.spotify_bkp.items():
        print("\n Backing up to playlist : ***** {} : {} *****".format(playlist_name, org_playlist_id))
        df_hist_pl_tracks = spotify.back_up_spotify_playlist(playlist_name, org_playlist_id, df_hist_pl_tracks)

    # Output
    sleep(5) # try to avoid read-write errors if running too quickly
    df_hist_pl_tracks = df_hist_pl_tracks.loc[:, ['playlist_id', 'track_id', 'datetime_added', 'artist_name']]
    df_hist_pl_tracks.to_pickle(file_name_hist)
    df_hist_pl_tracks.to_excel('hist_playlists_tracks.xlsx', index = False)
    # Save bkp
    df_hist_pl_tracks.to_excel('hist_playlists_tracks_{}.xlsx'.format(curr_date), index=False)
    end_time = datetime.now()
    print("\n[!] Done @ {}\n (Ran for: {})".format(end_time, end_time - start_time))

# TODO fix export, seem to add index col
# TODO fix match artist name, remove original
    # Log could not find track
    # Regex out feat. artist2 remove brackets on (extended mix)
    # Add option not to do regex
    # Check to include original mix then remove
# TODO add config digging mode : add duplicated anyway, if not in playlist, if not in all
# TODO check error on pickle
# TODO review imports
# TODO modify read me, add new packages
