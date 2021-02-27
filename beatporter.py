import spotify
import beatport
from datetime import datetime
from os import path
import pandas as pd
import openpyxl

file_name_hist = 'hist_playlists_tracks.parquet'

def dump_tracks(tracks):
    i = 1
    for track in tracks:
        print("{}: {} ({}) - {} ({})".format(i, track["name"], track["mix"], ", ".join(track["artists"]), track["duration"]))
        i += 1

def update_hist(master_refresh = False):
    # TODO testing, to remove

    if path.exists('hist_playlists_tracks.xlsx'):
        df_hist_pl_tracks = pd.read_excel('hist_playlists_tracks.xlsx')
    else:
        df_hist_pl_tracks = pd.DataFrame(columns=['playlist_id', 'track_id', 'datetime_added', 'artist_name'])

    beatport.charts = {beatport.parse_chart_url_datetime(k): beatport.parse_chart_url_datetime(v) for k, v in beatport.charts.items()}



    for chart, chart_bp_url_code in beatport.charts.items():

        df_hist_pl_tracks = spotify.update_hist_from_playlist(chart, df_hist_pl_tracks)

    for label, label_bp_url_code in beatport.labels.items():
        df_hist_pl_tracks = spotify.update_hist_from_playlist(label, df_hist_pl_tracks)

    if master_refresh:
        # Get track ids from all playlists from username from config
        from config import username
        all_playlists = spotify.get_all_playlists()
        for playlist in all_playlists:
            print(playlist['name'])
            if playlist['owner']['id'] == username:
                print(playlist['name'])
                playlist = {"name": playlist['name'], "id": playlist['id']}
                df_hist_pl_tracks = spotify.update_hist_pl_tracks(df_hist_pl_tracks, playlist)

    df_hist_pl_tracks = df_hist_pl_tracks.loc[:, ['playlist_id', 'track_id', 'datetime_added', 'artist_name']]
    df_hist_pl_tracks.to_pickle(file_name_hist)
    df_hist_pl_tracks.to_excel('hist_playlists_tracks.xlsx', index = False)

if __name__ == "__main__":

    # import os
    # path_cwd = os.getcwd()
    # print(path_cwd)

    start_time = datetime.now()
    print("[!] Starting @ {}\n".format(start_time))

    if path.exists('hist_playlists_tracks.xlsx'):
        df_hist_pl_tracks = pd.read_excel('hist_playlists_tracks.xlsx')
    else:
        df_hist_pl_tracks = pd.DataFrame(columns=['playlist_id', 'track_id', 'datetime_added', 'artist_name'])

    beatport.charts = {beatport.parse_chart_url_datetime(k): beatport.parse_chart_url_datetime(v) for k, v in beatport.charts.items()}

    # if path.exists(file_name_hist):
    #     df_hist_pl_tracks = pd.read_parquet(file_name_hist)
    # else:
    #     df_hist_pl_tracks = pd.DataFrame(columns=['playlist_id', 'track_id', 'datetime_added', 'artist_name'])

    top_100_charts = dict()
    for genre, genre_bp_url_code in beatport.genres.items():
        top_100_charts[genre] = beatport.get_top_100_tracks(genre)

    for genre in top_100_charts:
        print("\n***** {} *****".format(genre))
        dump_tracks(top_100_charts[genre])
        print("\n\n")
        spotify.add_new_tracks_to_playlist(genre, top_100_charts[genre])

    for chart, chart_bp_url_code in beatport.charts.items():

        #TODO handle return None, handle chart_bp_url_code has ID already or not
        print("\n***** {} : {} *****".format(chart, chart_bp_url_code))
        chart_url = beatport.find_chart(chart_bp_url_code)

        if chart_url:
            tracks_dict = beatport.get_chart(beatport.find_chart(chart_bp_url_code))
            print("Found {} tracks for {}".format(len(tracks_dict), chart))
        else:
            print("Chart not found")

        df_hist_pl_tracks = spotify.add_new_tracks_to_playlist_chart_label(chart, tracks_dict, df_hist_pl_tracks)

    for label, label_bp_url_code in beatport.labels.items():
        # TODO avoid looping through all pages if already parsed before
        # TODO Add tracks per EP rather than track by track
        tracks_dict = beatport.get_label_tracks(label, label_bp_url_code, df_hist_pl_tracks)
        print("Found {} tracks for {}".format(len(tracks_dict), label))
        df_hist_pl_tracks = spotify.add_new_tracks_to_playlist_chart_label(label, tracks_dict, df_hist_pl_tracks)

    df_hist_pl_tracks = df_hist_pl_tracks.loc[:, ['playlist_id', 'track_id', 'datetime_added', 'artist_name']]
    df_hist_pl_tracks.to_pickle(file_name_hist)
    df_hist_pl_tracks.to_excel('hist_playlists_tracks.xlsx', index = False)
    end_time = datetime.now()
    print("\n[!] Done @ {}\n (Ran for: {})".format(end_time, end_time - start_time))

# TODO fix export, seem to add index col
# TODO fix match artist name, remove original
# TODO add function add anyway, if not in playlist, if not in all
# TODO check error on pickle
# TODO review imports
# TODO modify read me, add new packages
