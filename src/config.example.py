# Copy this to config.py and update necessary values

# Spotify Username
username = "CHANGE_ME"

# Spotify Authentication Scopes
scope = "playlist-read-private playlist-modify-private playlist-modify-public"

# Spotify Application Details
client_id = "CHANGE_ME"
client_secret = "CHANGE_ME"
redirect_uri = "http://localhost:65000"

# Folder path where to save the history
folder_path = "./"

# Root path
root_path = "../"

# Add at top of playlist, if True will add at top, otherwise append
add_at_top_playlist = True

# Daily mode
daily_mode = True
daily_n_track = 15

# Refresh token every n track  to prevent timeout
refresh_token_n_tracks = 100

# Shuffle playlists
shuffle_label = False

# Digging mode
# Allows to avoid adding tracks that have been previously added,
# tracks listened and deleted will not be added again
# Match is done on artist - track name, not on spotify track ID
#   "" to do not skip tracks with similar artist - track name in playlists
#   "playlist" to skip tracks that have been already added to this playlist only
#   "all" to skip tracks that have been added to any user's playlists
digging_mode = "playlist"

# Overwrite labels
# If set to false,
# it will stop once reached the date of the last corresponding playlist update
overwrite_label = True

# Silent search
# If set to true will avoid displaying search information
silent_search = True

# Parse track
# If set to true will remove feat artist2 and original mix to improve the search
parse_track = True

# Playlist prefix
playlist_prefix = "Beatport: "

# Playlist description
playlist_description = "Created using github.com/sjgd/Beatporter."

# Genres on Beatport ("Arbitrary name": "URL path for genre")
genres = {
    "All Genres": "",
    "Afro House": "afro-house/89",
    "Bass/Club": "bass-club/85",
    "Bass House": "bass-house/91",
    "Big Room": "big-room/79",
    "Breaks": "breaks/9",
    "DJ Tools": "dj-tools/16",
    "Dance / Electro Pop": "dance/39",
    "Deep House": "deep-house/12",
    "Drum & Bass": "drum-and-bass/1",
    "Dubstep": "dubstep/18",
    "Electro House": "electro-house/17",
    "Electronica / Downtempo": "electronica-downtempo/3",
    "Funky / Groove / Jackin' House": "funky-groove-jackin-house/81",
    "Future House": "future-house/65",
    "Garage / Bassline / Grime": "garage-bassline-grime/86",
    "Hard Dance / Hardcore": "hard-dance-hardcore/8",
    "Hardcore / Hard Techno": "hardcore-hard-techno/2",
    "Hip-Hop & R&B": "hip-hop-r-and-b/38",
    "House": "house/5",
    "Indie Dance / Nu Disco": "indie-dance-nu-disco/37",
    "Leftfield Bass": "leftfield-bass/85",
    # "Leftfield House & Techno": "leftfield-house-and-techno/80", # Removed by Beatport
    "Melodic House & Techno": "melodic-house-and-techno/90",
    "Minimal / Deep Tech": "minimal-deep-tech/14",
    "Nu Disco / Disco": "nu-disco-disco/50",
    "Organic House / Downtempo": "organic-house-downtempo/93",
    "Progressive House": "progressive-house/15",
    "Psy Trance": "psy-trance/13",
    # "Reggae / Dancehall / Dub": "reggae-dancehall-dub/41", # Removed by Beatport
    "Tech House": "tech-house/11",
    "Techno (Peak Time / Driving / Hard)": "techno-peak-time-driving-hard/6",
    "Techno (Raw / Deep / Hypnotic)": "techno-raw-deep-hypnotic/92",
    "Trance": "trance/7",
    # "Trap / Future Bass": "trap-future-bass/87",  # Removed by Beatport
    "Trap / Hip-Hop / R&B": "trap-hip-hop-rb/38",
    "UK Garage / Bassline:": "uk-garage-bassline/86",
}

# Charts on Beatport ("Arbitrary name": "URL path for genre, without chart ID")
charts = {"Kalambo Bontan": "kalambo", "Weekend Picks %U (%Y)": "weekend-picks-%U"}

# Labels on Beatport ("Arbitrary name": "URL path for genre, with chart ID")
labels = {"8Bit Releases": "8bit/3248"}

# Spotify backup, save some spotify playlist to a new name,
# using same logic for digging mode
spotify_bkp = {
    "BKP Discover Weekly": "ORIGINAL_PLAYLIST_ID",
}
