# Beatporter

This tool scrapes Beatport charts and attempts to locate the tracks on Spotify and add them to playlists.
It can scrape:
* Top 100 charts, by genre
* Label tracks
* Beatport chart by url

There is also support for the overall, genre-agnostic, Top 100 chart.

Digging mode allows to save the history of track previously added (and removed) to a Spotify to avoid adding them again. (see config file for details)

An additional feature have been added to allow to backup Spotify playlist to a new playlist (e.g. backup your Discover Weekly) 

## Prerequisites

* Python 3
* Spotify Account
* Create / register an app on the [Spotify Developer](https://developer.spotify.com) site
    * Add the `redirect_uri` value to the _Redirect URIs_ within the app dashboard.  The value added to the dashboard must match the variable exactly. 

## Dev in progress [Beta]

* Adding chart and label scrapping
* Store history of track_id / playlist_id to avoid re-adding tracks that already have been added, see config.example.py for examples
* Add additional feature to backup spotify playlists

### Installation

Run `poetry install`

## Configuration
* Copy config.example.py to config.py and update the following values:
    * `username` is your Spotify username
    * `client_id` and `client_secret` are values provided by Spotify for your registered app (on the [Spotify Developer](https://developer.spotify.com) site under the app dashboard).
    * `redirect_uri` this is the local listener that is used to intercept the Spotify authentication callback.
        * As described above, the `redirect_uri` needs to be registered verbatim on the [Spotify Developer](https://developer.spotify.com) site.  Go to the app's dashboard and click _Edit Settings_.  Then under _Redirect URIs_ paste the value of `redirect_uri` and click _add_.  Finally, scroll to the bottom of the _Edit Settings_ window and click _Save_.
    * `genres` is a dictionary that maps genre names (which are arbitrary and will be used in playlist names) to Beatport URL components.
        * The presence of a genre in the dictionary toggles it on (its absence toggles it off)
            * Simply comment out (or remove) the line for a genre to stop it from syncing  
        * The URL component comes from the way Beatport structures their genre chart URLs.
            * For example, the Top 100 chart for Trance is at the URL https://www.beatport.com/genre/trance/7/top-100, so the URL component in the dictionary is `trance/7`
        * All known genres and their URL component mapping are included in the example config for easy reference.
            * If you know of other (hidden) genre URLs, please share in an issue or PR.

## Authentication
This application will never see your Spotify credentials.  Instead upon successful authentication and approval,  Spotify will provide this application with an API token that can be used to read and modify your library and playlists.

### Initial Authentication & Application Approval
The first time you run the application your web browser should open to the Spotify authentication page.  Upon successful authentication, you will be prompted to approve this application with the permission scope defined in the `config.scope` variable.  If approved, Spotify will "callback" to the application and provide the necessary API tokens.

After authenticating, your browser may display something like "This page isn’t working localhost didn’t send any data."  This is okay, simply close the window and you should see the application working.

### Token Refresh
The API token provided by Spotify is short lived, however, a refresh token is aso provided during authentication.  This refresh token can be used to retrieve a new API token (and a new refresh token) in the future.  These tokens are persisted to disk at `token.json` and `.cache-USERNAME`.

### Token Security
The `token.json` and `.cache-USERNAME` files should be protected from unauthorized access as the tokens contained within can be used to read your Spotify library and modify playlists.  If you are paranoid, you can simply remove these files manually after running the application.  You will simply need to go through the Spotify webpage authentication cycle again instead of having transparent auth via refresh.  If your authentication is cached in your browser this will happen automatically.  You will also not need to re-approve the app (unless the scope in the `config.scope` variable has changed). 

Your Spotify credentials are _not_ stored in these files.  The tokens in these files cannot be used to gain additional permissions.  Token permission scope is bound to whatever was in the `config.scope` variable at the time of initial application approval.

## Running

To run the application:
 
```make beatporter```

## Future Features / ToDo
This project was born out of a personal desire for this functionality.  As such, my original design decisions were not to support a community release.   There is a lot that can be done to clean this up if there is any outside interest.

* Implement other charts as sources for playlists
    * Top 10 Genre (higher quality than Top 100)
* Support playlists that track only the current chart status
    * Instead of adding new tracks to an ever growing playlist, the playlists would exactly match the most current chart and have a song cap (e.g. 100 songs on a Top 100 playlist)
    * This will provide shorter, but more current playlists
 * Implement a real HTTP listener that can return an actual HTTP response with a pretty HTML page.
    * Currently a hacky TCP socket listener is used.  It's not pretty, but it works.
 * Cleanup token caching
    * Use only `.cache-USERNAME` instead of `token.json`
    * Add option to auto delete on completion
 * Better state tracking (keep track of the tracks already searched and added to greatly reduce search API calls)
 * Implement this as a web app
    * Others could auth and use the app without running it themselves
 * Maintain curated, public Spotify playlists
    * I could simply maintain the playlists myself and make them public for consumption without everyone syncing their own playlists.
