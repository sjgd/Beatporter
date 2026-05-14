"""YouTube / YouTube Music Playlist Archiver + Deleter.

Features
--------
- Filters playlists by creation date.
- Supports dry-run mode (default).
- Allows actual deletion with the --delete flag.
- Syncs playlist tracks with an isolated local history (Parquet).

Requirements
------------
- google-api-python-client
- google-auth-oauthlib
- pandas

Google Cloud Setup
------------------
1. Go to the [Google Cloud Console](https://console.cloud.google.com/).
2. Create a project and enable the "YouTube Data API v3".
3. Create an "OAuth 2.0 Client ID" of type **Web application**.
4. Add `http://localhost:65000/` to the **Authorized redirect URIs**.
5. Download the JSON credentials and save them as `data/client_secret.json`.

Usage
-----
Dry run (safe, syncs history):
    python src/youtube_music.py --include-private

Actual deletion:
    python src/youtube_music.py --include-private --delete

Delete playlists created before a specific date:
    python src/youtube_music.py --include-private --before 2023-01-01 --delete
"""

import argparse
import gc
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import Resource, build

from src.config import ROOT_PATH
from src.configure_logging import configure_logging
from src.utils import (
    FILE_NAME_YT_HIST,
    PATH_HIST_LOCAL,
    append_to_hist_file,
    deduplicate_hist_file,
    load_hist_file,
)

logger = logging.getLogger("youtube_music")

SCOPES = ["https://www.googleapis.com/auth/youtube"]


def authenticate() -> Resource:
    """Authenticate with the YouTube API using OAuth 2.0.

    This function handles the OAuth 2.0 flow, including loading existing tokens,
    refreshing expired tokens, and initiating a local server for the initial
    authorization if necessary.

    Returns:
        Resource: An authenticated YouTube API resource object.

    Raises:
        FileNotFoundError: If the `client_secret.json` file is missing.
    """
    data_dir = Path(ROOT_PATH) / "data"
    client_secret_path = data_dir / "client_secret.json"
    token_path = data_dir / "youtube_token.json"

    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not client_secret_path.exists():
                logger.error(f"Please provide {client_secret_path}")
                raise FileNotFoundError(f"Missing {client_secret_path}")

            flow = InstalledAppFlow.from_client_secrets_file(
                str(client_secret_path), SCOPES
            )
            creds = flow.run_local_server(port=65000)

        with open(token_path, "w") as token:
            token.write(creds.to_json())

    return build("youtube", "v3", credentials=creds)


def get_all_playlists(youtube: Resource) -> list[dict[str, Any]]:
    """Fetch all playlists belonging to the authenticated user.

    Iterates through all pages of the user's playlists using the YouTube Data API.

    Args:
        youtube (Resource): The authenticated YouTube API resource object.

    Returns:
        list[dict[str, Any]]: A list of playlist resource dictionaries.
    """
    playlists: list[dict[str, Any]] = []

    request = youtube.playlists().list(
        part="snippet,status",
        mine=True,
        maxResults=50,
    )

    while request:
        response = request.execute()
        playlists.extend(response.get("items", []))
        request = youtube.playlists().list_next(request, response)

    return playlists


def get_playlist_tracks(youtube: Resource, playlist_id: str) -> list[dict[str, Any]]:
    """Fetch all tracks in a given YouTube playlist.

    Args:
        youtube (Resource): The authenticated YouTube API resource object.
        playlist_id (str): The ID of the playlist to fetch tracks from.

    Returns:
        list[dict[str, Any]]: A list of playlistItem resource dictionaries.
    """
    tracks: list[dict[str, Any]] = []

    request = youtube.playlistItems().list(
        part="snippet",
        playlistId=playlist_id,
        maxResults=50,
    )

    while request:
        response = request.execute()
        tracks.extend(response.get("items", []))
        request = youtube.playlistItems().list_next(request, response)

    return tracks


def sync_youtube_playlist_tracks(youtube: Resource, playlist: dict[str, Any]) -> None:
    """Fetch tracks for a playlist and sync them with the isolated YouTube history.

    Args:
        youtube (Resource): The authenticated YouTube API resource object.
        playlist (dict[str, Any]): A dictionary containing 'playlist_id' and 'title'.
    """
    playlist_id = playlist["playlist_id"]
    playlist_name = playlist["title"]

    logger.info(f"\t[+] Syncing tracks for YouTube playlist: {playlist_name}")

    yt_hist_path = PATH_HIST_LOCAL + FILE_NAME_YT_HIST
    df_playlist_hist = load_hist_file(file_path=yt_hist_path, playlist_id=playlist_id)

    youtube_tracks = get_playlist_tracks(youtube, playlist_id)
    if not youtube_tracks:
        logger.info(f"\t\t[-] Playlist {playlist_name} is empty.")
        return

    extracted_data = []
    for item in youtube_tracks:
        snippet = item.get("snippet", {})
        resource_id = snippet.get("resourceId", {})
        track_id = resource_id.get("videoId")

        if not track_id:
            continue

        published_at = snippet.get("publishedAt", "")
        if published_at:
            dt_added = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        else:
            dt_added = datetime.now(UTC)

        extracted_data.append(
            {
                "playlist_id": playlist_id,
                "playlist_name": playlist_name,
                "track_id": track_id,
                "datetime_added": dt_added,
                "track_title": snippet.get("title", "Unknown"),
                "channel_title": snippet.get("videoOwnerChannelTitle", "Unknown"),
            }
        )

    if not extracted_data:
        return

    df_result = pd.DataFrame(extracted_data)

    # Deduplicate within the current fetch to avoid adding the same track twice
    # if it appears multiple times in the same YouTube playlist
    df_result.drop_duplicates(subset=["track_id"], keep="first", inplace=True)

    new_tracks = df_result[~df_result["track_id"].isin(df_playlist_hist["track_id"])]

    if not new_tracks.empty:
        logger.info(f"\t\t[+] Adding {len(new_tracks)} new tracks to isolated history")
        append_to_hist_file(new_tracks, file_name=FILE_NAME_YT_HIST)
    else:
        logger.info("\t\t[-] No new tracks to add to history.")

    del df_playlist_hist, df_result, new_tracks, extracted_data, youtube_tracks
    gc.collect()


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the script.

    Returns:
        argparse.Namespace: An object containing the parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="YouTube Music Playlist Archiver + Deleter"
    )

    parser.add_argument(
        "--before",
        type=str,
        default=None,
        help="Delete playlists created before YYYY-MM-DD",
    )

    parser.add_argument(
        "--delete",
        action="store_true",
        help="Actually perform the deletion of matched playlists",
    )

    parser.add_argument(
        "--include-private",
        action="store_true",
        help="Include private playlists in the match and deletion process",
    )

    return parser.parse_args()


def playlist_to_row(p: dict[str, Any]) -> dict[str, Any]:
    """Transform a raw YouTube API playlist object into a simplified dictionary.

    Args:
        p (dict[str, Any]): A raw playlist item dictionary from the YouTube API.

    Returns:
        dict[str, Any]: A dictionary containing the formatted playlist metadata.
    """
    snippet = p["snippet"]

    created = datetime.fromisoformat(snippet["publishedAt"].replace("Z", "+00:00"))

    playlist_id = p["id"]

    return {
        "playlist_id": playlist_id,
        "title": snippet["title"],
        "created_at": created.isoformat(),
        "privacy": p["status"]["privacyStatus"],
        "url": f"https://music.youtube.com/playlist?list={playlist_id}",
    }


def filter_playlists(df: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    """Apply filters to the DataFrame of playlists based on user arguments.

    Args:
        df (pd.DataFrame): The DataFrame containing all fetched playlists.
        args (argparse.Namespace): The parsed command-line arguments.

    Returns:
        pd.DataFrame: A filtered and sorted DataFrame.
    """
    if df.empty:
        return df

    # Filter private playlists if desired
    if not args.include_private:
        df = df[df["privacy"] != "private"]

    # Filter by creation date
    if args.before:
        cutoff = datetime.strptime(args.before, "%Y-%m-%d").replace(tzinfo=UTC)
        df["created_at_dt"] = pd.to_datetime(df["created_at"])
        df = df[df["created_at_dt"] < cutoff]

    return df.sort_values("created_at")


def main() -> None:
    """Execute the main lifecycle of the YouTube Music archiver/deleter.

    Includes configuration, authentication, fetching, filtering,
    history syncing, and the optional deletion process.
    """
    configure_logging()
    args = parse_args()

    try:
        youtube = authenticate()
    except FileNotFoundError as e:
        logger.error(e)
        return

    logger.info("Fetching playlists...")
    playlists = get_all_playlists(youtube)

    rows = [playlist_to_row(p) for p in playlists]
    df_all_playlists = pd.DataFrame(rows)

    df_filtered = filter_playlists(df_all_playlists, args)

    if df_filtered.empty:
        logger.info("No matched playlists found.")
        return

    logger.info("Syncing tracks to isolated YouTube history...")
    for _, row in df_filtered.iterrows():
        sync_youtube_playlist_tracks(youtube, row.to_dict())
        logger.info("")

    deduplicate_hist_file(file_name=FILE_NAME_YT_HIST)

    logger.info("")
    logger.info("Playlists matched:")
    logger.info("")

    for _, row in df_filtered.iterrows():
        logger.info(f"- {row['created_at'][:10]} | {row['title']} | {row['playlist_id']}")

    logger.info("")

    if not args.delete:
        logger.info("DRY RUN ONLY")
        logger.info("No playlists deleted.")
        logger.info("")
        logger.info("To actually delete:")
        logger.info("")
        logger.info(
            f"python src/{Path(__file__).name} "
            f"--include-private "
            f"--before {args.before or 'YYYY-MM-DD'} "
            f"--delete"
        )
        return

    confirm = input("\nType DELETE to permanently remove these playlists: ")
    if confirm != "DELETE":
        logger.info("Cancelled.")
        return

    logger.info("")
    logger.info("Deleting playlists...")
    logger.info("")

    for _, row in df_filtered.iterrows():
        playlist_id = row["playlist_id"]
        try:
            youtube.playlists().delete(id=playlist_id).execute()
            logger.info(f"Deleted: {row['title']}")
        except Exception as e:
            logger.error(f"Failed: {row['title']}")
            logger.error(e)

    logger.info("")
    logger.info("Done.")


if __name__ == "__main__":
    main()
