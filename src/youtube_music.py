"""YouTube / YouTube Music Playlist Archiver + Deleter.

Features
--------
- Exports playlists to CSV before deletion
- Filters playlists by creation date
- Dry-run mode by default
- Optional actual deletion
- Exports:
    - playlist id
    - title
    - creation date
    - playlist URL

Requirements
------------
- google-api-python-client
- google-auth-oauthlib
- pandas

Google Cloud Setup
------------------
1. Go to:
   https://console.cloud.google.com/

2. Create a project and enable "YouTube Data API v3".

3. Create "OAuth 2.0 Client ID" (type: Desktop App).
   Note: If you use "Web application", you MUST add `http://localhost:65000/` to "Authorized redirect URIs".

4. Download the JSON and save it as `data/client_secret.json`.

Usage
-----
DRY RUN (safe):
python src/youtube_music.py

ACTUAL DELETE:
python src/youtube_music.py --delete

Optional date filter:
python src/youtube_music.py --before 2023-01-01

"""

import argparse
import csv
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import Resource, build

from src.configure_logging import configure_logging
from src.utils import ROOT_PATH

logger = logging.getLogger("youtube_music")

SCOPES = ["https://www.googleapis.com/auth/youtube"]


def authenticate() -> Resource:
    """Authenticate with YouTube API using OAuth 2.0.

    Returns:
        Resource: YouTube API resource object.
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
    """Fetch all playlists for the authenticated user.

    Args:
        youtube (Resource): YouTube API resource object.

    Returns:
        list[dict[str, Any]]: List of playlist items.
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


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--before",
        type=str,
        default=None,
        help="Delete playlists created before YYYY-MM-DD",
    )

    parser.add_argument(
        "--delete",
        action="store_true",
        help="Actually delete playlists",
    )

    parser.add_argument(
        "--include-private",
        action="store_true",
        help="Include private playlists",
    )

    return parser.parse_args()


def playlist_to_row(p: dict[str, Any]) -> dict[str, Any]:
    """Convert a playlist item to a dictionary for a DataFrame row.

    Args:
        p (dict[str, Any]): Playlist item from YouTube API.

    Returns:
        dict[str, Any]: Formatted row data.
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
    """Filter playlists based on arguments.

    Args:
        df (pd.DataFrame): DataFrame of playlists.
        args (argparse.Namespace): Command line arguments.

    Returns:
        pd.DataFrame: Filtered DataFrame.
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


def export_to_csv(df: pd.DataFrame) -> str:
    """Export filtered playlists to a CSV file.

    Args:
        df (pd.DataFrame): Filtered DataFrame of playlists.

    Returns:
        str: Name of the exported CSV file.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = f"youtube_playlists_export_{timestamp}.csv"

    export_columns = [
        "playlist_id",
        "title",
        "created_at",
        "privacy",
        "url",
    ]

    df[export_columns].to_csv(
        csv_file,
        index=False,
        quoting=csv.QUOTE_ALL,
    )
    return csv_file


def main() -> None:
    """Execute the main logic of the script."""
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
    df = pd.DataFrame(rows)

    df = filter_playlists(df, args)

    if df.empty:
        logger.info("No matched playlists found.")
        return

    csv_file = export_to_csv(df)

    logger.info("")
    logger.info(f"Exported CSV: {csv_file}")
    logger.info(f"Matched playlists: {len(df)}")
    logger.info("")

    logger.info("Playlists matched:")
    logger.info("")

    for _, row in df.iterrows():
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

    for _, row in df.iterrows():
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
