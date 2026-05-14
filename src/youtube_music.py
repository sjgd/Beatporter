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
- google-auth
- requests
- pandas

Google Cloud Setup
------------------
1. Go to:
   https://console.cloud.google.com/

2. Create a project

3. Enable:
   YouTube Data API v3

4. Ensure you have Application Default Credentials set up:
   gcloud auth application-default login --scopes="https://www.googleapis.com/auth/youtube"

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

import google.auth
import pandas as pd
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

from src.configure_logging import configure_logging
from src.utils import ROOT_PATH

logger = logging.getLogger("youtube_music")

SCOPES = ["https://www.googleapis.com/auth/youtube"]


def authenticate() -> AuthorizedSession:
    """Authenticate with YouTube API using service account.

    Returns:
        AuthorizedSession: Authenticated session object.
    """
    sa_path = Path(ROOT_PATH) / "data" / "beatporter-sa.json"
    creds = service_account.Credentials.from_service_account_file(
        str(sa_path), scopes=SCOPES
    )
    return AuthorizedSession(creds)


def get_all_playlists(session: AuthorizedSession) -> list[dict[str, Any]]:
    """Fetch all playlists for the authenticated user.

    Args:
        session (AuthorizedSession): Authenticated session object.

    Returns:
        list[dict[str, Any]]: List of playlist items.
    """
    playlists: list[dict[str, Any]] = []
    url = "https://www.googleapis.com/youtube/v3/playlists"
    params: dict[str, Any] = {
        "part": "snippet,status",
        "mine": "true",
        "maxResults": 50,
    }

    while True:
        response = session.get(url, params=params)
        if response.status_code != 200:
            logger.error(
                f"Error fetching playlists: {response.status_code} - {response.text}"
            )
            response.raise_for_status()
        data = response.json()

        playlists.extend(data.get("items", []))

        page_token = data.get("nextPageToken")
        if not page_token:
            break
        params["pageToken"] = page_token

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


def main() -> None:
    """Execute the main logic of the script."""
    configure_logging()
    args = parse_args()

    session = authenticate()

    logger.info("Fetching playlists...")
    playlists = get_all_playlists(session)

    rows = [playlist_to_row(p) for p in playlists]

    df = pd.DataFrame(rows)

    if df.empty:
        logger.info("No playlists found.")
        return

    # Filter private playlists if desired
    if not args.include_private:
        df = df[df["privacy"] != "private"]

    # Filter by creation date
    if args.before:
        cutoff = datetime.strptime(args.before, "%Y-%m-%d").replace(tzinfo=UTC)

        df["created_at_dt"] = pd.to_datetime(df["created_at"])

        df = df[df["created_at_dt"] < cutoff]

    df = df.sort_values("created_at")

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

    logger.info("")
    logger.info(f"Exported CSV: {csv_file}")
    logger.info(f"Matched playlists: {len(df)}")
    logger.info("")

    if len(df) == 0:
        logger.info("Nothing to delete.")
        return

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

    delete_url = "https://www.googleapis.com/youtube/v3/playlists"
    for _, row in df.iterrows():
        playlist_id = row["playlist_id"]

        try:
            resp = session.delete(delete_url, params={"id": playlist_id})
            resp.raise_for_status()

            logger.info(f"Deleted: {row['title']}")

        except Exception as e:
            logger.error(f"Failed: {row['title']}")
            logger.error(e)

    logger.info("")
    logger.info("Done.")


if __name__ == "__main__":
    main()
