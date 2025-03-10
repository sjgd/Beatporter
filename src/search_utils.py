"""Search utils module."""

import re


def clean_track_name(track_name: str) -> str:
    """Clean a track name by removing various unwanted parts.

    Args:
        track_name (str): The track name string.

    Returns:
        str: The cleaned track name string.

    """
    # Remove feat info (including variations in casing and parentheses)
    track_name = re.sub(
        r"\s*-\s*?(?:Feat|feat|Ft|ft)\.\s+[\w\s&]+\)?",
        "",
        track_name,
        flags=re.IGNORECASE,
    )
    # Remove feat info (including variations in casing and parentheses)
    track_name = re.sub(
        r"\s*\(?(?:Feat|feat|Ft|ft)\.\s+[\w\s&]+\)?", "", track_name, flags=re.IGNORECASE
    )
    # Remove feat info (after -)
    track_name = re.sub(
        r"\s*-\s*(?:Feat|feat|Ft|ft)\.\s+[\w\s&]+", "", track_name, flags=re.IGNORECASE
    )
    # Remove " - Radio Edit" (case-insensitive)
    track_name = re.sub(r"\s*-\s*Radio Edit", "", track_name, flags=re.IGNORECASE)
    # Remove "Radio Edit" (case-insensitive)
    track_name = re.sub(r"\s*\(?Radio Edit\)?", "", track_name, flags=re.IGNORECASE)
    # Remove " - Extended Mix" (case-insensitive)
    track_name = re.sub(r"\s*-\s*Extended Mix", "", track_name, flags=re.IGNORECASE)
    # Remove "(Extended Mix)" (case-insensitive)
    track_name = re.sub(r"\s*\(?Extended Mix\)?", "", track_name, flags=re.IGNORECASE)
    # Remove "- Original Mix" (case-insensitive)
    track_name = re.sub(
        r"\s*-?\s*[Oo]riginal [Mm]ix", "", track_name, flags=re.IGNORECASE
    )
    # Remove "Extended Vox Mix" (case-insensitive)
    track_name = re.sub(r"\s*\(?Extended Vox Mix\)?", "", track_name, flags=re.IGNORECASE)
    # Remove "- Extended" (case-insensitive)
    track_name = re.sub(r"\s*-?\s*[Ee]xtended", "", track_name, flags=re.IGNORECASE)
    # Remove double spaces
    track_name = re.sub(r"\s+", " ", track_name)
    # Remove leading/trailing spaces
    track_name = track_name.strip()

    return track_name
