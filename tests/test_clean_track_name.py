"""Test clean track name function."""

import logging
import re

import pytest

# RUN in debug for logs output in debug console
# Or logs are in the Output / Python Test Log

logger = logging.getLogger(__name__)


def clean_track_name(track_name: str) -> str:
    """Clean a track name by removing various unwanted parts.

    Args:
        track_name (str): The track name string.

    Returns:
        str: The cleaned track name string.

    """
    # Remove feat info (including variations in casing and parentheses)
    track_name = re.sub(r"\s*\(?(?:Feat|feat|Ft|ft)\.\s+[\w\s&]+\)?", "", track_name)

    # Remove "Radio Edit" (case-insensitive)
    track_name = re.sub(r"\s*\(?Radio Edit\)?", "", track_name, flags=re.IGNORECASE)

    # Remove "(Extended Mix)"
    track_name = re.sub(r"\s*\(?Extended Mix\)?", "", track_name, flags=re.IGNORECASE)

    # Remove "- Original Mix"
    track_name = re.sub(r"\s*-?\s*[Oo]riginal [Mm]ix", "", track_name)

    # Remove "Extended Vox Mix"
    track_name = re.sub(r"\s*\(?Extended Vox Mix\)?", "", track_name, flags=re.IGNORECASE)

    # Remove "- Extended"
    track_name = re.sub(r"\s*-?\s*[Ee]xtended", "", track_name)

    # Remove double spaces
    track_name = re.sub(r"\s+", " ", track_name)

    # Remove leading/trailing spaces
    track_name = track_name.strip()

    return track_name


# Example usage with various test cases
@pytest.mark.parametrize(
    "input_track_name,expected_cleaned_name",
    [
        ("Meduza - Freak (feat. Aya Anne)", "Meduza - Freak"),
        ("Track Name Radio Edit", "Track Name"),
        ("Another Track (Extended Mix)", "Another Track"),
        ("My Song - Original Mix", "My Song"),
        ("Some Track Extended Vox Mix", "Some Track"),
        ("Another Song (feat. bbyclose) - feat. bbyclose", "Another Song"),
        ("Another Song feat. bbyclose - feat. bbyclose", "Another Song"),
        ("Another Song - Extended", "Another Song"),
        ("Song   with    extra   spaces", "Song with extra spaces"),
        (
            "  Song with leading and trailing spaces   ",
            "Song with leading and trailing spaces",
        ),
        ("Track With No change", "Track With No change"),
        ("Track (feat. Artist) (Radio Edit)", "Track"),
        ("Track (feat. Artist)", "Track"),
        ("Track ft. Artist", "Track"),
        ("Track Ft. Artist", "Track"),
        ("Track Feat. Artist", "Track"),
        ("Track feat. Artist", "Track"),
        ("Track - Extended Mix", "Track"),
        ("Track (Extended Mix)", "Track"),
        ("Track - Original Mix", "Track"),
        ("Track Original Mix", "Track"),
        ("Track extended vox mix", "Track"),
        ("Track EXTENDED vox mix", "Track"),
        ("Track - radio edit", "Track"),
        ("Track (Radio edit)", "Track"),
    ],
)
def test_clean_track_names(input_track_name: str, expected_cleaned_name: str) -> None:
    """Test the clean_track_name function with various inputs.

    Args:
        input_track_name: The track name to clean.
        expected_cleaned_name: The expected cleaned track name.

    Returns:
        None.

    """
    cleaned_name = clean_track_name(input_track_name)
    assert cleaned_name == expected_cleaned_name
