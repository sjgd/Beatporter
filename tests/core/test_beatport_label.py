"""Unit tests for Beatport label scraping and track parsing."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.beatport import (
    _extract_label_tracks_data,
    _format_duration_ms,
    _get_label_max_pages,
    get_label_tracks,
    parse_tracks,
)


def test_format_duration_ms() -> None:
    """Verify millisecond duration formatting."""
    assert _format_duration_ms(296381) == "4:56"
    assert _format_duration_ms(65000) == "1:05"
    assert _format_duration_ms(0) == "0:00"


def test_parse_tracks_label_and_genre_schemas() -> None:
    """Verify parse_tracks handles both new label schema and genre/chart schema."""
    raw_tracks = [
        {
            "name": "Deeper Love",
            "mix_name": "Extended Mix",
            "artists": [{"name": "Solardo"}],
            "remixers": None,
            "release": {"name": "Deeper Love EP", "label": {"name": "Sola"}},
            "new_release_date": "2026-12-04",
            "length_ms": 296381,
            "genre": None,
            "bpm": 130,
            "key": None,
        },
        {
            "name": "Classic Track",
            "mix_name": None,
            "artists": [{"name": "Artist A"}],
            "remixers": [{"name": "Remixer B"}],
            "release": {"name": "Classic EP", "label": {"name": "Defected"}},
            "publish_date": "2026-10-01",
            "new_release_date": "2026-10-02",
            "length": "6:10",
            "length_ms": 370000,
            "genre": {"name": "House"},
            "bpm": 126,
            "key": {"name": "A Minor"},
        },
    ]

    tracks = parse_tracks(raw_tracks)
    assert len(tracks) == 2

    assert tracks[0].name == "Deeper Love"
    assert tracks[0].mix == "Extended Mix"
    assert tracks[0].artists == ["Solardo"]
    assert tracks[0].remixers == []
    assert tracks[0].published_date == "2026-12-04"
    assert tracks[0].duration == "4:56"
    assert tracks[0].duration_ms == 296381

    assert tracks[1].mix == ""
    assert tracks[1].remixers == ["Remixer B"]
    assert tracks[1].published_date == "2026-10-01"
    assert tracks[1].released_date == "2026-10-02"
    assert tracks[1].duration == "6:10"


def test_get_label_max_pages() -> None:
    """Verify max page calculation for count-based and legacy page-string payloads."""
    assert _get_label_max_pages({"page": "1/42"}, per_page=50) == 42
    assert _get_label_max_pages({"count": 2076, "results": [{}] * 50}, per_page=50) == 42
    assert _get_label_max_pages({"count": 2076, "results": [{}] * 25}, per_page=50) == 84
    assert _get_label_max_pages({"count": 12, "results": [{}] * 12}, per_page=50) == 1
    assert _get_label_max_pages({"count": 0, "results": []}, per_page=50) == 0

    with pytest.raises(KeyError):
        _get_label_max_pages({"results": []})


def test_extract_label_tracks_data() -> None:
    """Verify extracting the tracks query regardless of query index."""
    queries = [
        {"queryKey": ["label-63883"], "state": {"data": {"id": 63883}}},
        {
            "queryKey": ["tracks", {"page": 1, "page_size": 50}],
            "state": {"data": {"count": 2, "results": [{"id": 1}, {"id": 2}]}},
        },
    ]
    data = _extract_label_tracks_data(queries)
    assert data["count"] == 2
    assert len(data["results"]) == 2


@patch("src.beatport.load_hist_file")
@patch("src.beatport.update_hist_pl_tracks")
@patch("src.beatport.find_playlist_chart_label")
@patch("src.beatport.get_beatport_page_script_queries")
def test_get_label_tracks_stops_at_last_update(
    mock_queries: MagicMock,
    mock_find_pl: MagicMock,
    mock_update_hist: MagicMock,
    mock_load_hist: MagicMock,
) -> None:
    """Verify get_label_tracks paginates with count and stops at last_update."""
    mock_find_pl.return_value = {"id": "pl_123"}
    mock_load_hist.return_value = pd.DataFrame(
        {"datetime_added": [datetime(2026, 11, 10)]}
    )

    page1 = [
        {"queryKey": ["label-63883"], "state": {"data": {"id": 63883}}},
        {
            "queryKey": ["tracks", {"page": 1}],
            "state": {
                "data": {
                    "count": 100,
                    "results": [
                        {
                            "name": "New Track",
                            "mix_name": "Extended Mix",
                            "artists": [{"name": "Solardo"}],
                            "remixers": [],
                            "release": {"name": "EP 1", "label": {"name": "Sola"}},
                            "new_release_date": "2026-11-15",
                            "length_ms": 240000,
                        },
                        {
                            "name": "Old Track",
                            "mix_name": "Original Mix",
                            "artists": [{"name": "Solardo"}],
                            "remixers": [],
                            "release": {"name": "EP 0", "label": {"name": "Sola"}},
                            "new_release_date": "2026-11-01",
                            "length_ms": 210000,
                        },
                    ],
                }
            },
        },
    ]
    mock_queries.return_value = page1

    tracks = get_label_tracks("Sola Releases", "sola/63883", overwrite=False, silent=True)
    assert len(tracks) == 2
    assert [t.name for t in tracks] == ["Old Track", "New Track"]
    mock_update_hist.assert_called_once()
