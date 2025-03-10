"""Data models."""

from pydantic import BaseModel


class BeatportTrack(BaseModel):
    """Represent a track with its associated details."""

    title: str | None = ""
    name: str
    mix: str
    artists: list[str]
    remixers: list[str]
    release: str
    label: str
    published_date: str = ""
    released_date: str = ""
    duration: str
    duration_ms: int
    name_mix: str = ""
