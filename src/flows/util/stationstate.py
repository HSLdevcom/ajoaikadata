from typing import Any, Tuple, TypedDict
from datetime import datetime
from .eventstate import Event

class StationStateCache(TypedDict, total=False):
    station: str | None
    track: int | None
    time_arrived: datetime | None
    time_doors_last_closed: datetime | None
    time_departed: datetime | None


class StationEvent(TypedDict):
    vehicle: Any
    eke_timestamp: Any
    event_type: str
    state: StationStateCache


def create_station_event(data: Event, state: StationStateCache) -> StationEvent:
    return {
        "vehicle": data["vehicle"],
        "eke_timestamp": data["eke_timestamp"],
        "event_type": "station",
        "state": state,
    }


def create_empty_stationstate_cache() -> StationStateCache:
    """Create a new cache."""
    return {
        "station": None,
        "track": None,
        "time_arrived": None,
        "time_doors_last_closed": None,
        "time_departed": None,
    }


def update_stationstate(
    last_state: StationStateCache,
    new_fields: StationStateCache
) -> StationStateCache:
    return last_state | new_fields

