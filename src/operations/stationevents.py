"""
Operations related to create station events.
"""

from typing import Any, Tuple, TypedDict
from datetime import datetime

from .events import Event
from ..util.ajoaikadatamsg import AjoaikadataMsg, create_empty_msg

from ..util.config import logger

class StationStateCache(TypedDict):
    station: str | None
    track: int | None
    time_arrived: datetime | None
    time_doors_last_closed: datetime | None
    time_departed: datetime | None


class StationEvent(TypedDict):
    vehicle: Any
    ntp_timestamp: Any
    event_type: str
    data: StationStateCache


def _create_event(data: Event, state: StationStateCache) -> StationEvent | None:
    if not state["station"] or not state["track"]:
        return None

    return {
        "vehicle": data["vehicle"],
        "ntp_timestamp": data["ntp_timestamp"],
        "event_type": "station",
        "data": state,
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


def create_station_events(
    last_state: StationStateCache, value: AjoaikadataMsg
) -> Tuple[StationStateCache, AjoaikadataMsg]:
    data: Event = value["data"]

    station_event_to_send = None

    match data["event_type"]:
        case "arrival":
            # Init station and track. Send event if it wasn't released yet.
            if any(last_state.values()):
                station_event_to_send = _create_event(data, last_state)
                last_state = create_empty_stationstate_cache()

            last_state["station"] = data["data"]["station"]
            last_state["track"] = data["data"]["track"]

        case "stopped":
            # Update arrival time
            if not last_state.get("time_arrived"):
                last_state["time_arrived"] = data["ntp_timestamp"]

        case "doors_opened":
            # Does nothing at the moment
            pass

        case "doors_closed":
            # Update last closed time
            last_state["time_doors_last_closed"] = data["ntp_timestamp"]

        case "moving":
            # Update departure time
            last_state["time_departed"] = data["ntp_timestamp"]

        case "departure":
            # release the event
            station_event_to_send = _create_event(data, last_state)
            last_state = create_empty_stationstate_cache()

        case _:
            logger.warning("Unknown event type for station event.")

    if station_event_to_send:
        msg_out: AjoaikadataMsg = {"msgs": value.get("msgs", []), "data": station_event_to_send}
    else:
        msg_out = create_empty_msg()
    return last_state, msg_out
