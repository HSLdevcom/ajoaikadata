"""
Operations related to create station events.
"""

from typing import Any, Tuple, TypedDict
from datetime import datetime

from .events import Event
from ..util.ajoaikadatamsg import AjoaikadataMsg, create_empty_msg


class StationStateCache(TypedDict, total=False):
    station: str | None
    track: int | None
    time_arrived: datetime | None
    time_doors_last_closed: datetime | None
    time_departed: datetime | None


class StationEvent(TypedDict):
    vehicle: Any
    ntp_timestamp: Any
    event_type: str
    state: StationStateCache


def create_station_event(data: Event, state: StationStateCache) -> StationEvent:
    return {
        "vehicle": data["vehicle"],
        "ntp_timestamp": data["ntp_timestamp"],
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


def station_event_creator(
    last_state: StationStateCache, value: AjoaikadataMsg
) -> Tuple[StationStateCache, AjoaikadataMsg]:
    data: Event = value["data"]

    new_state: StationStateCache = {}
    station_event_to_send = None

    if not (last_state.get("station") or last_state.get("track")):
        new_state["station"] = data["state"]["last_station_event"]["station"]
        new_state["track"] = data["state"]["last_station_event"]["track"]

    match data["event_type"]:
        case "ARRIVAL":
            if any(last_state.values()):
                station_event_to_send = create_station_event(data, last_state)
                last_state = create_empty_stationstate_cache()

            new_state["station"] = data["state"]["last_station_event"]["station"]
            new_state["track"] = data["state"]["last_station_event"]["track"]

        case "stopped":
            if not last_state.get("time_arrived"):
                new_state["time_arrived"] = data["ntp_timestamp"]

        case "doors_opened":
            pass

        case "doors_closed":
            # update also the existing value, because we want to get the last one
            new_state["time_doors_last_closed"] = data["ntp_timestamp"]

        case "moving":
            new_state["time_departed"] = data["ntp_timestamp"]

        case "DEPARTURE":
            station_event_to_send = create_station_event(data, last_state)
            last_state = create_empty_stationstate_cache()
            new_state = {}

        case _:
            print("Unknown event type!")

    if station_event_to_send:
        msg_out: AjoaikadataMsg = {"msgs": value.get("msgs", []), "data": station_event_to_send}
    else:
        msg_out = create_empty_msg()
    return last_state | new_state, msg_out
