"""
Operations related to create events from eke message stream.
"""

from copy import deepcopy
from typing import Any, Tuple, TypedDict

from ..util.ajoaikadatamsg import AjoaikadataMsg
from ..util.balise_registry import balise_registry


class LastStationEvent(TypedDict):
    station: str | None
    track: int | None
    event: str | None
    triggered_by: str | None


class EventStateCache(TypedDict):
    doors_open: bool | None
    standstill: bool | None
    last_station_event: LastStationEvent


class Event(TypedDict):
    vehicle: Any
    ntp_timestamp: Any
    event_type: str
    state: EventStateCache


def create_event(data: dict, event_type: str, state: EventStateCache) -> Event:
    return {
        "vehicle": data["vehicle"],
        "ntp_timestamp": data["ntp_timestamp"],
        "event_type": event_type,
        "state": state,
    }


def create_empty_eventstate_cache() -> EventStateCache:
    """Create a new cache."""
    return {
        "doors_open": None,
        "standstill": None,
        "last_station_event": {"station": None, "track": None, "event": None, "triggered_by": None},
    }


def update_udp_state(last_state: EventStateCache, data: dict) -> Tuple[EventStateCache, Event | None]:
    last_state = deepcopy(last_state)

    doors_open = data["content"]["doors_open"]
    standstill = data["content"]["standstill"]

    # Initialize last_state if it is None
    if last_state["doors_open"] is None:
        last_state["doors_open"] = doors_open
    if last_state["standstill"] is None:
        last_state["standstill"] = standstill

    # Send event from doors open and standstill. It's not supported to send multiple events at the same time,
    # but that shouldn't be a problem, because the next update will be triggered almost immidiately
    # on the next message. Just keep sure only the one attribute is updated at once, which is related to the event.
    if doors_open != last_state["doors_open"]:
        last_state["doors_open"] = doors_open
        return last_state, create_event(data, "doors_opened" if doors_open else "doors_closed", last_state)

    if standstill != last_state["standstill"]:
        last_state["standstill"] = standstill
        return last_state, create_event(data, "stopped" if standstill else "moving", last_state)

    # Nothing was updated
    return last_state, None


def update_balise_state(last_state: EventStateCache, data: dict) -> Tuple[EventStateCache, Event | None]:
    balise_id = data["content"]["balise_id"]
    direction = data["content"]["direction"]
    balise_key = f"{balise_id}_{direction}"

    balise_data = balise_registry.get(balise_key)

    if not balise_data:
        # Early return if balise was no in registry
        return last_state, None

    last_state = deepcopy(last_state)

    # check if any of the last_station_event data has changed
    if (
        last_state["last_station_event"]["station"] != balise_data["station"]
        or last_state["last_station_event"]["track"] != balise_data["track"]
        or last_state["last_station_event"]["event"] != balise_data["type"]
    ):
        # if changed, update last_station_event data and send event
        last_station_event: LastStationEvent = {
            "station": balise_data["station"],
            "track": balise_data["track"],
            "event": balise_data["type"],
            "triggered_by": balise_key,
        }
        last_state["last_station_event"] = last_station_event

        return last_state, create_event(data, balise_data["type"], last_state)

    return last_state, None


def event_creator(last_state: EventStateCache, value: AjoaikadataMsg) -> Tuple[EventStateCache, AjoaikadataMsg]:
    data = value["data"]
    event = None

    if data["msg_type"] == 1:
        last_state, event = update_udp_state(last_state, data)

    elif data["msg_type"] == 5 and not data.get("incomplete"):
        last_state, event = update_balise_state(last_state, data)

    msg: AjoaikadataMsg = {"msgs": value.get("msgs", []), "data": event}
    return last_state, msg
