"""
Operations related to create events from eke message stream.
"""

from datetime import datetime
from typing import Tuple, TypeAlias, TypedDict

from ..util.ajoaikadatamsg import AjoaikadataMsg
from ..util.balise_registry import balise_registry

from ..util.config import logger


class UDPEventField(TypedDict):
    name: str  # Name of the UDP msg field
    true_event: str  # Event to be created when the field turns true
    false_event: str  # Event to be created when the field turns false
    ignore_none: bool  # If None value is normal and should not be updated.


UDP_EVENT_FIELDS: list[UDPEventField] = [
    {"name": "doors_open", "true_event": "doors_opened", "false_event": "doors_closed", "ignore_none": False},
    {"name": "standstill", "true_event": "stopped", "false_event": "moving", "ignore_none": False},
]


class UDPState(TypedDict):
    active_cabin: str | None
    doors_open: bool | None
    standstill: bool | None
    last_updated: datetime | None


class StationState(TypedDict):
    station: str | None
    track: str | None
    event: str | None
    last_updated: datetime | None


VehicleState: TypeAlias = tuple[UDPState, StationState]


class Event(TypedDict):
    vehicle: int
    ntp_timestamp: datetime
    mqtt_timestamp: datetime
    event_type: str
    data: dict


def _create_event(data: dict, event_type: str, event_data: dict) -> Event:
    return {
        "vehicle": data["vehicle"],
        "ntp_timestamp": data["ntp_timestamp"],
        "mqtt_timestamp": data["mqtt_timestamp"],
        "event_type": event_type,
        "data": event_data,
    }


def create_empty_state() -> VehicleState:
    """Create a new cache."""
    return (
        {"active_cabin": None, "doors_open": None, "standstill": None, "last_updated": None},
        {"station": None, "track": None, "event": None, "last_updated": None},
    )


def _check_udp_event(last_state: UDPState, data: dict) -> Tuple[UDPState, Event | None]:
    # Send event from preconfigured fields. It's not supported to send multiple events at the same time,
    # but that shouldn't be a problem, because the next update will be triggered almost immidiately
    # on the next message. Just keep sure only the one attribute is updated at once, which is related to the event.

    ntp_timestamp: datetime = data["ntp_timestamp"]
    # Initialize last_state if it is None
    for field in UDP_EVENT_FIELDS:
        if not field["ignore_none"] and last_state[field["name"]] is None:
            last_state[field["name"]] = data["content"][field["name"]]
            last_state["last_updated"] = ntp_timestamp

    for field in UDP_EVENT_FIELDS:
        data_field = data["content"][field["name"]]

        if data_field != last_state[field["name"]]:
            if last_state["last_updated"] and ntp_timestamp < last_state["last_updated"]:
                logger.warning(f"Tried to trigger {field['name']} event, but the message was old. Discarding {data}")
                return last_state, None
            last_state[field["name"]] = data_field
            last_state["last_updated"] = ntp_timestamp
            event_msg_data = {field["name"]: data_field}
            return last_state, _create_event(
                data, field["true_event"] if data_field else field["false_event"], event_msg_data
            )

    # Nothing was updated
    return last_state, None


def _check_station_event(last_state: StationState, data: dict) -> Tuple[StationState, Event | None]:
    ntp_timestamp = data["ntp_timestamp"]
    balise_id = data["content"]["balise_id"]
    direction = data["content"]["direction"]
    balise_key = f"{balise_id}_{direction}"

    balise_data = balise_registry.get(balise_key)

    if not balise_data:
        # Early return if balise didn't exist in the registry
        return last_state, None

    # check if any of the fields has changed
    if (
        last_state["station"] != balise_data["station"]
        or last_state["track"] != balise_data["track"]
        or last_state["event"] != balise_data["type"].lower()
    ):
        if last_state["last_updated"] and ntp_timestamp < last_state["last_updated"]:
            logger.warning(f"Tried to trigger balise event, but the message was old. Discarding {data}")
            return last_state, None

        # if changed, update last_station_event data and send event
        last_state["station"] = balise_data["station"]
        last_state["track"] = balise_data["track"]
        last_state["event"] = balise_data["type"].lower()
        last_state["last_updated"] = ntp_timestamp

        event_msg_data = {
            "station": balise_data["station"],
            "track": balise_data["track"],
            "triggered_by": balise_key
        }

        return last_state, _create_event(data, balise_data["type"].lower(), event_msg_data)

    return last_state, None


def create_events(state: VehicleState, value: AjoaikadataMsg) -> tuple[VehicleState, AjoaikadataMsg]:
    udp_state, balise_state = state

    data = value["data"]
    event = None

    if data["msg_type"] == 1:
        udp_state, event = _check_udp_event(udp_state, data)

    elif data["msg_type"] == 5 and not data.get("incomplete"):
        balise_state, event = _check_station_event(balise_state, data)

    msg: AjoaikadataMsg = {"msgs": value.get("msgs", []), "data": event}
    return (udp_state, balise_state), msg
