"""
Operations related to create events from eke message stream.
"""

from datetime import datetime
from typing import Tuple, TypeAlias, TypedDict

from ..util.ajoaikadatamsg import AjoaikadataMsg, EKEMessageTypeWithMQTTDetails
from ..util.balise_registry import balise_registry

from ..util.config import logger


class UDPEventField(TypedDict):
    name: str  # Name of the UDP msg field
    true_event: str | None  # Event to be created when the field turns true
    false_event: str | None  # Event to be created when the field turns false
    ignore_none: bool  # If None value is normal and should not be updated.


UDP_EVENT_FIELDS: list[UDPEventField] = [
    {"name": "doors_open", "true_event": "doors_opened", "false_event": "doors_closed", "ignore_none": False},
    {"name": "standstill", "true_event": "stopped", "false_event": "moving", "ignore_none": False},
    {"name": "active_cabin", "true_event": "cabin_changed", "false_event": "cabin_changed", "ignore_none": True},
    {"name": "train_no", "true_event": "train_no_changed", "false_event": None, "ignore_none": True},
    {
        "name": "vehicle_count",
        "true_event": "vehicle_count_changed",
        "false_event": "vehicle_count_changed",
        "ignore_none": True,
    },
    {
        "name": "all_vehicles",
        "true_event": "vehicle_ids_changed",
        "false_event": "vehicle_ids_changed",
        "ignore_none": True,
    },
]


class UDPState(TypedDict):
    active_cabin: str | None
    doors_open: bool | None
    standstill: bool | None
    train_no: int | None
    vehicle_count: int | None
    all_vehicles: list[int] | None
    last_updated: datetime | None
    tst_source: str | None


class StationState(TypedDict):
    station: str | None
    track: str | None
    direction: str | None
    event: str | None
    last_updated: datetime | None


VehicleState: TypeAlias = tuple[UDPState, StationState]


class Event(TypedDict):
    vehicle: int
    tst: datetime
    tst_corrected: datetime
    tst_source: str
    ntp_timestamp: datetime
    eke_timestamp: datetime
    mqtt_timestamp: datetime
    event_type: str
    data: dict


def _create_event(data: EKEMessageTypeWithMQTTDetails, event_type: str, event_data: dict) -> Event:
    return {
        "vehicle": data["vehicle"],
        "tst": data["tst"],
        "tst_corrected": data["tst_corrected"],
        "tst_source": data["tst_source"],
        "ntp_timestamp": data["ntp_timestamp"],
        "eke_timestamp": data["eke_timestamp"],
        "mqtt_timestamp": data["mqtt_timestamp"],
        "event_type": event_type,
        "data": event_data,
    }


def create_empty_state() -> VehicleState:
    """Create a new cache."""
    return (
        {
            "active_cabin": None,
            "doors_open": None,
            "standstill": None,
            "train_no": None,
            "vehicle_count": None,
            "all_vehicles": None,
            "last_updated": None,
            "tst_source": None,
        },
        {"station": None, "track": None, "direction": None, "event": None, "last_updated": None},
    )


def _check_udp_event(last_state: UDPState, data: EKEMessageTypeWithMQTTDetails) -> Tuple[UDPState, Event | None]:
    # Send event from preconfigured fields. It's not supported to send multiple events at the same time,
    # but that shouldn't be a problem, because the next update will be triggered almost immidiately
    # on the next message. Just keep sure only the one attribute is updated at once, which is related to the event.

    tst: datetime = data["tst"]

    # Initialize last_state if it is None
    for field in UDP_EVENT_FIELDS:
        if not field["ignore_none"] and last_state[field["name"]] is None:
            last_state[field["name"]] = data["content"][field["name"]]
            last_state["last_updated"] = tst

    for field in UDP_EVENT_FIELDS:
        data_field = data["content"][field["name"]]

        if data_field != last_state[field["name"]]:
            event_name = field["true_event"] if data_field else field["false_event"]

            if not event_name:
                return last_state, None

            if last_state["last_updated"] and tst < last_state["last_updated"]:
                logger.warning(f"Tried to trigger {field['name']} event, but the message was old. Discarding {data}")
                return last_state, None

            last_state[field["name"]] = data_field
            last_state["last_updated"] = tst
            event_msg_data = {field["name"]: data_field}
            return last_state, _create_event(data, event_name, event_msg_data)

    # Nothing was updated
    return last_state, None


def _check_station_event(last_state: StationState, data: EKEMessageTypeWithMQTTDetails) -> Tuple[StationState, Event | None]:
    tst = data["tst"]
    balise_id = data["content"]["balise_id"]
    direction = data["content"]["direction"]
    balise_key = f"{balise_id}_{direction}"

    balise_data = balise_registry.get(balise_key)

    if not balise_data:
        # Early return if balise didn't exist in the registry
        return last_state, None

    event_msg_data = {
        "station": balise_data["station"],
        "track": balise_data["track"],
        "direction": balise_data["train_direction"],
        "triggered_by": balise_key,
    }

    # check if any of the fields has changed
    if (
        last_state["station"] != balise_data["station"]
        or last_state["track"] != balise_data["track"]
        or last_state["direction"] != balise_data["train_direction"]
        or last_state["event"] != balise_data["type"].lower()
    ):
        if last_state["last_updated"] and tst < last_state["last_updated"]:
            logger.warning(f"Tried to trigger balise event, but the message was old. Discarding {data}")
            return last_state, None

        # if changed, update last_station_event data and send event
        last_state["station"] = balise_data["station"]
        last_state["track"] = balise_data["track"]
        last_state["direction"] = balise_data["train_direction"]
        last_state["event"] = balise_data["type"].lower()
        last_state["last_updated"] = tst

        return last_state, _create_event(data, balise_data["type"].lower(), event_msg_data)

    # Balise found but nothing needed to be updated. Send event for debuggin purposes.
    return last_state, _create_event(data, f"{balise_data['type'].lower()}_debug", event_msg_data)


def create_events(state: VehicleState | None, value: AjoaikadataMsg) -> tuple[VehicleState, AjoaikadataMsg]:
    if not state:
        state = create_empty_state()

    udp_state, balise_state = state

    data = value["data"]
    event = None

    if data["msg_type"] == 1 and not data.get("discard"):
        udp_state, event = _check_udp_event(udp_state, data)

    elif data["msg_type"] == 5 and not data.get("incomplete"):
        balise_state, event = _check_station_event(balise_state, data)

    msg: AjoaikadataMsg = {"msgs": value.get("msgs", []), "data": event}
    return (udp_state, balise_state), msg
