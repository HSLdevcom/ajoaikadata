"""
Operations related to create station events.
"""

from typing import Any, TypeAlias, TypedDict
from datetime import datetime

from .events import Event
from ..util.ajoaikadatamsg import AjoaikadataMsg, create_empty_msg

from ..util.config import logger

VehicleState: TypeAlias = dict[str, Any]


class StationStateCache(TypedDict):
    station: str | None
    track: int | None
    direction: str | None
    time_arrived: datetime | None
    time_doors_last_closed: datetime | None
    time_departed: datetime | None
    arrival_vehicle_state: VehicleState | None


class StationEvent(TypedDict):
    vehicle: Any
    ntp_timestamp: Any
    eke_timestamp: Any
    station: str
    track: int
    direction: str
    data: Any


def _create_event(data: Event, station_state: StationStateCache, trigger_time: datetime) -> StationEvent | None:
    # Station should have track information and either arrival or departure time.
    if (
        not station_state["station"]
        or not station_state["track"]
        or not station_state["direction"]
        or not (station_state["time_arrived"] or station_state["time_departed"])
    ):
        return None

    # Ensure the trigger timestamp is always greater than other timestamps.
    tsts = [station_state[tst] for tst in ("time_arrived", "time_doors_last_closed", "time_departed")]
    if any([trigger_time.timestamp() - (tst.timestamp() if tst else 0) < 0 for tst in tsts]):
        return None

    return {
        "vehicle": data["vehicle"],
        "ntp_timestamp": data["ntp_timestamp"],
        "eke_timestamp": data["eke_timestamp"],
        "station": station_state["station"],
        "track": station_state["track"],
        "direction": station_state["direction"],
        # Data is the combination of the existing vehicle and selected keys of station states
        "data": (station_state["arrival_vehicle_state"] or {})
        | {
            k: station_state[k]
            for k in station_state.keys() & {"time_arrived", "time_doors_last_closed", "time_departed"}
        },
    }


def create_empty_stationstate_cache() -> StationStateCache:
    """Create a new cache."""
    return {
        "station": None,
        "track": None,
        "direction": None,
        "time_arrived": None,
        "time_doors_last_closed": None,
        "time_departed": None,
        "arrival_vehicle_state": None,
    }


def init_vehicle_station_cache() -> tuple[VehicleState, StationStateCache]:
    return ({}, create_empty_stationstate_cache())


def create_station_events(
    last_state: tuple[VehicleState, StationStateCache], value: AjoaikadataMsg
) -> tuple[tuple[VehicleState, StationStateCache], AjoaikadataMsg]:
    vehicle_state, last_station_state = last_state

    data: Event = value["data"]

    station_event_to_send = None

    match data["event_type"]:
        case "arrival":
            # Init station and track. Send event if the existing station was there (means we didn't receive the departure event.)
            if last_station_state["station"]:
                station_event_to_send = _create_event(data, last_station_state, data["ntp_timestamp"])
                # Clear cache only if event will be sent.
                if station_event_to_send:
                    last_station_state = create_empty_stationstate_cache()

            last_station_state["arrival_vehicle_state"] = vehicle_state
            last_station_state["station"] = data["data"]["station"]
            last_station_state["track"] = data["data"]["track"]
            last_station_state["direction"] = data["data"]["direction"]

            # Override the values that are earlier than the event.
            for tst_field in ("time_arrived", "time_doors_last_closed", "time_departed"):
                tst = last_station_state[tst_field]
                if tst and tst.timestamp() < data["ntp_timestamp"].timestamp():
                    last_station_state[tst_field] = None

        case "stopped":
            # Update arrival time. If doors were not opened or station is missing, override the value.
            if not last_station_state.get("time_arrived") or not last_station_state.get("time_doors_last_closed"):
                last_station_state["time_arrived"] = data["ntp_timestamp"]

        case "doors_opened":
            # Does nothing at the moment
            pass

        case "doors_closed":
            # Update last closed time. Always override, because we want to have the last value.
            last_station_state["time_doors_last_closed"] = data["ntp_timestamp"]

        case "moving":
            # Update departure time
            last_station_state["time_departed"] = data["ntp_timestamp"]

        case "departure":
            # release the event
            # update first the station state if it's still missing (that's the case when the vehicle is leaving from the first station)
            if (
                not last_station_state["station"]
                or not last_station_state["track"]
                or not last_station_state["direction"]
            ):
                last_station_state["station"] = data["data"]["station"]
                last_station_state["track"] = data["data"]["track"]
                last_station_state["direction"] = data["data"]["direction"]

            if not last_station_state["arrival_vehicle_state"]:
                last_station_state["arrival_vehicle_state"] = vehicle_state
            station_event_to_send = _create_event(data, last_station_state, data["ntp_timestamp"])
            if station_event_to_send:
                last_station_state = create_empty_stationstate_cache()

        case "cabin_changed":
            # Train has stopped and probably will change the direction. Release the message. Never send doors / departed time.
            vehicle_state = vehicle_state | data["data"]
            last_station_state["time_departed"] = None
            last_station_state["time_doors_last_closed"] = None
            station_event_to_send = _create_event(data, last_station_state, data["ntp_timestamp"])
            last_station_state = create_empty_stationstate_cache()

        case "train_no_changed" | "vehicle_count_changed" | "vehicle_ids_changed":
            vehicle_state = vehicle_state | data["data"]

        case _:
            pass

    if station_event_to_send:
        msg_out: AjoaikadataMsg = {"msgs": value.get("msgs", []), "data": station_event_to_send}
    else:
        msg_out = create_empty_msg()
    return (vehicle_state, last_station_state), msg_out
