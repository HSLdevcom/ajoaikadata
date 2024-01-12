from typing import Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow

from ..connectors.pulsar import PulsarInput, PulsarOutput, PulsarClient
from ..types import AjoaikadataMsg, AjoaikadataMsgWithKey, create_empty_msg

from .util.eventstate import (
    EventStateCache,
    Event,
    create_empty_eventstate_cache,
    update_udp_state,
    update_balise_state,
)
from .util.stationstate import StationStateCache, create_empty_stationstate_cache, create_station_event


def event_creator(last_state: EventStateCache, value: AjoaikadataMsg) -> Tuple[EventStateCache, AjoaikadataMsg]:
    data = value["data"]
    event = None

    if data["msg_type"] == 1:
        last_state, event = update_udp_state(last_state, data)

    elif data["msg_type"] == 5:
        last_state, event = update_balise_state(last_state, data)

    msg: AjoaikadataMsg = {"msgs": value.get("msgs", []), "data": event}
    return last_state, msg


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
                new_state["time_arrived"] = data["eke_timestamp"]

        case "doors_opened":
            pass

        case "doors_closed":
            # update also the existing value, because we want to get the last one
            new_state["time_doors_last_closed"] = data["eke_timestamp"]

        case "moving":
            new_state["time_departed"] = data["eke_timestamp"]

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
