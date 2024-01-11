from copy import deepcopy
import os
from typing import Tuple

from bytewax.dataflow import Dataflow

from connectors.pulsar import PulsarInput, PulsarOutput, PulsarClient
from connectors.types import BytewaxMsgFromPulsar, PulsarMsg

from .util.eventstate import (
    EventStateCache,
    Event,
    create_empty_eventstate_cache,
    update_udp_state,
    update_balise_state,
)
from .util.stationstate import StationStateCache, create_empty_stationstate_cache, create_station_event

# read topic names from env
input_topic = os.environ.get("INPUT_TOPIC")
if not input_topic:
    raise ValueError("INPUT_TOPIC not set")

output_topic = os.environ.get("OUTPUT_TOPIC")
if not output_topic:
    raise ValueError("OUTPUT_TOPIC not set")

input_client = PulsarClient(input_topic)
output_client = PulsarClient(output_topic)


def eventer(last_state: EventStateCache, value: PulsarMsg) -> Tuple[EventStateCache, PulsarMsg | None]:
    data = value["data"]
    event = None

    if data["msg_type"] == 1:
        last_state, event = update_udp_state(last_state, data)

    elif data["msg_type"] == 5:
        last_state, event = update_balise_state(last_state, data)

    if not event:
        input_client.ack_msgs(value["msgs"])
        return last_state, None

    msg: PulsarMsg = {"msgs": value["msgs"], "data": event}
    return last_state, msg


def station_combiner(last_state: StationStateCache, value: PulsarMsg) -> Tuple[StationStateCache, PulsarMsg | None]:
    data: Event = value["data"]

    new_state: StationStateCache = {}
    station_event_to_send = None
    msg_out: PulsarMsg | None = None

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
        msg_out = {"msgs": value["msgs"], "data": station_event_to_send}

    return last_state | new_state, msg_out


def filter_none(data: BytewaxMsgFromPulsar):
    key, msg = data
    if not msg:
        return None
    return data


flow = Dataflow()
flow.input("inp", PulsarInput(input_client))
flow.stateful_map(
    "eventer",
    lambda: create_empty_eventstate_cache(),
    eventer,
)
flow.filter_map(filter_none)
flow.output("out_events", PulsarOutput(output_client))
flow.inspect(input_client.ack)  # ack here, because there is no need for original msgs any more
flow.stateful_map(
    "station_combiner",
    lambda: create_empty_stationstate_cache(),
    station_combiner,
)
flow.filter_map(filter_none)
flow.output("out_stations", PulsarOutput(output_client))
