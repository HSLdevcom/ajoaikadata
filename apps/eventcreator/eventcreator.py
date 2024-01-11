from typing import Tuple

import bytewax.operators as op
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

from config import read_from_env

input_topic, output_topic = read_from_env(("PULSAR_INPUT_TOPIC", "PULSAR_OUTPUT_TOPIC"))

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


def filter_none(data): # TODO: typing
    key, msg = data
    if not msg:
        return None
    return data


flow = Dataflow("eventcreator")
stream = op.input("eventcreator_in", flow, PulsarInput(input_client))
event_stream = op.stateful_map(
    "eventer",
    stream,
    lambda: create_empty_eventstate_cache(),
    eventer,
)
event_stream = op.filter_map("eventer_filtered", event_stream, filter_none)
op.output("events_out", event_stream, PulsarOutput(output_client))
op.inspect("eventcreator_ack", event_stream, input_client.ack)  # ack here, because there is no need for original msgs any more
station_stream = op.stateful_map(
    "station_combiner",
    event_stream,
    lambda: create_empty_stationstate_cache(),
    station_combiner,
)
station_stream = op.filter_map("station_combiner_filtered", station_stream, filter_none)
op.output("stations_out", station_stream, PulsarOutput(output_client))
