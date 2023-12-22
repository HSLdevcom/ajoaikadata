import os
from typing import Tuple

from bytewax.dataflow import Dataflow

from connectors.pulsar import PulsarInput, PulsarOutput, PulsarClient
from connectors.types import BytewaxMsgFromPulsar, PulsarMsg

from .util.eventstate import EventStateCache, Event, create_empty_state_cache, update_udp_state, update_balise_state

# read topic names from env
input_topic = os.environ.get("INPUT_TOPIC")
if not input_topic:
    raise ValueError("INPUT_TOPIC not set")

output_topic = os.environ.get("OUTPUT_TOPIC")
if not output_topic:
    raise ValueError("OUTPUT_TOPIC not set")

input_client = PulsarClient(input_topic)
output_client = PulsarClient(output_topic)


def create_event(data, event_type, state):
    new_data = {
        "vehicle": data["vehicle"],
        "eke_timestamp": data["eke_timestamp"],
        "event_type": event_type,
        "state": state,
    }
    return new_data


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


def filter_none(data: BytewaxMsgFromPulsar):
    key, msg = data
    if not msg:
        return None
    return data


def ack(data: BytewaxMsgFromPulsar):
    key, value = data
    input_client.ack_msgs(value["msgs"])


flow = Dataflow()
flow.input("inp", PulsarInput(input_client))
flow.stateful_map(
    "eventer",
    lambda: create_empty_state_cache(),
    eventer,
)
flow.filter_map(filter_none)
flow.output("out", PulsarOutput(output_client))
flow.inspect(ack)
