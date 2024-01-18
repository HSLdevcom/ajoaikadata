"""
Event creator reads parsed message stream from Pulsar, analyzes events 
(e.g. train stops, doors are opened), and creates station events from them.
Events are sent back to Pulsar.
"""

import bytewax.operators as op
from bytewax.dataflow import Dataflow

from .connectors.pulsar import PulsarInput, PulsarOutput, PulsarClient

from .operations.common import filter_none
from .operations.events import create_events, create_empty_state
from .operations.stationevents import create_station_events, init_vehicle_station_cache

from .util.config import read_from_env

input_topic, output_topic = read_from_env(("PULSAR_INPUT_TOPIC", "PULSAR_OUTPUT_TOPIC"))

input_client = PulsarClient(input_topic)
output_client = PulsarClient(output_topic)


flow = Dataflow("eventcreator")
stream = op.input("eventcreator_in", flow, PulsarInput(input_client))
event_stream = op.stateful_map(
    "event_creator",
    stream,
    lambda: create_empty_state(),
    create_events,
)
event_stream = op.filter_map("filter_none_event_creator", event_stream, input_client.ack_filter_none)
op.output("events_out", event_stream, PulsarOutput(output_client))
op.inspect(
    "eventcreator_ack", event_stream, input_client.ack
)  # ack here, because there is no need for original msgs any more
station_stream = op.stateful_map(
    "station_event_creator",
    event_stream,
    lambda: init_vehicle_station_cache(),
    create_station_events,
)
station_stream = op.filter_map("station_combiner_filtered", station_stream, filter_none)
op.output("stations_out", station_stream, PulsarOutput(output_client))
