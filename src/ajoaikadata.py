""" 
All ajoaikadata in the single dataflow
Reads data from Azure Storage, runs the ajoaikadata pipeline and stores results to Postgres. 
"""

import bytewax.operators as op
from bytewax.dataflow import Dataflow

from .connectors.azure_storage import AzureStorageInput
from .connectors.postgres import PostgresOutput, PostgresClient

from .ekeparser.schemas.jkv_beacon import JKVBeaconDataSchema

from .operations.common import filter_none
from .operations.balisedirection import create_directions_for_balises, create_empty_balise_cache
from .operations.baliseparts import combine_balise_parts, create_empty_parts_cache
from .operations.events import create_events, create_empty_state
from .operations.stationevents import create_station_events, init_vehicle_station_cache
from .operations.parsing import csv_to_bytewax_msg, raw_msg_to_eke

BEACON_DATA_SCHEMA = JKVBeaconDataSchema()


postgres_client_messages = PostgresClient("messages")
postgres_client_events = PostgresClient("events")
postgres_client_stationevents = PostgresClient("stationevents")


flow = Dataflow("readerparser")
stream = op.input("reader_in", flow, AzureStorageInput())
bytewax_msg_stream = op.map("csv_to_bytewax_msg", stream, csv_to_bytewax_msg)


eke_stream = op.map("raw_msg_to_eke", bytewax_msg_stream, raw_msg_to_eke)
eke_stream = op.filter_map("filter_none_raw_msg_to_eke", eke_stream, filter_none)

eke_stream_with_balises = op.stateful_map(
    "combine_balises", eke_stream, lambda: create_empty_parts_cache(), combine_balise_parts
)
eke_stream_with_balises = op.filter_map("filter_none_combine_balises", eke_stream_with_balises, filter_none)

eke_stream_complete = op.stateful_map(
    "balise_direction", eke_stream_with_balises, lambda: create_empty_balise_cache(), create_directions_for_balises
)
eke_stream_with_balises_dirs = op.filter_map("filter_none_balise_direction", eke_stream_complete, filter_none)

op.output("contentparser_out", eke_stream_with_balises_dirs, PostgresOutput(postgres_client_messages))

event_stream = op.stateful_map(
    "event_creator",
    eke_stream_with_balises_dirs,
    lambda: create_empty_state(),
    create_events,
)
event_stream = op.filter_map("filter_none_event_creator", event_stream, filter_none)
op.output("events_out", event_stream, PostgresOutput(postgres_client_events))

station_stream = op.stateful_map(
    "station_event_creator",
    event_stream,
    lambda: init_vehicle_station_cache(),
    create_station_events,
)
station_stream = op.filter_map("station_combiner_filtered", station_stream, filter_none)
op.output("stations_out", station_stream, PostgresOutput(postgres_client_stationevents))
