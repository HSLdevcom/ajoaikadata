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
from .operations.balisedirection import create_directions_for_balises
from .operations.baliseparts import combine_balise_parts
from .operations.deduplication import deduplicate
from .operations.events import create_events
from .operations.stationevents import create_station_events
from .operations.parsing import csv_to_bytewax_msg, raw_msg_to_eke
from .operations.tstvalidator import validate_tst
from .operations.udporder import reorder_messages

BEACON_DATA_SCHEMA = JKVBeaconDataSchema()


postgres_client_messages = PostgresClient("messages")
postgres_client_events = PostgresClient("events")
postgres_client_stationevents = PostgresClient("stationevents")


flow = Dataflow("readerparser")
stream = op.input("reader_in", flow, AzureStorageInput())

stream = op.map("csv_to_bytewax_msg", stream, csv_to_bytewax_msg)

# Is this needed?
# stream = op.stateful_map("deduplicate", stream, deduplicate).then(
#     op.filter_map, "filter_none_deduplicate", filter_none
# )

stream = op.map("raw_msg_to_eke", stream, raw_msg_to_eke).then(op.filter_map, "filter_none_raw_msg_to_eke", filter_none)

stream = op.stateful_map("validate_tst", stream, validate_tst) # TODO: Does not work reliable

stream = op.stateful_map("reorder_upd", stream, reorder_messages).then(
    # Remove batches coming from reordering
    op.flat_map_value,
    "flatten_reorder_upd",
    lambda x: x,
)

stream = op.stateful_map("combine_balises", stream, combine_balise_parts).then(
    op.filter_map, "filter_none_combine_balises", filter_none
)

stream = op.stateful_map("balise_direction", stream, create_directions_for_balises).then(
    op.filter_map, "filter_none_balise_direction", filter_none
)

op.output("contentparser_out", stream, PostgresOutput(postgres_client_messages))

stream = op.stateful_map(
    "event_creator",
    stream,
    create_events,
).then(op.filter_map, "filter_none_event_creator", filter_none)

op.output("events_out", stream, PostgresOutput(postgres_client_events))

stream = op.stateful_map(
    "station_event_creator",
    stream,
    create_station_events,
).then(op.filter_map, "filter_none_station_event_creator", filter_none)

op.output("stations_out", stream, PostgresOutput(postgres_client_stationevents))
