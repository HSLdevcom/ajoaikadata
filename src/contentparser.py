"""
Content parser is a bytewax app to convert binary content of messages to human readable format.
"""
import bytewax.operators as op
from bytewax.dataflow import Dataflow

from .connectors.pulsar import PulsarInput, PulsarOutput, PulsarClient

from .ekeparser.schemas.jkv_beacon import JKVBeaconDataSchema

from .operations.util.balisedirection import create_empty_balise_cache
from .operations.util.baliseparts import create_empty_parts_cache

from .operations.balises import combine_balise_parts, create_directions_for_balises
from .operations.parsing import raw_msg_to_eke
from .config import logger, read_from_env

input_topic, output_topic = read_from_env(("PULSAR_INPUT_TOPIC", "PULSAR_OUTPUT_TOPIC"))

input_client = PulsarClient(input_topic)
output_client = PulsarClient(output_topic)

BEACON_DATA_SCHEMA = JKVBeaconDataSchema()


flow = Dataflow("contentparser")
stream = op.input("contentparser_in", flow, PulsarInput(input_client))
eke_stream = op.map("raw_msg_to_eke", stream, raw_msg_to_eke)
eke_stream = op.filter_map("filter_none_raw_msg_to_eke", eke_stream, input_client.ack_filter_none)

eke_stream_with_balises = op.stateful_map(
    "combine_balises", eke_stream, lambda: create_empty_parts_cache(), combine_balise_parts
)
eke_stream_with_balises = op.filter_map(
    "filter_none_combine_balises", eke_stream_with_balises, input_client.ack_filter_none
)

eke_stream_complete = op.stateful_map(
    "balise_direction", eke_stream_with_balises, lambda: create_empty_balise_cache(), create_directions_for_balises
)
eke_stream_with_balises_dirs = op.filter_map(
    "filter_none_balise_direction", eke_stream_complete, input_client.ack_filter_none
)

op.output("contentparser_out", eke_stream_with_balises_dirs, PulsarOutput(output_client))
op.inspect("contentparser_ack", eke_stream_with_balises_dirs, input_client.ack)
