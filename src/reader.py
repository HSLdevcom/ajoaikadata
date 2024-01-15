"""
Reader reads messages from Azure Storage and sends them to Pulsar.
"""

import bytewax.operators as op
from bytewax.dataflow import Dataflow

from .connectors.azure_storage import AzureStorageInput
from .connectors.pulsar import PulsarOutput, PulsarClient

from .operations.parsing import csv_to_bytewax_msg

from .util.config import read_from_env

(output_topic,) = read_from_env(("PULSAR_OUTPUT_TOPIC",))
output_client = PulsarClient(output_topic)


flow = Dataflow("reader")
stream = op.input("reader_in", flow, AzureStorageInput())
pulsar_msg_stream = op.map("csv_to_bytewax_msg", stream, csv_to_bytewax_msg)
op.output("reader_out", pulsar_msg_stream, PulsarOutput(output_client))
