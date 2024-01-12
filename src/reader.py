"""
Reader reads messages from csv storage and sends them to Pulsar.
"""

from pathlib import Path

import bytewax.operators as op
from bytewax.dataflow import Dataflow

from .connectors.pulsar import PulsarOutput, PulsarClient
from .connectors.csv_directory import CSVDirInput

from .operations.parsing import csv_to_bytewax_msg

from .config import read_from_env

(output_topic,) = read_from_env(("PULSAR_OUTPUT_TOPIC",))
output_client = PulsarClient(output_topic)


flow = Dataflow("reader")
stream = op.input("reader_in", flow, CSVDirInput(Path("/data/")))  # TODO: Configure path
pulsar_msg_stream = op.map("csv_to_bytewax_msg", stream, csv_to_bytewax_msg)
op.output("reader_out", pulsar_msg_stream, PulsarOutput(output_client))
