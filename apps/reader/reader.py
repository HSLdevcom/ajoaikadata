from pathlib import Path

import bytewax.operators as op
from bytewax.dataflow import Dataflow

from connectors.pulsar import PulsarOutput, PulsarClient
from connectors.csv_directory import CSVDirInput
from connectors.types import BytewaxMsgFromCSV

import config

(output_topic,) = config.read_from_env(("PULSAR_OUTPUT_TOPIC",))
output_client = PulsarClient(output_topic)


def create_pulsar_msg(value) -> BytewaxMsgFromCSV:
    topic_name = value["mqtt_topic"]
    vehicle = topic_name.split("/")[3]

    data = {"raw": value["raw_data"], "topic": topic_name, "vehicle": vehicle}
    return vehicle, {"data": data}


flow = Dataflow("reader")
stream = op.input("reader_in", flow, CSVDirInput(Path("/data/"))) # TODO: Configure path
pulsar_msg_stream = op.map("create_pulsar_msg", stream, create_pulsar_msg)
op.output("reader_out", pulsar_msg_stream, PulsarOutput(output_client))
