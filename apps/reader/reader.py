from pathlib import Path

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


flow = Dataflow()
flow.input("inp", CSVDirInput(Path("/data/"))) # TODO: Configure
flow.map(create_pulsar_msg)
flow.output("out", PulsarOutput(output_client))
