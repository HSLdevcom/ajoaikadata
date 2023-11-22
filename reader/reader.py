import os
from pathlib import Path

from bytewax.dataflow import Dataflow

from connectors.pulsar import PulsarOutput, PulsarClient
from connectors.csv_directory import CSVDirInput


# read topic names from env
output_topic = os.environ.get("OUTPUT_TOPIC")

if not output_topic:
    raise ValueError("OUTPUT_TOPIC not set")

output_client = PulsarClient(output_topic)


def create_pulsar_msg(value):
    topic_name = value["mqtt_topic"]
    vehicle = topic_name.split("/")[3]

    data = {"raw": value["raw_data"], "topic": topic_name, "vehicle": vehicle}
    return vehicle, {"data": data}


flow = Dataflow()
flow.input("inp", CSVDirInput(Path("/data/")))
flow.map(create_pulsar_msg)
flow.output("out", PulsarOutput(output_client))
