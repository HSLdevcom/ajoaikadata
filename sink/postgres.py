import os

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput

from connectors.pulsar import PulsarInput, PulsarClient
from connectors.postgres import PostgresOutput, PostgresClient


# read topic names from env
input_topic = os.environ.get("INPUT_TOPIC")
if not input_topic:
    raise ValueError("INPUT_TOPIC not set")
pulsar_client = PulsarClient(input_topic)

postgres_client = PostgresClient()


flow = Dataflow()
flow.input("inp", PulsarInput(pulsar_client))
flow.output("out", PostgresOutput(postgres_client))
flow.inspect(pulsar_client.ack_msg)
