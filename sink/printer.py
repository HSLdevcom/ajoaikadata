import os

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput

from connectors.pulsar import PulsarInput, PulsarClient

# read topic names from env
input_topic = os.environ.get("INPUT_TOPIC")
client = PulsarClient(input_topic)


flow = Dataflow()
flow.input("inp", PulsarInput(client))
flow.output("out", StdOutput())
flow.inspect(client.ack_msg)
