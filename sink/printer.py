import json

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput

from .connectors.pulsar import PulsarInput, PulsarClient, handle_pulsar_msg
from .eke_parser.ekeparser import parse_eke_data


client = PulsarClient("modified")


@handle_pulsar_msg(client)
def parse_eke(data):
    data = parse_eke_data(data.get("raw"), data.get("topic"))
    return data


flow = Dataflow()
flow.input("inp", PulsarInput(client))
flow.filter_map(parse_eke)
flow.output("out", StdOutput())
flow.inspect(client.ack_msg)
