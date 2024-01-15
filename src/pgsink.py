"""
PG sink reads data from Pulsar and sends it to Postgres. The Postgres schema is defined in the connector module.
"""
import bytewax.operators as op
from bytewax.dataflow import Dataflow

from .connectors.pulsar import PulsarInput, PulsarClient
from .connectors.postgres import PostgresOutput, PostgresClient

from .util.config import read_from_env


input_topic, target = read_from_env(("PULSAR_INPUT_TOPIC", "POSTGRES_TARGET_TABLE"))

pulsar_client = PulsarClient(input_topic)
postgres_client = PostgresClient(target)


flow = Dataflow("pgsink")
stream = op.input("pgsink_in", flow, PulsarInput(pulsar_client))
op.output("pgsink_out", stream, PostgresOutput(postgres_client, identifier=input_topic))
op.inspect("pgsink_ack", stream, pulsar_client.ack)
