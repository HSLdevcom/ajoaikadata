import json
import os

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput

from psycopg import sql

from connectors.pulsar import PulsarInput, PulsarClient
from connectors.postgres import PostgresOutput, PostgresClient
from connectors.types import BytewaxMsgFromPulsar

PG_SCHEMA = {
    "MESSAGES": {
        "query": sql.SQL(
            """
                INSERT INTO messages (timestamp, msg_type, vehicle_id, message)
                VALUES(%(timestamp)s, %(msg_type)s, %(vehicle_id)s, %(message)s)
                ON CONFLICT (timestamp, msg_type, vehicle_id)
                DO UPDATE SET message = EXCLUDED.message; 
            """
        ),
        "mapper": lambda data_obj: {
            "timestamp": data_obj["eke_timestamp"],
            "msg_type": data_obj["msg_type"],
            "vehicle_id": data_obj["vehicle"],
            "message": json.dumps(data_obj),
        },
    },
    "EVENTS": {
        "query": sql.SQL(
            """
                INSERT INTO events (timestamp, event_type, vehicle_id, state)
                VALUES(%(timestamp)s, %(event_type)s, %(vehicle_id)s, %(state)s)
                ON CONFLICT (timestamp, event_type, vehicle_id)
                DO UPDATE SET state = EXCLUDED.state; 
            """
        ),
        "mapper": lambda data_obj: {
            "timestamp": data_obj["eke_timestamp"],
            "event_type": data_obj["event_type"],
            "vehicle_id": data_obj["vehicle"],
            "state": json.dumps(data_obj),
        },
    },
}

# read topic names from env
input_topic = os.environ.get("INPUT_TOPIC")
if not input_topic:
    raise ValueError("INPUT_TOPIC not set")

SINK_SCHEMA = os.environ.get("SINK_SCHEMA")
if not SINK_SCHEMA:
    raise ValueError("SINK_SCHEMA not set")
SCHEMA_DETAILS = PG_SCHEMA[SINK_SCHEMA]

pulsar_client = PulsarClient(input_topic)

postgres_client = PostgresClient(SCHEMA_DETAILS["query"], SCHEMA_DETAILS["mapper"])


def ack(data: BytewaxMsgFromPulsar):
    key, value = data
    pulsar_client.ack_msgs(value["msgs"])


flow = Dataflow()
flow.input("inp", PulsarInput(pulsar_client))
flow.output("out", PostgresOutput(postgres_client))
flow.inspect(ack)
