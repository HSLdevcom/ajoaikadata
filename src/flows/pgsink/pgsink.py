import json

import bytewax.operators as op
from bytewax.dataflow import Dataflow

from psycopg import sql

from ...connectors.pulsar import PulsarInput, PulsarClient
from ...connectors.postgres import PostgresOutput, PostgresClient

from ...config import read_from_env

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
            "state": json.dumps(data_obj["state"]),
        },
    },
}

input_topic, sink_schema = read_from_env(("PULSAR_INPUT_TOPIC", "POSTGRES_SINK_SCHEMA"))
schema_details = PG_SCHEMA[sink_schema]

pulsar_client = PulsarClient(input_topic)
postgres_client = PostgresClient(schema_details["query"], schema_details["mapper"])


flow = Dataflow("pgsink")
stream = op.input("pgsink_in", flow, PulsarInput(pulsar_client))
op.output("pgsink_out", stream, PostgresOutput(postgres_client))
op.inspect("pgsink_ack", stream, pulsar_client.ack)
