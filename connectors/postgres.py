from datetime import datetime
import json
import os

from bytewax.outputs import DynamicOutput, StatelessSink
import psycopg


# get postgres connection str from env, if not set, raise error
POSTGRES_CONN_STR = os.environ.get("POSTGRES_CONN_STR")
if not POSTGRES_CONN_STR:
    raise ValueError("POSTGRES_CONN_STR not set")


class PostgresClient:
    def __init__(self) -> None:
        self.connection = psycopg.connect(POSTGRES_CONN_STR)

    def insert(self, rows: list) -> None:
        with self.connection.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO messages (timestamp, msg_type, vehicle_id, message)
                VALUES(%(timestamp)s, %(msg_type)s, %(vehicle_id)s, %(message)s)
                ON CONFLICT (timestamp, msg_type, vehicle_id)
                DO UPDATE SET message = EXCLUDED.message; 
                """,
                rows,
            )
            self.connection.commit()

    def close(self):
        self.connection.commit()
        self.connection.close()


class PostgresSink(StatelessSink):
    def __init__(self, client: PostgresClient) -> None:
        self.client = client

    def write_batch(self, data: list[tuple[str, dict]]):
        msgs = []

        for msg in data:
            key, content = msg
            msg_data: dict = content.get("data", "")

            msgs.append(
                {
                    "timestamp": msg_data.get("eke_timestamp"),
                    "msg_type": msg_data.get("msg_type"),
                    "vehicle_id": msg_data.get("vehicle"),
                    "message": json.dumps(msg_data),
                }
            )

        self.client.insert(msgs)

    def close(self):
        self.client.close()


class PostgresOutput(DynamicOutput):
    def __init__(self, client: PostgresClient) -> None:
        super().__init__()
        self.client = client

    def build(self, worker_index, worker_count):
        return PostgresSink(self.client)
