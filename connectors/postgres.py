import json
import os
from typing import List, TypedDict
from datetime import datetime

from bytewax.outputs import DynamicOutput, StatelessSink
import psycopg_pool

from .types import BytewaxMsgFromPulsar

# get postgres connection str from env, if not set, raise error
POSTGRES_CONN_STR: str = os.environ.get("POSTGRES_CONN_STR", "")
if not POSTGRES_CONN_STR:
    raise ValueError("POSTGRES_CONN_STR not set")


class EkeMessageRow(TypedDict):
    timestamp: datetime
    msg_type: int
    vehicle_id: int
    message: str


class PostgresClient:
    def __init__(self) -> None:
        self.pool = psycopg_pool.ConnectionPool(POSTGRES_CONN_STR, min_size=1, max_size=20)

    def insert(self, rows: List[EkeMessageRow]) -> None:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO messages (timestamp, msg_type, vehicle_id, message)
                    VALUES(%(timestamp)s, %(msg_type)s, %(vehicle_id)s, %(message)s)
                    ON CONFLICT (timestamp, msg_type, vehicle_id)
                    DO UPDATE SET message = EXCLUDED.message; 
                    """,
                    rows,
                )
            conn.commit()

    def close(self):
        self.pool.close()


class PostgresSink(StatelessSink):
    def __init__(self, client: PostgresClient) -> None:
        self.client = client

    def write_batch(self, data: List[BytewaxMsgFromPulsar]):
        msgs: List[EkeMessageRow] = []

        for msg in data:
            key, content = msg
            msg_data = content["data"]

            msgs.append(
                {
                    "timestamp": msg_data["eke_timestamp"],
                    "msg_type": msg_data["msg_type"],
                    "vehicle_id": msg_data["vehicle"],
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
