import json
import os
from typing import Callable, List, TypedDict
from datetime import datetime

from bytewax.outputs import DynamicOutput, StatelessSink
from psycopg import sql
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
    def __init__(self, query: sql.SQL, mapper: Callable[[dict], dict]) -> None:
        self.pool = psycopg_pool.ConnectionPool(POSTGRES_CONN_STR, min_size=1, max_size=20)
        self.query = query
        self.mapper = mapper

    def insert(self, data: List[BytewaxMsgFromPulsar]) -> None:
        rows = []
        for msg in data:
            key, content = msg
            msg_data = content["data"]

            rows.append(self.mapper(msg_data))

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    self.query,
                    rows,
                )
            conn.commit()

    def close(self):
        self.pool.close()


class PostgresSink(StatelessSink):
    def __init__(self, client: PostgresClient) -> None:
        self.client = client

    def write_batch(self, data: List[BytewaxMsgFromPulsar]):
        self.client.insert(data)

    def close(self):
        self.client.close()


class PostgresOutput(DynamicOutput):
    def __init__(self, client: PostgresClient) -> None:
        super().__init__()
        self.client = client

    def build(self, worker_index, worker_count):
        return PostgresSink(self.client)
