"""
Output connection code for sending data to Postgres.
"""

import json
from typing import Callable, List

from bytewax.outputs import DynamicSink, StatelessSinkPartition
from psycopg.sql import SQL, Identifier
import psycopg_pool

from ..util.types import AjoaikadataMsgWithKey

from ..util.config import logger, read_from_env

(POSTGRES_CONN_STR,) = read_from_env(("POSTGRES_CONN_STR",))


# Key is the name of the postgres table.
# query is the copy command to the staging table
# post_query is the command to move data from the staging table to the main table
# mapper is the function to modify message data object to the database table schema
PG_TARGET_TABLE = {
    "messages": {
        "query": SQL("COPY staging.{staging} (timestamp, msg_type, vehicle_id, message) FROM STDIN;"),
        "post_query": SQL(
            """
            INSERT INTO messages (timestamp, msg_type, vehicle_id, message)
            SELECT timestamp, msg_type, vehicle_id, message FROM staging.{staging} ON CONFLICT DO NOTHING;
            DELETE FROM staging.{staging};
            """
        ),
        "mapper": lambda data_obj: (
            data_obj["eke_timestamp"],
            data_obj["msg_type"],
            data_obj["vehicle"],
            json.dumps(data_obj, default=str),
        ),
    },
    "events": {
        "query": SQL("COPY staging.{staging} (timestamp, event_type, vehicle_id, state) FROM STDIN;"),
        "post_query": SQL(
            """
            INSERT INTO events (timestamp, event_type, vehicle_id, state)
            SELECT timestamp, event_type, vehicle_id, state FROM staging.{staging} ON CONFLICT DO NOTHING;
            DELETE FROM staging.{staging};
            """
        ),
        "mapper": lambda data_obj: (
            data_obj["eke_timestamp"],
            data_obj["event_type"],
            data_obj["vehicle"],
            json.dumps(data_obj["state"], default=str),
        ),
    },
}


class PostgresClient:
    def __init__(self, target: str) -> None:
        self.pool = psycopg_pool.ConnectionPool(POSTGRES_CONN_STR, min_size=1, max_size=20)

        self.target = target
        self.query: SQL = PG_TARGET_TABLE[target]["query"]
        self.post_query: SQL = PG_TARGET_TABLE[target]["post_query"]
        self.mapper: Callable[[dict], tuple] = PG_TARGET_TABLE[target]["mapper"]
        self.staging_tables: List[str] = []

    def prepare_staging_table(self, for_id: str) -> None:
        """Create a staging table where the worker using this client copies data."""
        table_name = f"{self.target}-{for_id}"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    SQL("CREATE TABLE IF NOT EXISTS staging.{target} (LIKE {source})").format(
                        target=Identifier(table_name), source=Identifier(self.target)
                    )
                )
        # Store table name so that it could be deleted later
        self.staging_tables.append(table_name)

    def insert(self, data: List[AjoaikadataMsgWithKey], id: str) -> None:
        """Copy the batch of messages into the database."""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                # Should be empty, but just to be sure
                cur.execute(SQL("DELETE FROM staging.{staging}").format(staging=Identifier(f"{self.target}-{id}")))

                with cur.copy(self.query.format(staging=Identifier(f"{self.target}-{id}"))) as copy:
                    for msg in data:
                        key, content = msg
                        msg_data = content["data"]

                        copy.write_row(self.mapper(msg_data))

                cur.execute(self.post_query.format(staging=Identifier(f"{self.target}-{id}")))

        logger.info(f"Inserted {len(data)} rows to PG table {self.target}")

    def close(self):
        # Delete all staging tables
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                for table in self.staging_tables:
                    cur.execute(SQL("DROP TABLE staging.{table}").format(table={Identifier(table)}))
        self.pool.close()


class PostgresSink(StatelessSinkPartition):
    def __init__(self, client: PostgresClient, id: str) -> None:
        self.client = client
        self.id = id
        self.client.prepare_staging_table(self.id)

    def write_batch(self, data: List[AjoaikadataMsgWithKey]):
        self.client.insert(data, self.id)

    def close(self):
        self.client.close()


class PostgresOutput(DynamicSink):
    def __init__(self, client: PostgresClient, identifier: str = "") -> None:
        super().__init__()
        self.client = client
        # Identified is used to separate outputs if there are multiple outputs for the same target
        self.identifier = identifier

    def build(self, worker_index, worker_count):
        return PostgresSink(self.client, id=f"{self.identifier}{worker_index}")
