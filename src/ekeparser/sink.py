import json
from queue import Queue
import psycopg

from config import POSTGRES_CONN_STR


class Sink:
    def __init__(self, queue: Queue) -> None:
        self.queue = queue

    def process(self) -> None:
        while True:
            row = self.queue.get()
            if row:
                success = self.process_row(row)
                if success:
                    pass
            self.queue.task_done()

    def process_row(self, row: dict) -> None:
        pass

    def clear_resources(self) -> None:
        pass


class ConsoleSink(Sink):
    def process_row(self, row) -> bool:
        print(row)
        return True


class DummySink(Sink):
    def process_row(self, row) -> bool:
        return True


class PostgresSink(Sink):
    def __init__(self, queue: Queue) -> None:
        super().__init__(queue)
        self.connection = psycopg.connect(POSTGRES_CONN_STR)

    def clear_resources(self) -> None:
        self.connection.commit()
        self.connection.close()

    def process_row(self, row: dict) -> bool:
        with self.connection.cursor() as cur:
            cur.execute(
                """
                INSERT INTO eke_raw (msg_type, msg_name, vehicle, cabin, eke_timestamp, content)
                VALUES(%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
                """,
                (row["msg_type"],
                 row["msg_name"],
                 row["vehicle"],
                 row["cabin"],
                 row["eke_timestamp"],
                 json.dumps(row["content"]))
            )

        return True
