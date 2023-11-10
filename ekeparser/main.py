import argparse
import csv
from queue import Queue
from threading import Thread

from ekeparser import parse_eke_data
from sink import ConsoleSink, DummySink, PostgresSink

from config import SINK

AVAILABLE_SINKS = {
    "ConsoleSink": ConsoleSink,
    "DummySink": DummySink,
    "PostgresSink": PostgresSink
}


def main(file_name: str) -> None:
    q = Queue(maxsize=1000)

    Sink = AVAILABLE_SINKS.get(SINK)

    if not Sink:
        raise Exception("Sink cannot be configured! Check config.py.")

    sink = Sink(q)

    Thread(target=sink.process, daemon=True).start()

    print(f"Reading file {file_name}")
    with open(file_name, 'r') as f:
        eke_reader = csv.DictReader(f)
        for line in eke_reader:
            data = parse_eke_data(line["raw_data"], line["mqtt_topic"])
            q.put(data)

    q.join()

    sink.clear_resources()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Read eke .csv file to .json format.")
    parser.add_argument('file')
    args = parser.parse_args()
    main(args.file)
