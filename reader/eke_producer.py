import argparse
import csv
import json
import time

import pulsar


def main(file_name: str) -> None:
    client = pulsar.Client("pulsar://localhost:6650")
    producer = client.create_producer("raw")

    print(f"Reading file {file_name}")
    with open(file_name, "r") as f:
        eke_reader = csv.DictReader(f)
        for line in eke_reader:
            topic_name = line["mqtt_topic"]
            vehicle = topic_name.split("/")[3]

            data = {"raw": line["raw_data"], "topic": topic_name, "vehicle": vehicle}

            # print(data)
            producer.send(json.dumps(data).encode("utf-8"),) # partition_key=vehicle)
            # time.sleep(0.2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read eke .csv file to .json format.")
    parser.add_argument("file")
    args = parser.parse_args()
    main(args.file)
