from datetime import datetime
import json

from bytewax.outputs import PartitionedOutput, DynamicOutput, StatelessSink
from bytewax.inputs import PartitionedInput, DynamicInput, StatefulSource, StatelessSource

import pulsar


def handle_pulsar_msg(client: "PulsarClient", ack_filtered=True):
    def actual_decorator(fn):
        def decorated(*args):
            args_count = len(args)

            if args_count == 1:
                key, msg = args[0]
                data = fn(msg.get("data"))

            elif args_count == 2:
                [key, msg] = args
                key, data = fn(key, msg.get("data"))

            else:
                raise TypeError("Decorated function not supported. (Invalid amount of args.)")

            if not data:
                if ack_filtered:
                    client.ack_msg((key,msg))
                    return None
                return key, None
            msg["data"] = data
            return key, msg

        return decorated

    return actual_decorator


def prepare_for_pulsar(msg):
    return {"data": msg}


class PulsarClient:
    def __init__(self, topic_name: str) -> None:
        self.client = pulsar.Client("pulsar://pulsar:6650")
        self.topic_name = topic_name
        self.producer = None
        self.consumer = None

    def get_consumer(self):
        if not self.consumer:
            self.consumer = self.client.subscribe(self.topic_name, subscription_name="my-sub")

        return self.consumer

    def get_producer(self):
        if not self.producer:
            self.producer = self.client.create_producer(self.topic_name)
        return self.producer

    def ack_msg(self, msg):
        key, content = msg
        if not self.consumer:
            raise TypeError("Client not configured as a consumer. Cannot ack the messages")
        message_objects = content.get("msgs")
        if len(message_objects) > 1:
            print(f"Acking multiple messages {len(message_objects)}")
        for msg_obj in message_objects:
            self.consumer.acknowledge(msg_obj)

    def close(self):
        self.client.close()


class PulsarSource(StatelessSource):
    def __init__(self, client: PulsarClient):
        self.client = client
        self.consumer = self.client.get_consumer()

    def next_awake(self) -> datetime | None:
        return None

    def next_batch(self):
        msg = self.consumer.receive()

        if msg is None:
            return []
        return [(msg.partition_key(), {"msgs": [msg], "data": json.loads(msg.data())})]

    def close(self):
        self.client.close()


class PulsarInput(DynamicInput):
    def __init__(self, client: PulsarClient) -> None:
        super().__init__()
        self.client = client

    def build(self, worker_index, worker_count):
        return PulsarSource(self.client)


class PulsarSink(StatelessSink):
    def __init__(self, client: PulsarClient):
        self.client = client
        self.producer = self.client.get_producer()

    def write_batch(self, data):
        for msg in data:
            key, content = msg
            msg_data = json.dumps(content.get("data"), default=str)
            self.producer.send(msg_data.encode("utf-8"), partition_key=key)

    def close(self):
        self.client.close()


class PulsarOutput(DynamicOutput):
    def __init__(self, client: PulsarClient) -> None:
        super().__init__()
        self.client = client

    def build(self, worker_index, worker_count):
        return PulsarSink(self.client)
