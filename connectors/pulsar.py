from datetime import datetime
import json
import os

from bytewax.outputs import PartitionedOutput, DynamicOutput, StatelessSink
from bytewax.inputs import PartitionedInput, DynamicInput, StatefulSource, StatelessSource

import pulsar

# get subscription name from env, if not set, raise error
subscription_name = os.environ.get("SUBSCRIPTION_NAME")
if not subscription_name:
    raise ValueError("SUBSCRIPTION_NAME not set")


class PulsarClient:
    def __init__(self, topic_name: str) -> None:
        self.client = pulsar.Client("pulsar://pulsar:6650")
        self.topic_name = topic_name
        self.producer = None
        self.consumer = None

    def get_consumer(self, worker_index = 0):
        if not self.consumer:
            self.consumer = self.client.subscribe(self.topic_name, subscription_name=subscription_name, consumer_name=f"{subscription_name}-{worker_index}", consumer_type=pulsar.ConsumerType.KeyShared)

        return self.consumer

    def get_producer(self, worker_index = 0):
        if not self.producer:
            self.producer = self.client.create_producer(self.topic_name, producer_name=f"{subscription_name}-{worker_index}")
        return self.producer

    def ack_msg(self, msg):
        key, content = msg
        if not self.consumer:
            raise TypeError("Client not configured as a consumer. Cannot ack the messages")

        message_objects = content.get("msgs")

        for msg_obj in message_objects:
            self.consumer.acknowledge(pulsar.MessageId.deserialize(msg_obj))

    def close(self):
        self.client.close()


class PulsarSource(StatelessSource):
    def __init__(self, client: PulsarClient, worker_index):
        self.client = client
        self.consumer = self.client.get_consumer(worker_index)

    def next_awake(self) -> datetime | None:
        return None

    def next_batch(self):
        msgs = self.consumer.batch_receive()

        if not msgs:
            return []
        return [(msg.partition_key(), {"msgs": [msg.message_id().serialize()], "data": json.loads(msg.data())}) for msg in msgs]

    def close(self):
        self.consumer.close()


class PulsarInput(DynamicInput):
    def __init__(self, client: PulsarClient) -> None:
        super().__init__()
        self.client = client

    def build(self, worker_index, worker_count):
        return PulsarSource(self.client, worker_index)


class PulsarSink(StatelessSink):
    def __init__(self, client: PulsarClient, worker_index):
        self.client = client
        self.producer = self.client.get_producer(worker_index)

    def write_batch(self, data):
        for msg in data:
            key, content = msg
            msg_data = json.dumps(content.get("data"), default=str)
            self.producer.send(msg_data.encode("utf-8"), partition_key=key)

    def close(self):
        self.producer.flush()
        self.producer.close()


class PulsarOutput(DynamicOutput):
    def __init__(self, client: PulsarClient) -> None:
        super().__init__()
        self.client = client

    def build(self, worker_index, worker_count):
        return PulsarSink(self.client, worker_index)
