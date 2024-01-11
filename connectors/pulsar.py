from datetime import datetime
import json
from typing import List

from bytewax.outputs import DynamicOutput, StatelessSink
from bytewax.inputs import DynamicInput, StatelessSource

import pulsar

from .types import BytewaxMsgFromPulsar

import config

PULSAR_CONN_STR, PULSAR_CLIENT_NAME = config.read_from_env(
    ("PULSAR_CONN_STR", "PULSAR_CLIENT_NAME"), defaults=("pulsar://pulsar:6650",)
)


class PulsarClient:
    def __init__(self, topic_name: str) -> None:
        self.client = pulsar.Client(PULSAR_CONN_STR)
        self.topic_name = topic_name
        self.producer: pulsar.Producer | None = None
        self.consumer: pulsar.Consumer | None = None

    def get_consumer(self, worker_index: int = 0) -> pulsar.Consumer:
        """Get the pulsar consumer. If not already initialized, subscribe the configured topic."""
        if not self.consumer:
            self.consumer = self.client.subscribe(
                self.topic_name,
                subscription_name=PULSAR_CLIENT_NAME,
                consumer_name=f"{PULSAR_CLIENT_NAME}-{worker_index}",
                consumer_type=pulsar.ConsumerType.KeyShared,
            )

        return self.consumer

    def get_producer(self, worker_index: int = 0) -> pulsar.Producer:
        """Get the pulsar producer. If not already initialized, create one for the configured topic."""
        if not self.producer:
            self.producer = self.client.create_producer(
                self.topic_name, producer_name=f"{PULSAR_CLIENT_NAME}-{worker_index}"
            )
        return self.producer

    def ack_msgs(self, msgs: List[str]) -> None:
        """Acknowledge the pulsar msgs related to the bytewax message"""
        if not self.consumer:
            raise TypeError("Client not configured as a consumer. Cannot ack the messages")

        for msg in msgs:
            self.consumer.acknowledge(pulsar.MessageId.deserialize(msg))

    def ack(self, data: BytewaxMsgFromPulsar):
        """Ack all related pulsar messages from a bytewax message."""
        key, value = data
        self.ack_msgs(value["msgs"])

    def close(self) -> None:
        """Shutdown the connections of the client."""
        self.client.close()


class PulsarSource(StatelessSource):
    def __init__(self, client: PulsarClient, worker_index: int):
        self.client = client
        self.consumer = self.client.get_consumer(worker_index)

    def next_awake(self) -> datetime | None:
        return None

    def next_batch(self) -> List[BytewaxMsgFromPulsar]:
        msgs: List[pulsar.Message] = self.consumer.batch_receive()
        return [
            (
                msg.partition_key(),
                {"msgs": [msg.message_id().serialize()], "data": json.loads(msg.data())},
            )
            for msg in msgs
        ]

    def close(self) -> None:
        self.consumer.close()


class PulsarInput(DynamicInput):
    def __init__(self, client: PulsarClient) -> None:
        super().__init__()
        self.client = client

    def build(self, worker_index: int, worker_count: int):
        return PulsarSource(self.client, worker_index)


class PulsarSink(StatelessSink):
    def __init__(self, client: PulsarClient, worker_index: int):
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

    def build(self, worker_index: int, worker_count: int):
        return PulsarSink(self.client, worker_index)
