"""
Module to contain type definitions and helper functions for the messages that are processed in the dataflow.
"""

from datetime import datetime
from typing import Any, NotRequired, TypedDict

from ..ekeparser.ekeparser import EKEMessageType


class CSVRawMessage(TypedDict):
    raw: str
    topic: str
    vehicle: str
    mqtt_timestamp: str


class EKEMessageTypeWithMQTTDetails(EKEMessageType):
    vehicle: str
    mqtt_timestamp: datetime


class AjoaikadataMsg(TypedDict):
    data: EKEMessageTypeWithMQTTDetails | None
    msgs: NotRequired[list[str]]  # For Pulsar msg refs


class AjoaikadataRawMsg(TypedDict):
    data: CSVRawMessage


AjoaikadataRawMsgWithKey = tuple[str, AjoaikadataRawMsg]
AjoaikadataMsgWithKey = tuple[str, AjoaikadataMsg]


def create_empty_msg(with_refs: list = []) -> AjoaikadataMsg:
    return {"data": None, "msgs": with_refs}


def calculate_time_diff(msg1: AjoaikadataMsg, msg2: AjoaikadataMsg) -> float:
    """Compare ntp_timestamps of two messages."""
    tst1: datetime = msg1["data"]["ntp_timestamp"] if msg1["data"] else datetime.fromtimestamp(0)
    tst2: datetime = msg2["data"]["ntp_timestamp"] if msg2["data"] else datetime.fromtimestamp(0)

    return (tst2 - tst1).total_seconds()
