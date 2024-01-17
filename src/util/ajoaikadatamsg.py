"""
Module to contain type definitions and helper functions for the messages that are processed in the dataflow.
"""
from datetime import datetime
from typing import Any, NotRequired, TypedDict


class AjoaikadataMsg(TypedDict):
    data: Any
    msgs: NotRequired[list[str]]


AjoaikadataMsgWithKey = tuple[str, AjoaikadataMsg]


def create_empty_msg(with_refs: list = []) -> AjoaikadataMsg:
    return {"data": None, "msgs": with_refs}


def calculate_time_diff(msg1: AjoaikadataMsg, msg2: AjoaikadataMsg) -> float:
    """Compare ntp_timestamps of two messages."""
    tst1: datetime = msg1["data"]["ntp_timestamp"]
    tst2: datetime = msg2["data"]["ntp_timestamp"]

    return (tst2 - tst1).total_seconds()
