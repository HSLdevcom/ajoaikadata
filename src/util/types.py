"""
Module to contain type definitions for the messages that are processed in the dataflow.
"""

from typing import Any, NotRequired, TypedDict


class AjoaikadataMsg(TypedDict):
    data: Any
    msgs: NotRequired[list[str]]


AjoaikadataMsgWithKey = tuple[str, AjoaikadataMsg]


def create_empty_msg() -> AjoaikadataMsg:
    return {"data": None, "msgs": []}
