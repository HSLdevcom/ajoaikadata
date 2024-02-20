from datetime import datetime
from functools import partial
from typing import Any, TypedDict, cast

from .general_parsers import timestamp_with_ms_parser
from .schema import Schema, FieldParser, DataContentParser

from .stadler_udp import StadlerUDPSchema
from .jkv_beacon import JKVBeaconSchema

from ..config import SCHEMA_SETTINGS

DATA_SCHEMA_CLASS_MAPPING = {
    1: StadlerUDPSchema,
    2: Schema,
    3: Schema,
    4: Schema,
    5: JKVBeaconSchema,
    6: Schema,
    7: Schema,
    8: Schema,
    9: Schema,
    10: Schema,
}

DATA_SCHEMA_MAPPING: dict[int, Schema] = {
    key: value(
        SCHEMA_SETTINGS.get(key, {}).get("limit_fields"),
        SCHEMA_SETTINGS.get(key, {}).get("limit_type"),
        SCHEMA_SETTINGS.get(key, {}).get("ignore"),
    )
    for key, value in DATA_SCHEMA_CLASS_MAPPING.items()
}


MSG_TYPES = {
    1: "UDP",
    2: "EKE id Struct",
    3: "EKE JKV status",
    4: "EKE JKV event",
    5: "EKE JKV Beacon",
    6: "EKE JKV Train Msg",
    7: "EKE JKV Fault Msg",
    8: "EKE JKV Pressure sensor error",
    9: "EKE JKV Serial link CRC error",
    10: "EKE JKV Time change",
}


def header_parser(content: bytes) -> tuple[int, str, int, bool]:
    """16 bytes to be parsed"""
    head = int.from_bytes(content, "big")
    msg_type = head & 0x1F  # First 5 bits
    msg_version = head >> 5 & 0x3FF  # Next 10 bits
    ntp_time_valid = bool(head >> 15)  # The last bit

    msg_name = MSG_TYPES.get(msg_type, "Unknown")

    return msg_type, msg_name, msg_version, ntp_time_valid


class EKEMessageType(TypedDict):
    msg_type: int
    msg_name: str
    msg_version: int
    ntp_time_valid: bool
    eke_timestamp: datetime
    ntp_timestamp: datetime
    content: Any


class EKEMessageSchema(Schema):
    FIELDS = [
        FieldParser(["msg_type", "msg_name", "msg_version", "ntp_time_valid"], 0, 1, header_parser),
        FieldParser(["eke_timestamp"], 2, 6, partial(timestamp_with_ms_parser, endian="big", use_tz=False)),
        FieldParser(["ntp_timestamp"], 7, 11, partial(timestamp_with_ms_parser, endian="big")),
    ]
    DATA_CONTENT = DataContentParser(
        12,
        DATA_SCHEMA_MAPPING,
        "msg_type",
    )

    def parse_content(self, content: bytes) -> EKEMessageType | None:
        return cast(EKEMessageType, super().parse_content(content))
