"""
This module is for converting eke raw message to JSON format
The code is based on the parser made by Timo Töyry
"""

from datetime import datetime
from typing import TypedDict, NotRequired
from .schemas import EKEMessageSchema, EKEMessageType

from .config import HEADER_SETTINGS


EKE_SCHEMA = EKEMessageSchema(
    HEADER_SETTINGS.get("limit_fields"), "exclude" if HEADER_SETTINGS.get("limit_type") == "exclude" else None
)


def parse_topic(topic_name: str) -> tuple[str, str]:
    """Parse mqtt topic type and vehicle id from topic name"""
    topic_parts = topic_name.split("/")
    topic_msg_type = topic_parts[5]

    vehicle_id = topic_parts[3]

    return vehicle_id, topic_msg_type


def parse_eke_data(raw_data: str) -> EKEMessageType | None:
    """Parse Eke message from binary data to dict"""
    payload = bytes.fromhex(raw_data)
    return EKE_SCHEMA.parse_content(payload)
