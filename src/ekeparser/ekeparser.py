"""
This module is for converting eke raw message to JSON format
The code is based on the parser made by Timo Töyry
"""
from .schemas import EKEMessageSchema

from .config import HEADER_SETTINGS


EKE_SCHEMA = EKEMessageSchema(
    HEADER_SETTINGS.get("limit_fields"), "exclude" if HEADER_SETTINGS.get("limit_type") == "exclude" else None
)


def parse_eke_data(raw_data: str, topic_name: str | None = None) -> dict | None:
    """Parse Eke message from binary data to dict"""
    if not topic_name:
        return None

    topic_parts = topic_name.split("/")

    topic_msg_type = topic_parts[5]

    # connectionStatus is string, do not parse it
    if topic_msg_type == "connectionStatus":
        return None

    payload = bytes.fromhex(raw_data)

    data = EKE_SCHEMA.parse_content(payload)

    if not data:
        return None

    data["vehicle"] = topic_parts[3]

    return data
