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
    payload = bytes.fromhex(raw_data)

    data = EKE_SCHEMA.parse_content(payload)

    if not data:
        return None

    if topic_name:
        data["vehicle"] = topic_name.split("/")[3]
        data["cabin"] = topic_name.split("/")[4]

    return data
