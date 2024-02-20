"""
Operations related to parsing messages.
"""

from datetime import datetime

from ..ekeparser.ekeparser import parse_topic, parse_eke_data
from ..util.ajoaikadatamsg import (
    AjoaikadataMsgWithKey,
    AjoaikadataRawMsgWithKey,
    EKEMessageTypeWithMQTTDetails,
    CSVRawMessage,
)

from ..util.config import logger


def csv_to_bytewax_msg(value: dict) -> AjoaikadataRawMsgWithKey:
    topic_name = value["mqtt_topic"]
    vehicle = topic_name.split("/")[3]

    data: CSVRawMessage = {
        "raw": value["raw_data"],
        "topic": topic_name,
        "vehicle": vehicle,
        "mqtt_timestamp": value["mqtt_timestamp"],
    }
    return vehicle, {"data": data}


def raw_msg_to_eke(msg: AjoaikadataRawMsgWithKey) -> AjoaikadataMsgWithKey:
    key, value = msg
    data = value["data"]

    if not data:
        return (key, {"data": None})

    mqtt_timestamp = datetime.fromisoformat(data["mqtt_timestamp"])
    vehicle, msg_type = parse_topic(data["topic"])

    # Filter special case away. The message content should not be parsed.
    if msg_type == "connectionStatus":
        return (key, {"data": None})

    try:
        parsed = parse_eke_data(data["raw"])
        if parsed:
            result: EKEMessageTypeWithMQTTDetails = {**parsed, "mqtt_timestamp": mqtt_timestamp, "vehicle": vehicle}
            return (key, {"data": result})

        return (key, {"data": None})

    except Exception as e:
        logger.error(f"Failed to parse eke data.\n{e}\nValue was: {value}")
        return (key, {"data": None})
