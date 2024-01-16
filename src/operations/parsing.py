"""
Operations related to parsing messages.
"""
from datetime import datetime, timezone

from ..ekeparser.ekeparser import parse_eke_data
from ..util.types import AjoaikadataMsgWithKey

from ..util.config import logger


def csv_to_bytewax_msg(value) -> AjoaikadataMsgWithKey:
    topic_name = value["mqtt_topic"]
    vehicle = topic_name.split("/")[3]

    data = {"raw": value["raw_data"], "topic": topic_name, "vehicle": vehicle, "mqtt_timestamp": value["mqtt_timestamp"]}
    return vehicle, {"data": data}


def raw_msg_to_eke(msg: AjoaikadataMsgWithKey) -> AjoaikadataMsgWithKey:
    key, value = msg
    data = value["data"]
    mqtt_timestamp = datetime.fromisoformat(data["mqtt_timestamp"])
    try:
        data = parse_eke_data(data["raw"], data["topic"])
        if data:
            data["mqtt_timestamp"] = mqtt_timestamp
    except ValueError as e:
        logger.error(f"Failed to parse eke data.\n{e}\nValue was: {value}")
        data = None

    value["data"] = data  # assign parsed data to msg
    return (key, value)
