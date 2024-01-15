"""
Operations related to parsing messages.
"""

from ..ekeparser.ekeparser import parse_eke_data
from ..util.types import AjoaikadataMsgWithKey

from ..util.config import logger


def csv_to_bytewax_msg(value) -> AjoaikadataMsgWithKey:
    topic_name = value["mqtt_topic"]
    vehicle = topic_name.split("/")[3]

    data = {"raw": value["raw_data"], "topic": topic_name, "vehicle": vehicle}
    return vehicle, {"data": data}


def raw_msg_to_eke(msg: AjoaikadataMsgWithKey) -> AjoaikadataMsgWithKey:
    key, value = msg
    data = value["data"]
    try:
        data = parse_eke_data(data["raw"], data["topic"])
    except ValueError:
        logger.error(f"Failed to parse eke data. Value was: {value}")
        data = None

    value["data"] = data  # assign parsed data to msg
    return (key, value)
