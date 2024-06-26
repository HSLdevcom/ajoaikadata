"""
Operations related to combine balise message parts into one.
# TODO: The order could be resolved already in udporder -module and the cache could be removed?
"""
from typing import TypeAlias

from ..util.ajoaikadatamsg import AjoaikadataMsg, create_empty_msg, calculate_time_diff
from ..ekeparser.schemas.jkv_beacon import JKVBeaconDataSchema

from ..util.config import logger

# The max difference (in seconds) for timestamps to be allowed for balise messages to be combined
BALISE_MAX_MSG_TIME_DIFF = 5


BEACON_DATA_SCHEMA = JKVBeaconDataSchema()


BalisePartsCache: TypeAlias = list[AjoaikadataMsg | None]


def create_empty_parts_cache() -> BalisePartsCache:
    return [None] * 256


def _parse_balise_msg_from_parts(msg_part1: AjoaikadataMsg, msg_part2: AjoaikadataMsg) -> AjoaikadataMsg:
    payload = msg_part1["data"]["content"]["content"] + msg_part2["data"]["content"]["content"]
    parsed_data = BEACON_DATA_SCHEMA.parse_content(payload)

    # Get the base content from the first part
    data_obj = msg_part1["data"]
    data_obj["content"] = parsed_data

    # Get the last arrived timestamp, because it triggers the msg forward
    data_obj["mqtt_timestamp"] = max(msg_part1["data"]["mqtt_timestamp"], msg_part2["data"]["mqtt_timestamp"])

    combined_msg: AjoaikadataMsg = {"msgs": msg_part1.get("msgs", []) + msg_part2.get("msgs", []), "data": data_obj}

    return combined_msg


def combine_balise_parts(
    parts_cache: BalisePartsCache | None, value: AjoaikadataMsg
) -> tuple[BalisePartsCache, AjoaikadataMsg]:
    if not parts_cache:
        parts_cache = create_empty_parts_cache()

    data = value["data"]

    if not data:
        return parts_cache, value

    # No balise message, skip
    if data["msg_type"] != 5:
        return parts_cache, value

    msg_index: int = data["content"]["msg_index"]
    msg_part: int = data["content"]["transponder_msg_part"]

    # Get the index of the other part of the message. Msg index is the loop of int between 1-255.
    # Note the "or" to change 0 to 1 or 255, because 0 is not used as an index.
    # Depending on the part, the next line gets either next or previous index.
    msg_index_pair = (msg_index + 1) % 256 or 1 if msg_part == 0 else (msg_index - 1) % 256 or 255

    paired_data = parts_cache[msg_index_pair]

    # Combine messages if there was a pair and their time difference fits in the limit.
    if paired_data and abs(calculate_time_diff(value, paired_data)) < BALISE_MAX_MSG_TIME_DIFF:
        # Reset cache
        parts_cache[msg_index_pair] = None
        # Combine message, pass parts in the right order
        combined_msg = (
            _parse_balise_msg_from_parts(value, paired_data)
            if msg_part == 0
            else _parse_balise_msg_from_parts(paired_data, value)
        )
        return parts_cache, combined_msg

    # Store the message to the cache, release the old message marked with incomplete=True
    old_cache = parts_cache[msg_index]
    if old_cache:
        msg_to_send = old_cache
        logger.warning(f"Single balise msg in the cache which could not be resolved: {old_cache}")
        msg_to_send["data"]["released_mqtt_timestamp"] = value["data"]["mqtt_timestamp"]
        msg_to_send["data"]["incomplete"] = True
    else:
        msg_to_send = create_empty_msg()
    parts_cache[msg_index] = value

    return parts_cache, msg_to_send
