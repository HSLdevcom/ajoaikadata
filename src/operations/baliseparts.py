"""
Operations related to combine balise message parts into one.
"""
from typing import TypeAlias

from ..util.types import AjoaikadataMsg, create_empty_msg
from ..ekeparser.schemas.jkv_beacon import JKVBeaconDataSchema

from ..util.config import logger


BEACON_DATA_SCHEMA = JKVBeaconDataSchema()


BalisePartsCache: TypeAlias = list[AjoaikadataMsg | None]


def create_empty_parts_cache() -> BalisePartsCache:
    return [None] * 256


def _parse_balise_msg_from_parts(msg_part1: AjoaikadataMsg, msg_part2: AjoaikadataMsg) -> AjoaikadataMsg:
    payload = msg_part1["data"]["content"]["content"] + msg_part2["data"]["content"]["content"]
    parsed_data = BEACON_DATA_SCHEMA.parse_content(payload)

    data_obj = msg_part1["data"]
    data_obj["content"] = parsed_data

    combined_msg: AjoaikadataMsg = {"msgs": msg_part1.get("msgs", []) + msg_part2.get("msgs", []), "data": data_obj}

    return combined_msg


def _check_time_diff(msg1: AjoaikadataMsg, msg2: AjoaikadataMsg) -> bool:
    """Compare ntp_timestamps of two messages. Return true if they are within 5 seconds. (Considered to be from the same balise)"""
    tst1 = msg1["data"]["ntp_timestamp"]
    tst2 = msg2["data"]["ntp_timestamp"]

    time_diff = abs((tst1 - tst2).total_seconds())
    return time_diff < 5


def combine_balise_parts(
    parts_cache: BalisePartsCache, value: AjoaikadataMsg
) -> tuple[BalisePartsCache, AjoaikadataMsg]:
    data = value["data"]

    # No balise message, skip
    if data["msg_type"] != 5:
        return parts_cache, value

    msg_index: int = data["content"]["msg_index"]
    msg_part: int = data["content"]["transponder_msg_part"]

    # Get the index of the other part of the message. Msg index is the loop of int between 0-255.
    # Depending on the part, the next line gets either next or previous index.
    msg_index_pair = msg_index + 1 & 255 if msg_part == 0 else msg_index - 1 & 255

    paired_data = parts_cache[msg_index_pair]

    if paired_data and _check_time_diff(value, paired_data):
        # Reset cache
        parts_cache[msg_index_pair] = None
        combined_msg = (
            _parse_balise_msg_from_parts(value, paired_data)
            if msg_part == 0
            else _parse_balise_msg_from_parts(paired_data, value)
        )
        return parts_cache, combined_msg

    else:
        # Store the message to the cache, release the old message marked with invalid=True
        old_cache = parts_cache[msg_index]
        if old_cache:
            msg_to_send = old_cache
            logger.warning(f"Single balise msg in the cache which could not be resolved: {old_cache}")
            msg_to_send["data"]["incomplete"] = True
        else:
            msg_to_send = create_empty_msg()
        parts_cache[msg_index] = value

        return parts_cache, msg_to_send
