"""
Operations related to combine single balise messages from the same balise group as a one message with direction.
"""
from typing import TypeAlias

from ..util.ajoaikadatamsg import AjoaikadataMsg, create_empty_msg, calculate_time_diff

from ..util.config import logger

# The max difference (in seconds) for timestamps to be allowed for balise group messages to be considered in the same passage
BALISE_GROUP_MAX_MSG_TIME_DIFF = 30


BaliseDirectionCache: TypeAlias = dict[int, AjoaikadataMsg]


def create_empty_balise_cache() -> BaliseDirectionCache:
    """Create a new cache."""
    return {}


def _calculate_direction(balise_msg1: AjoaikadataMsg, balise_msg2: AjoaikadataMsg) -> AjoaikadataMsg:
    """Calculate direction from the content of balisecache data"""

    try:
        # Calculate direction based on balise data
        if balise_msg1["data"]["content"]["balise_cba"] == balise_msg2["data"]["content"]["balise_cba"]:
            raise ValueError(f"Balises have same direction: {balise_msg1}; {balise_msg2}")

        # Check the direction from the first balise. Direction 1 means the increasing cba direction and 2 the opposite.
        direction = 1 if balise_msg1["data"]["content"]["balise_cba"] == "1(2)" else 2

    except ValueError as e:
        logger.error(e)
        direction = 0

    # Use the first message as the base
    data_obj = balise_msg1["data"]
    # cba is not needed any more
    del data_obj["content"]["balise_cba"]
    data_obj["content"]["direction"] = direction

    combined_msg: AjoaikadataMsg = {"msgs": balise_msg1.get("msgs", []) + balise_msg2.get("msgs", []), "data": data_obj}

    return combined_msg


def create_directions_for_balises(
    balise_cache: BaliseDirectionCache, value: AjoaikadataMsg
) -> tuple[BaliseDirectionCache, AjoaikadataMsg]:
    data = value["data"]

    # No complete balise message, skip
    if data["msg_type"] != 5 or data.get("incomplete"):
        return balise_cache, value

    balise_id = data["content"]["balise_id"]
    prev_msg_with_same_id = balise_cache.get(balise_id)

    if prev_msg_with_same_id:
        time_diff = calculate_time_diff(prev_msg_with_same_id, value)

        # Check the time limit
        if abs(time_diff) < BALISE_GROUP_MAX_MSG_TIME_DIFF:
            # Reset cache
            del balise_cache[balise_id]
            # Combine message with direction. Pass messages in the order based on the time diff.
            combined_msg = (
                _calculate_direction(prev_msg_with_same_id, value)
                if time_diff > 0
                else _calculate_direction(value, prev_msg_with_same_id)
            )
            return balise_cache, combined_msg

        # Time diff was too big. Release the old message
        msg_to_send = prev_msg_with_same_id
        logger.warning(f"Balise direction could not be resolved for msg: {prev_msg_with_same_id}")
        msg_to_send["data"]["incomplete"] = True       

    else:
        msg_to_send = create_empty_msg()

    # Store balise data to the cache
    balise_cache[balise_id] = value
    return balise_cache, msg_to_send
