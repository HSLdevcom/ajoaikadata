from typing import Tuple

from ..types import AjoaikadataMsg, create_empty_msg

from .util.balisedirection import (
    BaliseDirectionCache,
    create_empty_balise_cache,
    add_msg_to_balise_cache,
    calculate_direction,
)
from .util.baliseparts import (
    BalisePartsCache,
    create_empty_parts_cache,
    add_msg_to_parts_cache,
    parse_balise_msg_from_parts,
)

from ..config import logger


def combine_balise_parts(
    parts_cache: BalisePartsCache, value: AjoaikadataMsg
) -> Tuple[BalisePartsCache, AjoaikadataMsg]:
    data = value["data"]

    # No balise message, skip
    if data["msg_type"] != 5:
        return parts_cache, value

    try:
        match data["content"]["transponder_msg_part"]:
            case 0:
                return add_msg_to_parts_cache(parts_cache, value), create_empty_msg()
            case 1:
                parts_cache = add_msg_to_parts_cache(parts_cache, value)
                try:
                    parsed_msg = parse_balise_msg_from_parts(parts_cache)
                    msg_to_send: AjoaikadataMsg = {"msgs": parts_cache["msg_refs"], "data": parsed_msg}
                except ValueError:
                    msg_to_send = create_empty_msg()
                return create_empty_parts_cache(), msg_to_send
            case _:
                raise ValueError("Unexpected msg part index.")
    except ValueError as e:
        logger.error(e)
        msg: AjoaikadataMsg = {"data": None, "msgs": parts_cache["msg_refs"]}

        return create_empty_parts_cache(), msg


def create_directions_for_balises(
    balise_cache: BaliseDirectionCache, value: AjoaikadataMsg
) -> Tuple[BaliseDirectionCache, AjoaikadataMsg]:
    data = value["data"]

    # No balise message, skip
    if data["msg_type"] != 5:
        return balise_cache, value

    balise_id = data["content"]["balise_id"]

    if balise_id == balise_cache["balise_id"]:
        # Second message with same balise_id, calculate direction
        balise_cache = add_msg_to_balise_cache(balise_cache, value)
        try:
            direction = calculate_direction(balise_cache)
            data = balise_cache["balises"][0]
            data["content"]["direction"] = direction
            msg_to_send: AjoaikadataMsg = {"msgs": balise_cache["msg_refs"], "data": data}
        except ValueError:
            msg_to_send = {"msgs": balise_cache["msg_refs"], "data": None}

        return create_empty_balise_cache(), msg_to_send

    if not balise_cache["balise_id"]:
        # First message with this balise_id, add to cache
        return add_msg_to_balise_cache(balise_cache, value), create_empty_msg()

    # First message with this balise_id, but cache is not empty
    # Send cache data and add new message to cache

    data = balise_cache["balises"][0]
    data["content"]["direction"] = 0
    msg_to_send = {"msgs": balise_cache["msg_refs"], "data": data}
    return add_msg_to_balise_cache(create_empty_balise_cache(), value), msg_to_send
