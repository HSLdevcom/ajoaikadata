"""
Operations related to combine single balise messages from the same balise group as a one message with direction.
"""

from copy import deepcopy
from typing import List, Literal, Tuple, TypedDict

from ..util.ajoaikadatamsg import AjoaikadataMsg, create_empty_msg


class BaliseDirectionCache(TypedDict):
    """Dict which contains information from multiple balises for direction calculation."""

    msg_refs: List[str]
    balises: List[dict]
    balise_id: str | None


def create_empty_balise_cache() -> BaliseDirectionCache:
    """Create a new cache."""
    return {
        "msg_refs": [],
        "balises": [],
        "balise_id": None,
    }


def add_msg_to_balise_cache(balise_cache: BaliseDirectionCache, value: AjoaikadataMsg) -> BaliseDirectionCache:
    """Copy cache and add new msg to it."""
    balise_cache = deepcopy(balise_cache)
    data = value["data"]
    msgs = value.get("msgs", [])
    balise_id = data["content"]["balise_id"]
    balise_cache["msg_refs"] += msgs
    balise_cache["balises"].append(data)

    if balise_cache["balise_id"] is None:
        balise_cache["balise_id"] = balise_id
    elif balise_cache["balise_id"] != balise_id:
        raise ValueError(f"Trying to combine balises with different ids. ({balise_cache['balise_id']},{balise_id})")
    return balise_cache


def calculate_direction(balise_cache: BaliseDirectionCache) -> Literal[1, 2]:
    """Calculate direction from the content of balisecache data"""
    balises = balise_cache["balises"]
    if len(balises) != 2:
        raise ValueError("Balise cache is not complete")

    balise1 = balises[0]
    balise2 = balises[1]

    if not balise1 or not balise2:
        raise ValueError("Missing balise data in cache")

    # Calculate direction based on balise data
    if balise1["content"]["balise_cba"] == balise2["content"]["balise_cba"]:
        raise ValueError("Balises have same direction")

    if balise1["content"]["balise_cba"] == "1(2)":
        direction = 1
    else:
        direction = 2

    return direction


def create_directions_for_balises(
    balise_cache: BaliseDirectionCache, value: AjoaikadataMsg
) -> Tuple[BaliseDirectionCache, AjoaikadataMsg]:
    data = value["data"]

    # No balise message, skip
    if data["msg_type"] != 5 or data.get("incomplete"):
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
