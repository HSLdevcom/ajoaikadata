from typing import List, Literal, TypedDict
from connectors.types import PulsarMsg

class BaliseDirectionCache(TypedDict):
    """Dict which contains information from multiple balises for direction calculation."""

    msg_refs: List
    balises: List[dict]
    balise_id: str | None


def create_empty_balise_cache() -> BaliseDirectionCache:
    """Create a new cache."""
    return {
        "msg_refs": [],
        "balises": [],
        "balise_id": None,
    }


def add_msg_to_balise_cache(balise_cache: BaliseDirectionCache, value: PulsarMsg):
    """Add new msg to the balise."""
    data = value["data"]
    balise_id = data["content"]["balise_id"]
    balise_cache["msg_refs"] += value["msgs"]
    balise_cache["balises"].append(data)

    if balise_cache["balise_id"] is None:
        balise_cache["balise_id"] = balise_id
    elif balise_cache["balise_id"] != balise_id:
        raise ValueError(f"Trying to combine balises with different ids. ({balise_cache['balise_id']},{balise_id})")


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
