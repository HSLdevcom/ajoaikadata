from copy import deepcopy
from typing import List, TypedDict

from ...connectors.types import PulsarMsg
from ...ekeparser.schemas.jkv_beacon import JKVBeaconDataSchema

BEACON_DATA_SCHEMA = JKVBeaconDataSchema()


class BalisePartsCache(TypedDict):
    msg_refs: List
    parts: List[dict]


def create_empty_parts_cache() -> BalisePartsCache:
    return {"msg_refs": [], "parts": []}


def add_msg_to_parts_cache(parts_cache: BalisePartsCache, value: PulsarMsg) -> BalisePartsCache:
    """Create copy of the cache and add new value"""
    parts_cache = deepcopy(parts_cache)
    data = value["data"]
    msgs = value["msgs"]
    match data["content"]["transponder_msg_part"]:
        case 0:
            if len(parts_cache["parts"]) != 0:
                raise ValueError(f"Unexpected msg part 1, there was already content in the cache.")
            parts_cache["parts"].append(data)
        case 1:
            if len(parts_cache["parts"]) != 1:
                raise ValueError(f"Unexpected msg part 2, there should be exact one part in the cache at this point.")
            parts_cache["parts"].append(data)
        case _:
            raise ValueError(f"Unexpected msg part index {data['content']['transponder_msg_part']}.")
    parts_cache["msg_refs"] += msgs
    return parts_cache


def parse_balise_msg_from_parts(parts_cache: BalisePartsCache) -> dict:
    if len(parts_cache["parts"]) != 2:
        raise ValueError(f"Wrong amount of balise parts. Should be 2 but was {len(parts_cache['parts'])}")

    first_msg = parts_cache["parts"][0]
    second_msg = parts_cache["parts"][1]

    payload = first_msg["content"]["content"] + second_msg["content"]["content"]
    data = BEACON_DATA_SCHEMA.parse_content(payload)

    data_obj = first_msg.copy()
    data_obj["content"].pop("transponder_msg_part", None)
    data_obj["content"].pop("msg_index", None)
    data_obj["content"].pop("content", None)

    # spread data in data_obj.content
    data_obj["content"].update(data)

    return data_obj
