import os
from typing import Tuple
from bytewax.dataflow import Dataflow

from connectors.pulsar import PulsarInput, PulsarOutput, PulsarClient
from ekeparser.ekeparser import parse_eke_data

from ekeparser.schemas.jkv_beacon import JKVBeaconDataSchema

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
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# read topic names from env
input_topic = os.environ.get("INPUT_TOPIC")
output_topic = os.environ.get("OUTPUT_TOPIC")

# test if input_topic and output_topic are set  (if not, raise error)
if not input_topic:
    raise ValueError("INPUT_TOPIC not set")
if not output_topic:
    raise ValueError("OUTPUT_TOPIC not set")

input_client = PulsarClient(input_topic)
output_client = PulsarClient(output_topic)

BEACON_DATA_SCHEMA = JKVBeaconDataSchema()


def parse_eke(msg):
    key, value = msg
    data = value.get("data")
    try:
        data = parse_eke_data(data.get("raw"), data.get("topic"))
    except ValueError:
        logger.error(f"Failed to parse eke data. Value was: {value}")
        data = None

    if not data:
        input_client.ack_msg(msg)
        return None

    value["data"] = data
    return (key, value)


def combine_balise_parts(parts_cache: BalisePartsCache, value: dict) -> Tuple[BalisePartsCache, dict | None]:
    data = value["data"]

    # No balise message, skip
    if data["msg_type"] != 5:
        return parts_cache, value

    try:
        match data["content"]["transponder_msg_part"]:
            case 0:
                add_msg_to_parts_cache(parts_cache, value)
                return parts_cache, None
            case 1:
                add_msg_to_parts_cache(parts_cache, value)
                try:
                    parsed_msg = parse_balise_msg_from_parts(parts_cache)
                    msg_to_send = {"msgs": parts_cache["msg_refs"], "data": parsed_msg}
                except ValueError:
                    msg_to_send = None
                return create_empty_parts_cache(), msg_to_send
            case _:
                raise ValueError("Unexpected msg part index.")
    except ValueError as e:
        logger.error(e)
        for msg in parts_cache["msg_refs"]:
            input_client.ack_msg(msg)
        return create_empty_parts_cache(), None

def create_directions_for_balises(
    balise_cache: BaliseDirectionCache, value: dict
) -> Tuple[BaliseDirectionCache, dict | None]:
    data = value["data"]

    # No balise message, skip
    if data["msg_type"] != 5:
        return balise_cache, value

    balise_id = data["content"]["balise_id"]

    if balise_id == balise_cache["balise_id"]:
        # Second message with same balise_id, calculate direction
        add_msg_to_balise_cache(balise_cache, value)
        try:
            direction = calculate_direction(balise_cache)
            data = balise_cache["balises"][0]
            data["content"]["direction"] = direction
            msg_to_send = {"msgs": balise_cache["msg_refs"], "data": data}
        except ValueError:
            msg_to_send = None

        balise_cache = create_empty_balise_cache()
        return balise_cache, msg_to_send

    if not balise_cache["balise_id"]:
        # First message with this balise_id, add to cache
        add_msg_to_balise_cache(balise_cache, value)
        return balise_cache, None

    # First message with this balise_id, but cache is not empty
    # Send cache data and add new message to cache

    data = balise_cache["balises"][0]
    data["content"]["direction"] = 0
    msg_to_send = {"msgs": balise_cache["msg_refs"], "data": data}
    balise_cache = create_empty_balise_cache()
    add_msg_to_balise_cache(balise_cache, value)
    return balise_cache, msg_to_send


def filter_none(data):
    key, msg = data
    if not msg:
        return None
    return data


flow = Dataflow()
flow.input("inp", PulsarInput(input_client))
flow.filter_map(parse_eke)
flow.stateful_map("balise_parts", lambda: create_empty_parts_cache(), combine_balise_parts)
flow.filter_map(filter_none)
flow.stateful_map("balise_direction", lambda: create_empty_balise_cache(), create_directions_for_balises)
flow.filter_map(filter_none)
flow.output("out", PulsarOutput(output_client))
flow.inspect(input_client.ack_msg)
