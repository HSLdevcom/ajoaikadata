from typing import Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow

from connectors.pulsar import PulsarInput, PulsarOutput, PulsarClient
from connectors.types import BytewaxMsgFromPulsar, PulsarMsg
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

from config import logger, read_from_env

input_topic, output_topic = read_from_env(("PULSAR_INPUT_TOPIC", "PULSAR_OUTPUT_TOPIC"))

input_client = PulsarClient(input_topic)
output_client = PulsarClient(output_topic)

BEACON_DATA_SCHEMA = JKVBeaconDataSchema()


def parse_eke(msg: BytewaxMsgFromPulsar) -> BytewaxMsgFromPulsar | None:
    key, value = msg
    data = value["data"]
    try:
        data = parse_eke_data(data["raw"], data["topic"])
    except ValueError:
        logger.error(f"Failed to parse eke data. Value was: {value}")
        data = None

    if not data:
        input_client.ack_msgs(value["msgs"])
        return None

    value["data"] = data  # assign parsed data to msg
    return (key, value)


def combine_balise_parts(parts_cache: BalisePartsCache, value: PulsarMsg) -> Tuple[BalisePartsCache, PulsarMsg | None]:
    data = value["data"]

    # No balise message, skip
    if data["msg_type"] != 5:
        return parts_cache, value

    try:
        match data["content"]["transponder_msg_part"]:
            case 0:
                return add_msg_to_parts_cache(parts_cache, value), None
            case 1:
                parts_cache = add_msg_to_parts_cache(parts_cache, value)
                try:
                    parsed_msg = parse_balise_msg_from_parts(parts_cache)
                    msg_to_send: PulsarMsg | None = {"msgs": parts_cache["msg_refs"], "data": parsed_msg}
                except ValueError:
                    msg_to_send = None
                return create_empty_parts_cache(), msg_to_send
            case _:
                raise ValueError("Unexpected msg part index.")
    except ValueError as e:
        logger.error(e)
        input_client.ack_msgs(parts_cache["msg_refs"])

        return create_empty_parts_cache(), None


def create_directions_for_balises(
    balise_cache: BaliseDirectionCache, value: PulsarMsg
) -> Tuple[BaliseDirectionCache, PulsarMsg | None]:
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
            msg_to_send: PulsarMsg | None = {"msgs": balise_cache["msg_refs"], "data": data}
        except ValueError:
            msg_to_send = None

        return create_empty_balise_cache(), msg_to_send

    if not balise_cache["balise_id"]:
        # First message with this balise_id, add to cache
        return add_msg_to_balise_cache(balise_cache, value), None

    # First message with this balise_id, but cache is not empty
    # Send cache data and add new message to cache

    data = balise_cache["balises"][0]
    data["content"]["direction"] = 0
    msg_to_send = {"msgs": balise_cache["msg_refs"], "data": data}
    return add_msg_to_balise_cache(create_empty_balise_cache(), value), msg_to_send


def filter_none(data):  # TODO: Add typing
    key, msg = data
    if not msg:
        return None
    return data


flow = Dataflow("contentparser")
stream = op.input("contentparser_in", flow, PulsarInput(input_client))
eke_stream = op.filter_map("parse_eke", stream, parse_eke)
eke_stream_with_balises = op.stateful_map(
    "combine_balises", eke_stream, lambda: create_empty_parts_cache(), combine_balise_parts
)
eke_stream_with_balises = op.filter_map("combine_balises_filtered", eke_stream_with_balises, filter_none)
eke_stream_with_balises_dirs = op.stateful_map(
    "balise_direction", eke_stream_with_balises, lambda: create_empty_balise_cache(), create_directions_for_balises
)
eke_stream_with_balises_dirs = op.filter_map("balise_direction_filtered", eke_stream_with_balises_dirs, filter_none)
op.output("contentparser_out", eke_stream_with_balises_dirs, PulsarOutput(output_client))
op.inspect("contentparser_ack", eke_stream_with_balises_dirs, input_client.ack)
