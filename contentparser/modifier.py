import os
from bytewax.dataflow import Dataflow

from connectors.pulsar import PulsarInput, PulsarOutput, PulsarClient
from ekeparser.ekeparser import parse_eke_data

from ekeparser.schemas.jkv_beacon import JKVBeaconDataSchema

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


class BalisePartCombiner:
    def __init__(self) -> None:
        self.msg_refs: list = []
        self.first: dict = None
        self.second: dict = None
        self.parsed: dict = None

    def add_msg(self, value):
        data = value.get("data")
        msgs = value.get("msgs")
        match data["content"]["transponder_msg_part"]:
            case 0:
                self.first = data
            case 1:
                self.second = data
            case _:
                raise ValueError("Unexpected msg part index.")
        self.msg_refs += msgs

    def parse(self):
        if not self.first or not self.second:
            raise ValueError("Missing balise parts")

        payload = self.first["content"]["content"] + self.second["content"]["content"]
        data = BEACON_DATA_SCHEMA.parse_content(payload)

        data_obj = self.first.copy()
        data_obj["content"].pop("transponder_msg_part", None)
        data_obj["content"].pop("msg_index", None)
        data_obj["content"].pop("content", None)

        # spread data in data_obj.content
        data_obj["content"].update(data)

        self.parsed = data_obj


def parse_eke(msg):
    key, value = msg
    data = value.get("data")
    data = parse_eke_data(data.get("raw"), data.get("topic"))

    if not data:
        input_client.ack_msg(msg)
        return None

    value["data"] = data
    return (key, value)


def combine_balise_parts(parts_cache: BalisePartCombiner, value):
    data = value.get("data")

    # No balise message, skip
    if data["msg_type"] != 5:
        return parts_cache, value

    match data["content"]["transponder_msg_part"]:
        case 0:
            parts_cache.add_msg(value)
            return parts_cache, None
        case 1:
            parts_cache.add_msg(value)
            try:
                parts_cache.parse()
                msg_to_send = {"msgs": parts_cache.msg_refs, "data": parts_cache.parsed}
            except ValueError:
                msg_to_send = None
            return BalisePartCombiner(), msg_to_send

        case _:
            raise ValueError("Unexpected msg part index.")


class BaliseCache:
    def __init__(self):
        self.msg_refs = []
        self.balises = []
        self.balise_ids = []

    def add_msg(self, value):
        data = value.get("data")
        balise_id = data["content"]["balise_id"]
        self.msg_refs += value.get("msgs")
        self.balises.append(data)
        self.balise_ids.append(balise_id)

    def is_complete(self):
        return len(self.balises) == 2

    def calculate_direction(self):
        if len(self.balises) != 2:
            raise ValueError("Balise cache is not complete")

        balise1 = self.balises[0]
        balise2 = self.balises[1]

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


def create_directions_for_balises(balise_cache, value: dict):
    data = value.get("data")

    # No balise message, skip
    if data["msg_type"] != 5:
        return balise_cache, value

    balise_id = data["content"]["balise_id"]

    if balise_id in balise_cache.balise_ids:
        # Second message with same balise_id, calculate direction
        balise_cache.add_msg(value)
        try:
            direction = balise_cache.calculate_direction()
            data = balise_cache.balises[0]
            data["content"]["direction"] = direction
            msg_to_send = {"msgs": balise_cache.msg_refs, "data": data}
        except ValueError:
            msg_to_send = None

        balise_cache = BaliseCache()
        return balise_cache, msg_to_send

    if not balise_cache.balise_ids:
        # First message with this balise_id, add to cache
        balise_cache.add_msg(value)
        return balise_cache, None

    # First message with this balise_id, but cache is not empty
    # Send cache data and add new message to cache

    data = balise_cache.balises[0]
    data["content"]["direction"] = 0
    msg_to_send = {"msgs": balise_cache.msg_refs, "data": data}
    balise_cache = BaliseCache()
    balise_cache.add_msg(value)
    return balise_cache, msg_to_send


def filter_none(data):
    key, msg = data
    if not msg:
        return None
    return data


flow = Dataflow()
flow.input("inp", PulsarInput(input_client))
flow.filter_map(parse_eke)
flow.stateful_map("balise", lambda: BalisePartCombiner(), combine_balise_parts)
flow.filter_map(filter_none)
flow.stateful_map("balise_direction", lambda: BaliseCache(), create_directions_for_balises)
flow.filter_map(filter_none)
flow.output("out", PulsarOutput(output_client))
flow.inspect(input_client.ack_msg)
