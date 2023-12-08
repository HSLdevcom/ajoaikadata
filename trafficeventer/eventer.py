import os
import csv

from bytewax.dataflow import Dataflow

from connectors.pulsar import PulsarInput, PulsarOutput, PulsarClient


# read topic names from env
input_topic = os.environ.get("INPUT_TOPIC")
if not input_topic:
    raise ValueError("INPUT_TOPIC not set")

output_topic = os.environ.get("OUTPUT_TOPIC")
if not output_topic:
    raise ValueError("OUTPUT_TOPIC not set")

input_client = PulsarClient(input_topic)
output_client = PulsarClient(output_topic)


# open csv file balise_registry.csv and read it into a dict using csv.DictReader
# dict key is balise, value is dict with other data
balise_registry = {}

with open("/bytewax/app/balise_registry.csv", "r", newline="") as f:
    reader = csv.DictReader(f, delimiter=",")
    for row in reader:
        balise_registry[f"{row['balise']}_{row['direction']}"] = row



def create_event(data, event_type, state):
    new_data = {
        "vehicle": data["vehicle"],
        "eke_timestamp": data["eke_timestamp"],
        "event_type": event_type,
        "state": state,
    }
    return new_data


def eventer(last_state, value):
    data = value.get("data")

    if data["msg_type"] == 1:
        doors_open = data["content"]["doors_open"]
        standstill = data["content"]["standstill"]

        # initialize last_state if it is None
        if last_state["doors_open"] is None:
            last_state["doors_open"] = doors_open
        if last_state["standstill"] is None:
            last_state["standstill"] = standstill

        if doors_open != last_state["doors_open"]:
            last_state["doors_open"] = doors_open
            value["data"] = create_event(data, "doors_opened" if doors_open else "doors_closed", last_state)
            return last_state.copy(), value

        if standstill != last_state["standstill"]:
            last_state["standstill"] = standstill
            value["data"] = create_event(data, "stopped" if standstill else "moving", last_state)
            return last_state.copy(), value

        return last_state, None

    elif data["msg_type"] == 5:
        balise_id = data["content"]["balise_id"]
        direction = data["content"]["direction"]
        balise_key = f"{balise_id}_{direction}"

        balise_data = balise_registry.get(balise_key)
        if balise_data:

            # check if any of the last_station_event data has changed
            if (
                last_state["last_station_event"]["station"] != balise_data["station"]
                or last_state["last_station_event"]["track"] != balise_data["track"]
                or last_state["last_station_event"]["event"] != balise_data["type"]
            ):
                # if changed, update last_station_event data and send event
                last_station_event = {
                    "station": balise_data["station"],
                    "track": balise_data["track"],
                    "event": balise_data["type"],
                    "triggered_by": balise_key,
                }
                last_state["last_station_event"] = last_station_event

                value["data"] = create_event(data, balise_data["type"], last_state)

                return last_state.copy(), value

        return last_state, None

    else:
        return last_state, None


def filter_none(data):
    key, msg = data
    if not msg:
        return None
    return data


def print_data(data):
    key, msg = data
    msg_data = msg.get("data")
    # if msg_data["event_type"] in ["ARRIVAL", "DEPARTURE"]:
    print(msg_data)


flow = Dataflow()
flow.input("inp", PulsarInput(input_client))
flow.stateful_map(
    "eventer",
    lambda: {
        "doors_open": None,
        "standstill": None,
        "last_station_event": {"station": None, "track": None, "event": None, "triggered_by": None},
    },
    eventer,
)
flow.filter_map(filter_none)
flow.inspect(print_data)
flow.output("out", PulsarOutput(output_client))
flow.inspect(input_client.ack_msg)