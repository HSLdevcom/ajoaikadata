""" Module to read balise registry """

import csv
from typing import Dict

from .config import read_from_env

(BALISE_DATA_FILE,) = read_from_env(("BALISE_DATA_FILE",), defaults=("./src/util/balise_registry.csv",))

# open csv file balise_registry.csv and read it into a dict using csv.DictReader
# dict key is balise, value is dict with other data
balise_registry: Dict[str, dict] = {}

# TODO: Parametrize csv location!
with open(BALISE_DATA_FILE, "r", newline="") as f:
    reader = csv.DictReader(f, delimiter=",")
    for row in reader:
        # Store to dict
        balise_registry[f"{row['balise']}_{row['direction']}"] = row

# Get balise data as keys to set. This is used to check if the certain station-track-type-direction combination exists.
balise_data_set = {
    f"{val['station']}_{val['track']}_{val['type']}_{val['train_direction']}" for val in balise_registry.values()
}

# Iterate over registry to generate missing station-track -directions
for val in list(balise_registry.values()):
    val = val.copy()

    # Check the key from the balise, if the content is already there
    opposite_dir = "2" if val["direction"] == "1" else "1"
    key = f"{val['balise']}_{opposite_dir}"

    if not key in balise_registry:
        # Create an opposite station event. Directions and types are flipped.
        val["direction"] = "2" if val["direction"] == "1" else "1"
        val["type"] = "ARRIVAL" if val["type"] == "DEPARTURE" else "DEPARTURE"
        val["train_direction"] = "2" if val["train_direction"] == "1" else "1"

        # Still check if the new combination is already in registry. (We prefer the existing information)
        data_key = f"{val['station']}_{val['track']}_{val['type']}_{val['train_direction']}"

        if not data_key in balise_data_set:
            # Add suffix to recognize the generated one.
            val["train_direction"] += "_g"
            balise_registry[key] = val
