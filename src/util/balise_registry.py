""" Module to read balise registry """

import csv
from typing import Dict

# open csv file balise_registry.csv and read it into a dict using csv.DictReader
# dict key is balise, value is dict with other data
balise_registry: Dict[str, dict] = {}

# TODO: Parametrize csv location!
with open("/bytewax/app/util/balise_registry.csv", "r", newline="") as f:
    reader = csv.DictReader(f, delimiter=",")
    for row in reader:
        balise_registry[f"{row['balise']}_{row['direction']}"] = row
