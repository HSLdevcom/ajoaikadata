from datetime import datetime

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main

from ...src.operations.common import filter_none
from ...src.operations.stationevents import create_station_events, init_vehicle_station_cache
from ...src.util.ajoaikadatamsg import AjoaikadataMsgWithKey


def get_test_flow(test_input: list[AjoaikadataMsgWithKey], output: list[AjoaikadataMsgWithKey]) -> Dataflow:
    """Init test dataflow with operations"""
    flow = Dataflow("test_flow")
    (
        op.input("test_input", flow, TestingSource(test_input))
        .then(op.stateful_map, "station_creation", lambda: init_vehicle_station_cache(), create_station_events)
        .then(op.filter_map, "none_filter", filter_none)
        .then(op.output, "test_output", TestingSink(output))
    )
    return flow


def test_normal_event_order():
    """Station event works on expected event order"""
    input_data = [
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 0),
            "event_type": "arrival",
            "data": {"track": "11", "station": "Pasila asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 4),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5),
            "event_type": "departure",
            "data": {"track": "11", "station": "Pasila asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 0),
            "event_type": "arrival",
            "data": {"track": "4", "station": "Ilmala asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 4),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 5),
            "event_type": "departure",
            "data": {"track": "4", "station": "Ilmala asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 0),
            "event_type": "arrival",
            "data": {"track": "4", "station": "Huopalahti", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 4),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 5),
            "event_type": "departure",
            "data": {"track": "4", "station": "Huopalahti", "direction": "1"},
        },
    ]
    input_msgs: list[AjoaikadataMsgWithKey] = [("12", {"data": d}) for d in input_data]
    result: list[AjoaikadataMsgWithKey] = []
    flow = get_test_flow(input_msgs, result)
    run_main(flow)

    test_result = [v["data"] for k, v in result]

    assert test_result == [
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5),
            "station": "Pasila asema",
            "track": "11",
            "direction": "1",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 0, 1),
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 0, 3),
                "time_departed": datetime(2024, 1, 1, 0, 0, 4),
            },
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 5),
            "station": "Ilmala asema",
            "track": "4",
            "direction": "1",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 1, 1),
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 1, 3),
                "time_departed": datetime(2024, 1, 1, 0, 1, 4),
            },
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 5),
            "station": "Huopalahti",
            "track": "4",
            "direction": "1",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 2, 1),
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 2, 3),
                "time_departed": datetime(2024, 1, 1, 0, 2, 4),
            },
        },
    ]


def test_skipped_station():
    """Skipped station will not be recorded"""
    input_data = [
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 0),
            "event_type": "arrival",
            "data": {"track": "11", "station": "Pasila asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 4),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5),
            "event_type": "departure",
            "data": {"track": "11", "station": "Pasila asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 0),
            "event_type": "arrival",
            "data": {"track": "4", "station": "Ilmala asema", "direction": "1"},
        },
        # No stop here!
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 5),
            "event_type": "departure",
            "data": {"track": "4", "station": "Ilmala asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 0),
            "event_type": "arrival",
            "data": {"track": "4", "station": "Huopalahti", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 4),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 5),
            "event_type": "departure",
            "data": {"track": "4", "station": "Huopalahti", "direction": "1"},
        },
    ]
    input_msgs: list[AjoaikadataMsgWithKey] = [("12", {"data": d}) for d in input_data]
    result: list[AjoaikadataMsgWithKey] = []
    flow = get_test_flow(input_msgs, result)
    run_main(flow)

    test_result = [v["data"] for k, v in result]

    assert test_result == [
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5),
            "station": "Pasila asema",
            "track": "11",
            "direction": "1",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 0, 1),
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 0, 3),
                "time_departed": datetime(2024, 1, 1, 0, 0, 4),
            },
        },
        # Ilmala should not be recorded.
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 5),
            "station": "Huopalahti",
            "track": "4",
            "direction": "1",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 2, 1),
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 2, 3),
                "time_departed": datetime(2024, 1, 1, 0, 2, 4),
            },
        },
    ]


def test_missing_departure():
    """Station event creation works with missing departure event"""
    input_data = [
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 0),
            "event_type": "arrival",
            "data": {"track": "11", "station": "Pasila asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 4),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5),
            "event_type": "departure",
            "data": {"track": "11", "station": "Pasila asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 0),
            "event_type": "arrival",
            "data": {"track": "4", "station": "Ilmala asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 4),
            "event_type": "moving",
            "data": {},
        },
        # Missed departure event
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 0),
            "event_type": "arrival",
            "data": {"track": "4", "station": "Huopalahti", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 4),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 5),
            "event_type": "departure",
            "data": {"track": "4", "station": "Huopalahti", "direction": "1"},
        },
    ]
    input_msgs: list[AjoaikadataMsgWithKey] = [("12", {"data": d}) for d in input_data]
    result: list[AjoaikadataMsgWithKey] = []
    flow = get_test_flow(input_msgs, result)
    run_main(flow)

    test_result = [v["data"] for k, v in result]

    assert test_result == [
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5),
            "station": "Pasila asema",
            "track": "11",
            "direction": "1",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 0, 1),
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 0, 3),
                "time_departed": datetime(2024, 1, 1, 0, 0, 4),
            },
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 0),  # trigger time was the next arrival
            "station": "Ilmala asema",
            "track": "4",
            "direction": "1",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 1, 1),
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 1, 3),
                "time_departed": datetime(2024, 1, 1, 0, 1, 4),
            },
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 5),
            "station": "Huopalahti",
            "track": "4",
            "direction": "1",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 2, 1),
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 2, 3),
                "time_departed": datetime(2024, 1, 1, 0, 2, 4),
            },
        },
    ]


def test_missing_arrival():
    """Station event creation works with missing arrival event"""
    input_data = [
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 0),
            "event_type": "arrival",
            "data": {"track": "11", "station": "Pasila asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 4),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5),
            "event_type": "departure",
            "data": {"track": "11", "station": "Pasila asema", "direction": "1"},
        },
        # Missed arrival event
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 4),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 5),
            "event_type": "departure",
            "data": {"track": "4", "station": "Ilmala asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 0),
            "event_type": "arrival",
            "data": {"track": "4", "station": "Huopalahti", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 4),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 5),
            "event_type": "departure",
            "data": {"track": "4", "station": "Huopalahti", "direction": "1"},
        },
    ]
    input_msgs: list[AjoaikadataMsgWithKey] = [("12", {"data": d}) for d in input_data]
    result: list[AjoaikadataMsgWithKey] = []
    flow = get_test_flow(input_msgs, result)
    run_main(flow)

    test_result = [v["data"] for k, v in result]

    assert test_result == [
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5),
            "station": "Pasila asema",
            "track": "11",
            "direction": "1",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 0, 1),
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 0, 3),
                "time_departed": datetime(2024, 1, 1, 0, 0, 4),
            },
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 5),
            "station": "Ilmala asema",
            "track": "4",
            "direction": "1",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 1, 1),
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 1, 3),
                "time_departed": datetime(2024, 1, 1, 0, 1, 4),
            },
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 5),
            "station": "Huopalahti",
            "track": "4",
            "direction": "1",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 2, 1),
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 2, 3),
                "time_departed": datetime(2024, 1, 1, 0, 2, 4),
            },
        },
    ]


def test_multiple_door_events():
    """Last door close timestamp update works"""
    input_data = [
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 0),
            "event_type": "arrival",
            "data": {"track": "11", "station": "Pasila asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        # Doors are opened again
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 4),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 6),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 7),
            "event_type": "departure",
            "data": {"track": "11", "station": "Pasila asema", "direction": "1"},
        },
    ]
    input_msgs: list[AjoaikadataMsgWithKey] = [("12", {"data": d}) for d in input_data]
    result: list[AjoaikadataMsgWithKey] = []
    flow = get_test_flow(input_msgs, result)
    run_main(flow)

    test_result = [v["data"] for k, v in result]

    assert test_result == [
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 7),
            "station": "Pasila asema",
            "track": "11",
            "direction": "1",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 0, 1),
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 0, 5),  # Should be taken from the last close
                "time_departed": datetime(2024, 1, 1, 0, 0, 6),
            },
        }
    ]


def test_multiple_movements1():
    """Accidental movement of the train works"""
    input_data = [
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 0),
            "event_type": "arrival",
            "data": {"track": "11", "station": "Pasila asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 1),
            "event_type": "stopped",
            "data": {},
        },
        # Train moves again!
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 2),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 3),
            "event_type": "stopped",
            "data": {},
        },
        # Now the doors are opened. Last stop was the arrival time, not the first one.
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 4),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 6),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 7),
            "event_type": "departure",
            "data": {"track": "11", "station": "Pasila asema", "direction": "1"},
        },
    ]
    input_msgs: list[AjoaikadataMsgWithKey] = [("12", {"data": d}) for d in input_data]
    result: list[AjoaikadataMsgWithKey] = []
    flow = get_test_flow(input_msgs, result)
    run_main(flow)

    test_result = [v["data"] for k, v in result]

    assert test_result == [
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 7),
            "station": "Pasila asema",
            "track": "11",
            "direction": "1",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 0, 3),  # Should be taken from the last stop
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 0, 5),
                "time_departed": datetime(2024, 1, 1, 0, 0, 6),
            },
        }
    ]


def test_multiple_movements2():
    """Accidental movement of the train works"""
    input_data = [
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 0),
            "event_type": "arrival",
            "data": {"track": "11", "station": "Pasila asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        # Train moves again but does not still leave the station!
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 4),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 6),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 7),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 8),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 9),
            "event_type": "departure",
            "data": {"track": "11", "station": "Pasila asema", "direction": "1"},
        },
    ]
    input_msgs: list[AjoaikadataMsgWithKey] = [("12", {"data": d}) for d in input_data]
    result: list[AjoaikadataMsgWithKey] = []
    flow = get_test_flow(input_msgs, result)
    run_main(flow)

    test_result = [v["data"] for k, v in result]

    assert test_result == [
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 9),
            "station": "Pasila asema",
            "track": "11",
            "direction": "1",
            "data": {
                "time_arrived": datetime(
                    2024, 1, 1, 0, 0, 1
                ),  # Should be taken from the first stop, because doors were opened.
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 0, 7),
                "time_departed": datetime(2024, 1, 1, 0, 0, 8),
            },
        }
    ]


def test_departure_station():
    """Station event works for the first station"""
    input_data = [
        # No events from arrival.
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 0),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 1),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 2),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 3),
            "event_type": "departure",
            "data": {"track": "16", "station": "Helsinki asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 0),
            "event_type": "arrival",
            "data": {"track": "11", "station": "Pasila asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 4),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 5),
            "event_type": "departure",
            "data": {"track": "11", "station": "Pasila asema", "direction": "1"},
        },
    ]
    input_msgs: list[AjoaikadataMsgWithKey] = [("12", {"data": d}) for d in input_data]
    result: list[AjoaikadataMsgWithKey] = []
    flow = get_test_flow(input_msgs, result)
    run_main(flow)

    test_result = [v["data"] for k, v in result]

    assert test_result == [
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 3),
            "station": "Helsinki asema",
            "track": "16",
            "direction": "1",
            "data": {
                "time_arrived": None,
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 0, 1),
                "time_departed": datetime(2024, 1, 1, 0, 0, 2),
            },
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 5),
            "station": "Pasila asema",
            "track": "11",
            "direction": "1",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 1, 1),
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 1, 3),
                "time_departed": datetime(2024, 1, 1, 0, 1, 4),
            },
        },
    ]


def test_arrival_station():
    """Station event works for the last station"""
    input_data = [
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 0),
            "event_type": "arrival",
            "data": {"track": "10", "station": "Pasila asema", "direction": "2"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 4),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5),
            "event_type": "departure",
            "data": {"track": "10", "station": "Pasila asema", "direction": "2"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 0),
            "event_type": "arrival",
            "data": {"track": "16", "station": "Helsinki asema", "direction": "2"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        # No departure event. Cabin event triggers the station event.
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 4),
            "event_type": "cabin_changed",
            "data": {},
        },
    ]
    input_msgs: list[AjoaikadataMsgWithKey] = [("12", {"data": d}) for d in input_data]
    result: list[AjoaikadataMsgWithKey] = []
    flow = get_test_flow(input_msgs, result)
    run_main(flow)

    test_result = [v["data"] for k, v in result]

    assert test_result == [
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5),
            "station": "Pasila asema",
            "track": "10",
            "direction": "2",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 0, 1),
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 0, 3),
                "time_departed": datetime(2024, 1, 1, 0, 0, 4),
            },
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 4),  # Cabin change timestamp
            "station": "Helsinki asema",
            "track": "16",
            "direction": "2",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 1, 1),
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 1, 3),
                "time_departed": None,
            },
        },
    ]


def test_late_balise_events():
    """Station event works on if balise message comes from stream a bit late"""
    input_data = [
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 0),
            "event_type": "arrival",
            "data": {"track": "11", "station": "Pasila asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 4),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5),
            "event_type": "departure",
            "data": {"track": "11", "station": "Pasila asema", "direction": "1"},
        },
        # A bit latency! Arrival signal comes after stopped and doors. Note that the timestamp is logical, just the order is different.
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        # HERE!
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 0),
            "event_type": "arrival",
            "data": {"track": "4", "station": "Ilmala asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 4),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 5),
            "event_type": "departure",
            "data": {"track": "4", "station": "Ilmala asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 0),
            "event_type": "arrival",
            "data": {"track": "4", "station": "Huopalahti", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 4),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 5),
            "event_type": "departure",
            "data": {"track": "4", "station": "Huopalahti", "direction": "1"},
        },
    ]
    input_msgs: list[AjoaikadataMsgWithKey] = [("12", {"data": d}) for d in input_data]
    result: list[AjoaikadataMsgWithKey] = []
    flow = get_test_flow(input_msgs, result)
    run_main(flow)

    test_result = [v["data"] for k, v in result]

    assert test_result == [
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5),
            "station": "Pasila asema",
            "track": "11",
            "direction": "1",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 0, 1),
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 0, 3),
                "time_departed": datetime(2024, 1, 1, 0, 0, 4),
            },
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 5),
            "station": "Ilmala asema",
            "track": "4",
            "direction": "1",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 1, 1),
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 1, 3),
                "time_departed": datetime(2024, 1, 1, 0, 1, 4),
            },
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 5),
            "station": "Huopalahti",
            "track": "4",
            "direction": "1",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 2, 1),
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 2, 3),
                "time_departed": datetime(2024, 1, 1, 0, 2, 4),
            },
        },
    ]


def test_late_balise_events2():
    """Station event works on if balise message comes from stream a bit more late"""
    input_data = [
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 0),
            "event_type": "arrival",
            "data": {"track": "11", "station": "Pasila asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 4),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5),
            "event_type": "departure",
            "data": {"track": "11", "station": "Pasila asema", "direction": "1"},
        },
        # Latency! Arrival signal comes after we already have left the station. Note that the timestamp is logical, just the order is different.
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 4),
            "event_type": "moving",
            "data": {},
        },
        # HERE!
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 0),
            "event_type": "arrival",
            "data": {"track": "4", "station": "Ilmala asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 5),
            "event_type": "departure",
            "data": {"track": "4", "station": "Ilmala asema", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 0),
            "event_type": "arrival",
            "data": {"track": "4", "station": "Huopalahti", "direction": "1"},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 1),
            "event_type": "stopped",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 2),
            "event_type": "doors_opened",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 3),
            "event_type": "doors_closed",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 4),
            "event_type": "moving",
            "data": {},
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 5),
            "event_type": "departure",
            "data": {"track": "4", "station": "Huopalahti", "direction": "1"},
        },
    ]
    input_msgs: list[AjoaikadataMsgWithKey] = [("12", {"data": d}) for d in input_data]
    result: list[AjoaikadataMsgWithKey] = []
    flow = get_test_flow(input_msgs, result)
    run_main(flow)

    test_result = [v["data"] for k, v in result]

    assert test_result == [
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5),
            "station": "Pasila asema",
            "track": "11",
            "direction": "1",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 0, 1),
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 0, 3),
                "time_departed": datetime(2024, 1, 1, 0, 0, 4),
            },
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 1, 5),
            "station": "Ilmala asema",
            "track": "4",
            "direction": "1",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 1, 1),
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 1, 3),
                "time_departed": datetime(2024, 1, 1, 0, 1, 4),
            },
        },
        {
            "vehicle": 12,
            "ntp_timestamp": datetime(2024, 1, 1, 0, 2, 5),
            "station": "Huopalahti",
            "track": "4",
            "direction": "1",
            "data": {
                "time_arrived": datetime(2024, 1, 1, 0, 2, 1),
                "time_doors_last_closed": datetime(2024, 1, 1, 0, 2, 3),
                "time_departed": datetime(2024, 1, 1, 0, 2, 4),
            },
        },
    ]
