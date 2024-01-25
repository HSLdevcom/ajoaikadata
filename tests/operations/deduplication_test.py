from datetime import datetime

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main

from ...src.operations.common import filter_none
from ...src.operations.deduplication import deduplicate

from ...src.util.ajoaikadatamsg import AjoaikadataMsgWithKey


def get_test_flow(test_input: list[AjoaikadataMsgWithKey], output: list[AjoaikadataMsgWithKey]) -> Dataflow:
    """Init test dataflow with operations"""
    flow = Dataflow("test_flow")
    (
        op.input("test_input", flow, TestingSource(test_input))
        .then(op.stateful_map, "deduplicate", lambda: {}, deduplicate)
        .then(op.filter_map, "none_filter", filter_none)
        .then(op.output, "test_output", TestingSink(output))
    )
    return flow


def test_deduplication():
    """Messages are deduplicated. Note! Do not use nested data as tests."""
    input_data = [
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5), "msg_type": 1, "content": 1},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 6), "msg_type": 1, "content": 2},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 6), "msg_type": 1, "content": 2},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 7), "msg_type": 1, "content": 3},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 8), "msg_type": 1, "content": 4},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 8), "msg_type": 1, "content": 4},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 6), "msg_type": 1, "content": 2},
    ]
    input_msgs: list[AjoaikadataMsgWithKey] = [("12", {"data": d}) for d in input_data]
    result: list[AjoaikadataMsgWithKey] = []
    flow = get_test_flow(input_msgs, result)
    run_main(flow)

    test_result = [v["data"] for k, v in result]

    assert test_result == [
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5), "msg_type": 1, "content": 1},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 6), "msg_type": 1, "content": 2},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 7), "msg_type": 1, "content": 3},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 8), "msg_type": 1, "content": 4},
    ]
