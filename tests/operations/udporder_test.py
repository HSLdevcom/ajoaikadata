from datetime import datetime

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main

from ...src.operations.udporder import reorder_messages, create_empty_udp_cache
from ...src.util.ajoaikadatamsg import AjoaikadataMsgWithKey


def get_test_flow(test_input: list[AjoaikadataMsgWithKey], output: list[AjoaikadataMsgWithKey]) -> Dataflow:
    """Init test dataflow with operations"""
    flow = Dataflow("test_flow")
    (
        op.input("test_input", flow, TestingSource(test_input))
        .then(op.stateful_map, "udp_order", lambda: create_empty_udp_cache(), reorder_messages)
        .then(op.flat_map_value, "flat_ordered_msgs", lambda x: x)
        .then(op.output, "test_output", TestingSink(output))
    )
    return flow


def test_normal_event_order():
    """Ordering works on the normal simple case"""
    input_data = [
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5), "msg_type": 1, "content": {"packet_no": 1}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 6), "msg_type": 1, "content": {"packet_no": 2}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 7), "msg_type": 1, "content": {"packet_no": 3}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 8), "msg_type": 1, "content": {"packet_no": 4}},
    ]
    input_msgs: list[AjoaikadataMsgWithKey] = [("12", {"data": d}) for d in input_data]
    result: list[AjoaikadataMsgWithKey] = []
    flow = get_test_flow(input_msgs, result)
    run_main(flow)

    test_result = [v["data"] for k, v in result]

    assert test_result == [
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5), "msg_type": 1, "content": {"packet_no": 1}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 6), "msg_type": 1, "content": {"packet_no": 2}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 7), "msg_type": 1, "content": {"packet_no": 3}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 8), "msg_type": 1, "content": {"packet_no": 4}},
    ]


def test_simple_order():
    """Ordering works on small data."""
    input_data = [
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 1), "msg_type": 1, "content": {"packet_no": 1}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 3), "msg_type": 1, "content": {"packet_no": 3}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 4), "msg_type": 1, "content": {"packet_no": 4}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 6), "msg_type": 1, "content": {"packet_no": 6}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5), "msg_type": 1, "content": {"packet_no": 5}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 7), "msg_type": 1, "content": {"packet_no": 7}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 2), "msg_type": 1, "content": {"packet_no": 2}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 8), "msg_type": 1, "content": {"packet_no": 8}},
    ]
    input_msgs: list[AjoaikadataMsgWithKey] = [("12", {"data": d}) for d in input_data]
    result: list[AjoaikadataMsgWithKey] = []
    flow = get_test_flow(input_msgs, result)
    run_main(flow)

    test_result = [v["data"] for k, v in result]

    assert test_result == [
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 1), "msg_type": 1, "content": {"packet_no": 1}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 2), "msg_type": 1, "content": {"packet_no": 2}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 3), "msg_type": 1, "content": {"packet_no": 3}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 4), "msg_type": 1, "content": {"packet_no": 4}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5), "msg_type": 1, "content": {"packet_no": 5}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 6), "msg_type": 1, "content": {"packet_no": 6}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 7), "msg_type": 1, "content": {"packet_no": 7}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 8), "msg_type": 1, "content": {"packet_no": 8}},
    ]


def test_loop_restart():
    """Ordering works if the packet_no goes to 0"""
    input_data = [
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 1), "msg_type": 1, "content": {"packet_no": 252}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 4), "msg_type": 1, "content": {"packet_no": 0}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 3), "msg_type": 1, "content": {"packet_no": 254}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5), "msg_type": 1, "content": {"packet_no": 1}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 2), "msg_type": 1, "content": {"packet_no": 253}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 7), "msg_type": 1, "content": {"packet_no": 3}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 6), "msg_type": 1, "content": {"packet_no": 2}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 8), "msg_type": 1, "content": {"packet_no": 4}},
    ]
    input_msgs: list[AjoaikadataMsgWithKey] = [("12", {"data": d}) for d in input_data]
    result: list[AjoaikadataMsgWithKey] = []
    flow = get_test_flow(input_msgs, result)
    run_main(flow)

    test_result = [v["data"] for k, v in result]

    assert test_result == [
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 1), "msg_type": 1, "content": {"packet_no": 252}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 2), "msg_type": 1, "content": {"packet_no": 253}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 3), "msg_type": 1, "content": {"packet_no": 254}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 4), "msg_type": 1, "content": {"packet_no": 0}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 5), "msg_type": 1, "content": {"packet_no": 1}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 6), "msg_type": 1, "content": {"packet_no": 2}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 7), "msg_type": 1, "content": {"packet_no": 3}},
        {"ntp_timestamp": datetime(2024, 1, 1, 0, 0, 8), "msg_type": 1, "content": {"packet_no": 4}},
    ]


def test_handle_late_msg():
    """Too late msgs as discarded."""
    # more messages than cache size
    input_data = [
        {"ntp_timestamp": datetime(2024, 1, 1, 0, i // 60, i % 60), "msg_type": 1, "content": {"packet_no": i % 255}}
        for i in range(0, 1000)
    ]

    late_msg = input_data.pop(3)
    input_data.insert(700, late_msg)  # cache should not wait for this message
    input_msgs: list[AjoaikadataMsgWithKey] = [("12", {"data": d}) for d in input_data]
    result: list[AjoaikadataMsgWithKey] = []
    flow = get_test_flow(input_msgs, result)
    run_main(flow)

    test_result = [v["data"] for k, v in result]

    expected = [
        {"ntp_timestamp": datetime(2024, 1, 1, 0, i // 60, i % 60), "msg_type": 1, "content": {"packet_no": i % 255}}
        for i in range(0, 1000)
    ]

    late_msg = expected.pop(3)
    late_msg["discard"] = True
    expected.insert(700, late_msg)

    assert test_result == expected


def test_swapped_msgs():
    """Ordering works with swap of two items with the same packet_no"""
    # Two loops
    input_data = [
        {"ntp_timestamp": datetime(2024, 1, 1, 0, i // 60, i % 60), "msg_type": 1, "content": {"packet_no": i % 255}}
        for i in range(0, 510)
    ]
    input_data[5], input_data[300] = input_data[300], input_data[5]

    input_msgs: list[AjoaikadataMsgWithKey] = [("12", {"data": d}) for d in input_data]
    result: list[AjoaikadataMsgWithKey] = []
    flow = get_test_flow(input_msgs, result)
    run_main(flow)

    test_result = [v["data"] for k, v in result]

    expected = [
        {"ntp_timestamp": datetime(2024, 1, 1, 0, i // 60, i % 60), "msg_type": 1, "content": {"packet_no": i % 255}}
        for i in range(0, 510)
    ]
    assert test_result == expected


def test_swapped_msgs_same_packet_no():
    """Ordering works with swap of two items with the same packet_no"""
    # Two loops
    input_data = [
        {"ntp_timestamp": datetime(2024, 1, 1, 0, i // 60, i % 60), "msg_type": 1, "content": {"packet_no": i % 255}}
        for i in range(0, 510)
    ]
    input_data[4], input_data[4 + 255] = input_data[4 + 255], input_data[4]

    input_msgs: list[AjoaikadataMsgWithKey] = [("12", {"data": d}) for d in input_data]
    result: list[AjoaikadataMsgWithKey] = []
    flow = get_test_flow(input_msgs, result)
    run_main(flow)

    test_result = [v["data"] for k, v in result]

    expected = [
        {"ntp_timestamp": datetime(2024, 1, 1, 0, i // 60, i % 60), "msg_type": 1, "content": {"packet_no": i % 255}}
        for i in range(0, 510)
    ]
    assert test_result == expected


def test_swapped_msgs_same_packet_too_big_for_cache():
    """Ordering works with swap of two items with the same packet_no with cache running to max size."""
    # Two loops
    input_data = [
        {"ntp_timestamp": datetime(2024, 1, 1, 0, i // 60, i % 60), "msg_type": 1, "content": {"packet_no": i % 255}}
        for i in range(0, 1000)
    ]
    print(input_data[4 + 765], input_data[4])
    input_data[4], input_data[4 + 765] = input_data[4 + 765], input_data[4]

    input_msgs: list[AjoaikadataMsgWithKey] = [("12", {"data": d}) for d in input_data]
    result: list[AjoaikadataMsgWithKey] = []
    flow = get_test_flow(input_msgs, result)
    run_main(flow)

    test_result = [v["data"] for k, v in result]

    expected = [
        {"ntp_timestamp": datetime(2024, 1, 1, 0, i // 60, i % 60), "msg_type": 1, "content": {"packet_no": i % 255}}
        for i in range(0, 1000)
    ]

    late_msg = expected.pop(4)
    late_msg["discard"] = True
    expected.insert(4 + 765, late_msg)

    assert test_result == expected
