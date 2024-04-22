from functools import partial

from .general_parsers import int_parser
from .schema import Schema, FieldParser


def io_struct_bit_parser(content: bytes) -> tuple[bool, bool, bool, bool, bool]:
    byte1 = content[0]
    braking = bool(byte1 & 128)
    sanding = bool(byte1 & 32)
    jkv_on = bool(byte1 & 8)

    byte2 = content[1]
    safety_device_on = bool(byte2 & 8)
    rail_brake = bool(byte2 & 2)

    return braking, sanding, jkv_on, safety_device_on, rail_brake


def brake_pressure_parser(content: bytes) -> float:
    return 0.01 * int_parser(content, endian="big")


class IOStructSchema(Schema):
    FIELDS = [
        FieldParser(["braking", "sanding", "jkv_on", "safety_device_on", "rail_brake"], 0, 1, io_struct_bit_parser),
        FieldParser(["brake_pressure"], 2, 3, brake_pressure_parser),
        FieldParser(["io_speed"], 17, 18, partial(int_parser, endian="big")),
    ]
