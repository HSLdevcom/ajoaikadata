from datetime import datetime, timedelta
import math
import struct
from typing import Literal


def int_parser(content: bytes, endian: Literal["big", "little"] = "little") -> int:
    """ Bytes to int """
    return int.from_bytes(content, endian)


def float_parser(content: bytes) -> float:
    """ Bytes to float """
    return struct.unpack("f", content)[0]


def timestamp_parser(content: bytes, endian: Literal["big", "little"] = "little") -> datetime:
    """ Big endian bytes to datetime """
    val = int_parser(content, endian)
    return datetime.utcfromtimestamp(val)


def timestamp_str_parser(content: bytes, endian: Literal["big", "little"] = "little") -> str:
    val = timestamp_parser(content, endian)
    return str(val)


def timestamp_with_ms_parser(content: bytes, endian: Literal["big", "little"] = "little") -> datetime:
    """ 5 bytes, where a first four are datetime in seconds and the last one tells milliseconds (actually centiseconds)"""
    dt = timestamp_parser(content[0:4], endian)
    ms = 10 * int_parser(content[4:5], endian) # centiseconds converted to milliseconds
    return dt + timedelta(milliseconds=ms)


def coordinate_parser(content: bytes) -> float:
    """ Bytes to coordinate value """
    val = float_parser(content)
    val_int = int(val / 100)
    return val_int + (val - (val_int * 100)) / 60.0


def calculate_polynomial_sum(multipliers: list[int], base: int) -> int:
    # Iterate over multipliers
    parts = [(multiplier - 1) * math.pow(base, exp)
             for exp, multiplier in enumerate(multipliers)]
    return int(sum(parts))  # Calculate sum and return as int
