from .general_parsers import float_parser, int_parser, coordinate_parser, timestamp_str_parser
from .schema import Schema, FieldParser

CABIN_TYPES = {0b10: "A", 0b01: "B", 0b11: "AB"}


def doors_parser(content: bytes) -> bool:
    """Check doors status and returns true if any of the doors is open."""
    # iterate over doors
    for byte in content:
        # check if any of the last 6 bits are set
        if byte & 0x3F:
            return True  # early return if any of the bits are set
    return False

def cabin_parser(content: bytes) -> str | None:
    bits = content[0] & 0x3  # Last 2 bits
    return CABIN_TYPES.get(bits)


def vehicle_parser(content: bytes) -> tuple[int, int, int, list[int]]:
    vehicle_count = int_parser(content[0:1])
    vehicle_pos_on_train = int_parser(content[1:2])
    vehicle_numbers = [byte for byte in content[2:6]]
    vehicle_no = int_parser(content[vehicle_pos_on_train : vehicle_pos_on_train + 1])
    return vehicle_count, vehicle_pos_on_train, vehicle_no, vehicle_numbers


class StadlerUDPSchema(Schema):
    FIELDS = [
        FieldParser(["packet_no"], 0, 0, int_parser),
        FieldParser(["speed"], 4, 7, float_parser),
        FieldParser(["odo"], 8, 9, int_parser),
        FieldParser(["standstill"], 20, 20, int_parser),
        FieldParser(["doors_open"], 21, 28, doors_parser),
        FieldParser(["main_brake_pipe_pressure"], 92, 95, float_parser),
        FieldParser(["active_cabin"], 143, 143, cabin_parser),
        FieldParser(["vehicle_count", "vehicle_pos_on_train", "vehicle_no", "all_vehicles"], 144, 149, vehicle_parser),
        FieldParser(["train_no"], 156, 157, int_parser),
        FieldParser(["loc_x"], 160, 163, coordinate_parser),
        FieldParser(["loc_y"], 164, 167, coordinate_parser),
        FieldParser(["teleste_timestamp"], 168, 171, timestamp_str_parser),
    ]
