from functools import partial

from .general_parsers import int_parser
from .schema import Schema, FieldParser


class JKVStructSchema(Schema):
    FIELDS = [
        FieldParser(["jkv_target_speed"], 0, 0, partial(int_parser, endian="big")),
        FieldParser(["jkv_speed"], 1, 1, partial(int_parser, endian="big")),
        FieldParser(["jkv_allowed_speed"], 5, 5, partial(int_parser, endian="big")),
    ]
