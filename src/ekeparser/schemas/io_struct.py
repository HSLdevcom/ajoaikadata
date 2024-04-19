from functools import partial

from .general_parsers import int_parser
from .schema import Schema, FieldParser


class IOStructSchema(Schema):
    FIELDS = [
        FieldParser(["io_speed"], 17, 18, partial(int_parser, endian="big")),
    ]
