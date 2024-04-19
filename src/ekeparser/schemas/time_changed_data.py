from functools import partial

from .general_parsers import timestamp_with_ms_parser
from .schema import Schema, FieldParser


class TimeChangedDataSchema(Schema):
    FIELDS = [
        FieldParser(["new_date"], 0, 3, partial(timestamp_with_ms_parser, endian="big")),
        FieldParser(["old_date"], 4, 7, partial(timestamp_with_ms_parser, endian="big")),
    ]
