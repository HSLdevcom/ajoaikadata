from .general_parsers import str_parser
from .schema import Schema, FieldParser


class JKVFaultMsgSchema(Schema):
    FIELDS = [
        FieldParser(["jkv_fault_msg_text"], 0, 7, str_parser),
    ]
