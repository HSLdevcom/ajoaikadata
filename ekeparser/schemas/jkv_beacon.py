from .general_parsers import calculate_polynomial_sum, int_parser
from .schema import Schema, FieldParser, DataContentParser

MSG_TYPES = {
    0x11: "Signal",
    0x21: "Rep.signal",
    0x31: "Speed board",
    0x41: "Warn. board",
    0x12: "OS",
    0x22: "OS",
    0x13: "RSS",
    0x23: "RSS",
    0x14: "DS",
    0x24: "DS",
    0x15: "RT",
    0x25: "RT",
    0x16: "DG",
    0x26: "DG",
    0x28: "Link Rep.",
    0x19: "ETS1",
    0x29: "ETS1",
    0x39: "ETB1",
    0x49: "ETB1",
    0x1A: "ETS2",
    0x2A: "ETS2",
    0x3A: "ETB2",
    0x4A: "ETB2",
    0x1B: "ETS3",
    0x2B: "ETS3",
    0x3B: "ETB3",
    0x4B: "ETB3",
    0x1C: "ETS4",
    0x2C: "ETS4",
    0x3C: "ETB4",
    0x4C: "ETB4",
    0x1D: "ETS5",
    0x2D: "ETS5",
    0x3D: "ETB5",
    0x4D: "ETB5",
    0x2E: "Rep. marker",
    0x4E: "W.B. marker",
    0x3A: "W.B. marker",
}

CBA_TYPES = {
    0x2: "1(2)", # Päätoimintasuuntaan nähden ensimmäinen kahdesta baliiisista
    0x3: "2(2)", # Päätoimintasuuntaan nähden jälkimmäinen kahdesta baliiisista
    0xB: "2(2)*", # Päätoimintasuuntaan nähden jälkimmäinen kahteen suuntaan toimivan informaatiopisteen kahdesta baliisista.
}

CBB_TYPES = {
    0x1: "Single", # Baliisiryhmässä erilaiset sanomat
    0x2: "Double", # Baliiisiryhmässä samanlaiset sanomat
}


def balise_identification_parser(content: bytes) -> tuple[str | None, str | None]:
    a_byte = content[0] >> 4  # First 4 bits
    b_byte = content[0] & 0x0F  # Last 4 bits

    balise_cba = CBA_TYPES.get(a_byte)
    balise_cbb = CBB_TYPES.get(b_byte)

    return balise_cba, balise_cbb


def balise_msg_type_parser(content: bytes) -> str | None:
    return MSG_TYPES.get(content[0])


def balise_id_parser(content: bytes) -> tuple[int, int]:
    half_byte_list = []
    for byte in content:
        half_byte_list.append(byte >> 4)
        half_byte_list.append(byte & 0x0F)
    balise_id = calculate_polynomial_sum(half_byte_list[0:5], base=14)
    balise_id_next = calculate_polynomial_sum(half_byte_list[5:10], base=14)

    return balise_id, balise_id_next


class JKVBeaconDataSchema(Schema):
    FIELDS = [
        FieldParser(["balise_cba", "balise_cbb"], 0, 0, balise_identification_parser),
        FieldParser(["balise_msg_type"], 1, 1, balise_msg_type_parser),
        FieldParser(["balise_id", "balise_id_next"], 2, 6, balise_id_parser),
    ]


class JKVBeaconSchema(Schema):
    FIELDS = [
        FieldParser(["msg_index"], 0, 0, int_parser),
        FieldParser(["transponder_msg_part"], 2, 2, int_parser),
    ]
    DATA_CONTENT = DataContentParser(6)  # Do not parse here, because transponder messages should be merged beforehands.
