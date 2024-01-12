from typing import Any, List, Tuple, TypedDict


class Msg(TypedDict):
    data: Any


class PulsarMsg(Msg):
    """Same as Msg but contains Pulsar msg ids as a list so that they can be acked."""

    msgs: List[str]


BytewaxMsgFromCSV = Tuple[str, Msg]
BytewaxMsgFromPulsar = Tuple[str, PulsarMsg]
