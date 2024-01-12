from typing import Any, List, NotRequired, Tuple, TypedDict


class AjoaikadataMsg(TypedDict):
    data: Any
    msgs: NotRequired[List[str]]


AjoaikadataMsgWithKey = Tuple[str, AjoaikadataMsg]


def create_empty_msg() -> AjoaikadataMsg:
    return {"data": None, "msgs": []}
