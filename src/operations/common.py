from ..types import AjoaikadataMsgWithKey

def filter_none(data: AjoaikadataMsgWithKey) -> AjoaikadataMsgWithKey | None:
    key, msg = data
    if not msg.get("data"):
        return None
    return data