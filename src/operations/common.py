"""
Common operations not necessarily related to any step.
"""

from ..util.ajoaikadatamsg import AjoaikadataMsgWithKey


def filter_none(data: AjoaikadataMsgWithKey) -> AjoaikadataMsgWithKey | None:
    key, msg = data
    if not msg.get("data"):
        return None
    return data
