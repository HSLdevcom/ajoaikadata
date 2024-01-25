"""
Operator to filter duplicate items from the message stream.
"""
from typing import TypeAlias

from ..util.ajoaikadatamsg import AjoaikadataMsg, create_empty_msg

CACHE_MAX_SIZE = 20000

DeduplicationCache: TypeAlias = dict[int, None]


def create_deduplication_cache() -> DeduplicationCache:
    return {}


def deduplicate(cache: DeduplicationCache, value: AjoaikadataMsg) -> tuple[DeduplicationCache, AjoaikadataMsg]:
    # Assume value is not nested! Otherwise this doesn't work.
    data = value["data"]
    hashed = hash(frozenset(data.items()))

    if hashed in cache:
        return cache, create_empty_msg()

    cache[hashed] = None

    # Remove the oldest message from the cache if it's full.
    # Python dict keys are ordered, so this is ok.
    if len(cache) > CACHE_MAX_SIZE:
        cache.pop(next(iter(cache)))

    return cache, value
