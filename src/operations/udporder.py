"""
Operations to order UDP messages.
"""

from datetime import datetime
import heapq
from typing import TypedDict

from ..util.ajoaikadatamsg import AjoaikadataMsg
from ..util.config import logger

# How many messages can be stored in the cache.
CACHE_MAX_SIZE = 1000
# If last message was more than this amount of seconds late, do not release the message immidiately.
UNEXPECTED_TIME_DIFF = 30


# Tuple structure for Ajoaikadata msg comparison
# Needed, because tuples can't be compared, if timestamps are the same. (Dicts are not comparable.)
class UDPCacheItem(tuple):
    def __new__(cls, item: tuple[datetime, AjoaikadataMsg]):
        return tuple.__new__(UDPCacheItem, item)

    def __lt__(self, other):  # To override > operator
        return self[0] < other[0]

    def __gt__(self, other):  # To override < operator
        return self[0] > other[0]


class UDPMsgCache(TypedDict):
    msgs: list[UDPCacheItem]
    waiting_for_no: int
    last_released_tst: datetime


def create_empty_udp_cache() -> UDPMsgCache:
    return {"msgs": [], "waiting_for_no": -1, "last_released_tst": datetime.fromtimestamp(0)}


def _get_next_id(packet_no: int) -> int:
    return (packet_no + 1) % 255


def _get_pattern_from_cache(cache: UDPMsgCache) -> list[AjoaikadataMsg]:
    msgs = []
    while len(cache["msgs"]) > 0:
        if cache["msgs"][0][1]["data"]["msg_type"] != 1:
            msg = heapq.heappop(cache["msgs"])[1]
            msgs.append(msg)
            continue
        if cache["msgs"][0][1]["data"]["content"]["packet_no"] != cache["waiting_for_no"]:
            break

        msg = heapq.heappop(cache["msgs"])[1]
        cache["waiting_for_no"] = _get_next_id(cache["waiting_for_no"])
        cache["last_released_tst"] = msg["data"]["ntp_timestamp"]
        msgs.append(msg)

    return msgs


def _add_to_cache(cache: UDPMsgCache, item: UDPCacheItem) -> list[AjoaikadataMsg]:
    heapq.heappush(cache["msgs"], item)

    if len(cache["msgs"]) > CACHE_MAX_SIZE:
        if cache["msgs"][0][1]["data"]["msg_type"] == 1:
            cache["waiting_for_no"] = cache["msgs"][0][1]["data"]["content"]["packet_no"]
        return _get_pattern_from_cache(cache)
    return []


def reorder_messages(udp_cache: UDPMsgCache, value: AjoaikadataMsg) -> tuple[UDPMsgCache, list[AjoaikadataMsg]]:
    data = value["data"]

    # No udp message, add to the cache if there were items.
    if data["msg_type"] != 1:
        tst: datetime = data["ntp_timestamp"]

        if (len(udp_cache["msgs"]) > 0 and tst > udp_cache["msgs"][0][0]):
            msgs = _add_to_cache(udp_cache, UDPCacheItem((tst, value)))
            return udp_cache, msgs
        
        return udp_cache, [value]

    packet_no: int = data["content"]["packet_no"]
    tst: datetime = data["ntp_timestamp"]

    if not data["ntp_time_valid"]:
        # Tst not valid. Discard.
        value["data"]["discard"] = True
        return udp_cache, [value]

    if udp_cache["waiting_for_no"] == -1:
        # first message, init metadata and return msg
        udp_cache["waiting_for_no"] = _get_next_id(packet_no)
        udp_cache["last_released_tst"] = tst
        return udp_cache, [value]

    if tst < udp_cache["last_released_tst"]:
        # Too old message, mark as discarded - we are not waiting for this.
        value["data"]["discard"] = True
        logger.debug(f"Discarded udp message, because it was too old: {value}")
        return udp_cache, [value]

    if (
        udp_cache["waiting_for_no"] != packet_no
        or (tst - udp_cache["last_released_tst"]).total_seconds() > UNEXPECTED_TIME_DIFF
        or (len(udp_cache["msgs"]) > 0 and tst > udp_cache["msgs"][0][0])
    ):
        # Not the message we are waiting for. Store to the cache.
        msgs = _add_to_cache(udp_cache, UDPCacheItem((tst, value)))
        return udp_cache, msgs

    # The message is okay and should be returned
    # check the cache if there are something to release

    msgs = [value]
    udp_cache["last_released_tst"] = tst
    udp_cache["waiting_for_no"] = _get_next_id(packet_no)

    msgs += _get_pattern_from_cache(udp_cache)
    return udp_cache, msgs
