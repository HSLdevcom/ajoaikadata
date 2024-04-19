"""
Operations to validate and select correct timestamp for messages.
"""

from datetime import timedelta, timezone
from typing import Any
from ..util.ajoaikadatamsg import AjoaikadataMsg
from ..util.config import logger


def validate_tst(last_correction: timedelta | None, value: AjoaikadataMsg) -> tuple[Any, AjoaikadataMsg]:
    if not last_correction:
        last_correction = timedelta()
    data = value["data"]
    if not data:
        return last_correction, value

    data["tst"] = data["eke_timestamp"]
    data["tst_source"] = "eke"

    # Update if marked as valid.
    if data["ntp_time_valid"] or (abs(data["mqtt_timestamp"] - data["ntp_timestamp"]) < timedelta(seconds=2)):
        last_correction = data["ntp_timestamp"].replace(tzinfo=None) - data["eke_timestamp"]

    data["tst_eke_correction_utc_secs"] = last_correction.total_seconds()
    data["tst_corrected"] = (data["eke_timestamp"] + last_correction).replace(tzinfo=timezone.utc)

    value["data"] = data
    return last_correction, value
