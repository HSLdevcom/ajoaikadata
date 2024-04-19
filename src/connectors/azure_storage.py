"""
Input connection code for reading EKE data blobs from Azure Storage.
"""

from collections.abc import Iterator, Sequence
from csv import DictReader
from datetime import datetime, timedelta
import gzip
from io import BytesIO
import logging
import re
import time
from typing import Any

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition, batch

from azure.storage.blob import ContainerClient

from ..util.config import logger, read_from_env

# Storage client is quite an aggressive to log, so calm it down.
logging.getLogger("azure").setLevel(logging.WARNING)

(AZ_STORAGE_CONNECTION_STRING, AZ_STORAGE_CONTAINER, START_DATE, END_DATE) = read_from_env(
    ("AZ_STORAGE_CONNECTION_STRING", "AZ_STORAGE_CONTAINER", "START_DATE", "END_DATE")
)
(BYTEWAX_BATCH_SIZE,) = read_from_env(("BYTEWAX_BATCH_SIZE",), ("1000",))
BYTEWAX_BATCH_SIZE = int(BYTEWAX_BATCH_SIZE)


def daterange(date1: str, date2: str) -> Iterator[str]:
    # Convert the input strings to datetime objects
    start = datetime.strptime(date1, "%Y-%m-%d")
    end = datetime.strptime(date2, "%Y-%m-%d")
    # Loop from date1 to date2 (inclusive) and yield each date
    for n in range(int((end - start).days) + 1):
        yield (start + timedelta(n)).strftime("%Y-%m-%d")


def _get_container_client() -> ContainerClient:
    return ContainerClient.from_connection_string(
        conn_str=AZ_STORAGE_CONNECTION_STRING, container_name=AZ_STORAGE_CONTAINER
    )


def _readlines(files: Sequence[str]) -> Iterator[str]:
    """Turn a list of files into a generator of lines but support `tell`.

    Python files don't support `tell` to learn the offset if you use
    them in iterator mode via `next`, so re-create that iterator using
    `readline`.

    """
    with _get_container_client() as container:
        for file_name in files:
            with container.get_blob_client(file_name) as blob_client:
                while True:
                    try:
                        downloader = blob_client.download_blob()
                        stream = BytesIO()
                        downloader.readinto(stream)
                    except Exception as e:
                        logger.error(e)
                        logger.error(f"Problem downloading blob {file_name}. Retrying in 10 seconds...")
                        time.sleep(10)
                        continue
                    break

                stream.seek(0)
                with gzip.open(stream, "rt", newline="") as f:
                    next(f)  # skip header
                    counter = 0

                    for line in f:
                        yield str(line)  # ensure it's string
                        counter += 1

                    logger.info(f"File {file_name} read complete. Read {counter} lines.")


class AzureStorageSource(StatefulSourcePartition):
    def __init__(self, blob_names: Sequence[str], pattern: str, batch_size, fmtparams):
        vehicle_id_regex = r"(\d+)\.csv\.gz"

        blobs_in_partition = list(filter(lambda x: re.search(vehicle_id_regex, x).group(1) == str(pattern), blob_names))

        self.reader = DictReader(
            _readlines(blobs_in_partition),
            fieldnames=[  ## TODO: parametrize
                "message_type",
                "ntp_timestamp",
                "ntp_ok",
                "eke_timestamp",
                "mqtt_timestamp",
                "mqtt_topic",
                "raw_data",
            ],
            **fmtparams,
        )
        self._batcher = batch(self.reader, batch_size)

    def next_batch(self):
        return next(self._batcher)

    def snapshot(self) -> Any:
        return None

    def close(self):
        del self.reader


class AzureStorageInput(FixedPartitionedSource):
    def __init__(self, batch_size: int = BYTEWAX_BATCH_SIZE, **fmtparams):
        dates = [date for date in daterange(START_DATE, END_DATE)]

        with _get_container_client() as container:
            logger.info("Downloading blob lists...")

            self.blob_names: list[str] = [
                blob.name for date_str in dates for blob in container.list_blobs(name_starts_with=date_str)
            ]

            logger.info(f"Blob names downloaded! {len(self.blob_names)} blobs will be processed.")

        self._batch_size = batch_size
        self._fmtparams = fmtparams

    def list_parts(self):
        """Each partition is a vehicle id. TODO: make this configurable. Currently 81 vehicles."""
        return [str(i) for i in range(1, 82)]

    def build_part(self, step_id, for_part, resume_state):
        return AzureStorageSource(self.blob_names, for_part, self._batch_size, self._fmtparams)
