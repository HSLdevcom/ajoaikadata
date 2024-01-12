from csv import DictReader
import gzip
from pathlib import Path
from typing import Any

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition, batch

from ..util.config import logger

def _readlines(files):
    """Turn a list of files into a generator of lines but support `tell`.

    Python files don't support `tell` to learn the offset if you use
    them in iterator mode via `next`, so re-create that iterator using
    `readline`.

    """
    for file in files:
        logger.info(f"Reading file: {file}")
        # if file is csv, use open, else use gzip.open
        if file.endswith(".csv"):
            f = open(file, "rt", newline="")
        else:
            f = gzip.open(file, "rt", newline="")

        f.readline()  # skip header
        counter = 0
        while True:
            line = str(f.readline())  # ensure it's string
            if len(line) <= 0:
                break
            yield line
            counter += 1

        logger.info(f"File {file} read complete. Read {counter} lines.")
        f.close()


class CSVDirSource(StatefulSourcePartition):
    def __init__(self, path, pattern, batch_size, fmtparams):
        # list all files in the directory, filter by pattern and sort
        # supports both csv and gzipped csv
        files = sorted([str(f) for f in Path(path).glob(f"*_{pattern}.csv*")])

        self.reader = DictReader(
            _readlines(files),
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

    def next_batch(self, sched):
        return next(self._batcher)

    def snapshot(self) -> Any:
        return None

    def close(self):
        del self.reader


class CSVDirInput(FixedPartitionedSource):
    def __init__(self, path: Path, batch_size: int = 1000, **fmtparams):
        if not isinstance(path, Path):
            path = Path(path)

        self._path = path
        self._batch_size = batch_size
        self._fmtparams = fmtparams

    def list_parts(self):
        """Each partition is a vehicle id. TODO: make this configurable."""
        return [str(i) for i in range(1, 101)]

    def build_part(self, now, for_part, resume_state):
        return CSVDirSource(self._path, for_part, self._batch_size, self._fmtparams)
