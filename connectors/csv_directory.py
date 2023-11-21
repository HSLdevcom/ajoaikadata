from csv import DictReader
from pathlib import Path
from typing import Any

from bytewax.inputs import PartitionedInput, StatefulSource, batch

def _readlines(files):
    """Turn a list of files into a generator of lines but support `tell`.

    Python files don't support `tell` to learn the offset if you use
    them in iterator mode via `next`, so re-create that iterator using
    `readline`.

    """
    for file in files:
        with open(file, "rt", newline="") as f:
            f.readline() # skip header
            while True:
                line = f.readline()
                if len(line) <= 0:
                    break
                yield line


class CSVDirSource(StatefulSource):
    def __init__(self, path, pattern, batch_size, fmtparams):
        # list all files in the directory, filter by pattern and sort
        files = sorted([str(f) for f in Path(path).glob(f"*{pattern}.csv")])

        self.reader = DictReader(_readlines(files), fieldnames=["filename", "value1", "value2"], **fmtparams)
        self._batcher = batch(self.reader, batch_size)

    def next_batch(self):
        return next(self._batcher)
    
    def snapshot(self) -> Any:
        return None

    def close(self):
        del self.reader


class CSVDirInput(PartitionedInput):
    def __init__(self, path: Path, batch_size: int = 1000, **fmtparams):
        if not isinstance(path, Path):
            path = Path(path)

        self._path = path
        self._batch_size = batch_size
        self._fmtparams = fmtparams

    def list_parts(self):
        # Patterns. TODO: Change to vehicle ids.
        return ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]

    def build_part(self, for_part, resume_state):
        """See ABC docstring."""
        # TODO: Warn and return None. Then we could support
        # continuation from a different file.
        return CSVDirSource(self._path, for_part, self._batch_size, self._fmtparams)
