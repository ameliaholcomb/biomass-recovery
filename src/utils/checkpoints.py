import logging
from pathlib import Path
from typing import List
from src.utils.logging import get_logger

logger = get_logger(__file__)
logger.setLevel(logging.INFO)

class FileProcessingCheckpointer(object):
    """Class to manage processing numerous files, with checkpointing."""

    def __init__(self, dir: Path, checkpoint_file: Path, shpattern: str):
        self.dir = dir
        self.checkpoint_file = checkpoint_file
        if not self.checkpoint_file.exists():
            fp = open(checkpoint_file, 'a')
            fp.close()

        self.files = set(dir.glob(shpattern))
        logger.info(f"Found {len(self.files)} files matching pattern {shpattern} in {dir}.")

        completed = set(Path(line.strip()) for line in open(checkpoint_file, 'r'))
        self.files = self.files - completed

        logger.info(f"Found {len(self.files)} h5 files remaining to import ...")

    @property
    def remaining_files(self) -> List[Path]:
        return sorted(list(self.files))

    def mark_file_completed(self, file: Path) -> None:
        self.files.discard(file)
        with open(self.checkpoint_file, 'a') as fp:
            fp.write(f"{file}\n")
        return
 