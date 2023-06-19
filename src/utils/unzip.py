"""Util to unzip .zip files to a given path in Python"""
import logging
import pathlib
import tempfile
import zipfile
from typing import Callable, Optional

from tqdm.autonotebook import tqdm

from src.utils import util_logging

logger = util_logging.get_logger(__name__)


def unzip(
    zip_path: pathlib.Path,
    out_path: pathlib.Path,
    overwrite: bool = False,
    remove_archive_name: bool = True,
    filter_func: Optional[Callable[[str], bool]] = None,
) -> bool:
    """
    Extract all files in a given .zip file at `zip_path` to `out_path`.

    Args:
        zip_path (pathlib.Path): Path to the .zip file which should be unzipped
        out_path (pathlib.Path): Path under which to save the extracted files
        overwrite (bool, optional): Whether to overwrite existing files at out_path.
            Defaults to False.
        remove_archive_name (bool, optional): Whether to remove the name of the zip
            archive from the path when extracting the zip files. Defaults to True.
        filter_func (Callable, optional): Pass a function which will be applied to the
            filenames (as str) in the zip directory to filter out only the and extract
            only those zip files for which `filter_func` evaluates to true. Defaults
            to None.

    Returns:
        bool: True, iff the extraction succeeded.
    """
    # Ensure that
    zip_path = pathlib.Path(zip_path)
    out_path = pathlib.Path(out_path)

    # Open zip archive
    with zipfile.ZipFile(zip_path, "r") as zip_archive:
        # Get all files in zip_archive
        files = [elem for elem in zip_archive.infolist() if not elem.is_dir()]
        if filter_func is not None:
            files = [elem for elem in files if filter_func(elem.filename)]
            logger.debug("Extracting files: %s", [elem.filename for elem in files])

        # Check for each file if it exists already. If yes, skip. If not, extract.
        files_progress = tqdm(files, leave=False)
        for compressed_file in files_progress:
            # Get relative path of file in zip archive
            if remove_archive_name:
                relative_path = compressed_file.filename.split("/", maxsplit=1)[1]
            else:
                relative_path = compressed_file.filename

            # Update progress bar
            files_progress.set_description(
                f"{relative_path} ({compressed_file.file_size / 1024**2:.2f} MB)"
            )

            # Check for existing files
            save_path = out_path / relative_path
            if not overwrite and save_path.exists():
                # print(f"File {out_path / relative_path} exists.")
                continue
            else:
                # To create a file at a certain out path, first extract to
                # temporary directory and then move to desired out path
                with tempfile.TemporaryDirectory() as tmp_dir:
                    logger.debug("Temporary dir: %s", tmp_dir)
                    tmp_path = pathlib.Path(tmp_dir)
                    zip_archive.extract(compressed_file, path=tmp_path)
                    extracted_file = tmp_path / compressed_file.filename
                    logger.debug(
                        "Extracted file: %s exists: %s",
                        extracted_file,
                        extracted_file.exists(),
                    )
                    logger.debug("Moving file to %s", save_path)
                    save_path.parent.mkdir(parents=True, exist_ok=True)
                    extracted_file.replace(save_path)

                assert save_path.exists()
                assert not save_path.is_dir()

    return True
