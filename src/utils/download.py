"""Utility functions for downloading resources from the internet."""
import os
import pathlib

import requests

from src.utils.logging import get_logger

logger = get_logger(__file__)


def download(
    url: str,
    file_path: pathlib.Path,
    overwrite: bool = False,
    chunk_size: int = 5 * 1024 * 1024,  # 5 MB chunks per default
) -> bool:
    """
    Downloads the resource at given `url` to the `file_path`

    Args:
        url (str): URL of the resource to download
        file_path (pathlib.Path): Path where the downloaded resource will be stored
        overwrite (bool, optional): Whether to overwrite the file at `file_path` if it
            exists already. Defaults to False.
        chunk_size (int, optional): The chunksize to use for streamed downloads.
            Defaults to 5*1024*1024 (i.e. 5 MB).

    Returns:
        bool: True if the download was successful.
    """

    if file_path.exists() and not overwrite:
        logger.info("File already exists at %s" % file_path)
        return True
    # Create parent folders
    file_path.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(url, stream=True)
    if response.ok:
        # TODO: total_length = response.headers.get("content-length")  # for progressbar
        logger.info(f"Saving to {file_path}")
        with open(file_path, "wb") as out_file:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    out_file.write(chunk)
                    out_file.flush()
                    os.fsync(out_file.fileno())
        return True
    else:  # HTTP status code 4XX/5XX
        logger.error(
            "Download failed: status code %s\n%s"
            % (response.status_code, response.text)
        )
        return False
