"""
Script for downloading data from EU TMF project
    https://forobs.jrc.ec.europa.eu/TMF/download/#download
"""
import argparse
import multiprocessing
import pathlib
import subprocess

import psutil
import requests

from src.constants import DATA_PATH
from src.utils.logging_util import get_logger

logger = get_logger(__name__)

JRC_LONGITUDES = [f"W{i}" for i in range(110, 0, -10)] + [
    f"E{i}" for i in range(0, 181, 10)
]
JRC_LATITUDES = [f"N{i}" for i in range(30, -1, -10)] + [
    f"S{i}" for i in range(10, 31, 10)
]
JRC_DATASETS = [
    "TransitionMap_Subtypes",
    "TransitionMap_MainClasses",
    "AnnualChange",
    "DegradationYear",
    "DeforestationYear",
]


def is_already_downloaded(
    outpath: pathlib.Path, dataset: str, lat: str, lon: str
) -> bool:
    """
    Returns true, iff given JCR dataset is already downloaded.

    Args:
        outpath (os.PathLike): The output directory to save tha data in
        dataset (str): The name of the dataset to download
        lat (str): The latitude of the tile to download
        lon (str): The longitude of the tile ot download

    Raises:
        ValueError: If more than one file were found

    Returns:
        bool: True if tile was already downloaded. False else.
    """
    matching_files = list(outpath.glob(f"*{dataset}*{lat}*{lon}.*"))
    if len(matching_files) == 0:
        return False
    elif len(matching_files) == 1:
        logger.info("Found existing file %s", matching_files[0])
        return True
    else:
        raise ValueError(f"More than one matching file found: {matching_files}")


def resource_exists(jrc_query: str) -> bool:
    with requests.get(jrc_query, stream=True) as response:
        if response.status_code != 200:
            logger.debug("Resource %s does not exist", jrc_query)
            return False
        else:
            logger.debug("Found resource %s", jrc_query)
            return True


def construct_jrc_query(dataset: str, lat: str, lon: str) -> str:
    return (
        "https://ies-ows.jrc.ec.europa.eu/iforce/tmf_v1/"
        f"download.py?type=tile&dataset={dataset}&lat={lat}&lon={lon}"
    )


def download_file(outpath: pathlib.Path, file_url: str) -> int:
    logger.info("Downloading resource %s", file_url)
    return subprocess.call(
        ["wget", "--content-disposition", "-P", outpath, file_url]
    )


def download_jrc_dataset(
    dataset: str, outdir: pathlib.Path = DATA_PATH / "JRC"
) -> None:
    """
    Check if given JRC dataset has already been downloaded in the output directory
    and download all tiles of the dataset if it is not there yet.

    Args:
        dataset (str): The name of the dataset to download.
        outdir (os.PathLike): The path to the directory in which to download the dataset
    """
    outpath = outdir / dataset
    outpath.mkdir(parents=True, exist_ok=True)

    for lat in JRC_LATITUDES:
        for lon in JRC_LONGITUDES:
            if is_already_downloaded(outpath, dataset, lat, lon):
                continue
            else:
                jrc_query = construct_jrc_query(dataset, lat, lon)
                if resource_exists(jrc_query):
                    download_file(outpath, file_url=jrc_query)


def download_jrc_all():
    for dataset in JRC_DATASETS:
        download_jrc_dataset(dataset)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="JRC download script"
    )
    parser.add_argument(
        "--use_multiple_workers",
        help=(
            "If set then a thread will be spawned for each dataset category to be downloaded."
        ),
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "-d",
        "--dataset",
        help=(
            "The dataset to look for a last observation for. Can be one of"
            f"{JRC_DATASETS}"
        ),
        type=str,
        default="",
        nargs="?",  # Argument is optional
    )

    args = parser.parse_args()
    datasets = JRC_DATASETS if not args.dataset else [args.dataset]

    if args.use_multiple_workers:
        print("Using multiple workers.")
        # Make sure num_workers isn't higher than available CPUs
        core_count = psutil.cpu_count(logical=False)
        dataset_count = len(datasets)
        n_workers = min(dataset_count, core_count)

        # Set up multiprocessing download
        pool = multiprocessing.Pool(n_workers)
        pool.map(download_jrc_dataset, datasets)
    else:
        # Download all data sequentially on one core
        download_jrc_all()
