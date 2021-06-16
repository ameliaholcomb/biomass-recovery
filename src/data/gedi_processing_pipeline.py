"""
Helper functions for processing GEDI L2A data from orders on the EarthSearch portal
"""
import argparse
import logging
import pathlib
import shutil
from typing import Union

import joblib
import pandas as pd
import psutil
from tqdm.autonotebook import tqdm

from src.constants import GEDI_L2A_PATH, LOG_PATH
from src.data.gedi_granule import GediGranule
from src.utils.logging import get_logger
from src.utils.unzip import unzip

logger = get_logger(__name__)
logger.setLevel(logging.INFO)


def _update_problematic_files(
    zip_path: pathlib.Path,
    granule_path: pathlib.Path,
    problem_file_path: pathlib.Path = LOG_PATH / "problem_files.json",
) -> None:
    """
    Write name of zip file with a problem to the json dict in `problem_file_path`.

    This internal function is useful for bulk processing of GEDI data. It keeps track
    of all granules that caused issues while procesisng and might need a manual treament

    Args:
        zip_path (pathlib.Path): Path to the zip file containing the issue
        granule_path (pathlib.Path): Path to the granule within the zip file that
            caused the processing issue.
        problem_file_path (pathlib.Path, optional): Path to the file in which all
            problematic files will be recorded.
            Defaults to LOG_PATH/"problem_files.json".
    """
    import json  # pylint: disable=import-outside-toplevel

    # Load JSON file
    with open(problem_file_path, "r") as json_file:
        problematic_files = json.load(json_file)

    # Add entry
    if zip_path.name in problematic_files.keys():
        if not granule_path.name in problematic_files[zip_path.name]:
            problematic_files[zip_path.name] += [granule_path.name]
    else:
        problematic_files[zip_path.name] = [granule_path.name]

    # Save JSON file
    with open(problem_file_path, "w") as json_file:
        json.dump(problematic_files, json_file)

    logger.debug("Noted problematic file.")


# pylint: disable=redefined-outer-name
def _should_granule_be_processed(
    filename: str, save_dir: pathlib.Path, order_number: Union[str, int]
) -> bool:
    """
    Return True iff the given granule should be processed (i.e. has not yet been saved)

    Args:
        filename (str): Name of the granule `.h5` file to extract
        save_dir (pathlib.Path): The directory in which the processed output should be
        saved.
        order_number (Union[str, int]): The order number of the GEDI order. (Specific
        to how I chose to save my data)

    Returns:
        bool: True, iff the given file is a GEDI granule and has not been processed yet.
    """
    # filter out non-GEDI granules
    if filename[-3:] != ".h5":
        return False

    filename = filename[:-3]  # remove .h5 suffix
    filename = filename.split("/")[-1]  # remove any prefixes

    # Assemble name under which file would be saved
    save_name = save_dir / f"order{order_number}/{filename}.feather"
    return not (save_name.exists() and save_name.is_file())


def _extract_granule_data(granule_path: pathlib.Path) -> pd.DataFrame:
    """
    Extract main data for all beams in a GEDI granule and return as DataFrame.

    Args:
        granule_path (pathlib.Path): Path to the granule (`.h5` file)

    Returns:
        pd.DataFrame: A DataFrame containing data for all shots for all beams in
            the granule.
    """
    granule = GediGranule(granule_path)
    # Gather data for whole granule into one table
    granule_data = []
    logger.debug("Processing granule %s", granule_path)
    for beam in granule.iter_beams():
        try:
            granule_data.append(beam.main_data)
        except KeyError:
            logger.warning(
                "Data for beam %s in granule %s incomplete. Beam skipped.",
                beam.name,
                granule_path.name,
            )
    return pd.concat(granule_data, ignore_index=True)


def process_gedi_l2a_zip(zip_path: pathlib.Path, save_dir: pathlib.Path) -> None:
    """
    Extract GEDI L2A data from `zip_path` and save it in `save_dir`.

    Main processing function to process GEDI level 2A data that was downloaded via
    the Landsat Earthsearch portal (https://search.earthdata.nasa.gov/). This function
    starts by unzipping the zip-file which contains multiple granules, extracts the
    data for all beams in all granules in the zip file into a pandas DataFrame, and
    saves them to disk as a `.feather` file. The unzipped files will then be deleted
    once the analysis is complete.

    Args:
        zip_path (pathlib.Path): Path to the zip-file containing the granules to be
            processed
        save_dir (pathlib.Path): Directory to save the processed granules under.
    """

    order_number = zip_path.name.split(".")[0]
    zip_number = zip_path.name.split("?")[1]
    logger.debug("Processing order zip %s-%s", order_number, zip_number)

    # Step 2: unzipping
    tmp_unzip_path = zip_path.parent / f"unzip{order_number}-{zip_number}"
    tmp_unzip_path.mkdir(parents=True, exist_ok=True)
    filter_func = lambda filename: _should_granule_be_processed(
        filename, save_dir=save_dir, order_number=order_number
    )
    unzip(
        zip_path,
        out_path=tmp_unzip_path,
        remove_archive_name=False,
        filter_func=filter_func,
    )

    # Step 3: Find granules
    granules = list(sorted(tmp_unzip_path.glob("*/*.h5")))
    logger.debug("Found %s unprocessed granules", len(granules))

    # Step 4: Process granules
    for granule_path in tqdm(granules, leave=False):
        try:
            granule_name = granule_path.name.split(".")[0]
            save_name = save_dir / f"order{order_number}/{granule_name}.feather"

            if not save_name.exists():
                granule_data = _extract_granule_data(granule_path)

                logger.debug("Saving granule %s", granule_path)
                save_name.parent.mkdir(exist_ok=True, parents=True)
                granule_data.to_feather(save_name)
                # granule_data.to_file(save_name, driver="GPKG")  # very slow

        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                "Problem with granule %s of %s", granule_path.name, zip_path.name
            )
            logger.error(e)
            _update_problematic_files(zip_path, granule_path)
            continue

    logger.debug("Deleting unzipped files")
    # Delete zip files from disk
    shutil.rmtree(tmp_unzip_path)
    # os.remove(zip_path)


if __name__ == "__main__":

    DEFAULT_SAVE_DIR = GEDI_L2A_PATH / "v002" / "amazon_basin"
    N_CPU_CORES = psutil.cpu_count(logical=False)

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="GEDI order download script")
    parser.add_argument(
        "order_folder",
        help="Path to the folder with the zip files",
        type=str,
    )
    parser.add_argument(
        "n_workers",
        help="The number of workers (python processes) to use for the processing",
        type=int,
        default=min(4, N_CPU_CORES - 1),
        nargs="?",  # Argument is optional
    )
    parser.add_argument(
        "save_dir",
        help="The folder in which to save the downloaded files.",
        type=str,
        default=DEFAULT_SAVE_DIR,
        nargs="?",  # Argument is optional
    )

    args = parser.parse_args()
    order_folder = pathlib.Path(args.order_folder)
    save_dir = pathlib.Path(args.save_dir)
    n_workers = args.n_workers

    if not order_folder.exists():
        raise FileExistsError(f"Order folder {order_folder} does not exist.")

    if n_workers > N_CPU_CORES:
        raise RuntimeError(
            f"Requested {n_workers} workers, but only {N_CPU_CORES} cores available."
        )

    logger.info(
        "Processing order %s to save directory %s with %s workers.",
        order_folder.absolute(),
        save_dir.absolute(),
        args.n_workers,
    )

    zip_files = list(
        sorted(
            order_folder.glob("*.zip?*"), key=lambda path: int(path.name.split("?")[-1])
        )
    )
    logger.info(
        "Found %s zip-files in order %s.", len(zip_files), order_folder.absolute()
    )

    inputs = tqdm(zip_files)

    joblib.Parallel(n_jobs=n_workers)(
        joblib.delayed(process_gedi_l2a_zip)(zip_file, save_dir) for zip_file in inputs
    )
