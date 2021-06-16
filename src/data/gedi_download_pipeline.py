"""Script to download GEDI order multithreaded"""
import argparse
import concurrent
import os
import subprocess
from typing import Union

from tqdm.autonotebook import tqdm

from src.utils.logging import get_logger

logger = get_logger(__file__)


def _generate_link(order_number: Union[str, int], zip_number: Union[str, int]) -> str:
    return f"https://e4ftl01.cr.usgs.gov/ops/esir/{order_number}.zip?{zip_number}"


def _wget(link: str) -> bool:
    try:
        subprocess.call(["wget", link])
        return True
    except Exception as e:  # pylint: disable=broad-except
        logger.exception(e)
        print(e)
        return False


def download_gedi_order(
    order_number: Union[str, int], n_zips: int, max_threads: int, save_dir: os.PathLike
) -> dict[str, bool]:
    """
    Download all zips for a GEDI order with given `order_number` and save them to
    save_dir.

    Args:
        order_number (Union[str, int]): The order number of the order to download
        n_zips (int): The number of zip files in the order
        max_threads (int): The maximum number of threads to be used during download
        save_dir (os.PathLike): The path to save the downloads under.

    Returns:
        dict[str, bool]: A dictionary where the key is the download link to the order
            and the value is whether the download was successful or not.
    """

    # change working directory
    os.makedirs(save_dir, exist_ok=True)
    os.chdir(save_dir)

    # set up all links to download
    links = [
        _generate_link(order_number, zip_number) for zip_number in range(1, n_zips + 1)
    ]
    results = {}

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_threads, thread_name_prefix=f"Order_{order_number}"
    ) as executor:
        futures = {executor.submit(_wget, link): link for link in links}

        for future in tqdm(concurrent.futures.as_completed(futures), total=len(links)):
            try:
                name = futures[future]
                was_successful = future.result()
                print(f"{name} successful: {was_successful}")
                logger.info("%s successful: %s", name, was_successful)
                results[name] = was_successful
            except Exception as e:  # pylint: disable=broad-except
                logger.exception(e)
                print(e)

    return results


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="GEDI order download script")
    parser.add_argument(
        "order_number",
        help="The number of the order to download",
        type=int,
    )
    parser.add_argument(
        "n_zips",
        help="The number of zip files in the order",
        type=int,
    )
    parser.add_argument(
        "max_threads",
        help="The maximum number of threads to use for the download",
        type=int,
        default=10,
        nargs="?",  # Argument is optional
    )
    parser.add_argument(
        "save_path",
        help="The folder in which to save the downloaded files.",
        type=str,
    )
    parser.add_argument(
        "-o",
        "--overwrite",
        help=(
            "If True, existing files are downloaded again and overwritten. "
            "Defaults to False."
        ),
        type=bool,
        default=False,
        nargs="?",  # Argument is optional
    )

    args = parser.parse_args()

    download_gedi_order(
        order_number=args.order_number,
        n_zips=args.n_zips,
        max_threads=args.max_threads,
        save_dir=args.save_path,
    )
