"""Functions to extract last year of deforestation or degradation from JRC data."""

import argparse
import re
from typing import Optional, Union

import numba
import numpy as np
import rioxarray as rxr
import xarray as xr
from tqdm.autonotebook import tqdm

from src.constants import JRC_PATH
from src.utils.logging import get_logger

logger = get_logger(__file__)

ANNUAL_CHANGE_PATH = JRC_PATH / "AnnualChange/tifs"
JRC_ANNUAL_CHANGE_UNDISTURBED = 1
JRC_ANNUAL_CHANGE_DEGRADATION = 2
JRC_ANNUAL_CHANGE_DEFORESTATION = 3
JRC_ANNUAL_CHANGE_RECOVERY = 4


@numba.njit
def last_of_value(
    array: np.ndarray,
    value: Union[int, float],
    fill_value: Union[int, float] = -9999,
    dtype: type = np.int16,
) -> np.ndarray:
    """
    Find index of last occurence of `value` along the first axis of the array.

    Args:
        array (np.ndarray): The 3D numpy array to investigate. The shape should be
            (obervations, rows, columns)
        value (Union[int, float]): The value for whichs last occurence should be noted
        fill_value (Union[int, float], optional): Fill value if `value` is not found
            along the first axis. Defaults to -9999.
        dtype (type, optional): The return datatype. Should be an integer type, since
            we deal with indices. Defaults to np.int16.

    Returns:
        np.array: A 2D numpy array of shape (rows, columns) and the value of the element
            [row, column] will be the last occurence of `value` in `array` (or
            `fill_value` if the value doesn't occur.)
    """
    assert len(array.shape) == 3, "This function assumes a 3d array"
    _, nrows, ncols = array.shape
    last_observation = np.ones_like(array[0], dtype=dtype) * fill_value
    for row in range(nrows):
        for col in range(ncols):
            value_observations = np.argwhere(array[:, row, col] == value)
            if len(value_observations) > 0:
                last_observation[row, col] = value_observations.max()

    return last_observation


@numba.njit
def first_of_value(
    array: np.ndarray,
    value: Union[int, float],
    fill_value: Union[int, float] = -9999,
    dtype: type = np.int16,
) -> np.ndarray:
    """
    Find index of first occurence of `value` along the first axis of the array.

    Args:
        array (np.ndarray): The 3D numpy array to investigate. The shape should be
            (obervations, rows, columns)
        value (Union[int, float]): The value for whichs first occurence should be noted
        fill_value (Union[int, float], optional): Fill value if `value` is not found
            along the first axis. Defaults to -9999.
        dtype (type, optional): The return datatype. Should be an integer type, since
            we deal with indices. Defaults to np.int16.

    Returns:
        np.array: A 2D numpy array of shape (rows, columns) and the value of the element
            [row, column] will be the first occurence of `value` in `array` (or
            `fill_value` if the value doesn't occur.)
    """
    assert len(array.shape) == 3, "This function assumes a 3d array"
    _, nrows, ncols = array.shape
    first_observation = np.ones_like(array[0], dtype=dtype) * fill_value
    for row in range(nrows):
        for col in range(ncols):
            value_observations = np.argwhere(array[:, row, col] == value)
            if len(value_observations) > 0:
                first_observation[row, col] = value_observations.min()

    return first_observation


def compute_last_observation(
    annual_change: xr.DataArray,
    jrc_class_value: int,
    first_observation: xr.DataArray = None,
    year_offset: int = 1990,
):

    if first_observation is not None:
        last_observation = xr.zeros_like(first_observation)
    else:
        last_observation = xr.zeros_like(annual_change[0])

    # Compute last year in which a given JRC class value was observed
    #  (Note: Due to the size of the datasets this takes
    #  ~10 min, even with numba speedup)
    last_observation.data = last_of_value(annual_change.data, value=jrc_class_value)

    # Fix all negative/fill values and all values prior to 1990 with the first year of
    #  deforestation.
    if first_observation is not None:
        last_year = annual_change.year.data[-1]
        last_observation = xr.concat(
            [
                first_observation.where(first_observation <= last_year, other=0),
                last_observation + year_offset,
            ],
            dim="observations",
        ).max(dim="observations")
    else:
        last_observation = (year_offset + last_observation).where(
            last_observation > 0, other=0
        )

    return last_observation.astype(float)


def compute_recovery_period(
    annual_change: xr.DataArray,
    first_deforestation: Optional[xr.DataArray] = None,
    first_degradation: Optional[xr.DataArray] = None,
    as_startyear: bool = False,
):

    last_deforested = compute_last_observation(
        annual_change, JRC_ANNUAL_CHANGE_DEFORESTATION, first_deforestation
    )
    last_degraded = compute_last_observation(
        annual_change, JRC_ANNUAL_CHANGE_DEGRADATION, first_degradation
    )

    survey_year = annual_change.year.data[-1]
    deforested_before_survey = last_deforested < survey_year
    not_degraded_since_last_deforested = last_deforested > last_degraded
    recovering = annual_change.loc[survey_year] == JRC_ANNUAL_CHANGE_RECOVERY

    undisturbed_recovery = (
        recovering & deforested_before_survey & not_degraded_since_last_deforested
    )

    if as_startyear:
        return last_deforested.where(undisturbed_recovery)
    return survey_year - last_deforested.where(undisturbed_recovery)


# pylint: disable=redefined-outer-name
def calculate_jrc_last_observation_year(
    tile_identifier: str,
    jrc_class_value: int = JRC_ANNUAL_CHANGE_DEFORESTATION,  # 3 is deforestation class
    dataset: str = "DeforestationYear",
    overwrite: bool = False,
    final_year: int = 2019,
) -> bool:
    """
    Calculates the year of last observation of `jrc_class_value`in the JRC AnnualChange
    dataset.

    JRC dataset user guide:
    https://forobs.jrc.ec.europa.eu/TMF/download/TMF_DataUsersGuide.pdf

    Args:
        tile_identifier (str): The identifier of the JRC tile for which to compute the
            last observation of `jrc_class_value` for each pixel in the JRC AnnualChange
            dataset.
        jrc_class_value (int, optional): JRC class value. Class value `2` corresponds
            to degradation (in the AnnualChange datasett) and class value `3`
            corresponds to deforestation. Defaults to 3.
            More generally:
                1 - undistrubed
                2 - degraded
                3 - deforested
                4 - regrowth
                5 - permanent and seasonal water
                6 - other
        dataset (str, optional): Which dataset to use as a stand-in template for the
            results. This will usually be `DeforestationYear` or `DegradationYear`.
            Defaults to "DeforestationYear".
        overwrite (bool, optional): Whether to overwrite existing files. Default to
            False.

    Returns:
        bool: True, iff the calculation was successful and the output was saved.
    """

    first_observation_path = (
        JRC_PATH
        / dataset
        / f"JRC_TMF_{dataset}_v1_1982_{final_year}_{tile_identifier}.tif"
    )
    annual_change_paths = {
        year: (
            JRC_PATH
            / "AnnualChange"
            / "tifs"
            / f"JRC_TMF_AnnualChange_v1_{year}_{tile_identifier}.tif"
        )
        for year in range(1990, final_year + 1)
    }

    if not first_observation_path.exists():
        raise FileNotFoundError(
            f"No tile with identifier {tile_identifier} found at "
            f"{first_observation_path}."
        )

    for path in annual_change_paths.values():
        if not path.exists():
            raise FileNotFoundError(
                f"No tile with identifier {tile_identifier} found at {path}."
            )

    save_path = (
        JRC_PATH
        / f"Last{dataset}"
        / f"JRC_TMF_Last{dataset}_v1_1982_2019_{tile_identifier}.tif"
    )
    if save_path.exists() and not overwrite:
        raise FileExistsError(
            f"File {save_path} exists. To overwrite set overwrite to true."
        )

    # Creation of "last_year_of_observation" dataset:
    logger.info("Load Annual change maps for tile %s", tile_identifier)
    annual_change_maps = xr.concat(
        objs=[
            rxr.open_rasterio(path).squeeze().assign_coords({"year": year})
            for year, path in tqdm(annual_change_paths.items())
        ],
        dim="year",
    )

    # Get first year of observation map
    logger.info("Load year of %s dataset", dataset)
    first_observation = rxr.open_rasterio(first_observation_path).squeeze()

    last_observation = compute_last_observation(
        annual_change=annual_change_maps,
        jrc_class_value=jrc_class_value,
        first_observation=first_observation,
    )

    # Save result
    logger.info("Save result to compressed GeoTiff")
    save_path.parent.mkdir(parents=True, exist_ok=True)
    last_observation.rio.to_raster(
        save_path,
        compress="lzw",
    )

    return True


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="GEDI order download script")
    parser.add_argument(
        "-v",
        "--jrc_class_value",
        help=(
            "JRC class value. Class value `2` corresponds to degradation (in the "
            "annual change data) and class value `3` corresponds to deforesttation. "
            "More generally: 1 - undistrubed, 2- degraded, 3 - deforested, 4 - "
            "regrowth, 5 - permanent and seasonal water, 6 - other. More here: "
            "https://forobs.jrc.ec.europa.eu/TMF/download/TMF_DataUsersGuide.pdf"
        ),
        type=int,
        default=3,
        nargs="?",  # Argument is optional
    )
    parser.add_argument(
        "-d",
        "--dataset",
        help=(
            "The dataset to look for a last observation for. Can be "
            "`DeforestationYear` or `DegradationYear`."
        ),
        type=str,
        default="DeforestationYear",
        nargs="?",  # Argument is optional
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

    relevant_tiles = list((JRC_PATH / f"{args.dataset}").glob("*_SAM*"))

    for tile in tqdm(relevant_tiles):
        tile_identifier = re.findall(r"\w+_(SAM_[\w_]+)\.tif", tile.name)[0]
        print("Processing", tile_identifier)
        try:
            calculate_jrc_last_observation_year(
                tile_identifier,
                jrc_class_value=args.jrc_class_value,
                dataset=args.dataset,
                overwrite=args.overwrite,
            )
        except FileExistsError:
            print(f"{tile_identifier} exists.")
        except FileNotFoundError:
            print("Did not find files.")
        except Exception as e:  # pylint: disable=broad-except
            print(e)
