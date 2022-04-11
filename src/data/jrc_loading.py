"""Functions to query targeted regions of specific JRC datasets from disk"""
import logging
import pathlib
from typing import Optional, Tuple, List

import numpy as np
import rioxarray as rxr
import xarray as xr
from tqdm.autonotebook import tqdm

from src.constants import JRC_PATH
from src.utils.logging import get_logger
from src.utils.os import list_content

logger = get_logger(__file__)
logger.setLevel(logging.INFO)

JRC_DATASETS = (
    "AnnualChange",
    "DeforestationYear",
    "DegradationYear",
    "TransitionMap_MainClasses",
    "TransitionMap_Subtypes",
    "LastDeforestationYear",  # custom computed
    "LastDegradationYear",  # custom computed
)


def _to_nesw(lon: float, lat: float) -> tuple[tuple[float, float], tuple[str, str]]:
    lon_ew = "W" if lon < 0 else "E"
    lat_ns = "S" if lat < 0 else "N"
    return (abs(lon), abs(lat)), (lon_ew, lat_ns)


def floor_to_nearest(number: float, nearest: int) -> int:
    return int(np.floor(number / nearest) * nearest)


def ceil_to_nearest(number: float, nearest: int) -> int:
    return int(np.ceil(number / nearest) * nearest)


def _enclosing_jrc_tile(lon: float, lat: float) -> str:

    left = floor_to_nearest(lon, 10)
    top = ceil_to_nearest(lat, 10)
    (left, top), (lon_dir, lat_dir) = _to_nesw(left, top)

    return f"{lat_dir}{top}_{lon_dir}{left}"


def _get_jrc_tiles(
    min_lon: float, min_lat: float, max_lon: float, max_lat: float
) -> tuple[str]:

    # Note: only works for shapes smaller than 10 degrees in each direction
    #  (i.e. smaller than JRC tile size. For larger sizes intermediate tiles
    #   inbetween the bounding box corners would need to be treated)
    assert max_lon - min_lon <= 10
    assert max_lat - min_lat <= 10

    relevant_tiles = set(
        [
            _enclosing_jrc_tile(min_lon, max_lat),
            _enclosing_jrc_tile(max_lon, max_lat),
            _enclosing_jrc_tile(min_lon, min_lat),
            _enclosing_jrc_tile(max_lon, min_lat),
        ]
    )

    return tuple(relevant_tiles)


def get_jrc_paths(
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
    dataset: str = "DeforestationYear",
    year: Optional[int] = None,
) -> tuple[pathlib.Path]:

    if dataset == "AnnualChange":
        if year is None:
            raise ValueError(
                "Year must be specified when using the AnnualChange dataset"
            )
        dataset_path = JRC_PATH / dataset / "tifs"
    else:
        if year is not None:
            raise ValueError(
                f"Cannot specify year `{year}` with dataset `{dataset}`. "
                "The year parameter is only meant for the AnnualChange dataset."
            )
        dataset_path = JRC_PATH / dataset
    if not dataset_path.exists():
        raise ValueError(
            f"Dataset {dataset} does not exit, or does not exist at {dataset_path}. "
            f"Possible datasets are {JRC_DATASETS}. "
        )

    tile_paths = []
    for tile in _get_jrc_tiles(min_lon, min_lat, max_lon, max_lat):
        relevant_tiles = list_content(
            dataset_path,
            f"*_{str(year) + '_*_' if year is not None else ''}{tile}*.tif",
        )
        logger.debug("Relevant tiles %s", relevant_tiles)
        assert len(
            relevant_tiles) == 1, f"Found no relevant tile {tile} for {dataset}."
        tile_paths.append(relevant_tiles[0])

    return tile_paths


def load_jrc_data(
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
    dataset: str = "DeforestationYear",
    years: Optional[int] = None,
) -> Tuple[xr.DataArray, List[xr.DataArray]]:
    return load_jrc_data_with_resources(min_lon, min_lat, max_lon, max_lat, dataset, years)[0]


def load_jrc_data_with_resources(
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
    dataset: str = "DeforestationYear",
    years: Optional[int] = None,
) -> Tuple[xr.DataArray, List[xr.DataArray]]:
    # Treat annual change dataset
    if dataset == "AnnualChange":
        if isinstance(years, int):
            years = [years]
        assert len(years) > 0, "years must be an iterable of integers"

        tile_paths = [
            get_jrc_paths(min_lon, min_lat, max_lon, max_lat, dataset, year)
            for year in years
        ]

        if not all(map(len, tile_paths)):
            raise NotImplementedError("Boundaries overlap multiple tiles")

        files_with_year = [
            (year, rxr.open_rasterio(tile_path[0])) for year, tile_path in
            zip(years, tile_paths)
        ]

        annual_change = xr.concat(
            [file.rio.slice_xy(min_lon, min_lat, max_lon, max_lat) .squeeze().assign_coords({"year": year})
             for year, file in tqdm(files_with_year)],
            dim="year")

        files = [file for _, file in files_with_year] + [annual_change]
        return annual_change, files

    # Treat all other datasets
    tile_paths = get_jrc_paths(
        min_lon, min_lat, max_lon, max_lat, dataset, years)
    if len(tile_paths) == 1:
        file = rxr.open_rasterio(tile_paths[0])
        return (
            file
            .rio.slice_xy(min_lon, min_lat, max_lon, max_lat)
            .squeeze(), [file]
        )
    else:
        raise NotImplementedError("Boundaries overlap multiple tiles")
