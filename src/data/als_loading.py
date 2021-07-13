import logging
import os
import pathlib
import re
from typing import Optional

import geopandas as gpd
import rioxarray as rxr
import xarray as xr

from src.constants import EBALIDAR_PATH, PAISAGENSLIDAR_PATH, WEBMERCATOR
from src.utils.logging import get_logger

logger = get_logger(__name__)
logger.setLevel(logging.INFO)

# Load master data for ALS (Airborne lidar surveys) data
paisagens_master = gpd.read_feather(
    PAISAGENSLIDAR_PATH / "paisagens_master_table.feather"
)
eba_master = gpd.read_feather(EBALIDAR_PATH / "eba_master_table.feather")


def load_als_survey(survey_name: str) -> gpd.GeoSeries:
    if survey_name in paisagens_master.index:
        survey = paisagens_master.loc[survey_name]
    elif survey_name in eba_master.index:
        survey = eba_master.loc[survey_name]
    else:
        raise ValueError(f"Survey {survey_name} not found in master data.")
    return survey


def _get_available_metrics(path: os.PathLike) -> dict:
    """
    Parse all available metric files at `path` into a dictionary.

    Args:
        path (os.PathLike): The path to inspect for metric files.

    Returns:
        dict: A dictionary with all available metric files with the
            hierarchical keys "survey_name", "metric", "grid_resolution"
    """

    path = pathlib.Path(path)
    survey_name = path.parent.name
    assert path.parent.exists(), f"Survey {survey_name} does not exist."
    assert "grid_metrics" in str(
        path
    ), "This function is made to work with grid_metrics folders"

    tif_files = list(path.glob("*.tif"))
    assert len(tif_files) > 0, "No tif files."
    result = {survey_name: {}}

    for tif_path in tif_files:
        metric_gridsize = tif_path.name[len(survey_name) + 1 :]
        metric = re.findall(r"([a-z_]+[\d\.]*)_[\d\.]+m.tif", metric_gridsize)[0]
        resolution = re.findall(r"[a-z_]+[\d\.]*_([\d\.]+)m.tif", metric_gridsize)[0]

        if metric not in result[survey_name].keys():
            result[survey_name][metric] = {}
        if resolution not in result[survey_name][metric].keys():
            result[survey_name][metric][float(resolution)] = tif_path

    return result


def _fetch_paisagens_metrics(paisagens_survey_name: str) -> dict:
    year = re.findall(r"[A-Z]+_[A-Z\da-z]+_(\d{4})", paisagens_survey_name)[0]
    paisagens_path = (
        PAISAGENSLIDAR_PATH
        / "processed"
        / year
        / paisagens_survey_name
        / "grid_metrics"
    )
    return _get_available_metrics(paisagens_path)[paisagens_survey_name]


def _fetch_eba_metrics(eba_survey_name: str) -> dict:
    eba_path = EBALIDAR_PATH / "laz_EBA_processed" / eba_survey_name / "grid_metrics"
    return _get_available_metrics(eba_path)[eba_survey_name]


def fetch_metrics(survey_name: str) -> dict:
    if "NP_T" in survey_name:
        return _fetch_eba_metrics(survey_name)
    else:
        return _fetch_paisagens_metrics(survey_name)


def load_raster(
    survey_name: str,
    metric: str,
    grid_size: float,
    from_crs: Optional[str] = None,
    to_crs: Optional[str] = WEBMERCATOR,
) -> xr.DataArray:
    """
    Load raster with NaN values masked and reproject to desired CRS.

    Args:
        from_crs (Optional[str], optional): CRS of the raster that is loaded.
            Defaults to None.
        to_crs (Optional[str], optional): CRS to which to reproject. Defaults to WGS84.

    Returns:
        xr.DataArray: The output raster as xarray object
    """
    logger.debug("Fetching raster path")
    available_metrics = fetch_metrics(survey_name)
    if metric in available_metrics.keys():
        data_path = available_metrics[metric][grid_size]
    else:
        raise KeyError(f"No metric {metric} found. Found: {available_metrics.keys()}")

    logger.debug("Loading raster %s", data_path)
    raster = rxr.open_rasterio(data_path, masked=True)

    if from_crs is not None:
        logger.debug("Setting crs %s", from_crs)
        raster = raster.rio.set_crs(from_crs)

    if to_crs is not None:
        logger.debug("Reprojecting to crs %s", to_crs)
        raster = raster.rio.reproject(to_crs)

    return raster
