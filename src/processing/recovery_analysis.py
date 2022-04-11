import logging

import numba
import numpy as np
import pandas as pd
import xarray as xr
from rasterio.enums import Resampling
from tqdm.autonotebook import tqdm

from src.data.als_loading import load_als_survey, load_raster_with_resources
from src.data.jrc_loading import load_jrc_data_with_resources
from src.processing.canopy_height_models import (
    asner2014_acd,
    asner2014_colombia_acd,
    longo2016_acd,
)
from src.processing.jrc_processing import compute_recovery_period
from src.utils.logging import get_logger
import gc

logger = get_logger(__name__)
logger.setLevel(logging.INFO)

EXTRA_ALS_DATASETS = {
    "mean": 50,
    "max": 50,
    "quantile0.05": 50,
    "quantile0.1": 50,
    "interquartile_range": 50,
    "kurtosis": 50,
    "longo_biomass": 50,
    "n_pulses": 1,
    "n_points": 1,
    "n_ground_points": 1,
}

CHM_RESAMPLING_METHODS = dict(
    mean=Resampling.average,
    min=Resampling.min,
    max=Resampling.max,
    med=Resampling.med,
    mode=Resampling.mode,
    q1=Resampling.q1,
    q3=Resampling.q3,
)


@numba.njit
def quality_judge(arr: np.array) -> np.array:
    quality = np.zeros_like(arr, dtype=np.uint8)

    n_rows, n_cols = arr.shape
    for row in range(1, n_rows - 1):
        for col in range(1, n_cols - 1):
            if np.isnan(arr[row, col]):
                continue
            else:
                patch = arr[row - 1: row + 2, col - 1: col + 2]
                if np.prod(patch.flatten() >= 0):
                    if np.prod(patch.flatten() == patch[0, 0]):
                        quality[row, col] = 2
                    else:
                        quality[row, col] = 1
    return quality


def compute_recovery_dataset(
    survey_name: str, extra_als_datasets: dict = EXTRA_ALS_DATASETS
):
    tracked_resources = []

    def track_resources(dataset, resources):
        tracked_resources.extend(resources)
        return dataset

    try:
        survey = load_als_survey(survey_name)

        # Load canopy height model (CHM)
        logger.info("Loading canopy height model.")
        chm = track_resources(*load_raster_with_resources(
            survey.name, "canopy_height_model", 1, from_crs=survey.crs, to_crs=None))
        logger.info("Looading extra ALS datasets")
        als_data = {
            dataset: track_resources(*load_raster_with_resources(
                survey.name, dataset, resolution, from_crs=survey.crs, to_crs=None
            ))
            for dataset, resolution in tqdm(extra_als_datasets.items(), leave=False)
        }

        # Extract relevant JRC datasets
        logger.info("Loading relevant JRC data.")
        relevant_datasets = (
            "AnnualChange", "DeforestationYear", "DegradationYear")
        jrc_data = {
            dataset: track_resources(*load_jrc_data_with_resources(
                survey.lon_min,
                survey.lat_min,
                survey.lon_max,
                survey.lat_max,
                dataset=dataset,
                years=list(range(1990, survey.survey_year + 1))
                if dataset == "AnnualChange"
                else None,
            ))
            for dataset in tqdm(relevant_datasets, leave=False)
        }

        # Compute spatially aggregated chm statistics
        logger.info("Computing spatially aggregated CHM statistics.")
        chm_data = {
            name: chm.rio.reproject_match(
                als_data["mean"], resampling=method).squeeze()
            for name, method in tqdm(CHM_RESAMPLING_METHODS.items(), leave=False)
        }

        # Compute recovery period
        logger.info("Computing recovery period.")
        recovery_period = compute_recovery_period(
            jrc_data["AnnualChange"],
            jrc_data["DeforestationYear"],
            jrc_data["DegradationYear"],
        )
        # Compute quality indicator
        logger.info("Computing quality indicator.")
        quality_indicator = xr.zeros_like(recovery_period)
        quality_indicator.data = quality_judge(recovery_period.data)

        # Reproject recovery period and quality indicator
        logger.info("Reprojecting recovery period")
        recovery_period = recovery_period.rio.reproject_match(
            als_data["mean"], resampling=Resampling.nearest
        )
        if not recovery_period.max() > 0:
            raise RuntimeError("Found no recovering forest in survey area.")
        quality_indicator = quality_indicator.rio.reproject_match(
            als_data["mean"], resampling=Resampling.nearest
        )

        # Build final dataframe
        logger.info("Assembling dataframe.")
        # Add recovery data
        final_data = {
            "recovery_period": recovery_period.data.flatten().astype(np.uint8),
            "overlap_quality": quality_indicator.data.flatten().astype(np.uint8),
        }

        # Add CHM data
        for key, val in chm_data.items():
            final_data[f"chm_{key}"] = val.where(
                recovery_period > 0).data.flatten()

        # Add ALS point cloud statistics data
        for key, val in als_data.items():
            resolution = extra_als_datasets[key]
            if resolution == extra_als_datasets["mean"]:
                # To treat grid metrics
                final_data[key] = val.where(recovery_period > 0).data.flatten()
            else:
                # To treat number of pulses, points and ground points
                final_data[key] = (
                    val.rio.reproject_match(
                        recovery_period, resampling=Resampling.sum)
                    .where(recovery_period > 0)
                    .data.flatten()
                    .astype(np.uint64)
                )

        # Add index data
        # Extract indices and coordinates
        x_coord, y_coord = np.meshgrid(recovery_period.x, recovery_period.y)
        x_ind, y_ind = np.meshgrid(
            np.arange(0, len(recovery_period.x)), np.arange(
                0, len(recovery_period.y))
        )
        final_data["x_coord"] = x_coord.flatten()
        final_data["y_coord"] = y_coord.flatten()
        final_data["x_index"] = x_ind.flatten().astype(np.uint16)
        final_data["y_index"] = y_ind.flatten().astype(np.uint16)

        # Add Biomass data
        df = pd.DataFrame(final_data).dropna()
        df["acd_longo2016"] = df["chm_mean"].apply(longo2016_acd)
        df["acd_asner2014"] = df["chm_mean"].apply(asner2014_acd)
        df["acd_asner2014c"] = df["chm_mean"].apply(asner2014_colombia_acd)

        # Add survey metadata
        df["survey"] = survey_name
        df["survey_year"] = survey.survey_year
        df["crs"] = recovery_period.rio.crs.to_proj4()

        return df.reset_index(drop=True)
    finally:
        for f in tracked_resources:
            try:
                f.close()
            except Exception as e:
                logging.error(
                    'Received error while closing file: {}'.format(e))
        gc.collect()
        
