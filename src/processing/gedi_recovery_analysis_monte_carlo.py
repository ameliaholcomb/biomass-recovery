from multiprocessing.sharedctypes import Value
import geopandas as gpd
import logging
import math
import numpy as np
import os
import pandas as pd
import pathlib
import pyproj
import utm

from src import constants
from src.processing.gedi_recovery_analysis import (
    overlay_gedi_shots_and_recovery_raster,
)
from src.processing.jrc_processing import compute_recovery_period
from src.data.jrc_loading import load_jrc_data
from src.data.gedi_database import GediDatabase
from src.utils.logging import get_logger

logger = get_logger(__file__)
logger.setLevel(logging.DEBUG)


# Mean GEDI shot geolocation error in meters
# See Dubayah et al. 2021
MEAN_GEOLOCATION_ERROR_M = 10.2

# TODO(amelia): Simplify this explanation based on the
# formula for mean euclidean distance of normal distribution.

# Standard deviation of the distribution of GEDI geolocation error in 1 dimension.
# Assume the geolocation error follows a multivariate
# normal distribution with covariance matrix of the form
# [w 0]
# [0 w]
# That is, the geolocation error variance in the x and y directions
# are assumed to be equal and independent.
# This is a multivariate Gaussian centered at [0,0] with circular contours.
# We can derive
# w = (MEAN_GEOLOCATION_ERROR * sqrt(2/pi)) ** 2
# based on the mean of the distribution given in Dubayah et al.
#
# Rather than drawing from a multivariate normal,
# for coding simplicity we will equivalently sample (X,Y) independently from
# X = N(x_observed, sigma)
# Y = N(y_observed, sigma)
# where sigma = sqrt(w)
LOCATION_DIST_SD = MEAN_GEOLOCATION_ERROR_M * math.sqrt(2 / math.pi)


# Note: Safe in the UTM coordinate system, where step size
# differs by less than 0.00000001 across the raster
def get_idx(array, values):
    step = array[1] - array[0]
    return np.floor((values - array[0]) / step).astype(np.int64)


def quickfilter_shots(gedi_shots, recovery_period):
    # Remove all shots where fewer than four of nine surrounding pixels are recovering
    # Otherwise, we'll be saving a lot of data unnecessarily
    x_inds = get_idx(recovery_period.x.data, gedi_shots.lon_lowestmode.values)
    y_inds = get_idx(recovery_period.y.data, gedi_shots.lat_lowestmode.values)
    len_x = recovery_period.x.data.shape[0]
    len_y = recovery_period.y.data.shape[0]
    filtered_shots_idx = []
    for i in range(len(x_inds)):
        y_ind = y_inds[i]
        x_ind = x_inds[i]
        if (
            y_ind + 1 < len_y
            and y_ind - 1 > 0
            and x_ind + 1 < len_x
            and x_ind - 1 > 0
        ):
            recovery_around_shot = recovery_period.data[
                y_inds[i] - 1 : y_inds[i] + 2,
                x_inds[i] - 1 : x_inds[i] + 2,
            ]
            if np.sum(recovery_around_shot > 0) > 4:
                filtered_shots_idx.append(i)
    return gedi_shots.iloc[filtered_shots_idx]


def overlay_utm_sample_and_recovery_raster(
    easting, northing, recovery_period_utm
):
    x_inds = get_idx(recovery_period_utm.x.data, easting).clip(
        0, len(recovery_period_utm.x.data) - 1
    )
    y_inds = get_idx(recovery_period_utm.y.data, northing).clip(
        0, len(recovery_period_utm.y.data) - 1
    )
    # TODO: fill value in reprojected recovery period raster?
    return recovery_period_utm.data[y_inds, x_inds]


def jrc_recovery(
    geometry: gpd.GeoSeries,
    year: int,
    include_degraded: bool = False,
    include_nonforest: bool = False,
):
    # Load JRC data within tile
    logger.info("Loading JRC data from 1990 to %s", year)
    first_deforestation = load_jrc_data(
        *geometry.bounds.values[0], dataset="DeforestationYear"
    )
    first_degradation = None
    if not include_degraded:
        first_degradation = load_jrc_data(
            *geometry.bounds.values[0], dataset="DegradationYear"
        )
    annual_change = load_jrc_data(
        *geometry.bounds.values[0],
        dataset="AnnualChange",
        years=list(range(1990, year + 1)),
    )

    # Compute recovery period from JRC
    # Note that we have to compute from the annual change data directly
    # rather than using saved values in LastDeforestationYear
    # because these saved values are all for the full period (1982-2021)
    # but the GEDI shots may have been taken earlier
    # TODO(amelia): Find a way to usefully cache these values.
    logger.info("Computing recovery period until year %s", year)
    recovery_period = compute_recovery_period(
        annual_change,
        first_deforestation,
        first_degradation,
        include_degraded=include_degraded,
        include_nonforest=include_nonforest,
    )
    annual_change.close()
    first_degradation.close()
    first_deforestation.close()
    return recovery_period


def get_gedi_shots(
    geometry: gpd.GeoDataFrame,
    year: int,
    crs: str = constants.WGS84,
):
    database = GediDatabase()

    # Load GEDI data within tile
    logger.info("Loading Level 4a GEDI shots for %s in this geometry", year)
    gedi_shots = database.query(
        table_name="level_4a",
        columns=[
            "shot_number",
            "absolute_time",
            "lon_lowestmode",
            "lat_lowestmode",
            "agbd",
            "agbd_pi_lower",
            "agbd_pi_upper",
            "agbd_se",
            "l2_quality_flag",
            "l4_quality_flag",
            "degrade_flag",
            "beam_type",
            "sensitivity",
        ],
        geometry=geometry,
        crs=crs,
        start_time=f"{year}-01-01",
        end_time=f"{year+1}-01-01",
    )
    logger.debug(
        "Found %s shots in %s in the specified geometry", len(gedi_shots), year
    )

    # Preliminary filtering to reduce computation size
    gedi_shots = gedi_shots[
        (gedi_shots.l2_quality_flag == 1)
        & (gedi_shots.l4_quality_flag == 1)
        & (gedi_shots.degrade_flag == 0)
    ]
    return gedi_shots


def compute_monte_carlo_recovery(
    geometry: gpd.GeoDataFrame,
    year: int,
    token: str,
    finterface,
    num_iterations: int = 1000,
    include_degraded: bool = False,
    include_nonforest: bool = False,
):
    ## 1. Load GEDI shots and recovery periods
    gedi_shots = get_gedi_shots(geometry, year, constants.WGS84)
    if len(gedi_shots) == 0:
        logger.warning(f"Found 0 shots in the specified geometry in {year}.")
        return pd.DataFrame(
            {
                "token": [token],
                "year": [year],
                "has_data": [False],
                "error": [False],
                "error_message": [None],
            }
        )

    recovery_period = jrc_recovery(
        geometry=geometry,
        year=year,
        include_degraded=include_degraded,
        include_nonforest=include_nonforest,
    )

    ## 2. Quickly filter GEDI shots to reduce the computation size
    gedi_shots = quickfilter_shots(gedi_shots, recovery_period)
    if len(gedi_shots) == 0:
        logger.warning(
            f"No shots found overlapping with JRC recovery data in {year}"
        )
        return pd.DataFrame(
            {
                "token": [token],
                "year": [year],
                "has_data": [False],
                "error": [False],
                "error_message": [None],
            }
        )

    ## 3. Reproject onto local UTM zone for metric computations
    easting, northing, zone_num, zone_let = utm.from_latlon(
        gedi_shots.lat_lowestmode.values, gedi_shots.lon_lowestmode.values
    )
    utm_crs = pyproj.CRS.from_dict(
        {
            "proj": "utm",
            "zone": zone_num,
            "north": zone_let > "M",
            "south": zone_let <= "M",
        }
    )
    # Note: this reprojection does not 'smear' pixel values
    # However, it may add fill pixels. These will be set to np.nan --
    # worth noting that this is the same fill value as non-recovering pixels
    recovery_period_utm = recovery_period.rio.reproject(utm_crs, nodata=np.nan)

    ## 4. Generate random sample of shot locations and AGBD values
    sample_shape = (len(gedi_shots), num_iterations)
    rng = np.random.default_rng()
    easting_sample = rng.normal(
        np.expand_dims(easting, axis=1),
        LOCATION_DIST_SD,
        size=sample_shape,
    )
    northing_sample = rng.normal(
        np.expand_dims(northing, axis=1),
        LOCATION_DIST_SD,
        size=sample_shape,
    )
    agbd_sample = rng.normal(
        np.expand_dims(gedi_shots.agbd, axis=1),
        np.expand_dims(gedi_shots.agbd_se, axis=1),
        size=sample_shape,
    )
    agbd_sample = agbd_sample.clip(0, None)

    ## 5. Using sampled shot locations, get sample of recovery values
    recovery_sample = overlay_utm_sample_and_recovery_raster(
        easting_sample, northing_sample, recovery_period_utm
    )

    if not (recovery_sample.shape[0] == len(agbd_sample)):
        raise RuntimeError(
            (
                "Cannot match recovery samples (shape = {}) with agbd (n = {})"
            ).format(recovery_sample.shape, len(agbd_sample))
        )

    # Close rasters and return results
    recovery_period_utm.close()
    recovery_period.close()
    recovery_cols = ["r_{}".format(i) for i in range(num_iterations)]
    recovery_sample_df = pd.DataFrame(recovery_sample, columns=recovery_cols)
    agbd_cols = ["a_{}".format(i) for i in range(num_iterations)]
    agbd_sample_df = pd.DataFrame(agbd_sample, columns=agbd_cols)
    gedi_shots = gedi_shots.reset_index(drop=True)
    master_df = pd.concat(
        [gedi_shots, recovery_sample_df, agbd_sample_df], axis=1
    )

    finterface.save_data(
        token=token, year=year, data_type="shotinfo", data=gedi_shots
    )
    finterface.save_data(
        token=token, year=year, data_type="recovery", data=recovery_sample
    )
    finterface.save_data(
        token=token, year=year, data_type="agbd", data=agbd_sample
    )
    finterface.save_data(
        token=token, year=year, data_type="master", data=master_df
    )

    return pd.DataFrame(
        {
            "token": [token],
            "year": [year],
            "has_data": [True],
            "error": [False],
            "error_message": [None],
        }
    )
