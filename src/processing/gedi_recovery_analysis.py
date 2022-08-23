import logging

import numba
import numpy as np
import pandas as pd
import geopandas as gpd

from src.constants import WGS84
from src.data.gedi_database import GediDatabase
from src.data.jrc_loading import load_jrc_data
from src.processing.jrc_processing import compute_recovery_period
from src.utils.logging import get_logger

logger = get_logger(__file__)
logger.setLevel(logging.DEBUG)


@numba.njit
def argnearest(array, values):
    argmins = np.zeros_like(values, dtype=np.int64)
    for i, value in enumerate(values):
        argmins[i] = (np.abs(array - value)).argmin()
    return argmins


@numba.njit
def arg_toptwo_nearest_centers(array, values):
    half_pixel = (array[1] - array[0]) / 2
    array_center = array + half_pixel
    argmins = np.zeros((*values.shape, 2), dtype=np.int64)
    for i, value in enumerate(values):
        argmins[i, 0] = (np.abs(array_center - value)).argmin()
        if value < array_center[argmins[i, 0]]:
            argmins[i, 1] = argmins[i, 0] - 1
        else:
            argmins[i, 1] = argmins[i, 0] + 1

    return argmins


def overlay_gedi_shots_and_recovery_raster(
    gedi_shots, recovery_period, keep_distribution: bool = False
):

    shot_ids = gedi_shots.shot_number.values
    x_inds = arg_toptwo_nearest_centers(
        recovery_period.x.data, gedi_shots.lon_lowestmode.values
    )
    y_inds = arg_toptwo_nearest_centers(
        recovery_period.y.data, gedi_shots.lat_lowestmode.values
    )
    len_x = recovery_period.x.data.shape[0]
    len_y = recovery_period.y.data.shape[0]
    logger.info("Filtering shots")
    # This loop is quite fast in my test, does not need numba.
    filtered_shots = []
    for shot_id, y_ind, x_ind in zip(shot_ids, y_inds, x_inds):
        recovery_at_shot = recovery_period.data[y_ind[0], x_ind[0]]

        if recovery_at_shot > 0:
            # OLD QUALITY: Check the nine surrounding pixels.
            recovery_around_shot = recovery_period.data[
                y_ind[0] - 1 : y_ind[0] + 2, x_ind[0] - 1 : x_ind[0] + 2
            ]
            nine_recovering = recovery_around_shot.shape == (
                3,
                3,
            ) and np.alltrue(recovery_around_shot > 0)
            if keep_distribution:
                if nine_recovering:
                    filtered_shots.append(
                        (
                            shot_id,
                            *recovery_around_shot[0],
                            *recovery_around_shot[1],
                            *recovery_around_shot[2],
                            recovery_around_shot.max()
                            - recovery_around_shot.min(),
                        )
                    )
                continue

            if nine_recovering and np.alltrue(
                recovery_around_shot == recovery_at_shot
            ):
                nine_recovering_same_age = True
            else:
                nine_recovering_same_age = False

            # NEW QUALITY: Check three surrounding pixels.
            if y_ind[1] < len_y and x_ind[1] < len_x:
                p2 = recovery_period.data[y_ind[0], x_ind[1]]
                p3 = recovery_period.data[y_ind[1], x_ind[0]]
                p4 = recovery_period.data[y_ind[1], x_ind[1]]

                four_recovering = p2 > 0 and p3 > 0 and p4 > 0
                if four_recovering and p2 == p3 == p4 == recovery_at_shot:
                    four_recovering_same_age = True
                else:
                    four_recovering_same_age = False
            else:
                four_recovering = False
                four_recovering_same_age = False

            if nine_recovering_same_age:
                quality = 5
            elif four_recovering_same_age and nine_recovering:
                quality = 4
            elif nine_recovering:
                quality = 3
            elif four_recovering_same_age:
                quality = 2
            elif four_recovering:
                quality = 1
            else:
                quality = 0
            filtered_shots.append((shot_id, recovery_at_shot, quality))
    # Return the shots
    return filtered_shots


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
    logger.info("Computing recovery period until year %s", year)
    recovery_period = compute_recovery_period(
        annual_change,
        first_deforestation,
        first_degradation,
        include_degraded=include_degraded,
        include_nonforest=include_nonforest,
    )
    return recovery_period


def compute_gedi_recovery_l2a(
    geometry: gpd.GeoSeries, year: int, crs: str = WGS84
):

    database = GediDatabase()

    # Load GEDI data within tile
    logger.info("Loading Level 2a GEDI shots for %s", year)
    gedi_shots = database.query(
        table_name="level_2a",
        columns=[
            "shot_number",
            "absolute_time",
            "lon_highestreturn",
            "lat_highestreturn",
            "rh95",
            "geometry",
        ],
        geometry=geometry,
        crs=crs,
        start_time=f"{year}-01-01",
        end_time=f"{year+1}-01-01",
    )
    logger.debug(
        "Found %s shots in %s in the specified geometry", len(gedi_shots), year
    )
    if len(gedi_shots) == 0:
        raise RuntimeError("Found 0 shots in the specified geometry and year.")

    recovery_period = jrc_recovery(geometry=geometry, year=year)

    # Extract JRC locations that correspond to GEDI shots
    filtered_shots = overlay_gedi_shots_and_recovery_raster(
        gedi_shots, recovery_period
    )
    if len(filtered_shots) == 0:
        logger.warning(
            f"No shots found overlapping with JRC recovery data in {year}"
        )
        return None
    dataset = pd.DataFrame(
        filtered_shots,
        columns=["shot_number", "recovery_period", "overlap_quality"],
    )
    return dataset.join(
        gedi_shots[["shot_number", "rh95"]].set_index("shot_number"),
        on="shot_number",
    )


def compute_gedi_recovery_l4a(
    geometry: gpd.GeoDataFrame,
    year: int,
    crs: str = WGS84,
    include_degraded: bool = False,
    include_nonforest: bool = False,
    keep_distribution: bool = False,
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
            "geometry",
        ],
        geometry=geometry,
        crs=crs,
        start_time=f"{year}-01-01",
        end_time=f"{year+1}-01-01",
    )
    logger.debug(
        "Found %s shots in %s in the specified geometry", len(gedi_shots), year
    )
    if len(gedi_shots) == 0:
        logger.warning(f"Found 0 shots in the specified geometry in {year}.")
        return pd.DataFrame({})

    recovery_period = jrc_recovery(
        geometry=geometry,
        year=year,
        include_degraded=include_degraded,
        include_nonforest=include_nonforest,
    )

    # Extract JRC locations that correspond to GEDI shots
    filtered_shots = overlay_gedi_shots_and_recovery_raster(
        gedi_shots, recovery_period, keep_distribution=keep_distribution
    )
    if len(filtered_shots) == 0:
        logger.warning(
            f"No shots found overlapping with JRC recovery data in {year}"
        )
        return pd.DataFrame({})
    if keep_distribution:
        dataset = pd.DataFrame(
            filtered_shots,
            columns=[
                "shot_number",
                "r00",
                "r01",
                "r02",
                "r10",
                "r11",
                "r12",
                "r20",
                "r21",
                "r22",
                "recovery_range",
            ],
        )
        dataset["year"] = np.repeat(year, len(filtered_shots))
    else:
        dataset = pd.DataFrame(
            filtered_shots,
            columns=["shot_number", "recovery_period", "overlap_quality"],
        )
    return dataset.join(
        gedi_shots[
            [
                "shot_number",
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
            ]
        ].set_index("shot_number"),
        on="shot_number",
    )
