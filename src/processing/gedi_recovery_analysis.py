import logging

import numba
import numpy as np
import pandas as pd
import geopandas as gpd

from src.constants import WGS84
from src.data.gedi_loading import load_gedi_data
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


def overlay_gedi_shots_and_recovery_raster(gedi_shots, recovery_period):

    shot_ids = gedi_shots.shot_number.values
    x_inds = argnearest(recovery_period.x.data, gedi_shots.lon_highestreturn.values)
    y_inds = argnearest(recovery_period.y.data, gedi_shots.lat_highestreturn.values)

    logger.info("Filtering shots")
    # This loop is quite fast in my test, does not need numba.
    filtered_shots = []
    for shot_id, y_ind, x_ind in zip(shot_ids, y_inds, x_inds):
        recovery_at_shot = recovery_period.data[y_ind, x_ind]

        # If recovery_period at shot > 0
        if recovery_at_shot > 0:
            # Check the surrounding pixels as well
            recovery_around_shot = recovery_period.data[
                y_ind - 1 : y_ind + 2, x_ind - 1 : x_ind + 2
            ]
            # If the surrouding pixels also have recovery_period > 0 ...
            if recovery_around_shot.shape == (3, 3) and np.alltrue(
                recovery_around_shot > 0
            ):
                if np.alltrue(recovery_around_shot == recovery_at_shot):
                    # ... and all have the same recovery length, set quality flag 2
                    filtered_shots.append((shot_id, recovery_at_shot, 2))
                else:
                    # ... and have different recovery lengths, set quality flag 1
                    filtered_shots.append((shot_id, recovery_at_shot, 1))
            else:
                # ... else, set quality flag 0
                filtered_shots.append((shot_id, recovery_at_shot, 0))
    # Return the shots
    return filtered_shots


def compute_gedi_recovery(geometry: gpd.GeoSeries, year: int, crs: str = WGS84):

    # Load GEDI data within tile
    logger.info("Loading GEDI shots for %s", year)
    gedi_shots = load_gedi_data(
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

    # Load JRC data within tile
    logger.info("Loading JRC data from 1990 to %s", year)
    first_deforestation = load_jrc_data(*geometry.bounds.values[0], dataset="DeforestationYear")
    first_degradation = load_jrc_data(*geometry.bounds.values[0], dataset="DegradationYear")
    annual_change = load_jrc_data(
        *geometry.bounds.values[0], dataset="AnnualChange", years=list(range(1990, year + 1))
    )

    # Compute recovery period from JRC
    logger.info("Computing recovery period until year %s", year)
    recovery_period = compute_recovery_period(
        annual_change, first_deforestation, first_degradation
    )

    # Extract JRC locations that correspond to GEDI shots
    filtered_shots = overlay_gedi_shots_and_recovery_raster(gedi_shots, recovery_period)
    if len(filtered_shots) == 0:
        logger.warning(f"No shots found overlapping with JRC recovery data in {year}")
        return None
    dataset = pd.DataFrame(
        filtered_shots,
        columns=["shot_number", "recovery_period", "overlap_quality"],
    )
    return dataset.join(gedi_shots[["shot_number", "rh95"]].set_index("shot_number"), on="shot_number")
