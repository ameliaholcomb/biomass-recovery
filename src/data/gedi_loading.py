import logging

import geopandas as gpd
import numpy as np
import pandas as pd
import sqlalchemy as db
from sqlalchemy import create_engine
from tqdm.autonotebook import tqdm

from src.constants import DB_CONFIG, WGS84
from src.utils.logging import get_logger

logger = get_logger(__file__)
logger.setLevel(logging.INFO)

engine = create_engine(DB_CONFIG, echo=False)
gedi_l2a = db.Table("level_2a", db.MetaData(), autoload=True, autoload_with=engine)

ALLOWED_COLUMNS = [
    "granule_name",
    "shot_number",
    "beam_type",
    "beam_name",
    "delta_time",
    "absolute_time",
    "sensitivity",
    "quality_flag",
    "solar_elevation",
    "solar_azimuth",
    "energy_total",
    "dem_tandemx",
    "dem_srtm",
    "selected_algorithm",
    "selected_mode",
    "lon_lowestmode",
    "longitude_bin0_error",
    "lat_lowestmode",
    "latitude_bin0_error",
    "elev_lowestmode",
    "elevation_bin0_error",
    "lon_highestreturn",
    "lat_highestreturn",
    "elev_highestreturn",
    "rh0",
    "rh1",
    "rh2",
    "rh3",
    "rh4",
    "rh5",
    "rh6",
    "rh7",
    "rh8",
    "rh9",
    "rh10",
    "rh11",
    "rh12",
    "rh13",
    "rh14",
    "rh15",
    "rh16",
    "rh17",
    "rh18",
    "rh19",
    "rh20",
    "rh21",
    "rh22",
    "rh23",
    "rh24",
    "rh25",
    "rh26",
    "rh27",
    "rh28",
    "rh29",
    "rh30",
    "rh31",
    "rh32",
    "rh33",
    "rh34",
    "rh35",
    "rh36",
    "rh37",
    "rh38",
    "rh39",
    "rh40",
    "rh41",
    "rh42",
    "rh43",
    "rh44",
    "rh45",
    "rh46",
    "rh47",
    "rh48",
    "rh49",
    "rh50",
    "rh51",
    "rh52",
    "rh53",
    "rh54",
    "rh55",
    "rh56",
    "rh57",
    "rh58",
    "rh59",
    "rh60",
    "rh61",
    "rh62",
    "rh63",
    "rh64",
    "rh65",
    "rh66",
    "rh67",
    "rh68",
    "rh69",
    "rh70",
    "rh71",
    "rh72",
    "rh73",
    "rh74",
    "rh75",
    "rh76",
    "rh77",
    "rh78",
    "rh79",
    "rh80",
    "rh81",
    "rh82",
    "rh83",
    "rh84",
    "rh85",
    "rh86",
    "rh87",
    "rh88",
    "rh89",
    "rh90",
    "rh91",
    "rh92",
    "rh93",
    "rh94",
    "rh95",
    "rh96",
    "rh97",
    "rh98",
    "rh99",
    "rh100",
    "geometry",
]


def gedi_sql_query(
    columns: str = "*",
    geometry=None,
    crs: str = WGS84,
    start_time: str = None,
    end_time: str = None,
    limit: int = None,
    force: bool = False,
):

    if columns != "*":
        for column in columns:
            if not column in ALLOWED_COLUMNS:
                raise ValueError(
                    f"`{column}` not allowed. Must be in {ALLOWED_COLUMNS}"
                )

    conditions = []
    # Temporal conditions
    if start_time is not None and end_time is not None:
        conditions += [f"absolute_time between '{start_time}' and '{end_time}'"]
    # Spatial conditions
    if geometry is not None:
        crs = gpd.tools.crs.CRS(crs)
        conditions += [
            "ST_Intersects(geometry, "
            f"ST_GeomFromText('{geometry.to_wkt()}', {crs.to_epsg()}))"
        ]
    # Combining conditions
    condition = f" WHERE {' and '.join(conditions)}" if len(conditions) > 0 else ""
    # Setting limits
    limits = f" LIMIT {limit}" if limit is not None else ""

    if not force and condition == "" and limit is None:
        raise UserWarning(
            "Warning! This will load the entire table. To proceed set `force`=True."
        )

    sql_query = f"SELECT {', '.join(columns)} " "FROM level_2a" + condition + limits
    return sql_query


def load_gedi_data(
    columns: str = "*",
    geometry=None,
    crs: str = WGS84,
    start_time: str = None,
    end_time: str = None,
    limit: int = None,
    use_geopandas: bool = False,
    force: bool = False,
):

    if use_geopandas or geometry is not None:
        if columns != "*" and "geometry" not in columns:
            columns += ["geometry"]

        # Construct sql query
        sql_query = gedi_sql_query(
            columns=columns,
            geometry=geometry,
            crs=crs,
            limit=limit,
            start_time=start_time,
            end_time=end_time,
            force=force,
        )

        logger.debug("SQL Query: %s", sql_query)
        return gpd.read_postgis(sql_query, con=engine, geom_col="geometry")

    else:
        # Construct sql query
        sql_query = gedi_sql_query(
            columns=columns,
            geometry=geometry,
            crs=crs,
            limit=limit,
            start_time=start_time,
            end_time=end_time,
            force=force,
        )

        logger.debug("SQL Query: %s", sql_query)
        return pd.read_sql(sql_query, con=engine)


def upload_gedi_data(data: gpd.GeoDataFrame, chunksize: int = 100000):
    n_chunks = int(np.ceil(len(data) / chunksize))
    logger.info("Uploading %s with total length %s to database.", n_chunks, len(data))

    for i in tqdm(range(n_chunks), total=n_chunks):
        chunk = data[i * chunksize : (i + 1) * chunksize]
        # Query potentially existing shots
        sql_query = gedi_l2a.select().where(
            gedi_l2a.c.shot_number.in_(chunk.shot_number.tolist())
        )
        existing_shots = pd.read_sql(sql_query, con=engine).shot_number
        # Upload only new, nique shots
        new_shots = chunk[~chunk.shot_number.isin(existing_shots)]
        new_shots.to_postgis(
            name="level_2a", con=engine, index=False, if_exists="append"
        )
