from typing import List, Optional
from tqdm.auto import tqdm
from logging import INFO
import geopandas as gpd
import numpy as np
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
import sqlalchemy as db
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql.schema import Table
import warnings
from src.data.gedi_granule import GediGranule
from src.utils import logging
from src.constants import WGS84, DB_CONFIG

logger = logging.get_logger("data_logger", INFO)


class FileTracker(object):

    def __init__(self, dir: Path, checkpoint_file: Path):
        self.dir = dir
        self.checkpoint_file = checkpoint_file
        if not self.checkpoint_file.exists():
            fp = open(checkpoint_file, 'a')
            fp.close()

        self.files = set(dir.glob('*.h5'))
        logger.info(f"Found {len(self.files)} h5 files in {dir}.")

        completed = set(Path(line.strip()) for line in open(checkpoint_file, 'r'))
        self.files = self.files - completed

        logger.info(f"Found {len(self.files)} h5 files remaining to import ...")

    @property
    def remaining_files(self) -> List[Path]:
        return sorted(list(self.files))

    def mark_file_completed(self, file: Path) -> None:
        self.files.discard(file)
        with open(self.checkpoint_file, 'a') as fp:
            fp.write(f"{file}\n")
        return
    
def parse_file(file: Path) -> gpd.GeoDataFrame:
    granule = GediGranule(file)
    granule_data = []
    for beam in tqdm(granule.iter_beams(), total=granule.n_beams):
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                granule_data.append(beam.main_data)
        except KeyError:
            logger.warn(f"Data for beam {beam.name} in granule {file.name} incomplete")
    df = pd.concat(granule_data, ignore_index=True)
    gdf = gpd.GeoDataFrame(df, crs=WGS84)
    granule.close()
    return gdf

def filter_granules(gdf: gpd.GeoDataFrame, roi: Optional[gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
    # Filter for shots where the algorithm was runnable
    gdf = gdf[gdf.algorithm_run_flag == 1]
    if roi is None:
        return gdf
    
    # Filter for shots that fall within the ROI
    gdf_filtered = gpd.sjoin(gdf, roi, predicate='within', how='inner')
    gdf_filtered = gdf_filtered.drop('index_right', axis=1)
    return gdf_filtered

def _to_postgis(gdf: gpd.GeoDataFrame, tablename: str, table: Table, engine: Engine, check_duplicates: bool = False):

    if check_duplicates:
        sql_query = table.select().where(
            table.c.shot_number.in_(gdf.shot_number.tolist())
        )
        existing_shots = pd.read_sql(sql_query, con=engine).shot_number
        gdf = gdf[~gdf.shot_number.isin(existing_shots)]
    
    gdf = gdf.astype({'shot_number': 'int64'})

    gdf.to_postgis(name=tablename, con=engine, index=False, if_exists="append")

def bulk_import_from_directory(dir: Path, roi: Optional[gpd.GeoDataFrame], table_name: str, checkpoint: Path = None, restore: bool = False):

    engine = create_engine(DB_CONFIG, echo=False)
    table = db.Table(table_name, db.MetaData(), autoload=True, autoload_with=engine)
    logger.info(f"Successfully found and connected to {table_name} ...")
    
    file_tracker = FileTracker(dir, checkpoint)

    # Usually, it is expensive to check for duplicate shots before inserting
    # into PostGIS. However, if the program exited uncleanly,
    # some shots in the first file may already have been uploaded, even though
    # the file was not marked as completed.
    # Therefore, if the user requests a restore from an unclean exit,
    # on the first iteration we check for duplicate shots before inserting into PostGIS.
    check_duplicates = restore

    for file in tqdm(file_tracker.remaining_files):
        gdf = parse_file(file)
        gdf = filter_granules(gdf, roi)
        _to_postgis(gdf, table_name, table, engine, check_duplicates)
        file_tracker.mark_file_completed(file)
        check_duplicates = False
    
    return





