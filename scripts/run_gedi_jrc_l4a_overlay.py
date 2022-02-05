import argparse
import geopandas as gpd
import logging
import numpy as np
from pathlib import Path
from string import Template
from shapely.geometry import Polygon
from tqdm.auto import tqdm
from typing import List
from src.constants import WGS84
from src.data.jrc_loading import _to_nesw
from src.processing.gedi_recovery_analysis import compute_gedi_recovery_l4a
from src.utils.logging import get_logger


logger = get_logger(__file__)
logger.setLevel(logging.INFO)

SAVEFILE_TEMPL = Template('jrc_l4a_overlay_${year}_${tile}.feather')


def _box_to_gdf(minx, miny, maxx, maxy):
    miny = miny + 0.00001
    maxx = maxx - 0.00001
    geo = Polygon([(minx, miny), (minx, maxy), (maxx, maxy), (maxx, miny)])
    return gpd.GeoDataFrame(geometry=[geo], crs=WGS84)


def main(outdir: Path, shapefile: Path, years: List[int], overwrite: bool) -> None:
    if not outdir.exists():
        raise FileNotFoundError(f'Could not find specified outdir {outdir}')
    
    if not shapefile.exists():
        raise FileNotFoundError(f'Could not find specified shapefile {shapefile}')

    geometry = gpd.read_file(shapefile)
    minx, miny, maxx, maxy = geometry.bounds.values[0]
    minx, miny, maxx, maxy = np.floor(minx), np.floor(miny), np.ceil(maxx), np.ceil(maxy)

    for year in tqdm(years):
        # iterating over boxes starting at the bottom right
        for y in range(int(miny), int(maxy)):
            for x in range(int(minx), int(maxx)):
                try: 
                    tile_gdf = _box_to_gdf(x, y, x + 1, y + 1)
                    (left, top), (lon_dir, lat_dir) = _to_nesw(x, y + 1)  # top left corner
                    tiletext = f"{lat_dir}{int(top)}_{lon_dir}{int(left)}"
                    outfile = Path(outdir / SAVEFILE_TEMPL.substitute(year=year, tile=tiletext))
                    logger.info(f'Preparing file {outfile}')
                    if outfile.exists() and not overwrite:
                        logger.warning(f'Found existing output file for {year} tile {tiletext}, skipping.')
                        continue
                    
                    tile_region = geometry.overlay(tile_gdf, how="intersection")
                    if tile_region.empty:
                        continue
                    df = compute_gedi_recovery_l4a(geometry=tile_region.geometry, year=year)
                    if df.empty:
                        continue
                    df.to_feather(outfile)
                except Exception as e:
                    if type(e) == KeyboardInterrupt:
                        exit(1)
                    logger.error(f'UNEXPECTED ERROR in loop iteration year {year}, x {x}, y {y}')
                    logger.error(e)



if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Script to compute GEDI recovery period")
    parser.add_argument(
        "-o",
        "--outdir",
        help="The name of the directory to save output dataframes (as feather files).",
        type=str,
    )
    parser.add_argument(
        "-s",
        "--shapefile",
        help="Shapefile (zip) containing search area polygon.",
        type=str,
    )
    parser.add_argument(
        "--years",
        help="List of years over which to search GEDI data",
        nargs="*",
        type=int,
        default=[2019, 2020, 2021]
    )
    parser.add_argument(
        "--overwrite",
        help="Whether or not to overwrite existing saved files. Default is False",
        type=bool,
        default=False,
        nargs="?",  # Argument is optional
    )
    args = parser.parse_args()
    main(outdir=Path(args.outdir), shapefile=Path(args.shapefile), years=args.years, overwrite=args.overwrite)