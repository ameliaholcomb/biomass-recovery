import argparse
import logging
import geopandas as gpd
from functools import partial
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import pathlib
from pyspark.sql import SparkSession
from shapely.geometry import Polygon
from typing import List, Tuple

from src import constants
from src.data.jrc_loading import _to_nesw
from src.processing.gedi_recovery_analysis_monte_carlo import (
    compute_monte_carlo_recovery,
)
from src.utils.logging import get_logger

logger = get_logger(__name__)
logger.setLevel(logging.INFO)

planloc = "/tmp/gedi_analysis_workplan.png"


def print_plan(shape, chunks):
    world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
    base = world.plot(color="white", edgecolor="black", figsize=(20, 20))
    shape.plot(ax=base, color="white", edgecolor="green", alpha=1)

    year = chunks[0][1]
    for chunk in chunks:
        if chunk[1] != year:
            break
        chunk[0].plot(ax=base, edgecolor="blue", alpha=0.5)

    plt.savefig(planloc)


def _box_to_gdf(minx, miny, maxx, maxy):
    miny = miny + 0.00001
    maxx = maxx - 0.00001
    geo = Polygon([(minx, miny), (minx, maxy), (maxx, maxy), (maxx, miny)])
    return gpd.GeoDataFrame(geometry=[geo], crs=constants.WGS84)


def get_chunks(
    shapefile: pathlib.Path, years: List[int], print_work_plan: bool
) -> List[Tuple[gpd.GeoDataFrame, int, str]]:
    """Split the region and coverage years into chunks for Spark workers.

    Divides the region along a 1 deg x 1 deg grid that aligns with both
    1) the JRC data files (10 deg x 10 deg)
    2) the UTM coord system (6 deg longitude slices)
    For this reason, modifications to the chunking strategy must take into
    account that *only* chunks with width that divides both 10 and 6 are suitable.
    In addition, care must be taken to ensure that the grid corners align.

    Each year will be processed separately.

    Returns:
        List of region, year pairs that can be independently processed ("chunk").
        The final tuple element is a unique text descriptor of the chunk, for debugging purposes.
    """
    print("Creating chunks to split work among workers ...")
    geometry = gpd.read_file(shapefile).to_crs(constants.WGS84)
    minx, miny, maxx, maxy = geometry.bounds.values[0]
    minx, miny, maxx, maxy = (
        np.floor(minx),
        np.floor(miny),
        np.ceil(maxx),
        np.ceil(maxy),
    )

    chunks = []
    for year in years:
        # iterating over boxes starting at the bottom right
        for y in range(int(miny), int(maxy)):
            for x in range(int(minx), int(maxx)):
                tile_gdf = _box_to_gdf(x, y, x + 1, y + 1)
                (left, top), (lon_dir, lat_dir) = _to_nesw(
                    x, y + 1
                )  # top left corner
                tiletext = f"{lat_dir}{int(top)}_{lon_dir}{int(left)}"
                tile_region = geometry.overlay(tile_gdf, how="intersection")
                if tile_region.empty:
                    continue
                chunks.append((tile_region, year, tiletext))
    if print_work_plan:
        print("Printing work plan ...")
        print("I will work on {} chunks".format(len(chunks)))
        # [print(" {} ".format(chunk[2])) for chunk in chunks]
        print_plan(geometry, chunks)
        print("Work plan saved to {}".format(planloc))
        input("To continue with this work plan, press ENTER >>> ")

    return chunks


def compute_recovery(opts, chunk: Tuple[gpd.GeoDataFrame, int, str]):
    try:
        return compute_monte_carlo_recovery(
            geometry=chunk[0].geometry,
            year=chunk[1],
            token=chunk[2],
            num_iterations=1000,
            save_dir=opts.save_dir,
            include_degraded=opts.include_degraded,
            include_nonforest=opts.include_nonforest,
        )
    except (KeyError, ValueError, RuntimeError) as e:
        logger.warning(
            "Encountered error in chunk {}, skipping: {}".format(chunk[2], e)
        )
        return pd.DataFrame(
            {
                "token": [chunk[2]],
                "year": [chunk[1]],
                "has_data": [False],
                "error": [True],
                "error_message": "{}".format(e),
            }
        )
    except Exception as e:
        logger.error("Unusual error {}!".format(e))
        print(e)
        raise e


def exec_spark(
    chunks: List[Tuple[gpd.GeoDataFrame, int, str]], opts
) -> pd.DataFrame:

    print("Initializing Spark ...")
    spark = (
        SparkSession.builder.config("spark.executor.memory", "10g")
        .config("spark.driver.memory", "32g")
        .getOrCreate()
    )
    rdd = spark.sparkContext.parallelize(chunks, 32)
    rdd_processed = rdd.map(partial(compute_recovery, opts))
    all_chunks = rdd_processed.collect()
    return pd.concat(all_chunks, ignore_index=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run recovery analysis jobs")
    parser.add_argument(
        "-p",
        "--save_dir",
        help="Directory path to save the output data.",
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
        default=[2019, 2020, 2021],
    )
    parser.add_argument(
        "--include_degraded",
        help=(
            "Whether or not to include samples that have been degraded "
            "since the last deforestation. Turned off by default.",
        ),
        action=argparse.BooleanOptionalAction,
    )
    parser.set_defaults(include_degraded=False)
    parser.add_argument(
        "--include_nonforest",
        help=(
            "Whether or not to include samples that were previously classified as 'other land use'"
            "prior to recovery, as well as those that were previously deforested. If true, the recovery"
            "starts at the end of the last deforestation or last nonforest classification, whichever is later."
            "Turned off by default.",
        ),
        action=argparse.BooleanOptionalAction,
    )
    parser.set_defaults(include_nonforest=False)
    parser.add_argument(
        "--overwrite",
        help="Whether or not to overwrite existing saved files. Turned off by default",
        action=argparse.BooleanOptionalAction,
    )
    parser.set_defaults(overwrite=False)
    parser.add_argument(
        "--print_work_plan",
        help="Draw a plan of the areas to be processed and ask before proceeding",
        action=argparse.BooleanOptionalAction,
    )
    parser.set_defaults(print_work_plan=True)
    args = parser.parse_args()

    save_dir = pathlib.Path(args.save_dir)
    assert save_dir.exists()
    os.mkdir(save_dir / "shotinfo")
    os.mkdir(save_dir / "recovery")
    os.mkdir(save_dir / "agbd")
    shapefile = pathlib.Path(args.shapefile)
    assert shapefile.exists()
    years = args.years
    overwrite = args.overwrite
    index_file = "chunk_index.feather"
    # TODO(amelia): Make this more checkpoint-like so partial runs can be continued
    if (save_dir / index_file).exists() and not overwrite:
        print(
            "Exiting: Output file {} already exists but --overwrite not specified".format(
                (save_dir / index_file)
            )
        )
        exit(1)
    print_work_plan = args.print_work_plan

    chunks = get_chunks(shapefile, years, print_work_plan)
    processed_chunks = exec_spark(chunks, args)
    print(
        "Spark processing pipeline complete, saving processed chunk information to {}".format(
            (save_dir / index_file)
        )
    )
    processed_chunks.to_feather((save_dir / index_file))
