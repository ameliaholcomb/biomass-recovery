import argparse
import logging
import geopandas as gpd
from functools import partial
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd

import pathlib
from pyspark.sql import DataFrame, SparkSession
from shapely.geometry import Polygon
from sklearn.model_selection import KFold
from typing import List, Tuple

from src import constants
from src.data.jrc_loading import _to_nesw
from src.processing.gedi_recovery_match_monte_carlo import match_monte_carlo
from src.processing.recovery_analysis_models import (
    filter_shots,
    run_median_regression_model,
    run_ols_medians_model,
)
from src.utils.logging import get_logger

logger = get_logger(__name__)
logger.setLevel(logging.INFO)

planloc = "/tmp/gedi_analysis_workplan.png"


class FileInterface(object):
    # __init__ is *not* threadsafe, please only create a FileInterface from within the driver
    data_types = {
        "master": "parquet",
        "shotinfo": "parquet",
        "recovery": "npy",
        "agbd": "npy",
        "hist": "parquet",
        "filtered": "parquet",
    }
    file_pattern = "{token}_{year}_{data_type}.{ext}"

    def __init__(self, save_path: str):
        self.save_dir = pathlib.Path(save_path)
        assert self.save_dir.exists()
        for data_type in self.data_types.keys():
            if not (self.save_dir / data_type).exists():
                os.mkdir(self.save_dir / data_type)

    def save_data(self, token: str, year: int, data_type: str, data):
        if data_type not in self.data_types.keys():
            raise ValueError("Unknown data type {}".format(data_type))
        file_name = self.file_pattern.format(
            token=token,
            year=year,
            data_type=data_type,
            ext=self.data_types[data_type],
        )
        file_path = self.save_dir / data_type / file_name

        if self.data_types[data_type] == "parquet":
            data.to_parquet(file_path)
        if self.data_types[data_type] == "npy":
            np.save(file_path, data)
        return

    def load_data(self, token: str, year: int, data_type: str):
        if data_type not in self.data_types.keys():
            raise ValueError("Unknown data type {}".format(data_type))
        file_name = self.file_pattern.format(
            token=token,
            year=year,
            data_type=data_type,
            ext=self.data_types[data_type],
        )
        file_path = self.save_dir / data_type / file_name

        if self.data_types[data_type] == "parquet":
            return pd.read_parquet(file_path)
        if self.data_types[data_type] == "npy":
            return np.load(file_path)


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
        print_plan(geometry, chunks)
        print("Work plan saved to {}".format(planloc))
        input("To continue with this work plan, press ENTER >>> ")

    return chunks


def match_monte_carlo_wrapper(
    opts, finterface: FileInterface, chunk: Tuple[gpd.GeoDataFrame, int, str]
):
    """Wrapper for match_monte_carlo that catches errors and collects metadata."""
    try:
        return match_monte_carlo(
            geometry=chunk[0].geometry,
            year=chunk[1],
            token=chunk[2],
            num_iterations=opts.num_iterations,
            finterface=finterface,
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


def generate_data_spark(
    spark, chunks: List[Tuple[gpd.GeoDataFrame, int, str]], opts
) -> pd.DataFrame:
    finterface = FileInterface(opts.save_dir)
    rdd = spark.sparkContext.parallelize(chunks, 32)
    rdd_processed = rdd.map(
        partial(match_monte_carlo_wrapper, opts, finterface)
    )
    chunk_rdd = rdd_processed.collect()
    chunk_index = pd.concat(chunk_rdd, ignore_index=True)
    return chunk_index


def _batch_produce_experiments(start, end, df):
    experiment_data = []
    for experiment_id in range(start, end):
        experiment_data.append(
            (
                experiment_id,
                df[
                    ["r_{}".format(experiment_id), "a_{}".format(experiment_id)]
                ],
            )
        )
    return experiment_data


def _combine_dfs(experiment_data):
    experiment_id, dataframes = experiment_data
    return experiment_id, pd.concat(dataframes)


def _run_experiment(experiment_data):
    experiment_id, dataframe = experiment_data
    # Require that the values are non-nan and that the recovery age is 3-22
    recovery_col = "r_{}".format(experiment_id)
    dataframe = dataframe[
        (dataframe[recovery_col] != np.nan)
        & (dataframe[recovery_col] >= 3)
        & (dataframe[recovery_col] <= 22)
    ].copy()
    # Martin et al. (2011) conversion for AGCD from AGBD
    dataframe["agcd_{}".format(experiment_id)] = (
        dataframe["a_{}".format(experiment_id)] * 0.47
    )
    return run_ols_medians_model(experiment_id, dataframe)


def run_model_spark(spark, chunk_metadata, opts):
    finterface = FileInterface(opts.save_dir)

    processed_chunks = chunk_metadata[chunk_metadata.has_data == True]
    chunk_ids = [(x.year, x.token) for _, x in processed_chunks.iterrows()]

    rdd = spark.sparkContext.parallelize(chunk_ids, 32)
    filtered_shots = rdd.map(partial(filter_shots, opts, finterface))
    print("n = {}".format(filtered_shots.map(len).sum()))

    # batch size should divide num_iterations evenly
    batch_size = 100
    results = []
    for batch in range(0, opts.num_iterations, batch_size):
        exploded = filtered_shots.flatMap(
            partial(_batch_produce_experiments, batch, batch + 10)
        )
        experiments = exploded.groupByKey().map(_combine_dfs)
        results.extend(experiments.map(_run_experiment).collect())
    return pd.concat(results, ignore_index=True)


def exec_spark(args):
    print("Initializing Spark ...")
    spark = (
        SparkSession.builder.config("spark.executor.memory", "10g")
        .config("spark.driver.memory", "32g")
        .getOrCreate()
    )

    # Stage 1: Generate monte carlo sample data or restore from checkpoint
    metadata_file = pathlib.Path(args.save_dir) / "chunk_metadata.feather"
    if metadata_file.exists() and not args.overwrite:
        chunk_metadata = pd.read_feather(metadata_file)
    else:
        chunks = get_chunks(args.shapefile, args.years, args.print_work_plan)
        chunk_metadata = generate_data_spark(spark, chunks, args)
        chunk_metadata.to_feather(metadata_file)

    # Stage 2: Run model
    return run_model_spark(spark, chunk_metadata, args)


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
        "--pct_agreement",
        help=(
            "Percent of sampled recovery periods for a GEDI shot that need to agree"
            "for the GEDI shot to pass quality filtering. (0-100)"
        ),
        type=int,
        default=90,
    )
    parser.add_argument(
        "--num_iterations",
        help=("Number of model iterations to run"),
        type=int,
        default=1000,
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

    assert pathlib.Path(args.save_dir).exists()
    assert pathlib.Path(args.shapefile).exists()
    if not args.pct_agreement <= 100 and args.pct_agreement >= 0:
        logger.error(
            "Invalid value for pct_agreement, must be an integer from 0 to 100."
        )
        exit(1)

    results_file = pathlib.Path(
        args.save_dir
    ) / "model_results_p{}.feather".format(args.pct_agreement)
    if results_file.exists() and not args.overwrite:
        logger.error(
            "Results file {} already exists and --overwrite=False, exiting".format(
                results_file
            )
        )
        exit(1)

    results = exec_spark(args)
    results.to_feather(results_file)
