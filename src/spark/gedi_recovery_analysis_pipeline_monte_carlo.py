import argparse
import logging
import geopandas as gpd
from functools import partial
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd

pd.options.mode.chained_assignment = None  # default='warn'
import pathlib
from pyspark.sql import DataFrame, SparkSession
from shapely.geometry import Polygon
from sklearn.model_selection import KFold
import statsmodels.formula.api as smf
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
        # [print(" {} ".format(chunk[2])) for chunk in chunks]
        print_plan(geometry, chunks)
        print("Work plan saved to {}".format(planloc))
        input("To continue with this work plan, press ENTER >>> ")

    return chunks


def compute_recovery(
    opts, finterface: FileInterface, chunk: Tuple[gpd.GeoDataFrame, int, str]
):
    try:
        return compute_monte_carlo_recovery(
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


def exec_spark(
    spark, chunks: List[Tuple[gpd.GeoDataFrame, int, str]], opts
) -> pd.DataFrame:
    finterface = FileInterface(opts.save_dir)
    rdd = spark.sparkContext.parallelize(chunks, 32)
    rdd_processed = rdd.map(partial(compute_recovery, opts, finterface))
    chunk_rdd = rdd_processed.collect()
    chunk_index = pd.concat(chunk_rdd, ignore_index=True)
    return chunk_index


def _filter_pct_agreement(pct_agreement, recovery_sample):
    # Filter for points with at least x% agreement on recovery age.
    nbins = np.arange(
        0, np.max(recovery_sample[~np.isnan(recovery_sample)]) + 2
    )
    hist = (
        np.apply_along_axis(
            lambda a: np.histogram(a, bins=nbins)[0],
            axis=1,
            arr=recovery_sample,
        )
        / recovery_sample.shape[1]
    )
    return np.max(hist, axis=1) >= (pct_agreement / 100)


def _filter_pct_nonnan(pct_agreement, recovery_sample):
    # Filter for points with at least x% non-nan values (x% recovering forest)
    return (
        np.sum(~np.isnan(recovery_sample), axis=1) / recovery_sample.shape[1]
    ) >= pct_agreement / 100


def _mode(arr, axis):
    if arr.shape[0] > 0:
        nbins = np.arange(0, np.max(arr[~np.isnan(arr)]) + 2)
        hist = np.apply_along_axis(
            lambda a: np.histogram(a, bins=nbins)[0],
            axis=axis,
            arr=arr,
        )
        mode_count = np.max(hist, axis=axis)
        mode_val = np.argmax(hist, axis=axis)
        nan_count = np.sum(np.isnan(arr), axis=axis)

        mode_is_nan = np.where(nan_count > mode_count)

        mode_count[mode_is_nan] = nan_count[mode_is_nan]
        mode_val = np.argmax(hist, axis=axis)
        mode_val[mode_is_nan] = -1
        return mode_count, mode_val
    return np.array([], dtype=np.float64), np.array([], dtype=np.float64)


def _filter_shots(opts, finterface: FileInterface, chunk_id: Tuple[int, str]):

    year, token = chunk_id
    recovery_sample = finterface.load_data(
        token=token, year=year, data_type="recovery"
    )

    filter_idx = _filter_pct_nonnan(opts.pct_agreement, recovery_sample)
    filtered_recovery = recovery_sample[filter_idx]
    filter_idx2 = np.std(filtered_recovery, axis=1) <= 1
    filtered_recovery = filtered_recovery[filter_idx2]

    hist_summary = pd.DataFrame(
        {
            "min": np.min(filtered_recovery, axis=1),
            "max": np.max(filtered_recovery, axis=1),
            "p75": np.quantile(filtered_recovery, q=0.75, axis=1),
            "median": np.quantile(filtered_recovery, q=0.50, axis=1),
            "p25": np.quantile(filtered_recovery, q=0.25, axis=1),
            "mean": np.mean(filtered_recovery, axis=1),
            "std": np.std(filtered_recovery, axis=1),
            "var": np.var(filtered_recovery, axis=1),
        }
    )
    counts, values = _mode(filtered_recovery, axis=1)
    hist_summary["mode_counts"] = counts
    hist_summary["mode_vals"] = values

    # Free up some memory before loading master df
    del filtered_recovery
    del recovery_sample
    master_df = finterface.load_data(token=token, year=year, data_type="master")
    filtered = master_df[filter_idx]
    filtered = filtered[filter_idx2]
    # Note: Cannot assign df["shot_number"] = filtered["shot_number"]
    # This implicitly converts to float64 (for unknown reasons)
    # which is not big enough to hold the shot numbers, and silently makes them NaN.
    hist_summary["shot_number"] = filtered.shot_number.values
    finterface.save_data(
        token=token, year=year, data_type="filtered", data=filtered
    )
    finterface.save_data(
        token=token,
        year=year,
        data_type="hist",
        data=hist_summary,
    )
    return filtered


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


def _run_median_regression_model(experiment_id, dataframe):
    recovery_col = "r_{}".format(experiment_id)
    model = smf.quantreg(
        "agcd_{experiment_id} ~ r_{experiment_id}".format(
            experiment_id=experiment_id
        ),
        dataframe,
    )
    res = model.fit(q=0.5)
    result = [
        [
            res.params["Intercept"],
            *res.conf_int().loc["Intercept"],
            res.params[recovery_col],
            *res.conf_int().loc[recovery_col],
            res.prsquared,
        ]
    ]
    result_df = pd.DataFrame(
        result, columns=["a", "la", "ua", "b", "lb", "ub", "prs"]
    )
    return result_df


def _run_ols_medians_model(experiment_id, dataframe):
    recovery_col = "r_{}".format(experiment_id)
    agcd_col = "agcd_{}".format(experiment_id)
    recovery_periods = dataframe[recovery_col].unique()
    median_agcds = np.array(
        [
            dataframe.loc[dataframe[recovery_col] == x, agcd_col].median()
            for x in recovery_periods
        ]
    )
    df = pd.DataFrame(
        {
            "recovery": recovery_periods,
            "agcd": median_agcds,
        }
    )
    df.to_feather("/maps/forecol/data/Overlays/monte_carlo/medians.feather")
    ols = smf.ols("agcd ~ recovery", df).fit()
    ols_ci = ols.conf_int().loc["recovery"].tolist()
    ols_intercept_ci = ols.conf_int().loc["Intercept"].tolist()
    result = [
        [
            ols.params["Intercept"],
            ols_intercept_ci[0],
            ols_intercept_ci[1],
            ols.params["recovery"],
            ols_ci[0],
            ols_ci[1],
            ols.rsquared,
        ]
    ]
    result_df = pd.DataFrame(
        result, columns=["a", "la", "ua", "b", "lb", "ub", "rs"]
    )
    return result_df


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
    return _run_ols_medians_model(experiment_id, dataframe)


def run_model_spark(spark, chunk_metadata, opts):
    finterface = FileInterface(opts.save_dir)

    processed_chunks = chunk_metadata[chunk_metadata.has_data == True]
    chunk_ids = [(x.year, x.token) for _, x in processed_chunks.iterrows()]

    rdd = spark.sparkContext.parallelize(chunk_ids, 32)
    filtered_shots = rdd.map(partial(_filter_shots, opts, finterface))
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

    save_dir = pathlib.Path(args.save_dir)
    assert save_dir.exists()
    shapefile = pathlib.Path(args.shapefile)
    assert shapefile.exists()
    years = args.years
    pct_agreement = args.pct_agreement
    if not pct_agreement <= 100 and pct_agreement >= 0:
        logger.error(
            "Invalid value for pct_agreement, must be an integer from 0 to 100."
        )
        exit(1)
    print_work_plan = args.print_work_plan

    print("Initializing Spark ...")
    spark = (
        SparkSession.builder.config("spark.executor.memory", "10g")
        .config("spark.driver.memory", "32g")
        .getOrCreate()
    )

    metadata_file = pathlib.Path(args.save_dir) / "chunk_metadata.feather"
    if metadata_file.exists() and not args.overwrite:
        chunk_metadata = pd.read_feather(metadata_file)
    else:
        chunks = get_chunks(shapefile, years, print_work_plan)
        chunk_metadata = exec_spark(spark, chunks, args)
        chunk_metadata.to_feather(metadata_file)

    results_file = pathlib.Path(
        args.save_dir
    ) / "model_results_p{}.feather".format(pct_agreement)
    if results_file.exists() and not args.overwrite:
        logger.error(
            "Results file {} already exists and --overwrite=False, exiting".format(
                results_file
            )
        )
        exit(1)
    results = run_model_spark(spark, chunk_metadata, args)
    results.to_feather(results_file)
