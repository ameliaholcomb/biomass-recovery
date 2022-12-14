import argparse
import logging
import pandas as pd
import pathlib
from pyspark.sql import SparkSession
from typing import List


from src import constants
from src.processing import recovery_analysis

from src.utils import util_logging

logger = util_logging.get_logger(__name__)


def _get_paisagenslidar_path() -> pathlib.Path:
    return


def _get_files(survey_name: str) -> List[pathlib.Path]:
    if survey_name == 'eba':
        filepath = constants.EBALIDAR_PATH / 'laz_EBA_processed'
        return [survey for survey in filepath.iterdir()
                if (survey / "grid_metrics").exists() and
                any((survey / "grid_metrics").iterdir())
                ]
    elif survey_name == 'paisagenslidar':
        filepath = constants.PAISAGENSLIDAR_PATH / 'processed'
        surveys = []
        for subdir in filepath.iterdir():
            for survey in subdir.iterdir():
                surveys.append(survey)

        return [survey for survey in surveys
                if (survey / "grid_metrics").exists() and
                any((survey / "grid_metrics").iterdir())
                ]

    else:
        raise ValueError(f'Survey {survey_name} not supported')


def compute_recovery(file: pathlib.Path):
    try:
        return recovery_analysis.compute_recovery_dataset(file.name)
    except (KeyError, ValueError, RuntimeError) as e:
        logger.warning(
            "Encountered error in survey {}, skipping: {}".format(
                file.name, e)
        )
        return pd.DataFrame({})
    except Exception as e:
        logger.warning('Unusual error {}!'.format(e))
        print(e)
        raise e


def exec_spark(files: List[pathlib.Path]) -> pd.DataFrame:

    spark = SparkSession \
        .builder \
        .config("spark.executor.memory", "10g") \
        .config("spark.driver.memory", "32g") \
        .getOrCreate()
    rdd = spark.sparkContext.parallelize(files, 32)
    rdd_processed = rdd.map(compute_recovery)
    all_dfs = rdd_processed.collect()
    return pd.concat(all_dfs, ignore_index=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run recovery analysis jobs")
    parser.add_argument(
        "--survey_name",
        help="Name of survey (eba or paisagenslidar)",
        type=str
    )
    parser.add_argument(
        "--save_path",
        help="Path in which to save final results",
        type=str
    )
    args = parser.parse_args()
    survey_name = args.survey_name
    save_path = pathlib.Path(args.save_path)
    assert save_path.parent.exists()

    files = _get_files(survey_name)
    if len(files) == 0:
        print('No files found for survey; exiting')
        exit(1)
    df = exec_spark(files)
    df.to_feather(save_path)
