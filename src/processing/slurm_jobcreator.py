"""Tools for automatically generating slurm jobs for lidR analysis"""
import datetime
import os
import pathlib
from typing import Union

from src.constants import SRC_PATH, CONDA_ENV
from src.utils.slurm import sbatch_header

LIDR_PROCESSING_SCRIPT = SRC_PATH / "processing" / "lidar_processing_script.R"
LIDR_GRID_METRICS_SCRIPT = SRC_PATH / "processing" / "lidar_gridmetrics_script.R"
LIDR_CANOPY_HEIGHT_SCRIPT = SRC_PATH / "processing" / "lidar_canopy_height_script.R"
assert all(
    (
        LIDR_PROCESSING_SCRIPT.exists(),
        LIDR_CANOPY_HEIGHT_SCRIPT.exists(),
        LIDR_GRID_METRICS_SCRIPT.exists(),
    )
), "Not all processing scripts could be found. Check paths."

# Allowed grid metrics in LIDR_GRID_METRICS_SCRIPT
ALLOWED_METRICS = (
    "point_density",
    "pulse_density",
    "ground_point_density",
    "n_points",
    "n_pulses",
    "n_ground_points",
    "max",
    "standard_dev",
    "mask",
    "mean",
    "kurtosis",
    "interquartile_range",
    "quantile",
    "longo_biomass",
)

# Helper functions
seperator = lambda x: f"\necho \"{'='*20} {x} {'='*20}\"\n"
echo = lambda x: f'echo "{x}"\n'


def lidR_processing_job(  # pylint: disable=invalid-name
    lidar_path: pathlib.Path,
    survey_name: str,
    out_dir: os.PathLike,
    buffer: int = 50,
    chunk_size: int = 0,
    filter_quantile: float = 0.95,
    filter_sensitivity: float = 1.1,
    filter_gridsize: float = 10,
    compress_intermediates: bool = True,
    perform_decimation: bool = False,
    perform_check: bool = False,
    perform_classification: bool = True,
    perform_normalisation: bool = True,
    perform_filter: bool = True,
    as_file: bool = True,
    **slurm_kwargs,
) -> Union[str, pathlib.Path]:
    """
    Create a `sbatch` file for ground-classifying, normalising and filtering lidar point
    cloud data on the JASMIN lotus cluster.

    Args:
        survey (Union[dict, pd.Series]): The survey data in the form of a dictionary or
        a pandas series. Must have the keys `path` and `name`.
        out_dir (os.PathLike): The path at which to store the processing results.
        buffer (int, optional): Buffer to use during the lidR computations.
            Defaults to 50.
        chunk_size (int, optional): Chunk size to use when processing the lidar data.
            This will be passed to the lidR processing script. Defaults to 0.
        filter_quantile (float, optional): Quantile to use as a reference for filtering
            outliers with high z-values. Defaults to 0.95.
        filter_sensitivity (float, optional): Sensitivity above reference quantile to
            use for filtering outliers with high z-values. Outliers with a z value
            higher than height_at_filter_quantile * sensitivity will be removed.
            Defaults to 1.1.
        filter_gridsize (float, optional): Gridsize to use for calculating the reference
            quantile height for filtering. Defaults to 10.
        compress_intermediates (bool, optional): Whether compress any intermediate
            files, if they are saved. Defaults to True.
        perform_check (bool, optional): Whether to run the lidR `las_check` before and
            print the output. Defaults to False.
        as_file (bool, optional): Whether to return the job-file. Defaults to True.
        **slurm_kwargs: Kwargs to the `sbatch_header` utils function.

    Returns:
        Union[str, pathlib.Path]: The path to the file at which the `.sbatch` file
            with commands is saved, or the commands as a string (depending on the
            `as_file` argument)
    """
    # Path for lidar data
    lidar_path = pathlib.Path(lidar_path)
    assert lidar_path.exists()

    # Set up save prefix
    save_pattern = survey_name + "_" + r"\{XLEFT\}_\{YBOTTOM\}"

    # Create working directory path
    out_dir = pathlib.Path(out_dir)
    slurm_dir = out_dir / "slurm"
    slurm_dir.mkdir(parents=True, exist_ok=True)

    # Create sbatch header
    header = sbatch_header(
        job_name=survey_name,
        working_directory=slurm_dir,
        output_file_pattern=f"slurm_{survey_name}-%j.out",
        error_file_pattern=f"slurm_{survey_name}-%j.err",
        **slurm_kwargs,
    )

    body = seperator("Activating conda environment")
    body += "source ~/.bashrc\n"
    body += "conda activate " + CONDA_ENV + "\n"
    body += seperator("Checking R version")
    body += echo("$(which R)")
    # body += "echo $(which R)\n"
    body += echo("$(R --version)")
    body += seperator("Analysing file")
    body += echo(f"template generation time: {datetime.datetime.now()}")
    body += echo(f"survey: {survey_name}")
    body += echo(f"path: {lidar_path}")
    body += seperator("Executing R script")
    body += (
        f"Rscript --verbose --no-save {LIDR_PROCESSING_SCRIPT}"
        f" --lidar_path={lidar_path}"
        f" --save_path={out_dir}"
        f" --buffer={buffer}"
        f" --chunk_size={chunk_size}"
        f" --perform_check={'TRUE' if perform_check else 'FALSE'}"
        f" --perform_decimation={'TRUE' if perform_decimation else 'FALSE'}"
        f" --perform_classification={'TRUE' if perform_classification else 'FALSE'}"
        f" --perform_normalisation={'TRUE' if perform_normalisation else 'FALSE'}"
        f" --perform_filter={'TRUE' if perform_filter else 'FALSE'}"
        f" --filter_quantile={filter_quantile}"
        f" --filter_sensitivity={filter_sensitivity}"
        f" --filter_gridsize={filter_gridsize}"
        f" --save_pattern={save_pattern}"
        f" --compress_intermediates={'TRUE' if compress_intermediates else 'FALSE'}"
    )

    if as_file:
        job_file = slurm_dir / f"slurm_{survey_name}.sh"
        with open(job_file, "w") as file:
            file.write(header + body)
        job_file.chmod(0o777)
        return job_file
    else:
        return header + body


def lidR_gridmetrics_job(  # pylint: disable=invalid-name
    lidar_path: os.PathLike,
    survey_name: str,
    out_dir: os.PathLike,
    metrics: dict,
    buffer: int = 10,
    chunk_size: int = 0,
    save_prefix: str = "",
    check_index: bool = True,
    overwrite: bool = False,
    as_file: bool = False,
    **slurm_kwargs,
) -> Union[str, pathlib.Path]:
    """
    Create a `sbatch` file for calculating the given `metrics` on normalised, filtered
    lidar point cloud data on the JASMIN lotus cluster.

    Args:
        survey (Union[dict, pd.Series]): The survey data in the form of a dictionary or
        a pandas series. Must have the keys `path` and `name`.
        out_dir (os.PathLike): The path at which to store the results.
        buffer (int, optional): Buffer to use during the lidR computations.
            Defaults to 50.
        chunk_size (int, optional): Chunk size to use when processing the lidar data.
            This will be passed to the lidR processing script. Defaults to 0.
        check_index (bool, optional): Whether to check the lidar point cloud data for
            a spatial index (.lax file) and create one if there is none. This can
            significantly improve processing times. Defaults to True.
        overwrite (bool, optional): Whether to overwrite existing files. If set to
            false, existing files will simply be skipped. Defaults to False.
        as_file (bool, optional): Whether to return the job-file. Defaults to True.
        **slurm_kwargs: Kwargs to the `sbatch_header` utils function.

    Returns:
        Union[str, pathlib.Path]: The path to the file at which the `.sbatch` file
            with commands is saved, or the commands as a string (depending on the
            `as_file` argument)
    """
    # Path for lidar data
    lidar_path = pathlib.Path(lidar_path)
    assert lidar_path.exists()

    # Set up save prefix
    save_prefix = survey_name + "_"

    # Create working directory path
    slurm_dir = out_dir / "slurm"
    slurm_dir.mkdir(parents=True, exist_ok=True)
    grid_metrics_dir = out_dir / "grid_metrics"
    grid_metrics_dir.mkdir(parents=True, exist_ok=True)

    # Create sbatch header
    header = sbatch_header(
        job_name=survey_name + "_grid_metrics",
        working_directory=slurm_dir,
        output_file_pattern=f"slurm_{survey_name}_metrics-%j.out",
        error_file_pattern=f"slurm_{survey_name}_metrics-%j.err",
        **slurm_kwargs,
    )

    body = seperator("Activating conda environment")
    body += "source ~/.bashrc\n"
    body += "conda activate " + CONDA_ENV + "\n"
    body += seperator("Checking R version")
    body += echo("$(which R)")
    body += echo("$(R --version)")
    body += seperator("Analysing file")
    body += echo(f"template generation time: {datetime.datetime.now()}")
    body += echo(f"survey: {survey_name}")
    body += echo(f"path: {lidar_path}")
    body += echo(f"metrics: {metrics}")
    body += seperator("Executing R script")

    if "canopy_height" in metrics.keys():
        for gridsize in metrics.pop("canopy_height"):
            body += f"\n# `Canopy height` at grid size `{gridsize}m`\n"
            body += lidR_canopy_height_call(
                lidar_path=lidar_path,
                gridsize=gridsize,
                out_dir=out_dir,
                buffer=buffer,
                chunk_size=chunk_size,
                save_prefix=save_prefix,
                overwrite=overwrite,
                check_index=check_index,
            )
            body += "\n"

    for metric in metrics.keys():
        for gridsize in metrics[metric]:
            body += f"\n# Metric `{metric}` at grid size `{gridsize}m`\n"
            body += lidR_grid_metrics_call(
                metric=metric,
                lidar_path=lidar_path,
                gridsize=gridsize,
                out_dir=out_dir,
                buffer=buffer,
                chunk_size=chunk_size,
                save_prefix=save_prefix,
                overwrite=overwrite,
                check_index=check_index,
            )
            body += "\n"

    if as_file:
        job_file = slurm_dir / f"slurm_{survey_name}_grid_metrics.sh"
        with open(job_file, "w") as file:
            file.write(header + body)
        job_file.chmod(0o777)
        return job_file
    else:
        return header + body


def lidR_grid_metrics_call(
    metric: str,
    lidar_path: os.PathLike,
    gridsize: float,
    out_dir: os.PathLike,
    buffer: float,
    chunk_size: float,
    save_prefix: str,
    overwrite: bool,
    check_index: bool,
) -> str:
    assert metric in ALLOWED_METRICS or "quantile_" in metric, f"{metric} not allowed."

    body = (
        f"Rscript --verbose --no-save {LIDR_GRID_METRICS_SCRIPT}"
        f" --lidar_path={lidar_path}"
        f" --metric={metric if 'quantile' not in metric else 'quantile'}"
        f" --gridsize={gridsize}"
        f" --save_path={out_dir}"
        f" --buffer={buffer}"
        f" --chunk_size={chunk_size}"
        f" --save_prefix={save_prefix}"
        f" --overwrite={'TRUE' if overwrite else 'FALSE'}"
        f" --check_index={'TRUE' if check_index else 'FALSE'}"
    )
    if "quantile" in metric:
        body += f" --quantile={str(metric.split('_')[-1])}"
    return body


def lidR_canopy_height_call(
    lidar_path: os.PathLike,
    gridsize: float,
    out_dir: os.PathLike,
    buffer: float,
    chunk_size: float,
    save_prefix: str,
    overwrite: bool,
    check_index: bool,
) -> str:
    return (
        f"Rscript --verbose --no-save {LIDR_CANOPY_HEIGHT_SCRIPT}"
        f" --lidar_path={lidar_path}"
        f" --gridsize={gridsize}"
        f" --save_path={out_dir}"
        f" --buffer={buffer}"
        f" --chunk_size={chunk_size}"
        f" --save_prefix={save_prefix}"
        f" --overwrite={'TRUE' if overwrite else 'FALSE'}"
        f" --check_index={'TRUE' if check_index else 'FALSE'}"
    )
