"""Utilities to create and submit jobs to the JASMIN SLURM scheduler"""
import datetime
import os
import pathlib
import subprocess

# Allowed nodes on JASMIN
ALLOWED_NODE_CONSTRAINTS = [
    "ivybridge128G",
    "haswell256G",
    "broadwell256G",
    "ivybridge512G",
    "ivybridge2000G",
    "skylake348G",
    "epyctwo1024G",
]

# Allowed queues on JASMIN
SERIAL_QUEUES = ["test", "short-serial", "long-serial", "high-mem"]
PARALLEL_QUEUES = ["par-single", "par-multi"]
GPU_QUEUES = ["lotus_gpu"]
ALLOWED_QUEUES = SERIAL_QUEUES + PARALLEL_QUEUES + GPU_QUEUES

# Allowed slurm open modes
ALLOWED_OPEN_MODES = ["append", "truncate"]


def sbatch_header(
    queue: str = "short-serial",
    account: str = None,
    begin_time: datetime.time = None,
    max_time: datetime.time = None,
    expected_time: datetime.time = None,
    required_memory_per_node: int = None,  # memory per node in MB
    required_memory_per_cpu: int = None,
    n_cpu_cores: int = None,
    n_gpus: int = None,
    job_name: str = None,
    working_directory: os.PathLike = None,
    output_file_pattern: str = "slurm-%j.err",
    error_file_pattern: str = "slurm-%j.out",
    exclusive: bool = False,
    node_constraint: str = None,
    open_mode: str = None,
) -> str:
    """
    Create an SBATCH header for a valid SLURM sbatch file

    Args:
        queue (str, optional): The queue to submit the job to. Defaults to
            "short-serial".
        account (str, optional): The account to use. Defaults to None.
        begin_time (datetime.time, optional): Time to start the job at for delayed
            scheudling. Defaults to None.
        max_time (datetime.time, optional): Maximal runtime (job will be killed after
            this time). Defaults to None.
        expected_time (datetime.time, optional): Expected runtime. Defaults to None.
        required_memory_per_node (int, optional): Memory to request per node.
            Defaults to None.
        n_cpu_cores (int, optional): Number of CPU cores to request per node.
            Defaults to None.
        n_gpus (int, optional): Number of GPUs to request. Defaults to None.
        job_name (str, optional): Name of the job. Defaults to None.
        working_directory (os.PathLike, optional): Path to the working directory in
            which the output and err files will be written and in which the job will
            run. Defaults to None.
        output_file_pattern (str, optional): File pattern for the output file.
            Defaults to "slurm-%j.out".
        error_file_pattern (str, optional): File pattern for the error file.
            Defaults to "slurm-%j.err".
        exclusive (bool, optional): Whether to request exclusive node access (no other)
            jobs will be run on that node. Defaults to False.
        node_constraint (str, optional): The node constraint for specifying only cetain
            allowed nodes. Defaults to None.
        open_mode (str, optional): The open mode for output and err files. Defaults
            to None.

    Returns:
        str: The valid sbatch header with the given configuration
    """
    batch_template = "#!/bin/bash\n"

    if queue:
        if queue not in ALLOWED_QUEUES:
            raise ValueError(f"Queue {queue} invalid. Must be one of {ALLOWED_QUEUES}")
        batch_template += f"#SBATCH --partition={queue}\n"

    if queue in GPU_QUEUES and not account:
        batch_template += "#SBATCH --account=lotus_gpu\n"
    elif account:
        batch_template += f"#SBATCH --account={account}\n"

    if max_time:
        if not isinstance(max_time, datetime.time):
            raise ValueError(
                f"`max_time` {max_time} is invalid. Specify as `datetime.time` object."
            )
        batch_template += (
            f"#SBATCH --time={max_time}  # Set a timeout for the job in HH:MM:SS\n"
        )

    if expected_time:
        if not isinstance(expected_time, datetime.time):
            raise ValueError(
                f"`expected_time` {expected_time} is invalid."
                "Specify as `datetime.time` object."
            )
        batch_template += (
            f"#SBATCH --time-min={expected_time}  "
            "# Set expected runtime for the job in HH:MM:SS\n"
        )

    if begin_time:
        if not isinstance(begin_time, datetime.time):
            raise ValueError(
                f"`begin_time` {begin_time} is invalid. "
                "Specify as `datetime.time` object."
            )
        batch_template += f"#SBATCH --begin={begin_time}\n"

    if required_memory_per_node:
        batch_template += (
            f"#SBATCH --mem={required_memory_per_node} "
            "# Set the amount of memory for per node in MB.\n"
        )

    if required_memory_per_cpu:
        if queue not in PARALLEL_QUEUES:
            raise ValueError(
                f"Specified serial queue {queue} but requested "
                f"{required_memory_per_cpu} MB memory per CPU core. "
                f"For multiple cores, choose a parallel queue from {PARALLEL_QUEUES}"
            )
        batch_template += (
            f"#SBATCH --mem-per-cpu={required_memory_per_cpu}  "
            "# Set the amount of memory for per cpu in MB.\n"
        )

    if n_cpu_cores:
        if queue not in PARALLEL_QUEUES:
            raise ValueError(
                f"Specified serial queue {queue} but requested {n_cpu_cores} "
                "CPU cores. For multiple cores, choose a parallel queue from "
                f"{PARALLEL_QUEUES}"
            )
        batch_template += f"#SBATCH --ntasks={n_cpu_cores}\n"

    if n_gpus:
        if queue not in GPU_QUEUES:
            raise ValueError(
                f"Specified non-GPU queue {queue} but requested {n_gpus} GPU cores. "
                f"For GPU cores, choose a GPU queue from {GPU_QUEUES}"
            )
        batch_template += f"#SBATCH --gres=gpu:{n_gpus}  # Request a number of GPUs\n"

    if job_name:
        batch_template += f"#SBATCH --job-name={job_name}\n"

    if working_directory:
        assert pathlib.Path(working_directory).exists()
        assert pathlib.Path(working_directory).is_dir()
        batch_template += (
            f"#SBATCH --chdir={pathlib.Path(working_directory).absolute()}\n"
        )

    batch_template += f"#SBATCH --output={output_file_pattern}\n"
    batch_template += f"#SBATCH --error={error_file_pattern}\n"

    if node_constraint:
        if not node_constraint in ALLOWED_NODE_CONSTRAINTS:
            raise ValueError(
                f"Node constraint {node_constraint} invalid. "
                f"Must be one of {ALLOWED_NODE_CONSTRAINTS}"
            )
        batch_template += f'#SBATCH --constraint="{error_file_pattern}"\n'

    if open_mode:
        if not open_mode in ALLOWED_OPEN_MODES:
            raise ValueError(
                f"Invalid open mode {open_mode}. Must be one of {ALLOWED_OPEN_MODES}"
            )
        batch_template += f"#SBATCH --open-mode={open_mode}\n"

    if exclusive:
        batch_template += "#SBATCH --exclusive\n"

    batch_template += "\n\n# --- TEMPLATE AUTOGENERATED ---\n\n"

    return batch_template


def sbatch(file_path: os.PathLike) -> bool:
    file_path = pathlib.Path(file_path)

    assert file_path.exists()
    assert file_path.is_file()
    assert file_path.suffix == ".sh"

    subprocess.call(["sbatch", file_path.absolute()])
    return True
