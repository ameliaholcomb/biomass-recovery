"""OS utils"""
import os
import pathlib
import subprocess
from typing import Optional


def list_content(path: os.PathLike, glob_query: str = "*") -> list[pathlib.Path]:
    path = pathlib.Path(path)
    return list(path.glob(glob_query))


def latest_matching_file(path: os.PathLike) -> pathlib.Path:
    path = pathlib.Path(path)
    options = list(str(filepath) for filepath in path.parent.glob(f"{path.name}*"))
    return pathlib.Path(sorted(options)[-1])


def run_shell_script(
    file_path: os.PathLike, output_path: Optional[os.PathLike] = None
) -> bool:
    file_path = pathlib.Path(file_path)
    if output_path is not None:
        output_path = pathlib.Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.touch(exist_ok=True)
        output = open(output_path, "w")
    else:
        output = None

    assert file_path.exists()
    assert file_path.is_file()
    assert file_path.suffix == ".sh"

    subprocess.call(["bash", file_path.absolute()], stdout=output, stderr=output)
    return True
