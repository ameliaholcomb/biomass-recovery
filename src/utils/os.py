"""OS utils"""
import pathlib


def list_content(path: pathlib.Path, glob_query: str = "*") -> list[pathlib.Path]:
    path = pathlib.Path(path)
    return list(path.glob(glob_query))


def latest_matching_file(path: pathlib.Path) -> pathlib.Path:
    path = pathlib.Path(path)
    options = list(str(filepath) for filepath in path.parent.glob(f"{path.name}*"))
    return pathlib.Path(sorted(options)[-1])
