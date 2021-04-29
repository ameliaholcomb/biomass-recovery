"""OS utils"""
import pathlib


def show_content(path: pathlib.Path, glob_query: str = "*") -> list[pathlib.Path]:
    return list(path.glob(glob_query))
