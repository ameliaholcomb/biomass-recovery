"""Util to unzip .zip files to a given path in Python"""
import pathlib
import zipfile

from tqdm.autonotebook import tqdm


def unzip(
    zip_path: pathlib.Path,
    out_path: pathlib.Path,
    overwrite: bool = False,
    remove_archive_name: bool = True,
) -> bool:
    """
    Extract all files in a given .zip file at `zip_path` to `out_path`.

    Args:
        zip_path (pathlib.Path): Path to the .zip file which should be unzipped
        out_path (pathlib.Path): Path under which to save the extracted files
        overwrite (bool, optional): Whether to overwrite existing files at out_path.
            Defaults to False.
        remove_archive_name (bool, optional): Whether to remove the name of the zip
            archive from the path when extracting the zip files. Defaults to True.

    Returns:
        bool: True, iff the extraction succeeded.
    """
    # Ensure that
    zip_path = pathlib.Path(zip_path)
    out_path = pathlib.Path(out_path)

    # Open zip archive
    with zipfile.ZipFile(zip_path, "r") as zip_archive:
        # Get all files in zip_archive
        files = [elem for elem in zip_archive.infolist() if not elem.is_dir()]

        # Check for each file if it exists already. If yes, skip. If not, extract.
        files_progress = tqdm(files)
        for compressed_file in files_progress:
            # Get relative path of file in zip archive
            if remove_archive_name:
                relative_path = compressed_file.filename.split("/", maxsplit=1)[1]
            else:
                relative_path = compressed_file.filename

            # Update progress bar
            files_progress.set_description(
                f"{relative_path} ({compressed_file.file_size / 1024**2:.2f} MB)"
            )

            # Check for existing files
            if not overwrite and (out_path / relative_path).exists():
                # print(f"File {out_path / relative_path} exists.")
                continue
            else:
                zip_archive.extract(compressed_file, path=out_path / relative_path)
                assert (out_path / relative_path).exists()

    return True
