"""Upload script for local folders to Google Drive"""

import argparse
import logging
import pathlib

from src.data.gdrive import DriveAPI, logger

logger.setLevel(logging.INFO)


if __name__ == "__main__":

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Google Drive folder upload script")
    parser.add_argument(
        "folder_path",
        help="The path of the folder to upload to gdrive",
        type=str,
    )
    parser.add_argument(
        "gdrive_folder_name",
        help="The name of the gdrive folder under which to upload the local folder.",
        type=str,
    )

    args = parser.parse_args()

    # Make sure folder to upload exits
    folder_path = pathlib.Path(args.folder_path)
    if not folder_path.exists():
        raise ValueError(f"Path `{folder_path}` not found.")
    if not folder_path.is_dir():
        raise ValueError(f"Path `{folder_path}` is a file, not a directory.")

    # Connect to google drive and set folders to download and save path
    gdrive = DriveAPI()
    print("Signing in to Google")
    print(f"Signed in as {gdrive.username} ({gdrive.user_email})")

    # Get GDrive folder
    gdrive_folder = gdrive.get_folder(args.gdrive_folder_name)

    # Start download
    print(f"Starting upload of {folder_path}. Saving to {args.gdrive_folder_name}")
gdrive.upload_folder(folder_path, parent=gdrive_folder)  # TODO: Multithread suppot
