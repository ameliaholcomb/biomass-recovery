"""Module contains all project wide constants."""
import logging
import os
from pathlib import Path

import dotenv

# ---------------- PATH CONSTANTS -------------------
#  Source folder path
constants_path = Path(__file__)
SRC_PATH = constants_path.parent
PROJECT_PATH = SRC_PATH.parent
dotenv.load_dotenv()

# Log relatedd paths
LOG_PATH = PROJECT_PATH / "logs"
LOG_PATH.mkdir(parents=True, exist_ok=True)

#  Data related paths
DATA_PATH = Path("/gws/nopw/j04/forecol/data")

GEDI_PATH = DATA_PATH / "GEDI"
GEDI_L1B_PATH = GEDI_PATH / "level1B"
GEDI_L2A_PATH = GEDI_PATH / "level2A"

JRC_PATH = DATA_PATH / "JRC"

PLANET_PATH = DATA_PATH / "Planet"

# ---------------- API KEYS -------------------------
PLANET_API_KEY = os.getenv("PLANET_API_KEY")

# ---------------- LOGGING CONSTANTS ----------------
DEFAULT_FORMATTER = logging.Formatter(
    (
        "%(asctime)s %(levelname)s: %(message)s "
        "[in %(funcName)s at %(pathname)s:%(lineno)d]"
    )
)
DEFAULT_LOG_FILE = LOG_PATH / "default_log.log"
DEFAULT_LOG_LEVEL = logging.DEBUG  # verbose logging per default

# ---------------- PROJECT CONSTANTS ----------------
# Coordinate reference systems (crs)
WGS84 = "EPSG:4326"  # WGS84 standard crs (latitude, longitude)
