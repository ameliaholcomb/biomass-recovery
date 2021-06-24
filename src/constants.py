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
USER_PATH = Path("/gws/nopw/j04/forecol/svm34")
DATA_PATH = Path("/gws/nopw/j04/forecol/data")

GEDI_PATH = DATA_PATH / "GEDI"
GEDI_L1B_PATH = GEDI_PATH / "level1B"
GEDI_L2A_PATH = GEDI_PATH / "level2A"
JRC_PATH = DATA_PATH / "JRC"
PLANET_PATH = DATA_PATH / "Planet"
PAISAGENSLIDAR_PATH = DATA_PATH / "Paisagenslidar"
EBALIDAR_PATH = DATA_PATH / "EBA_lidar"

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

WGS84_UTM18S = "EPSG:32718"  # https://epsg.io/32718
WGS84_UTM19S = "EPSG:32719"  # https://epsg.io/32719
WGS84_UTM20S = "EPSG:32720"  # https://epsg.io/32720
WGS84_UTM21S = "EPSG:32721"  # https://epsg.io/32721
WGS84_UTM22S = "EPSG:32722"  # https://epsg.io/32722
WGS84_UTM23S = "EPSG:32723"  # https://epsg.io/32723

SIGRAS2000_UTM18S = "EPSG:31978"  # https://epsg.io/31978
SIGRAS2000_UTM19S = "EPSG:31979"  # https://epsg.io/31979
SIGRAS2000_UTM20S = "EPSG:31980"  # https://epsg.io/31980
SIGRAS2000_UTM21S = "EPSG:31981"  # https://epsg.io/31981
SIGRAS2000_UTM22S = "EPSG:31982"  # https://epsg.io/31982
SIGRAS2000_UTM23S = "EPSG:31983"  # https://epsg.io/31983

# ---------------- DATABASE CONSTANTS ----------------
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_CONFIG = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"
