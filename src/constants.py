"""Module contains all project wide constants."""
import logging
import os
from pathlib import Path
from enum import Enum

import dotenv

dotenv.load_dotenv()


class GediProduct(Enum):
    L1B = "level1B"
    L2A = "level2A"
    L2B = "level2B"
    L3 = "level3"
    L4A = "level4A"
    L4B = "level4B"


# ---------------- PATH CONSTANTS -------------------
#  Source folder path
constants_path = Path(__file__)
SRC_PATH = constants_path.parent
PROJECT_PATH = SRC_PATH.parent
CONDA_ENV = os.getenv("CONDA_DEFAULT_ENV")

# Log relatedd paths
LOG_PATH = PROJECT_PATH / "logs"
LOG_PATH.mkdir(parents=True, exist_ok=True)

#  Data related paths
DATA_PATH = Path(os.getenv("DATA_PATH"))
USER_PATH = Path(os.getenv("USER_PATH"))
EARTHDATA_USER = os.getenv("EARTHDATA_USER")
EARTHDATA_PASSWORD = os.getenv("EARTHDATA_PASSWORD")
EARTH_DATA_COOKIE_FILE = Path(os.getenv("EARTH_DATA_COOKIE_FILE"))

GEDI_PATH = DATA_PATH / "GEDI"


def gedi_product_path(product):
    return GEDI_PATH / product.value


GEDI_L1B_PATH = gedi_product_path(GediProduct.L1B)
GEDI_L2A_PATH = gedi_product_path(GediProduct.L2A)
GEDI_L4A_PATH = gedi_product_path(GediProduct.L4A)
JRC_PATH = DATA_PATH / "JRC"
ENV_VARS_PATH = DATA_PATH / "EnvVars"
PLANET_PATH = DATA_PATH / "Planet"
PAISAGENSLIDAR_PATH = DATA_PATH / "Paisagenslidar"
EBALIDAR_PATH = DATA_PATH / "EBA_lidar"

ENV_VARS_NAMES = ["defMean", "SCCsoil", "fpar", "lightning", "srtm"]

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
DEFAULT_LOG_LEVEL = logging.INFO  # verbose logging per default

# ---------------- PROJECT CONSTANTS ----------------
# Coordinate reference systems (crs)
WGS84 = "EPSG:4326"  # WGS84 standard crs (latitude, longitude)
WEBMERCATOR = "EPSG:3857"  # CRS for web maps
SIRGAS_BRAZIL = "EPSG:5880"  # Polyconic projected CRS for Brazil

WGS84_UTM18S = "EPSG:32718"  # https://epsg.io/32718
WGS84_UTM19S = "EPSG:32719"  # https://epsg.io/32719
WGS84_UTM20S = "EPSG:32720"  # https://epsg.io/32720
WGS84_UTM21S = "EPSG:32721"  # https://epsg.io/32721
WGS84_UTM22S = "EPSG:32722"  # https://epsg.io/32722
WGS84_UTM23S = "EPSG:32723"  # https://epsg.io/32723
WGS84_UTM24S = "EPSG:32724"  # https://epsg.io/32724

SIRGAS2000_UTM18S = "EPSG:31978"  # https://epsg.io/31978
SIRGAS2000_UTM19S = "EPSG:31979"  # https://epsg.io/31979
SIRGAS2000_UTM20S = "EPSG:31980"  # https://epsg.io/31980
SIRGAS2000_UTM21S = "EPSG:31981"  # https://epsg.io/31981
SIRGAS2000_UTM22S = "EPSG:31982"  # https://epsg.io/31982
SIRGAS2000_UTM23S = "EPSG:31983"  # https://epsg.io/31983
SIRGAS2000_UTM24S = "EPSG:31984"  # https://epsg.io/31984

# ---------------- DATABASE CONSTANTS ----------------
DB_HOST = os.getenv("DB_HOST")  # JASMIN database server
DB_NAME = os.getenv("DB_NAME")  # Database for GEDI shots
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_CONFIG = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"
