"""Module contains all project wide constants."""
from enum import Enum

class GediProduct(Enum):
    L1B = "level1B"
    L2A = "level2A"
    L2B = "level2B"
    L3 = "level3"
    L4A = "level4A"
    L4B = "level4B"


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
