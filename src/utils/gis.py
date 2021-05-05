"""General GIS tools and convenience functions"""
import math


def get_utm_zone(lon: float, lat: float) -> str:
    """
    Return the EPSG code of the corresponding UTM zone given (lon, lat) coordinates.

    Args:
        lon (float): Longitude (in WGS84)
        lat (float): Latitude  (in WGS84)

    Returns:
        str: The EPSG code of the corresponding UTM zone. Can be used directly to set
            crs in geopandas.
    """
    utm_band = str((math.floor((lon + 180) / 6) % 60) + 1)
    if len(utm_band) == 1:
        utm_band = "0" + utm_band
    if lat >= 0:
        epsg_code = "326" + utm_band
    else:
        epsg_code = "327" + utm_band
    return f"EPSG:{epsg_code}"
