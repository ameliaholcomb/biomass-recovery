from geopandas import gpd
from shapely.geometry.polygon import orient
from shapely.geometry import box

class DetailError(Exception):
    """Used when too many points in a shape for NASA's API"""
    def __init__(self, n_coords: int):
        self.n_coords = n_coords

def check_and_format_shape(shp: gpd.GeoDataFrame, simplify: bool = False, max_coords: int = 4999) -> gpd.GeoSeries:
    """
    Checks a shape for compatibility with NASA's API.

    Args:
        shp (gpd.GeoDataFrame): The shape to check and format.
        simplify (bool): Whether to simplify the shape if it doesn't meet the max_coords threshold.
        max_coords (int): Threshold for simplifying, must be less than 5000 (NASA's upper-bound).

    Raises:
        ValueError: If max_coords is not less than 5000 or more than one polygon is supplied.
        DetailError: If simplify is not true and the shape does not have less than max_coord points.

    Returns:
        GeoSeries: The possibly simplified shape.
    """
    if len(shp) > 1:
        raise ValueError("Only one polygon at a time supported.")
    if max_coords > 4999:
        raise ValueError("NASA's API can only cope with less than 5000 points")

    row = shp.geometry.values[0]
    multi = row.geom_type.startswith("Multi")
    oriented = None

    # The CMR API cannot accept a shapefile with more than 5000 points,
    # so we offer to simplify the query to just the bounding box around the region.
    if multi:
        n_coords = sum([len(part.exterior.coords) for part in row.geoms])
    else:
        n_coords = len(row.exterior.coords)
    if n_coords > max_coords:
        if not simplify:
            raise DetailError(n_coords)
        oriented = gpd.GeoSeries(box(*row.bounds))

    if multi and oriented is None:
        oriented = gpd.GeoSeries([orient(s) for s in row.geoms])
    if not multi and oriented is None:
        oriented = gpd.GeoSeries(orient(row))

    return oriented
