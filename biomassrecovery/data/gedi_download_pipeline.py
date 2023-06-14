from geopandas import gpd
from shapely.geometry.polygon import orient
from shapely.geometry import box

class DetailError(Exception):
    """Used when too many points in a shape for NASA's API"""
    def __init__(self, n_coords: int):
        self.n_coords = n_coords

def check_and_format_shape(shp: gpd.GeoDataFrame, simplify: bool = False) -> gpd.GeoSeries:
    if len(shp) > 1:
        raise ValueError("Only one polygon at a time supported.")
    row = shp.geometry.values[0]
    multi = row.geom_type.startswith("Multi")
    oriented = None

    # The CMR API cannot accept a shapefile with more than 5000 points,
    # so we offer to simplify the query to just the bounding box around the region.
    if multi:
        n_coords = sum([len(part.exterior.coords) for part in row.geoms])
    else:
        n_coords = len(row.exterior.coords)
    if n_coords > 4999:
        if not simplify:
            raise DetailError(n_coords)
        oriented = gpd.GeoSeries(box(*row.bounds))

    if multi and oriented is None:
        oriented = gpd.GeoSeries([orient(s) for s in row.geoms])
    if not multi and oriented is None:
        oriented = gpd.GeoSeries(orient(row))

    return oriented
