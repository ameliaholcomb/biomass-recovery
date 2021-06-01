"""Module to conveniently query GEDI v002 data (primarily L1B and L2A) locally"""
import pathlib
from dataclasses import dataclass

import folium
import folium.features
import folium.plugins
import geopandas as gpd
import pandas as pd
import shapely
import shapely.geometry

from src.constants import GEDI_L2A_PATH


@dataclass
class QueryParameters:
    base_url: str
    provider: str
    product: str


GEDI_L2A_QUERY_PARAMS = QueryParameters(
    base_url="https://search.earthdata.nasa.gov/search/granules",
    provider="p=C1908348134-LPDAAC_ECS",
    product="q=gedi%20",
)
GEDI_L1B_QUERY_PARAMS = QueryParameters(
    base_url="https://search.earthdata.nasa.gov/search/granules",
    provider="p=C1908344278-LPDAAC_ECS",
    product="q=gedi%20",
)

GEDI_L2A_METADATA_PATH = GEDI_L2A_PATH / "GEDI_L2A_v002_metadata_2021-04-20.gpkg"


def _load_gedi_metadata(metadata_path: pathlib.Path) -> gpd.GeoDataFrame:
    """
    Loads meta data from file via geopandas

    Args:
        metadata_path (pathlib.Path): Path to the metadata

    Returns:
        gpd.GeoDataFrame: The metadata with dates and locations parsed
    """
    # Load data from path
    l2a_meta = gpd.GeoDataFrame.from_file(metadata_path)

    # Parse dates
    date_cols = l2a_meta.columns[l2a_meta.columns.str.contains(r"\w*_time")]
    for col in date_cols:
        l2a_meta[col] = pd.to_datetime(l2a_meta[col])

    l2a_meta["measurement_date"] = l2a_meta["range_beginning_time"].dt.date.astype(
        "str"
    )
    l2a_meta["measurement_start_time"] = l2a_meta[
        "range_beginning_time"
    ].dt.time.astype("str")
    l2a_meta["measurement_end_time"] = l2a_meta["range_ending_time"].dt.time.astype(
        "str"
    )

    return l2a_meta


def _find_gedi_granules(
    roi: shapely.geometry.Polygon, metadata: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:

    candidates: list[int] = metadata.sindex.query(roi)
    overlapping_granules = metadata.iloc[candidates].overlaps(roi)

    return metadata.iloc[candidates][overlapping_granules]


def _polygon_to_earthdata_query_url(roi: shapely.geometry.Polygon) -> str:
    return "polygon[0]=" + "%2C".join(
        ["%2C".join([str(lon), str(lat)]) for (lon, lat) in roi.boundary.coords[::-1]]
    )


def _construct_earthdata_query(
    roi: shapely.geometry.Polygon, query_params: QueryParameters = GEDI_L2A_QUERY_PARAMS
) -> str:
    roi_request = _polygon_to_earthdata_query_url(roi)
    return (
        f"{query_params.base_url}"
        f"?{query_params.provider}"
        f"&{query_params.product}"
        f"&{roi_request}"
    )


def _visualize_query_result(
    roi: shapely.geometry.Polygon, granules: gpd.GeoDataFrame
) -> folium.Map:
    """
    Displays a map with the region of interest and the GEDI granule tracks overlaid

    Args:
        roi (shapely.geometry.Polygon): The region of interest as a Polygon
        granules (gpd.GeoDataFrame): The GEDI granules that overlap as GeoDataFrame

    Returns:
        folium.Map: A folium map with the GEDI granules and ROI overlaid.
    """

    relevant_columns = [
        "filename",
        "granule_uri",
        "measurement_date",
        "measurement_start_time",
        "measurement_end_time",
        "start_orbit_number",
        "reference_ground_track",
    ]
    granules_data = granules[relevant_columns + ["geometry"]]

    # Create map and add layers
    world_map = folium.Map(
        location=roi.centroid.coords[0][::-1],
        control_scale=True,
        zoom_start=8,
        tiles="OpenStreetMap",
    )

    # Add minimap
    folium.plugins.MiniMap(zoom_level_fixed=2).add_to(world_map)

    # Add ROI
    roi_style = {"fillColor": "#2a74ac", "color": "#2a74ac"}
    folium.GeoJson(
        data=roi.__geo_interface__,
        name="Region of interest",
        style_function=lambda x: roi_style,
    ).add_to(world_map)

    # Add granules
    granules_style = {"fillColor": "#1f4e13", "color": "#1b4611"}
    tooltip = folium.features.GeoJsonTooltip(fields=relevant_columns)
    folium.GeoJson(
        data=granules_data.__geo_interface__,
        name="GEDI granule footprints",
        tooltip=tooltip,
        style_function=lambda x: granules_style,
    ).add_to(world_map)

    folium.TileLayer(
        tiles=(
            "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/"
            "MapServer/tile/{z}/{y}/{x}"
        ),
        attr="Esri",
        name="Esri Satellite",
        overlay=False,
        control=True,
    ).add_to(world_map)

    # Add map controls
    folium.LayerControl().add_to(world_map)

    # Retunr map map
    return world_map


def gedi_query(
    roi: shapely.geometry.Polygon,
    visualize: bool = True,
    metadata_path: pathlib.Path = GEDI_L2A_METADATA_PATH,
    query_params: QueryParameters = GEDI_L2A_QUERY_PARAMS,
) -> dict:
    """
    Returns URL to corresponding query on earth data as well as a table with metadata
    on all overlapping GEDI granules for the selected metadata.

    Args:
        roi (shapely.geometry.Polygon): The region of interest to query.
        visualize (bool, optional): For use in notebooks. If True, the query will
            be displayed visually in the notebook. Defaults to True.
        metadata_path (pathlib.Path, optional): The path to the metadata that will
            be used to perfomr the spatial query. Defaults to GEDI_L2A_METADATA_PATH.
        query_params (QueryParameters, optional): The query parameters for constructing
            the earth data query URL. Defaults to GEDI_L2A_QUERY_PARAMS.

    Returns:
        dict: A dictionary containing the earth data query URL and a geopandas Dataframe
            with the granules that overlap the ROI.
    """

    metadata = _load_gedi_metadata(metadata_path)
    granules = _find_gedi_granules(roi, metadata)
    earthdata_query = _construct_earthdata_query(roi, query_params)

    query_result = {"query_link": earthdata_query, "granules": granules}

    if visualize:
        print(f"Corresponding Earthdata query:  {earthdata_query}")

        from IPython import display  # pylint: disable=import-outside-toplevel

        display.display(_visualize_query_result(roi, granules))

    return query_result
