import datetime as dt
from collections import defaultdict
from requests.models import HTTPError
import geopandas as gpd
import pandas as pd
from pathlib import Path
import requests
from shapely.geometry import MultiPolygon, Polygon
from typing import Dict, Optional, Tuple
from biomassrecovery.constants import GediProduct


CMR_URL = "https://cmr.earthdata.nasa.gov/search/"
GRANULE_SEARCH_URL = CMR_URL + "granules.json"
CMR_DT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
CMR_PROJECT_IDS = {
    GediProduct.L1B: "C1908344278-LPDAAC_ECS",  # v002
    GediProduct.L2A: "C1908348134-LPDAAC_ECS",  # v002
    GediProduct.L2B: "C1908350066-LPDAAC_ECS",  # v002
    GediProduct.L3: "C2153683336-ORNL_CLOUD",
    # "gedi_l4a": "C2114031882-ORNL_CLOUD", # They have decided to change this??
    GediProduct.L4A: "C2237824918-ORNL_CLOUD",
}


def _get_cmr_id(product: GediProduct) -> str:
    if CMR_PROJECT_IDS.get(product):
        return CMR_PROJECT_IDS[product]
    else:
        raise ValueError("Product {} not supported".format(product))


def _construct_temporal_params(
    date_range: Optional[Tuple[dt.datetime, dt.datetime]]
) -> Optional[str]:

    param_str = ""
    if not date_range:
        return {}
    if len(date_range) != 2:
        raise ValueError("Must specify tuple of (startdate, enddate)")
    param_str += date_range[0].strftime(CMR_DT_FORMAT)
    param_str += ","
    param_str += date_range[1].strftime(CMR_DT_FORMAT)
    return {"temporal": param_str}


def _construct_spatial_params(spatial: gpd.GeoSeries) -> str:
    params = defaultdict(list)
    for elem in spatial:
        if elem.geom_type == "MultiPolygon":
            params.update(_construct_spatial_params(elem.geoms))
        elif elem.geom_type == "Polygon":
            if len(elem.interiors) != 0:
                raise ValueError(
                    f"Must use a shapefile for a polygon with holes"
                )
            params["polygon[]"].append(
                ",".join([",".join(map(str, e)) for e in elem.exterior.coords])
            )
        else:
            raise TypeError(f"Unsupported spatial type {elem.geom_type}")
    params["options[polygon][or]"] = "true"
    return params


def _check_shapefile(shapefile: Path) -> None:
    if not shapefile.exists():
        raise FileNotFoundError(f"Could not find file {shapefile}")
    if not str(shapefile).endswith(".zip"):
        raise TypeError(f"File {shapefile} must be an ESRI format zip file")
    return


def _construct_query_params(
    product: GediProduct,
    date_range: Optional[Tuple[dt.datetime, dt.datetime]],
    shapefile: Optional[Path],
    spatial: Optional[gpd.GeoSeries],
    page_size: int = 2000,
    page_num: int = 1,
) -> Tuple[Optional[Dict], Dict]:
    """Constructs query parameters and data for a request to the CMR API.
    See https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html
    for details.

    Returns:
        cmr_files: param dictionary for shapefiles to submit with API request.
            May be None, in which case a GET request can be used. If present,
            a POST request is required.
        cmr_params: param dictionary for API request.
    """

    cmr_files = None
    cmr_params = {}
    cmr_params["collection_concept_id"] = _get_cmr_id(product)
    cmr_params["page_size"] = page_size
    cmr_params["page_num"] = page_num
    cmr_params.update(_construct_temporal_params(date_range))

    if shapefile is not None and spatial is not None:
        raise ValueError("Must specify only one of shapefile and spatial range")
    elif shapefile is not None:
        _check_shapefile(shapefile)
        cmr_files = {
            "shapefile": (
                str(shapefile),
                open(shapefile, "rb"),
                "application/shapefile+zip",
            )
        }
        cmr_params["provider"] = "PROV1"
    elif spatial is not None:
        spatial = _construct_spatial_params(spatial)
        cmr_params.update(spatial)

    return cmr_files, cmr_params


def _parse_granules(granules):
    granule_array = []
    for g in granules:
        if not g["online_access_flag"]:
            continue

        if "LPDAAC" in g["data_center"]:
            granule_name = g["producer_granule_id"]
        if "ORNL" in g["data_center"]:
            granule_name = g["title"].split(".", maxsplit=1)[1]

        granule_url = ""
        granule_poly = ""

        # Read the file size in Mb
        granule_size = float(g["granule_size"])

        # Read the bounding polygons in this granule
        if "polygons" in g:
            polygons = g["polygons"]
            multipolygons = []
            for poly in polygons:
                i = iter(poly[0].split(" "))
                lat_lon = list(map(" ".join, zip(i, i)))
                multipolygons.append(
                    Polygon(
                        [
                            [float(p.split(" ")[1]), float(p.split(" ")[0])]
                            for p in lat_lon
                        ]
                    )
                )
            granule_poly = MultiPolygon(multipolygons)

        # Get URL to HDF5 files
        # Note that the LP DAAC and ORNL DAAC label download links slightly differently
        for link in g["links"]:
            if "LPDAAC" in g["data_center"]:
                if "type" in link and link["type"] == "application/x-hdfeos":
                    granule_url = link["href"]
            elif "ORNL" in g["data_center"]:
                if (
                    "title" in link
                    and link["title"].startswith("Download")
                    and link["title"].endswith(".h5")
                ):
                    granule_url = link["href"]
        granule_array.append(
            [granule_name, granule_url, granule_size, granule_poly]
        )

    return granule_array


def query(
    product: GediProduct,
    date_range: Optional[Tuple[dt.datetime, dt.datetime]] = None,
    shapefile: Optional[Path] = None,
    spatial: Optional[gpd.GeoSeries] = None,
    page_size: int = 2000,
) -> gpd.GeoDataFrame:
    """Query NASA CMR API for granule metadata within given spatiotemporal bounds.

    Queries the NASA CMR API for the specified product
    (see https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html)
    to get download links and other metadata of granules intersecting the given
    spatiotemporal bounds.

    Params:
        product: GEDI product name, e.g. "GEDI_L4A".
        date_range: Temporal range to query. If left blank, will query over all
            available date ranges.
        shapefile: Path to zip file of ESRI Shapefile folder (containing, at minimum, .shp, .shx, .dbf)
            See https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html#c-shapefile
            for restrictions.
            Only one of shapefile and spatial can be specified.
        spatial: Spatial range to query.
        page_size: Page size for response granules
    Returns:
        GeoDataFrame containing
    """

    page_num = 1
    cmr_files, cmr_params = _construct_query_params(
        product, date_range, shapefile, spatial, page_size, page_num
    )

    granule_array = []
    while True:
        cmr_params["page_num"] = page_num
        if cmr_files:
            response = requests.post(
                GRANULE_SEARCH_URL, files=cmr_files, params=cmr_params
            )
        else:
            response = requests.get(GRANULE_SEARCH_URL, params=cmr_params)

        if not response.ok:
            ## TODO: Something smarter here?
            raise HTTPError(f"{response.status_code}: {response.content}")

        granules = response.json()["feed"]["entry"]
        if granules:
            granule_array.extend(_parse_granules(granules))
            page_num += 1
        else:
            break

    df = pd.DataFrame(
        granule_array,
        columns=["granule_name", "granule_url", "granule_size", "granule_poly"],
    )
    df = df[df["granule_poly"] != ""]
    return gpd.GeoDataFrame(df, geometry=df.granule_poly)
