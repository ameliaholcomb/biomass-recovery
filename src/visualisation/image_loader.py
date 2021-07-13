"""Convenience functions to create holoviews visualisations from raster images"""

import logging
import os
import pathlib
import re
from typing import Any, Optional

import geopandas as gpd
import holoviews as hv
import numpy as np
import rioxarray as rxr
import xarray as xr
from holoviews.operation.datashader import rasterize

from src.constants import (
    EBALIDAR_PATH,
    PAISAGENSLIDAR_PATH,
    SIRGAS_BRAZIL,
    WEBMERCATOR,
    WGS84,
)
from src.data.als_loading import load_raster
from src.utils.logging import get_logger
from src.visualisation.visualisation_params import (
    legend,
    opt_biomass,
    opt_densities,
    opt_difference,
    opt_esri,
    opt_metrics,
    opt_poly,
    opt_size,
)

logger = get_logger(__name__)
logger.setLevel(logging.INFO)


class VisualisationCache:
    def __init__(self) -> None:
        self._cache = {}

    def clear(self):
        logger.info("Clearning %s images from cache.", len(self))
        self._cache = {}

    @property
    def names(self):
        return list(self._cache.keys())

    def pop(self, name: str):
        return self._cache.pop(name)

    def __getitem__(self, name: str) -> Any:
        return self._cache[name]

    def __setitem__(self, name: str, content: Any) -> None:
        if not isinstance(
            content, (hv.Image, hv.core.layout.Layout, hv.core.layout.AdjointLayout)
        ):
            raise ValueError(
                f"Cache input must be a holoviews image, not {type(content)}"
            )
        self._cache[name] = content

    def bounds(self, name: str) -> tuple[float, float, float, float]:
        return self._cache[name].data["main"].bounds.lbrt()

    def __len__(self) -> int:
        return len(self._cache)

    def __repr__(self) -> str:
        return (
            f"Visualisation cache ({len(self)} images stored):\n"
            + self._cache.__repr__()
        )


# Set up visualisation cache to store visualisations
VISUALISATION_CACHE = VisualisationCache()


def create_raster_image(  # pylint: disable=dangerous-default-value
    raster: xr.DataArray, kdims: list[str] = ["Easting", "Northing"], **kwargs
) -> hv.Image:

    if "bounds" in kwargs.keys():
        bounds = kwargs["bounds"]
        kwargs.pop("bounds")
    else:
        bounds = (
            raster.x.data[0],
            raster.y.data[0],
            raster.x.data[-1],
            raster.y.data[-1],
        )

    img = hv.Image(
        raster.data.squeeze(),
        kdims=kdims,
        bounds=bounds,
        **kwargs,
    )
    return img


def load_image(
    survey_name: str,
    metric: str,
    grid_size: float,
    raster: Optional[xr.DataArray] = None,
    cache: bool = True,
    **load_raster_kwargs,
) -> hv.core.layout.AdjointLayout:

    if raster is None:
        raster = load_raster(survey_name, metric, grid_size, **load_raster_kwargs)
    if metric in legend.keys():
        label = legend[metric]["name"]
        unit = legend[metric]["unit"]
    else:
        label = metric
        unit = ""
    logger.debug("Creating image")
    img = create_raster_image(
        raster,
        vdims=[f"{label} [{unit}]"],
        label=f"{survey_name}: {label} at {grid_size}m",
    ).hist(normed=True)
    if cache:
        logger.debug("Adding to cache.")
        VISUALISATION_CACHE[f"{survey_name}-{metric}-{grid_size}"] = img
    logger.debug("Done.")
    return img


def load_bounds(survey_name: str, metric: str, grid_size: float, **load_raster_kwargs):
    img_name = f"{survey_name}-{metric}-{grid_size}"
    if img_name not in VISUALISATION_CACHE.names:
        load_image(survey_name, metric, grid_size, **load_raster_kwargs)
    return VISUALISATION_CACHE.bounds(img_name)


def stylise(img, metric, cnorm="linear"):

    if metric in legend.keys():
        label = legend[metric]["name"]
        unit = legend[metric]["unit"]
    else:
        label = metric
        unit = ""

    if "density" in metric or "point" in metric or "pulse" in metric:
        styling = opt_densities
    elif "difference" in metric:
        styling = opt_difference
    elif "biomass" in metric:
        styling = opt_biomass
    else:
        styling = opt_metrics

    return img.opts(
        hv.opts.Image(
            clabel=f"{label} [{unit}]",
            cformatter="%.0f",
            xformatter="%.0f",
            yformatter="%.0f",
            cnorm=cnorm,
            **opt_size,
            **styling,
        ),
        hv.opts.Histogram(
            xlabel=f"{label} [{unit}]",
            xaxis="bare",
            normalize=True,
            tools=["hover"],
            axiswise=False,
        ),
    )


def visualise(
    survey_name: str,
    metric: str,
    grid_size: float,
    raster: Optional[xr.DataArray] = None,
    datashader_aggregator: Optional[str] = None,
    cnorm: str = "linear",
    force_reload: bool = False,
    **load_raster_kwargs,
) -> hv.core.layout.AdjointLayout:

    img_name = f"{survey_name}-{metric}-{grid_size}"
    if force_reload or img_name not in VISUALISATION_CACHE.names:
        load_image(survey_name, metric, grid_size, raster=raster, **load_raster_kwargs)

    # if crs == WGS84:
    #    VISUALISATION_CACHE.easting_northing_to_lon_lat(img_name)
    # elif crs == WEBMERCATOR:
    #    VISUALISATION_CACHE.lon_lat_to_easting_northing(img_name)
    # else:
    #    raise NotImplementedError

    img = VISUALISATION_CACHE[img_name]
    if datashader_aggregator is not None:
        logger.debug("Datashading with %s", datashader_aggregator)
        img = rasterize(img, aggregator=datashader_aggregator, dynamic=True)

    return stylise(img, metric=metric, cnorm=cnorm)


def poly(geometry, crs: str = SIRGAS_BRAZIL):
    xs, ys = np.array(
        list(gpd.GeoSeries(geometry, crs=crs).to_crs(WEBMERCATOR)[0].exterior.coords)
    ).T
    return (
        hv.Polygons({"x": xs, "y": ys})
        .redim(x="Easting", y="Northing")
        .opts(**opt_poly)
    )


def esri(geometry, crs: str = SIRGAS_BRAZIL, tiles=hv.element.tiles.ESRI()):
    l, b, r, t = gpd.GeoSeries(geometry, crs=crs).to_crs(WEBMERCATOR).bounds.values[0]

    return tiles.redim(x="Easting", y="Northing").opts(
        **opt_esri,
        xlim=(l, r),
        ylim=(b, t),
    )
