"""Utils for extracting the bounding (polygon) shape(s) from a raster file"""
from typing import Optional

import affine
import geopandas as gpd
import numpy as np
import xarray as xr
from rasterio.features import shapes
from skimage.morphology import convex_hull_image, dilation, erosion, square


def polygonise(
    data_array: np.ndarray,
    mask: Optional[np.ndarray] = None,
    transform: affine.Affine = affine.identity,
    crs: Optional[str] = None,
    apply_buffer: bool = True,
):
    """
    Convert 2D numpy array containing raster data into polygons.
    This implementation uses rasterio.features.shapes, which uses GDALpolygonize
    under the hood.
    References:
    (1) https://rasterio.readthedocs.io/en/latest/api/rasterio.features.html
    (2) https://gdal.org/programs/gdal_polygonize.html
    Args:
        data_array (np.ndarray): 2D numpy array with the raster data.
        mask (np.ndarray, optional): Boolean mask that can be applied over
        the polygonisation. Defaults to None.
        transform (affine.Affine, optional): Affine transformation to apply
        when polygonising. Defaults to the identity transform.
        crs (str, optional): Coordinate reference system to set on the
        resulting dataframe. Defaults to None.
        apply_buffer (bool, optional): Apply shapely buffer function to the
        polygons after polygonising. This can fix issues with the
        polygonisation creating invalid geometries.
    Returns:
        gpd.GeoDataFrame: GeoDataFrame containing polygon objects.
    """
    polygon_generator = shapes(
        data_array, mask=mask, connectivity=4, transform=transform
    )
    results = list(
        {"properties": {"class_label": int(val)}, "geometry": shape}
        for shape, val in polygon_generator
    )
    df = gpd.GeoDataFrame.from_features(results, crs=crs)

    if apply_buffer:
        # Redraw geometries to ensure polygons are valid.
        df.geometry = df.geometry.buffer(0)

    return df


def extract_bounding_shape(
    raster: xr.DataArray,
    target_crs: Optional[str] = None,
    target_resolution: float = 50,
    dilation_erosion_pixels: float = 5,
    buffer: float = 10,
    simplify_tolerance: float = 10,
) -> gpd.GeoDataFrame:
    """
    Return the bounding polygon(s) of the valid entries in the raster data.

    Args:
        raster (xr.DataArray): The raster data for which to extract the bounding shapes
        target_crs (Optional[str], optional): The target crs in which the extracted
            bounding shapes should be cast. Defaults to None.
        target_resolution (float, optional): The target resolution of the extracted
            bounding shape. Defaults to 50.
        dilation_erosion_pixels (float, optional): The number of pixels to use for
            dilation and erosion operations to fix small holes in the data.
            Defaults to 5.
        buffer (float, optional): The buffer to use on the output to ensure all data is
            contained. Defaults to 10.
        simplify_tolerance (float, optional): The simplification tolerance for
            simplyfing the bouding shape before outputting. Defaults to 10.

    Returns:
        gpd.GeoDataFrame: A geopandas dataframe of the bouding polygon(s) of the valid
            entries in the raster
    """

    if target_crs is None:
        target_crs = raster.rio.crs

    tmp_raster = raster.rio.reproject(target_crs, resolution=target_resolution)
    data = tmp_raster.data.squeeze() > 0

    # Create mask of image (and remove holes)
    dialate_eroded = erosion(
        dilation(data, square(dilation_erosion_pixels)), square(dilation_erosion_pixels)
    )
    convex_hull = convex_hull_image(data)
    mask = (dialate_eroded & convex_hull) | data

    # Polygonise elements in mask
    gdf = (
        polygonise(
            mask.astype("uint8"),
            mask,
            transform=tmp_raster.rio.transform(),
            crs=tmp_raster.rio.crs,
        )
        .buffer(buffer)
        .simplify(simplify_tolerance)
    )

    # Return polygonised image
    return gdf
