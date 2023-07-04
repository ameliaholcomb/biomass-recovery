import tempfile

import pytest
from geopandas import gpd
from shapely import MultiPolygon, Polygon

from src.data.gedi_download_pipeline import check_and_format_shape, DetailError

def _make_polygon(lat: float, lng: float, radius: float) -> Polygon:
	origin_lat = lat + radius
	origin_lng = lng - radius
	far_lat = lat - radius
	far_lng = lng + radius

	return Polygon([
		[origin_lng, origin_lat],
		[far_lng,    origin_lat],
		[far_lng,    far_lat],
		[origin_lng, far_lat],
		[origin_lng, origin_lat],
	])

def test_simple_shape():

	polygon = _make_polygon(10.0, 10.0, 0.3)
	geometry = gpd.GeoSeries(polygon)

	checked = check_and_format_shape(geometry)
	assert checked is not None

def test_too_many_shapes():

	polygon1 = _make_polygon(10.0, 10.0, 0.3)
	polygon2 = _make_polygon(12.0, 12.0, 0.4)
	geometry = gpd.GeoSeries([polygon1, polygon2])

	with pytest.raises(ValueError):
		_ = check_and_format_shape(geometry)

def test_multi_polygon_shapes():

	polygon1 = _make_polygon(10.0, 10.0, 0.3)
	polygon2 = _make_polygon(12.0, 12.0, 0.4)
	multipolygon = MultiPolygon([polygon1, polygon2])
	geometry = gpd.GeoSeries(multipolygon)

	checked = check_and_format_shape(geometry)
	assert checked is not None

def test_too_many_points_dont_simplify():
	points = []
	for i in range(5001):
		points.append((0.1, 0.0001 * i))
	polygon = Polygon(points)
	geometry = gpd.GeoSeries(polygon)

	with pytest.raises(DetailError):
		_ = check_and_format_shape(geometry)

def test_too_many_points_simplify():
	points = []
	for i in range(5001):
		points.append((0.1, 0.0001 * i))
	polygon = Polygon(points)
	geometry = gpd.GeoSeries(polygon)

	checked = check_and_format_shape(geometry, simplify=True)
	checked.contains(geometry)
