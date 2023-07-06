import pytest

from biomassrecovery.utils.gis import get_utm_zone

@pytest.mark.parametrize(
	"lat,lon,expected",
	[
		(52.205276, 0.119167, "EPSG:32631"), # Cambridge
		(-52.205276, 0.119167, "EPSG:32731"), # Anti-Cambridge
		(6.152938, 38.202316, "EPSG:32637"), # Yirgacheffe
		(4.710989, -74.072090, "EPSG:32618"), # Bogotá
	]
)
def test_utm_band(lat, lon, expected):

	utm_code = get_utm_zone(lon, lat)
	assert utm_code == expected

