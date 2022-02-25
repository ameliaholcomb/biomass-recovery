from geopandas import gpd

from src.data.gedi_database_loader import bulk_import_from_directory
from src.constants import USER_PATH, GEDI_L4A_PATH


shapefile = USER_PATH / 'shapefiles' / 'district_onepoly'
shp = gpd.read_file(shapefile)

checkpoint_file = GEDI_L4A_PATH / "oneill" / "bulk_import_checkpoint.txt"
roi = gpd.GeoDataFrame(shp.geometry)

bulk_import_from_directory(
    dir=GEDI_L4A_PATH / "oneill", 
    roi=roi,
    table_name="level_4a", 
    checkpoint=checkpoint_file,
    restore=False,
)