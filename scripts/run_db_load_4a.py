from geopandas import gpd

from src.data.gedi_database_loader import bulk_import_from_directory
from src.constants import USER_PATH, GEDI_L2A_PATH, GEDI_L4A_PATH


shapefile = USER_PATH / "Amazon_rainforest_shapefile.zip"
shp = gpd.read_file(shapefile)

checkpoint_file = GEDI_L4A_PATH / "bulk_import_checkpoint.txt"
roi = gpd.GeoDataFrame(shp.geometry)

bulk_import_from_directory(
    dir=GEDI_L4A_PATH, 
    roi=roi,
    table_name="level_4a", 
    checkpoint=checkpoint_file,
    restore=False,
)