from pyspark.sql import SparkSession


from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

from geopandas import gpd
import datetime as dt
from shapely.geometry import box

from src.data.gedi_cmr_query import query
from src.data.gedi_database_loader import bulk_import_from_directory
from src.constants import USER_PATH, GEDI_L2A_PATH, GEDI_L4A_PATH


def _query_granule_metadata(bound, product):
    granules_metadata = query(
        product=product,
        spatial=bound,
    )
    print ("Total granules found: ", len(granules_metadata.index)-1)
    print ("Total file size (MB): ", granules_metadata['granule_size'].sum())
    print(granules_metadata.granule_name.head(5))
    return granules_metadata

## DO DOWNLOAD
def _postprocess_download():
    checkpoint_file = GEDI_L4A_PATH / "bulk_import_checkpoint.txt"
    roi = gpd.GeoDataFrame(shp.geometry)

    bulk_import_from_directory(
        dir=GEDI_L4A_PATH, 
        roi=roi,
        table_name="level_4a", 
        checkpoint=checkpoint_file,
        restore=False,
    )


spark = SparkSession.builder.getOrCreate()


def _exec_spark(bounds, product):
    granule_metadata = _query_granule_metadata(bounds, product)
    spark_frame = spark.createDataFrame(granule_metadata)
    print(spark_frame)


# AMAZON SHAPEFILE
shapefile = USER_PATH / 'shapefiles' / 'Amazon_rainforest_shapefile.zip'
shp = gpd.read_file(shapefile)

# The CMR API cannot accept a shapefile with more than 5000 points,
# so we simplify our query to just the bounding box around the region.
bbox = gpd.GeoSeries(box(*shp.geometry.values[0].bounds))
print(shp.geometry.values[0].bounds)

_exec_spark(bbox, 'gedi_l4a')
