from pyspark.sql import SparkSession


from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

from geopandas import gpd
import datetime as dt
import subprocess
from shapely.geometry import box
from shapely.geometry.polygon import orient
import tempfile
import os.path
import shutil
import sqlalchemy

from src.data.gedi_cmr_query import query
from src.data import gedi_database_loader
from src import constants
from functools import partial
from typing import List



def _get_engine():
    # Since spark runs workers in their own process, we cannot share database connections
    # between workers. We just create a new connection for each query. This is reasonable because
    # most of our queries involve inserting a large amount of data into the database.
    return sqlalchemy.create_engine(constants.DB_CONFIG, echo=False)


def _query_granule_metadata(bounds, product):
    granule_metadatas = [
        query(product=product, spatial=bound) for bound in bounds
    ]
    return pd.concat(granule_metadatas)


def _download_url(product, input):
    name, url = input
    outfile_path = constants.gedi_product_path(product) / name
    if os.path.exists(outfile_path):
        return outfile_path
    try:
        temp = tempfile.NamedTemporaryFile(
            dir=constants.gedi_product_path(product)
        )
        subprocess.run(
            [
                "wget",
                "--load-cookies",
                constants.EARTH_DATA_COOKIE_FILE,
                "--save-cookies",
                constants.EARTH_DATA_COOKIE_FILE,
                "--auth-no-challenge=on",
                "--keep-session-cookies",
                "--content-disposition",
                "-nc",
                "-O",
                temp.name,
                url,
            ],
            check=True,
        )
        shutil.move(temp.name, outfile_path)
    finally:
        try:
            temp.close()
        except:
            pass
    return outfile_path


def _parse_file(file_path):
    return gedi_database_loader.parse_file(file_path)


def _filter_file(geo_data_frame):
    return gedi_database_loader.filter_granules(geo_data_frame, None)


def _write_db(product, gedi_data):
    # Write all shots in the gedi dataframe in transaction while inserting the granule name into the granules table.
    # As long as the assumption that no two gedi granules hold the same shots, this ensures uniqueness of the shots
    # in the database without requiring a unique key constraint on the shot table. This is important since it allows
    # us to drop indexes and key constraints on the shots table during # the insert which increases performance
    # considerably.
    with _get_engine().begin() as con:
        if gedi_data.empty:
            return
        gedi_data = gedi_data.astype({"shot_number": "int64"})
        granules_entry = pd.DataFrame(
            data={
                "granule_name": [gedi_data["granule_name"].head(1).item()],
                "created_date": [pd.Timestamp.utcnow()],
            }
        )
        granules_entry.to_sql(
            name=_granules_table(product),
            con=con,
            index=False,
            if_exists="append",
        )

        gedi_data.to_postgis(
            name=_product_table(product),
            con=con,
            index=False,
            if_exists="append",
        )
    return granules_entry


def _product_table(product):
    if product == constants.GediProduct.L4A:
        return "level_4a"
    raise ValueError("No product table defined for the product " + product)


def _granules_table(product):
    return _product_table(product) + "_granules"


def _query_downloaded(table_name):
    return pd.read_sql_table(
        table_name=table_name, columns=["granule_name"], con=_get_engine()
    )


def exec_spark(
    bounds: List[gpd.GeoSeries],
    product: constants.GediProduct,
    download_only: bool,
):
    granule_metadata = _query_granule_metadata(bounds, product).drop_duplicates(
        subset="granule_name"
    )

    if download_only:
        required_granules = granule_metadata
    else:
        stored_granules = _query_downloaded(_granules_table(product))
        required_granules = granule_metadata.loc[
            ~granule_metadata["granule_name"].isin(
                stored_granules["granule_name"]
            )
        ]

    if required_granules.empty:
        print("No granules required")
        return

    print("Total granules found: ", len(granule_metadata.index) - 1)
    print("Total file size (MB): ", granule_metadata["granule_size"].sum())
    print("Required granules found: ", len(required_granules.index) - 1)
    print("Required file size (MB): ", required_granules["granule_size"].sum())

    # Spark starts here
    spark = SparkSession.builder.getOrCreate()
    name_url = required_granules[["granule_name", "granule_url"]].to_records(
        index=False
    )
    urls = spark.sparkContext.parallelize(name_url)
    # Download the files to a stable location and return the file name
    files = urls.map(partial(_download_url, product))

    if download_only:
        files.count()
    else:
        # Parse each file into a geo data frame
        parsed_files = files.map(_parse_file)
        # Filter the geodataframe for suitable shots
        filtered_files = parsed_files.map(_filter_file)
        # coalesce to 8 partitions to avoid overloading the database with many connections.
        # The number 8 was chosen sort of arbitrarily, could increase or decrease.
        out = filtered_files.coalesce(8).map(partial(_write_db, product))
        # count forces evaluation of the rdd
        out.count()

    spark.stop()
    print("done")

    # reinsert database index on points


# CREATE TABLE IF NOT EXISTS public.level_4a (
#     granule_name text,
#     shot_number bigint,
#     beam_type text,
#     beam_name text,
#     delta_time double precision,
#     absolute_time timestamp with time zone,
#     sensitivity real,
#     algorithm_run_flag smallint,
#     degrade_flag smallint,
#     l2_quality_flag smallint,
#     l4_quality_flag smallint,
#     predictor_limit_flag smallint,
#     response_limit_flag smallint,
#     surface_flag smallint,
#     selected_algorithm smallint,
#     selected_mode smallint,
#     elev_lowestmode double precision,
#     lat_lowestmode double precision,
#     lon_lowestmode double precision,
#     agbd double precision,
#     agbd_pi_lower double precision,
#     agbd_pi_upper double precision,
#     agbd_se double precision,
#     agbd_t double precision,
#     agbd_t_se double precision,
#     pft_class smallint,
#     region_class smallint,
#     geometry public.geometry(Point,4326)
# );
# CREATE TABLE IF NOT EXISTS public.level_4a_granules (
#     granule_name text PRIMARY KEY,
#     created_date timestamptz
# );

if __name__ == "__main__":
    # AMAZON SHAPEFILE
    # shapefile = USER_PATH / 'shapefiles' / 'Amazon_rainforest_shapefile.zip'
    # shp = gpd.read_file(shapefile)

    # # The CMR API cannot accept a shapefile with more than 5000 points,
    # # so we simplify our query to just the bounding box around the region.
    # bbox = gpd.GeoSeries(box(*shp.geometry.values[0].bounds))
    # print(shp.geometry.values[0].bounds)
    # bbox1 = gpd.GeoSeries(box(-60, 0, -59.75, 0.10))
    # bbox2 = gpd.GeoSeries(box(-60, 0.05, -59.75, 0.20))
    # exec_spark([bbox1, bbox2], constants.GediProduct.L4A, download_only=False)

    bbox = gpd.read_file(
        constants.USER_PATH / "shapefiles" / "bbox_formatted.gpd"
    )
    boxes = [gpd.GeoSeries(orient(b)) for b in bbox.geometry.values[0].geoms]

    exec_spark(boxes, constants.GediProduct.L4A, download_only=False)
