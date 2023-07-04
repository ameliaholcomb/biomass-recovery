import argparse
from typing import List
import numba
import numpy as np
import pandas as pd
import pathlib
import rioxarray as rxr

from src.environment import ENV_VARS_PATH
from src.utils.logging import get_logger

logger = get_logger(__file__)

@numba.njit
def argnearest_centers(array, values):
    half_pixel = (array[1] - array[0]) / 2
    array_center = array + half_pixel
    argmins = np.zeros_like(values, dtype=np.int64)
    for i, value in enumerate(values):
        argmins[i] = (np.abs(array_center - value)).argmin()
    return argmins

def main(
    outdir: pathlib.Path,
    gedifile: pathlib.Path,
    layerdir: pathlib.Path,
    overwrite: bool,
):
    if not outdir.exists():
        raise FileNotFoundError(f'Could not find specified outdir {outdir}')

    if (outdir / 'gedi_envlayers.feather').exists() and not overwrite:
        print(f'Outfile {outdir}/gedi_envlayers.feather already exists and overwrite set to false, returning')
        return 1

    if not gedifile.exists():
        raise FileNotFoundError(f'Could not find specified gedifile {gedifile}')

    if not layerdir.exists():
        raise FileNotFoundError(f'Could not find specified layerdir {layerdir}')

    layers = layerdir.glob('*.tif')

    gedi = pd.read_feather(gedifile)

    for layer in layers:
        print(f'Processing layer in file {layer}')
        layer_rst = rxr.open_rasterio(layer)
        print('Calculating nearest x,y coordinates for each GEDI shot')
        x_inds = argnearest_centers(layer_rst.x.data, gedi.lon_lowestmode.values)
        y_inds = argnearest_centers(layer_rst.y.data, gedi.lat_lowestmode.values)
        print('Adding data to dataframe')
        name = layer.name.split('.')[0]
        gedi[name] = layer_rst.data[0][y_inds, x_inds]

    print(f'Writing out results')
    gedi.to_feather(outdir / 'gedi_envlayers.feather')


if __name__ == '__main__':
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Script to compute GEDI recovery period")
    parser.add_argument(
        "-o",
        "--outdir",
        help="The name of the directory to save output dataframes (as feather files).",
        type=str,
    )

    parser.add_argument(
       "-g",
        "--gedifile",
        help="The path to the feather file containing GEDI shots for overlay.",
        type=str,
    )
    parser.add_argument(
        "-l",
        "--layerdir",
        help="The name of the directory containing environmental layers.",
        type=str,
    )
    parser.add_argument(
        "--overwrite",
        help="Whether or not to overwrite existing saved files. Default is False",
        type=bool,
        default=False,
        nargs="?",  # Argument is optional
    )
    args = parser.parse_args()
    main(
        outdir=pathlib.Path(args.outdir),
        gedifile=pathlib.Path(args.gedifile),
        layerdir=pathlib.Path(args.layerdir),
        overwrite=args.overwrite
    )

