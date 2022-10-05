import math
import numpy as np


def axial_round(qs: np.array, rs: np.array):
    """Round floating point hexagonal coordinates to integers.
    Effectively, this maps each point in hexagonal space to the canonical
    name of the grid hexagon that contains it.

    See https://observablehq.com/@jrus/hexround
    for algorithm and details.

    All coordinates are given in axial notation."""
    qgrid = np.round(qs)
    rgrid = np.round(rs)
    q_diff = qs - qgrid
    r_diff = rs - rgrid

    q_larger = np.abs(q_diff) >= np.abs(r_diff)

    qgrid[q_larger] += np.round(q_diff[q_larger] + 0.5 * r_diff[q_larger])
    rgrid[~q_larger] += np.round(r_diff[~q_larger] + 0.5 * q_diff[~q_larger])
    return qgrid.astype(int), rgrid.astype(int)


def xy_to_axial(xs: np.array, ys: np.array, diameter: int):
    """Convert an array of x,y points to coordinates on a hexagonal grid
    with the given hexagon diameter.
    The result will be given in axial integer coordinates.

    See https://www.redblobgames.com/grids/hexagons/#pixel-to-hex
    for algorithm and details.
    """
    qs_float = (2.0 * xs / 3) / diameter
    rs_float = (-1.0 / 3 * xs + math.sqrt(3) / 3 * ys) / diameter
    return axial_round(qs_float, rs_float)


def pair_to_label(xs: np.array, ys: np.array):
    """Map integer pairs to a unique integer value.

    See https://stackoverflow.com/questions/919612/
    mapping-two-integers-to-one-in-a-unique-and-deterministic-way

    for algorithm and details.
    The idea is to be able to label each hexagon, identified by its (q,r) coords,
    with a single unique id number.
    """
    x = np.copy(xs)
    y = np.copy(ys)
    labels = np.zeros(x.shape)
    x[x >= 0] = 2 * x[x >= 0]
    x[x < 0] = -2 * x[x < 0] - 1

    y[y >= 0] = 2 * y[y >= 0]
    y[y < 0] = -2 * y[y < 0] - 1

    x_larger = x >= y
    labels[x_larger] = x[x_larger] * x[x_larger] + x[x_larger] + y[x_larger]
    labels[~x_larger] = x[~x_larger] + y[~x_larger] * y[~x_larger]
    return labels
