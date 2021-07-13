"""Styling parameters and legend for visualisations"""
import colorcet as cc
import matplotlib as mpl
from bokeh.models import FixedTicker

opt_size = {"width": 750, "height": 320, "show_grid": True}
opt_size2 = {"width": 550, "height": 350, "show_grid": True}
opt_size3 = {"width": 550, "height": 160, "show_grid": True}


opt_densities = {
    "colorbar": True,
    "tools": ["hover"],
    "cmap": "magma",
}

opt_biomass = {
    "colorbar": True,
    "tools": ["hover"],
    "cmap": "kgy_r",
}

opt_difference = {
    "colorbar": True,
    "tools": ["hover"],
    "cmap": "bwr",
    "clim": (-15, 15),
}

opt_metrics = {
    "colorbar": True,
    "tools": ["hover"],
    "cmap": "bgy_r",
}

legend = {
    "longo_biomass": dict(name="Longo 2016 formula ACD", unit="kgC/m2"),
    "longo2016_tch_biomass": dict(name="Longo 2016 ACD from TCH", unit="kgC/m2"),
    "asner2014_tch_biomass": dict(name="Asner 2014 ACD from TCH", unit="kgC/m2"),
    "asner2014_tch_colombia_biomass": dict(
        name="Asner 2014 ACD (Colombia-model) from TCH", unit="kgC/m2"
    ),
    "max": dict(name="Max-height", unit="m"),
    "mean": dict(name="Mean-height", unit="m"),
    "interquartile_range": dict(name="IQR-height", unit="m"),
    "canopy_height_model": dict(name="Canopy height", unit="m"),
    "kurtosis": dict(name="Kurtosis", unit="1"),
    "quantile0.05": dict(name="5th-Percentile-height", unit="m"),
    "quantile0.1": dict(name="10th-Percentile-height", unit="m"),
    "quantile0.2": dict(name="20th-Percentile-height", unit="m"),
    "quantile0.95": dict(name="95th-Percentile-height", unit="m"),
    "quantile0.98": dict(name="98th-Percentile-height", unit="m"),
    "point_density": dict(name="Point density", unit="1/m2"),
    "pulse_density": dict(name="Pulse density", unit="1/m2"),
    "ground_point_density": dict(name="Ground point density", unit="1/m2"),
    "n_points": dict(name="No. points", unit="1"),
    "n_pulses": dict(name="No. pulses", unit="1"),
    "n_ground_points": dict(name="No. ground points", unit="1"),
    "acd_difference": dict(name="ACD difference", unit="kgC/m2"),
    "chm_difference": dict(name="Canopy height difference", unit="m"),
}

# Satellite data visualisation parameters
opt_esri = dict(
    title="ESRI Satellite Imagery (2021)",
    xlabel="Longitude",
    ylabel="Latitude",
)

# Polygon options
opt_poly = dict(fill_alpha=0.3, hover_fill_alpha=0.0, tools=["hover"])


# Annual change visualisation parameters
rgb = lambda r, g, b: "#%02x%02x%02x" % (r, g, b)
annual_change_legend = {
    1: dict(name="Undisturbed TMF", color=rgb(0, 90, 0)),
    2: dict(name="Degraded TMF", color=rgb(100, 135, 35)),
    3: dict(name="Deforested land", color=rgb(255, 190, 45)),
    4: dict(name="Forest regrowth", color=rgb(210, 250, 60)),
    5: dict(
        name="Other land cover", color=rgb(0, 140, 190)
    ),  # Permanent or seasonal water
    6: dict(name="Other land cover", color=rgb(255, 255, 255))
    # 241: dict(name="fill value", color=rgb(0, 0, 0)),
}

opt_annual_change = dict(
    tools=["hover"],
    colorbar=True,
    clabel="JRC Annual Change class",
    cmap=mpl.colors.ListedColormap(
        [val["color"] for val in annual_change_legend.values()]
    ),
    color_levels=[key for key in annual_change_legend],
    colorbar_opts={
        "ticker": FixedTicker(ticks=[key + 0.5 for key in annual_change_legend]),
        "major_label_overrides": {
            key + 0.5: val["name"] for key, val in annual_change_legend.items()
        },
    },
    cformatter="%d",
    xformatter="%.0f",
    yformatter="%.0f",
)

# Change year visualisation parameters
def opt_change_year(start_year=1985, end_year=2020):
    return dict(
        tools=["hover"],
        colorbar=True,
        clabel="Year",
        cmap=mpl.colors.LinearSegmentedColormap.from_list(
            "cmap", cc.bgy, N=end_year - start_year
        ),
        colorbar_opts={
            "major_label_text_align": "left",
            "ticker": FixedTicker(
                ticks=[key - 0.5 for key in range(start_year, end_year + 1)]
            ),
            "major_label_overrides": {
                key - 0.5: str(key) for key in range(start_year, end_year + 1)
            },
        },
        cformatter="%d",
        xformatter="%.0f",
        yformatter="%.0f",
    )
