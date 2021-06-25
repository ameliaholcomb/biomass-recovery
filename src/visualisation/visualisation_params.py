"""Styling parameters and legend for visualisations"""

opt_size = {"width": 750, "height": 320, "show_grid": True}

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
    "cmap": "bkr",
}

opt_metrics = {
    "colorbar": True,
    "tools": ["hover"],
    "cmap": "bgy_r",
}

legend = {
    "longo_biomass": dict(name="Longo ACD", unit="kgC/m2"),
    "longo_tch_biomass": dict(name="Longo TCH ACD", unit="kgC/m2"),
    "asner_tch_biomass": dict(name="Asner TCH ACD", unit="MgC/ha"),
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
    "acd_difference": dict(name="Difference", unit="kgC/m2"),
}
