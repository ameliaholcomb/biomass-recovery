import holoviews as hv
import numpy as np
import xarray as xr
from holoviews.operation.datashader import rasterize
from sklearn.linear_model import HuberRegressor, LinearRegression

from src.constants import WEBMERCATOR
from src.data.als_loading import load_als_survey
from src.data.jrc_loading import load_jrc_data
from src.processing.jrc_processing import (
    compute_last_observation,
    compute_recovery_period,
)
from src.visualisation.image_loader import esri, poly, visualise
from src.visualisation.visualisation_params import (
    opt_annual_change,
    opt_change_year,
    opt_size,
    opt_size2,
    opt_size3,
)


def plot_jrc_analysis(survey_name: str) -> hv.Layout:
    survey = load_als_survey(survey_name)

    annual_change = load_jrc_data(
        survey.lon_min,
        survey.lat_min,
        survey.lon_max,
        survey.lat_max,
        dataset="AnnualChange",
        years=list(range(1990, survey.survey_year + 1)),
    )

    first_deforested = load_jrc_data(
        survey.lon_min,
        survey.lat_min,
        survey.lon_max,
        survey.lat_max,
        dataset="DeforestationYear",
    )

    last_deforested = compute_last_observation(
        annual_change, jrc_class_value=3, first_observation=first_deforested
    ).rio.reproject(WEBMERCATOR)

    visualisation = (
        hv.Image(
            annual_change.loc[survey.survey_year]
            .rio.reproject(WEBMERCATOR)
            .rename({"x": "Easting", "y": "Northing"}),
            kdims=["Easting", "Northing"],
        ).opts(
            title=f"JRC Annual change map {survey.survey_year}",
            **opt_annual_change,
            **opt_size,
        )
        * poly(survey.geometry_metrics)
        + hv.Image(
            last_deforested.where(last_deforested > 0).rename(
                {"x": "Easting", "y": "Northing"}
            ),
            kdims=["Easting", "Northing"],
            vdims=["deforestation_year"],
        ).opts(
            title=f"Last deforested as per year {survey.survey_year}",
            **opt_size,
            **opt_change_year(end_year=survey.survey_year),
        )
        * poly(survey.geometry_metrics)
        + visualise(
            survey.name,
            "canopy_height_model",
            1,
            datashader_aggregator="mean",
            from_crs=survey.crs,
        )
        + esri(survey.geometry_bbox).opts(**opt_size) * poly(survey.geometry_metrics)
    ).cols(2)

    return visualisation


def plot_recovery_period(
    survey_name: str = None,
    recovery_raster: xr.DataArray = None,
    as_startyear=False,
) -> hv.Image:

    if survey_name is not None:
        survey = load_als_survey(survey_name)

        annual_change = load_jrc_data(
            survey.lon_min,
            survey.lat_min,
            survey.lon_max,
            survey.lat_max,
            dataset="AnnualChange",
            years=list(range(1990, survey.survey_year + 1)),
        )

        first_deforestation = load_jrc_data(
            survey.lon_min,
            survey.lat_min,
            survey.lon_max,
            survey.lat_max,
            dataset="DeforestationYear",
        )
        first_degradation = load_jrc_data(
            survey.lon_min,
            survey.lat_min,
            survey.lon_max,
            survey.lat_max,
            dataset="DegradationYear",
        )
        recovery_raster = compute_recovery_period(
            annual_change, first_deforestation, first_degradation, as_startyear
        )

    elif recovery_raster is not None:
        pass
    else:
        return ValueError("Must specify survey name or provide recovery_raster.")

    start_year = int(np.nanmin(recovery_raster.data))
    end_year = int(np.nanmax(recovery_raster.data))

    return hv.Image(
        recovery_raster.rio.reproject(WEBMERCATOR).rename(
            {"x": "Easting", "y": "Northing"}
        ),
        kdims=["Easting", "Northing"],
        vdims=["Recovery period"],
    ).opts(**opt_size, **opt_change_year(start_year, end_year))


def plot_recovery(
    table: hv.Table,
    k_dim: str = "Recovery Period [years]",
    v_dim: str = "CHM Mean Top of Canopy Height [m]",
    fit_low=3,
    fit_high=22,
    perform_fit=True,
) -> hv.Layout:

    # Compute values
    max_observation = int(table[k_dim].max())
    max_value = table[v_dim].max()

    # Box whisker plot
    box_whisker = hv.BoxWhisker(table, kdims=[k_dim], vdims=[v_dim]).sort()

    # Point scatter plot
    points = hv.Scatter(table, kdims=[k_dim], vdims=[v_dim])

    # No. observations histogram
    hist = table.hist(
        dimension=[k_dim],
        num_bins=max_observation,
        bin_range=(0.5, max_observation + 0.5),
        adjoin=False,
    )

    # Fit plot
    if perform_fit:
        filter = (table.data[k_dim] >= fit_low) & (table.data[k_dim] <= fit_high)
        X = table.data.loc[filter][k_dim].values.reshape(-1, 1)
        y = table.data.loc[filter][v_dim].values
        reg = LinearRegression(fit_intercept=True).fit(X, y)
        r_score = reg.score(X, y)
        X_pred = np.arange(0, max_observation + 2).reshape(-1, 1)
        y_pred = reg.predict(X_pred)
        fit = hv.Curve({"x": X_pred, "y": y_pred}).redim(x=k_dim, y=v_dim)

        # Fit plot 2
        reg2 = HuberRegressor(fit_intercept=True, epsilon=1.1).fit(X, y)
        r_score2 = reg2.score(X, y)
        y_pred2 = reg2.predict(X_pred)
        fit2 = hv.Curve({"x": X_pred, "y": y_pred2}).redim(x=k_dim, y=v_dim)

        # Text
        text = hv.Text(
            int(max_observation * 0.85),
            int((max_value) * 0.9),
            (
                f"R-score:  \t{r_score:.2f}\n"
                f"Slope:    \t{reg.coef_[0]:.2f} / {reg2.coef_[0]:.2f}\n"
                f"Intercept:\t{reg.intercept_:.1f}  / {reg2.intercept_:.1f}"
            ),
            halign="left",
            fontsize=10,
        )

    # Combine the plots
    if perform_fit:
        full_plot = (
            (
                fit.opts(color="red", tools=["hover"])
                * fit2.opts(color="green", tools=["hover"])
                * box_whisker.opts(box_alpha=0.15, box_color="light_blue")
                # * points.opts(color="k", tools=["hover"])
                * text
            ).opts(**opt_size2)
            + hist.opts(axiswise="yaxis", **opt_size3, tools=["hover"])
        ).cols(1)
    else:
        full_plot = (
            (box_whisker.opts(box_alpha=0.15, box_color="light_blue")).opts(**opt_size2)
            + hist.opts(axiswise="yaxis", **opt_size3, tools=["hover"])
        ).cols(1)

    return full_plot
