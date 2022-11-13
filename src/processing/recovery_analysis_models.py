from __future__ import annotations
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from typing import Tuple

from src.utils import wild_bootstrap as wb


def _filter_pct_agreement(pct_agreement, recovery_sample):
    # Filter for points with at least x% agreement on recovery age.
    nbins = np.arange(
        0, np.max(recovery_sample[~np.isnan(recovery_sample)]) + 2
    )
    hist = (
        np.apply_along_axis(
            lambda a: np.histogram(a, bins=nbins)[0],
            axis=1,
            arr=recovery_sample,
        )
        / recovery_sample.shape[1]
    )
    return np.max(hist, axis=1) >= (pct_agreement / 100)


def _filter_pct_nonnan(pct_agreement, recovery_sample):
    # Filter for points with at least x% non-nan values (x% recovering forest)
    return (
        np.sum(~np.isnan(recovery_sample), axis=1) / recovery_sample.shape[1]
    ) >= pct_agreement / 100


def _mode(arr, axis):
    if arr.shape[0] > 0:
        nbins = np.arange(0, np.max(arr[~np.isnan(arr)]) + 2)
        hist = np.apply_along_axis(
            lambda a: np.histogram(a, bins=nbins)[0],
            axis=axis,
            arr=arr,
        )
        mode_count = np.max(hist, axis=axis)
        mode_val = np.argmax(hist, axis=axis)
        nan_count = np.sum(np.isnan(arr), axis=axis)

        mode_is_nan = np.where(nan_count > mode_count)

        mode_count[mode_is_nan] = nan_count[mode_is_nan]
        mode_val = np.argmax(hist, axis=axis)
        mode_val[mode_is_nan] = -1
        return mode_count, mode_val
    return np.array([], dtype=np.float64), np.array([], dtype=np.float64)


def filter_shots(opts, finterface, chunk_id: Tuple[int, str]):

    year, token = chunk_id
    recovery_sample = finterface.load_data(
        token=token, year=year, data_type="recovery"
    )

    filter_idx = _filter_pct_nonnan(opts.pct_agreement, recovery_sample)
    filtered_recovery = recovery_sample[filter_idx]
    # filter_idx2 = np.std(filtered_recovery, axis=1) <= 1
    # filtered_recovery = filtered_recovery[filter_idx2]

    hist_summary = pd.DataFrame(
        {
            "min": np.min(filtered_recovery, axis=1),
            "max": np.max(filtered_recovery, axis=1),
            "p75": np.quantile(filtered_recovery, q=0.75, axis=1),
            "median": np.quantile(filtered_recovery, q=0.50, axis=1),
            "p25": np.quantile(filtered_recovery, q=0.25, axis=1),
            "mean": np.mean(filtered_recovery, axis=1),
            "std": np.std(filtered_recovery, axis=1),
            "var": np.var(filtered_recovery, axis=1),
        }
    )
    counts, values = _mode(filtered_recovery, axis=1)
    hist_summary["mode_counts"] = counts
    hist_summary["mode_vals"] = values

    # Free up some memory before loading master df
    del filtered_recovery
    del recovery_sample
    master_df = finterface.load_data(token=token, year=year, data_type="master")
    filtered = master_df[filter_idx]
    # filtered = filtered[filter_idx2]
    # Note: Cannot assign df["shot_number"] = filtered["shot_number"]
    # This implicitly converts to float64 (for unknown reasons)
    # which is not big enough to hold the shot numbers, and silently makes them NaN.
    hist_summary["shot_number"] = filtered.shot_number.values
    finterface.save_data(
        token=token, year=year, data_type="filtered", data=filtered
    )
    finterface.save_data(
        token=token,
        year=year,
        data_type="hist",
        data=hist_summary,
    )
    return filtered


def run_median_regression_model(experiment_id, dataframe):
    formula = "agcd_{experiment_id} ~ r_{experiment_id}".format(
        experiment_id=experiment_id
    )
    result = wb.wild_bootstrap(
        data=dataframe, formula=formula, tau=0.5, num_samples=10
    )
    result_df = pd.DataFrame(result, columns=["b0", "b1"])
    return result_df


def run_ols_medians_model(experiment_id, dataframe):
    recovery_col = "r_{}".format(experiment_id)
    agcd_col = "agcd_{}".format(experiment_id)
    recovery_periods = dataframe[recovery_col].unique()
    median_agcds = np.array(
        [
            dataframe.loc[dataframe[recovery_col] == x, agcd_col].median()
            for x in recovery_periods
        ]
    )
    df = pd.DataFrame(
        {
            "recovery": recovery_periods,
            "agcd": median_agcds,
        }
    )
    df.to_feather("/maps/forecol/data/Overlays/monte_carlo/medians.feather")
    ols = smf.ols("agcd ~ recovery", df).fit()
    ols_ci = ols.conf_int().loc["recovery"].tolist()
    ols_intercept_ci = ols.conf_int().loc["Intercept"].tolist()
    result = [
        [
            ols.params["Intercept"],
            ols_intercept_ci[0],
            ols_intercept_ci[1],
            ols.params["recovery"],
            ols_ci[0],
            ols_ci[1],
            ols.rsquared,
        ]
    ]
    result_df = pd.DataFrame(
        result, columns=["a", "la", "ua", "b", "lb", "ub", "rs"]
    )
    return result_df
