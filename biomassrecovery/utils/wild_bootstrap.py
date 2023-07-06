import numpy as np
import pandas as pd
import rpy2.robjects.packages as rpackages
from rpy2.robjects.vectors import StrVector
from rpy2.robjects import pandas2ri
from rpy2.rinterface_lib.callbacks import logger as rpy2_logger
import logging


pandas2ri.activate()
_QUANTREG_PACKAGE = "quantreg"
# Avoid warnings output to console -- Comment out to see warnings
rpy2_logger.setLevel(logging.ERROR)


def _import_quantreg():
    utils = rpackages.importr("utils")
    utils.chooseCRANmirror(ind=1)  # select the first mirror in the list
    if not rpackages.isinstalled(_QUANTREG_PACKAGE):
        utils.install_packages(StrVector([_QUANTREG_PACKAGE]))
    return rpackages.importr("quantreg")


_quantreg = _import_quantreg()


def wild_bootstrap(data: pd.DataFrame, formula: str, tau: float, num_samples: int):
    result = _quantreg.rq(formula=formula, tau=tau, data=data)
    info = _quantreg.summary_rq(
        result,
        tau=tau,
        se="boot",
        bsmethod="wild",
        R=num_samples,
        covariance=True,
    )
    return info.rx2("B")


if __name__ == "__main__":
    x = np.array([0, 4, 2, 36, 4, 100, 6, -10, 8, 0], dtype="float64")
    y = np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], dtype="float64")
    tau = 0.5
    num_samples = 5
    df = pd.DataFrame({"x": x, "y": y})
    formula = "x ~ y"
    print(wild_bootstrap(df, formula, tau=tau, num_samples=num_samples))
    print(wild_bootstrap(df, formula, tau=tau, num_samples=num_samples))
