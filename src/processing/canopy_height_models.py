"""Functions for calculating biomass from TCH of canopy height models"""


def longo2016_acd(top_of_canopy_height: float) -> float:
    """
    Convert `top_of_canopy_height` (tch) to biomass according to
    the formula in Longo et al. 2016.

    Source:
        https://agupubs.onlinelibrary.wiley.com/action/downloadSupplement?
        doi=10.1002%2F2016GB005465&file=gbc20478-sup-0001-supplementary.pdf

    Args:
        top_of_canopy_height (float): Top of canopy height in meters
            (meant to work with mean TCH at 50x50m resolution as that
            is what was used in the creation of the formula)

    Returns:
        float: Above ground carbon density in kgC/m2.
    """
    # Note: ACD units are in kgC/m2
    return 0.054 * top_of_canopy_height ** 1.76


def asner2014_acd(top_of_canopy_height: float) -> float:
    """
    Convert `top_of_canopy_height` (tch) to biomass according to
    the general ACD formula in Asner & Mascaro 2014.

    Source:
        https://www.sciencedirect.com/science/article/pii/S003442571300360X

    Note:
        The original formula is in units of MgC/ha. We therefore
        divide the result by 10 to get the ACD values in kgC/m2

    Args:
        top_of_canopy_height (float): Top of canopy height in meters
            (meant to work with mean TCH at 50x50m resolution as that
            is what was used in the creation of the formula)

    Returns:
        float: Above ground carbon density in kgC/m2.
    """
    # Note: native ACD unit of formula is MgC/ha. To convert to
    #  kgC/m2, we divide by 10.
    return 6.8500 * top_of_canopy_height ** 0.9520 / 10.0


def asner2014_colombia_acd(top_of_canopy_height: float) -> float:
    """
    Convert `top_of_canopy_height` (tch) to biomass according to
    the Colombia ACD formula in Asner & Mascaro 2014.

    Source:
        https://www.sciencedirect.com/science/article/pii/S003442571300360X

    Note:
        The original formula is in units of MgC/ha. We therefore
        divide the result by 10 to get the ACD values in kgC/m2

    Args:
        top_of_canopy_height (float): Top of canopy height in meters
            (meant to work with mean TCH at 50x50m resolution as that
            is what was used in the creation of the formula)

    Returns:
        float: Above ground carbon density in kgC/m2.
    """
    # Note: native ACD unit of formula is MgC/ha. To convert to
    #  kgC/m2, we divide by 10.
    return 2.1011 * top_of_canopy_height ** 1.2682 / 10.0
