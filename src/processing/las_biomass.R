# Function to calculate the kurtosis of a distribution
kurtosis <- function(x, remove_nan = FALSE, excess_kurtosis = FALSE) {

    # Remove any NAN values if `remove_nan` is TRUE.
    if (any(ina <- is.na(x))) {
        if (remove_nan) {
            x <- x[!ina]
        } else {
            return(NA)
        }
    }

    # Calculate kurtosis
    n <- length(x)
    x <- x - mean(x)
    kurt <- n * sum(x^4) / (sum(x^2)^2)

    # In case of dealing with the excess kurtosis (ie. comparision vs
    #  a normal distribution), subtract the value for the normal
    #  distribution (=3)
    if (excess_kurtosis) {
        return(kurt - 3)
    }

    # Return kurtosis
    return(kurt)
}

# Longo et al. 2016 formula for above-ground carbon density (ACD)
longo_formula <- function(z) {
    # Mean calculation
    zmean <- mean(z)
    # kurtosis calculation
    zkurt <- kurtosis(z, remove_nan = TRUE, excess_kurtosis = FALSE)
    # 5%, 10% and 100% (max) quantile calculation
    zquantiles <- stats::quantile(z, c(.05, .10, 1.))
    # Interquartile range calculation
    ziqr <- stats::IQR(z)

    # Parameters taken from Longo et al. 2016 for the above-ground carbon
    #  density (ACD) formula in kg/m2. Link to formula:
    #  https://agupubs.onlinelibrary.wiley.com/doi/full/10.1002/2016GB005465
    longo_val <- (0.20
    * zmean^2.02
        * zkurt^0.66
        * zquantiles[["5%"]]^0.11
        * zquantiles[["10%"]]^-0.32
        * zquantiles[["100%"]]^-0.82
        * ziqr^0.50
    )

    # Note: this is needed for cases where the 5% quantile is 0 and the
    #  10% quantile is 0 as well. In this case the result will be NAN.
    #  Since this happens in predominantly open areaswithout forest cover
    #  we will set the ACD value to 0 in this case.
    if (is.na(longo_val)) {
        longo_val <- 0
    }

    # Return the ACD (above ground carbon density) value as calculated by
    # the Longo formula.
    list(longo_val)
}