OPT_FILTER_QUANTILE <- 0.95

# Function to filter noise (S3 dispatch)
filter_noise <- function(las, ...) {
    UseMethod("filter_noise", las)
}

filter_noise.LAS <- function(las, sensitivity, grid_size = 10) {

    # Construct grid of quantile to use for filtering
    filter_quantile <- lidR::grid_metrics(las,
        func = ~ stats::quantile(Z, probs = OPT_FILTER_QUANTILE),
        grid_size
    )

    # Filter points which are higher than sensitivity * filter_quantile
    las <- lidR::merge_spatial(las, filter_quantile, "filter_quantile")
    las <- lidR::filter_poi(las, Z < filter_quantile * sensitivity)
    # Remove temporary filter_quantile column
    las$filter_quantile <- NULL

    # Filter points which are lower than the ground
    las <- lidR::filter_poi(las, Z >= 0)
    return(las)
}

filter_noise.LAScluster <- function(las, sensitivity, grid_size = 10) {
    # The function is automatically fed with LAScluster objects
    # Here the input 'las' will a LAScluster

    las <- lidR::readLAS(las) # Read the LAScluster
    if (is.empty(las)) {
        return(NULL)
    } # Exit early for empty chunks (needed to make it work, see documentation)

    # Filter the noise
    las <- lidR::filter_noise(las,
        sensitivity = sensitivity,
        grid_size = grid_size
    )
    # Remove buffer before returning
    las <- lidR::filter_poi(las, buffer == 0)
    # Return filtered point cloud
    return(las)
}

filter_noise.LAScatalog <- function(las, sensitivity, grid_size = 10) {
    lidR::opt_select(las) <- "*" # Do not respect the select argument by overwriting it

    options <- list(
        need_output_file = TRUE, # Throw an error if no output template is provided
        need_buffer = TRUE, # Throw an error if buffer is 0
        automerge = TRUE
    ) # Automatically merge the output list (here into a LAScatalog)

    lidR::catalog_apply(las, filter_noise,
        sensitivity = sensitivity,
        grid_size = grid_size,
        .options = options
    )
}