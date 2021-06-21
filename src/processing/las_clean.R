OPT_FILTER_QUANTILE <- 0.95

filter_noise <- function(las, sensitivity = 1.1, grid_size = 10) {
    if (is(las, "LAS")) {
        # Construct grid of quantile to use for filtering
        filter <- lidR::grid_metrics(las,
            func = ~ stats::quantile(Z, probs = OPT_FILTER_QUANTILE),
            grid_size
        )

        # Filter points which are higher than sensitivity * filter_quantile
        las <- lidR::merge_spatial(las, filter, "filter")
        las <- lidR::filter_poi(las, Z < filter * sensitivity)
        # Remove temporary filter_quantile column
        las$filter <- NULL

        # Filter points which are lower than the ground
        las <- lidR::filter_poi(las, Z >= 0)
        return(las)
    }

    if (is(las, "LAScluster")) {
        # Here the input 'las' will a LAScluster

        las <- lidR::readLAS(las) # Read the LAScluster
        if (is.empty(las)) {
            return(NULL)
        } # Exit early for empty chunks (needed to make it work, see documentation)

        # Filter the noise
        las <- filter_noise(las,
            sensitivity = sensitivity,
            grid_size = grid_size
        )
        # Remove buffer before returning
        las <- lidR::filter_poi(las, buffer == 0)
        # Return filtered point cloud
        return(las)
    }

    if (is(las, "LAScatalog")) {
        lidR::opt_select(las) <- "*" # Do not respect the select argument by overwriting it

        options <- list(
            need_output_file = FALSE, # Throw an error if no output template is provided
            need_buffer = TRUE, # Throw an error if buffer is 0
            automerge = TRUE, # Automatically merge the output list (here into a LAScatalog)
            drop_null = TRUE # Automatically drop empty tiles
        )

        output <- lidR::catalog_apply(las, filter_noise,
            sensitivity = sensitivity,
            grid_size = grid_size,
            .options = options
        )
        return(output)
    }

    stop("Invalid input")
}