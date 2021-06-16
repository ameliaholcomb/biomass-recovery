# Imports
# library("geojsonio")
# library(e1071) # for calculating kurtosis
library("lidR") # for lidar processing
library("stringr") # for string processing routines
# library("tidyverse")

# Fix random seed
set.seed(32456)

# Implement Longo formula
longo_formula <- function(z) {
    zmean <- mean(z)

    # TODO: Figure out which kurtosis to use (compare python, Longo vals)
    zkurt <- e1071::kurtosis(z)
    zquantiles <- stats::quantile(z, c(.05, .10, 1.))
    ziqr <- stats::IQR(z)

    # Yet another definition of kurtosis?
    # n <- length(z)
    # zkurt <- n * sum( (z-zmean)^4 ) / sum( (z - zmean)^2 )^2

    # For debugging:
    # print(mean ^ 2.02)
    # print(zkurt)
    # print(quantiles[["5%"]] ^ 0.11)
    # print(quantiles[["10%"]] ^ -0.32)

    longo_val <- (0.20
    * zmean^2.02
        * zkurt^0.66
        * zquantiles[["5%"]]^0.11
        * zquantiles[["10%"]]^-0.32
        * zquantiles[["100%"]]^-0.82
        * ziqr^0.50
    )

    # Note: this is needed for cases where the 5% quantile is 0 and the 10% quantile
    #  is 0 as well. In this case the result will be NAN. This happens in open areas
    #  without forest cover
    if (is.na(longo_val)) {
        longo_val <- 0
    }


    list(longo_val)
}

check_las_file_integrity <- function(las, out_path) {
    print("Checking LAS file integrity.")
    check_result <- capture.output(lidR::las_check(las))
    check_result <- str_replace_all(
        paste(check_result,
            collapse = " \n "
        ),
        regex("\\033\\[32m|\\033\\[39m|\\033\\[33m|\\033\\[31m"),
        ""
    )
    file_handler <- file(out_path)
    writeLines(check_result, file_handler)
    close(file_handler)
}

process_las <- function(las,
                        path_data,
                        check_file = FALSE,
                        reclassify_ground = FALSE,
                        ground_clf_algorithm = csf(),
                        remove_duplicates = TRUE,
                        thin_down_data = FALSE,
                        thin_algorithm = homogenize(density = 4, res = 2),
                        plot_densities = TRUE,
                        plot_densities_res = 5,
                        dtm_algorithm = tin(),
                        biomass_res = 50) {

    # TODO: Set buffer size
    # TODO: Set saving directory
    # TODO: Set progress reporter
    # TODO: Set whether to compress laz output
    # TODO: Set whether to merge output files
    # TODO: Set whether to create a spatial index

    ## 1: Integrity check
    if (check_file) {
        out_path <- str_c(
            path_data$directory,
            "/",
            path_data$filename,
            "_las_check.txt"
        )

        # Perform the check if it does not exist yet
        if (file.exists(out_path)) {
            print("LAS integrity check already performed.")
        } else {
            print("Checking LAS file integrity.")
            check_las_file_integrity(las, out_path = out_path)
        }
    }
    else {
        print("Skipping integrity check.")
    }

    ## 1.1 Remove duplicates
    if (remove_duplicates) {
        print("Removing duplicates.")
        las <- lidR::filter_duplicates(las)
    }

    ## 1.2 Perform thinning
    if (thin_down_data) {
        print("Thinning data.")
        las <- lidR::decimate_points(las,
            algorithm = thin_algorithm
        )
    }

    ## 2: Ground classificattion
    # Check for existing ground normalisation
    has_ground <- (2L %in% las$Classification)

    if (!has_ground || reclassify_ground) {
        print("Ground classifcation")
        las <- lidR::classify_ground(las,
            ground_clf_algorithm,
            last_returns = TRUE
        )
    }
    else {
        print("Found points classified as ground.")
    }

    ## 3: Plot the point, pulse and ground point densities
    if (plot_densities) {
        point_density <- lidR::grid_metrics(las,
            ~ length(Z) / res^2,
            res = res
        )
        plot(point_density)

        pulse_density <- lidR::grid_metrics(las,
            ~ length(Z) / res^2,
            res = res,
            filter = ~ ReturnNumber == 1L
        )
        plot(pulse_density)

        ground_point_density <- lidR::grid_metrics(las,
            ~ length(Z) / res^2, res,
            filter = ~ (Classification == 2L | Classification == 9L)
        )
        plot(ground_point_density)

        # TODO: Save
    }

    ## 4: Perform height normalisation
    las <- normalize_height(las,
        algorithm = dtm_algorithm
    )
    # TODO: Save

    ## 5: Computing the gridded biomass via Longo
    biomass <- lidR::grid_metrics(las, ~ longo_formula(z = Z), res = biomass_res)
    plot(biomass)
    # TODO: Save

    return(las)
}