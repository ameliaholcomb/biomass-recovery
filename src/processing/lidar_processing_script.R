#!/usr/bin/env Rscript
library("optparse")

option_list <- list(
    make_option(c("-i", "--lidar_path"),
        type = "character",
        default = NULL,
        help = "Path to las/laz file or directory containing laz/las
         files to be processed as LAScatalog",
        metavar = "character"
    ),
    make_option(c("-o", "--save_path"),
        type = "character",
        default = NULL,
        help = "Path in which to save the output results",
        metavar = "character"
    ),
    make_option(c("--save_pattern"),
        type = "character",
        default = "{XLEFT}_{YBOTTOM}",
        help = "Pattern to use for saving files",
        metavar = "character"
    ),
    make_option(c("--save_intermediates"),
        type = "logical",
        default = FALSE,
        help = "Whether to save intermediate files
            (ground classified, normalised, ...)",
        metavar = "bool"
    ),
    make_option(c("--compress_intermediates"),
        type = "logical",
        default = TRUE,
        help = "Whether to compress intermediate files",
        metavar = "bool"
    ),
    make_option(c("--buffer"),
        type = "integer",
        default = 50,
        help = "Buffer to use when processing files (in meter)",
        metavar = "number"
    ),
    make_option(c("--chunk_size"),
        type = "integer",
        default = 0,
        help = "Chunk size to use for processing the data.
            0 means file by file.",
        metavar = "number"
    ),
    make_option(c("--lidar_crs"),
        type = "character",
        default = NULL,
        help = "Proj4 string for the crs of the lidar data.",
        metavar = "character"
    ),
    make_option(c("--desired_density"),
        type = "double",
        default = 6,
        help = "Desired point/pulse density after point cloud decimation.
         Defaults to 6.",
        metavar = "number"
    ),
    make_option(c("--decimation_gridsize"),
        type = "double",
        default = 5,
        help = "Grid size to use for decimating the points/pulses. Defautls
         to 5.",
        metavar = "number"
    ),
    make_option(c("--decimate_by_pulse"),
        type = "logical",
        default = TRUE,
        help = "Whether to decimate by pulse (if TRUE) or by points
         (if FALSE). Defaults to True.",
        metavar = "bool"
    ),
    make_option(c("--filter_quantile"),
        type = "double",
        default = 0.95,
        help = "Quantile to use for as reference for height noise filtering.
         Defaults to 0.95.",
        metavar = "number"
    ),
    make_option(c("--filter_sensitivity"),
        type = "double",
        default = 1.1,
        help = "Points higher than `sensitivity`*`height at filter_quantile`
            will be filtered. Defaults to 1.1 (10%).",
        metavar = "number"
    ),
    make_option(c("--filter_gridsize"),
        type = "integer",
        default = 10,
        help = "Grid size at which the filter quantile will be computed.
         Defaults to 10.",
        metavar = "number"
    ),
    make_option(c("--perform_check"),
        type = "logical",
        default = FALSE,
        help = "Whether to perform a deep check on the files. Defaults to
         FALSE.",
        metavar = "bool"
    ),
    make_option(c("--perform_decimation"),
        type = "logical",
        default = FALSE,
        help = "Whether to decimate the point cloud to a targeted pulse / point
         density before running all subsequent processing steps. Defaults to
         FALSE",
        metavar = "bool"
    ),
    make_option(c("--perform_classification"),
        type = "logical",
        default = TRUE,
        help = "Whether to classify the ground.
         If FALSE, uses existing Classifiation. Defaults to TRUE.",
        metavar = "bool"
    ),
    make_option(c("--perform_normalisation"),
        type = "logical",
        default = TRUE,
        help = "Whether to normalise the data.",
        metavar = "bool"
    ),
    make_option(c("--perform_filter"),
        type = "logical",
        default = TRUE,
        help = "Whether to filter the data.",
        metavar = "bool"
    ),
    make_option(c("--overwrite"),
        type = "logical",
        default = FALSE,
        help = "Whether to overwrite existing files.",
        metavar = "number"
    ),
    make_option(c("--seed"),
        type = "integer",
        default = 94153,
        help = "Random seed for stochastic computations",
        metavar = "number"
    ),
    make_option(c("--height_map_path"),
        type = "character",
        default = "/gws/nopw/j04/forecol/data/Amazon_height_map/rfHeightRasterCor80v14042020.tif",
        help = "Path to a heightmap of maximum heights to
         filter too high values.",
        metavar = "character"
    )
)

# Import custom scripts
R_SCRIPT_PATH <- "/home/users/svm/Code/gedi_biomass_mapping/src/processing/"

source(paste0(R_SCRIPT_PATH, "timestamp.R"))
source(paste0(R_SCRIPT_PATH, "las_clean.R"))
source(paste0(R_SCRIPT_PATH, "las_indexing.R"))
source(paste0(R_SCRIPT_PATH, "las_utils.R"))


# Create first timestamp
tmp_ <- timestamp(write_output = FALSE)

# Parse arguments
opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

# Parse and verify inputs
if (is.null(opt$lidar_path)) {
    print_help(opt_parser)
    stop("At least one argument must be supplied (lidar path).", call. = FALSE)
}

if (is.null(opt$save_path)) {
    print_help(opt_parser)
    stop("At least one argument must be supplied (save path).", call. = FALSE)
}

# Fix random seed for stochastic computations
set.seed(opt$seed)

# Load catalog
write(paste(
    "\n", timestamp(write_output = FALSE)$now,
    ": ... Reading in catalog ...\n"
), stdout())
ctg <- lidR::readLAScatalog(opt$lidar_path)
print(ctg)

# Perform check of the input files
if (opt$perform_check) {
    write(paste("\n", Sys.time(), ": ... Performing check ...\n"), stdout())
    lidR::las_check(ctg, deep = TRUE)
}

# Set catalog processing parameters
lidR::opt_select(ctg) <- "*" # Select all variables
lidR::opt_filter(ctg) <- "" # Do not filter any points
lidR::opt_chunk_size(ctg) <- opt$chunk_size # Process by original files
lidR::opt_chunk_buffer(ctg) <- opt$buffer # Use a buffer of 50 m
lidR::opt_laz_compression(ctg) <- opt$compress_intermediates # Save as .laz
lidR::opt_progress(ctg) <- TRUE # Show progress
lidR::opt_stop_early(ctg) <- FALSE # Continue upon errors

# Summarise processing options and write to output
write(
    paste(
        "\nFiles:", length(ctg$filename),
        "\nChunk size:", opt$chunk_size,
        "\nBuffer:", opt$buffer,
        "\nSave path:", opt$save_path,
        "\nFilter quantile:", opt$filter_quantile,
        "\nFilter sensitivity:", opt$filter_sensitivity,
        "\nFilter grid size:", opt$filter_gridsize,
        "\nSeed:", opt$seed,
        "\nSave intermediate files:", opt$save_intermediates,
        "\n\n"
    ),
    stdout()
)
summary(ctg)

# Step 0: Optionally decimate point cloud ===================================
if (opt$perform_decimation) {
    write(
        paste("\n", timestamp()$now, ": ... Decimating pointcloud ...\n"),
        stdout()
    )
    # Set output directory
    lidR::opt_output_files(ctg) <- paste0(
        opt$save_path,
        "/decimated/",
        opt$save_pattern
    )

    # Deactivate lidR processing for chunks which are fully
    #  contained in the files at the output path (unless
    #  overwrite is TRUE.)
    if (opt$overwrite) {
        ctg$processed <- rep(TRUE, length(ctg))
    } else {
        ctg$processed <- unprocessed_files(ctg)
    }

    n_files_to_process <- sum(ctg$processed)
    n_files_total <- length(ctg)
    write(
        paste(
            "Files to process:", n_files_to_process,
            "of", n_files_total
        ),
        stdout()
    )

    if (n_files_to_process > 0) {
        # Ensure files are spatially indexed to greatly speed up computations
        ensure_lax_index(ctg, verbose = TRUE)
        # Perform point decimation
        ctg <- lidR::decimate_points(ctg,
            algorithm = lidR::homogenize(
                density = opt$desired_density,
                res = opt$decimation_gridsize,
                use_pulse = opt$decimate_by_pulse
            )
        )
    }

    # Reload the output catalog with same processing options as ctg if
    # not all files where processed to ensure that all files are included
    # in the next step.
    if (n_files_to_process < n_files_total) {
        write("Reloading catalog", stdout())
        ctg <- reload_ctg_output(ctg)
    }
}



# Step 1: Classify ground ===================================================
if (opt$perform_classification) {
    write(
        paste("\n", timestamp()$now, ": ... Classifying ground ...\n"),
        stdout()
    )
    # Set output path
    lidR::opt_output_files(ctg) <- paste0(
        opt$save_path,
        "/csf_ground/",
        opt$save_pattern
    )

    # Deactivate lidR processing for chunks which are fully
    #  contained in the files at the output path (unless
    #  overwrite is TRUE.)
    if (opt$overwrite) {
        ctg$processed <- rep(TRUE, length(ctg))
    } else {
        ctg$processed <- unprocessed_files(ctg)
    }

    n_files_to_process <- sum(ctg$processed)
    n_files_total <- length(ctg)
    write(
        paste(
            "Files to process:", n_files_to_process,
            "of", n_files_total
        ),
        stdout()
    )

    if (n_files_to_process > 0) {
        # Ensure files are spatially indexed to greatly speed up computations
        ensure_lax_index(ctg, verbose = TRUE)
        # Perform ground classification with cloth simulation function
        ctg <- lidR::classify_ground(ctg,
            algorithm = lidR::csf(),
            last_returns = TRUE
        )
    }

    # Reload the output catalog with same processing options as ctg if
    # not all files where processed to ensure that all files are included
    # in the next step.
    if (n_files_to_process < n_files_total) {
        write("Reloading catalog", stdout())
        ctg <- reload_ctg_output(ctg)
    }
} else {
    write("Using pre-classified ground.")
}



# Step 2: Perform height normalisation ======================================
if (opt$perform_normalisation) {
    write(
        paste("\n", timestamp()$now, ": ... Normalising heights ...\n"),
        stdout()
    )
    # Set output path
    lidR::opt_output_files(ctg) <- paste0(
        opt$save_path,
        "/normalised/",
        opt$save_pattern
    )

    # Deactivate lidR processing for chunks which are fully
    #  contained in the files at the output path (unless
    #  overwrite is TRUE.)
    if (opt$overwrite) {
        ctg$processed <- rep(TRUE, length(ctg))
    } else {
        ctg$processed <- unprocessed_files(ctg)
    }

    n_files_to_process <- sum(ctg$processed)
    n_files_total <- length(ctg)
    write(
        paste(
            "Files to process:", n_files_to_process,
            "of", n_files_total
        ),
        stdout()
    )

    if (n_files_to_process > 0) {
        # Ensure files are spatially indexed to greatly speed up computations
        ensure_lax_index(ctg, verbose = TRUE)
        # Normalise heights
        ctg <- lidR::normalize_height(ctg, algorithm = lidR::tin())
    }

    # Reload the output catalog with same processing options as ctg if
    # not all files where processed to ensure that all files are included
    # in the next step.
    if (n_files_to_process < n_files_total) {
        write("Reloading catalog")
        ctg <- reload_ctg_output(ctg)
    }
} else {
    write("Skipping normalisation.")
}


# Step 3: Filter noise ======================================================
if (opt$perform_filter) {
    write(
        paste("\n", timestamp()$now, ": ... Filtering noise ...\n"),
        stdout()
    )
    # Set output path
    lidR::opt_output_files(ctg) <- paste0(
        opt$save_path,
        "/processed/",
        opt$save_pattern
    )

    # Deactivate lidR processing for chunks which are fully
    #  contained in the files at the output path (unless
    #  overwrite is TRUE.)
    if (opt$overwrite) {
        ctg$processed <- rep(TRUE, length(ctg))
    } else {
        ctg$processed <- unprocessed_files(ctg)
    }

    n_files_to_process <- sum(ctg$processed)
    n_files_total <- length(ctg)
    write(
        paste(
            "Files to process:", n_files_to_process,
            "of", n_files_total
        ),
        stdout()
    )

    if (n_files_to_process > 0) {

        # Activate height filter from height map
        if (!is.null(opt$lidar_crs)) {
            # Load height map
            height_map <- raster::raster(opt$height_map_path)
            # Get ALS raster
            extents <- raster::raster(raster::extent(ctg), crs = opt$lidar_crs)
            # Transform extents to WGS84
            extents <- raster::projectExtent(
                extents,
                crs = "+proj=longlat +datum=WGS84 +no_defs"
            )@extent
            # Crop tree height map values from tree height map
            height_map <- raster::crop(height_map, extents)
            # Determine max filter value (either 10% above max value or more
            #  than 5m, whichever is higher)
            filter_height <- max(c(
                height_map@data@max * 1.1,
                height_map@data@max + 5
            ))
            lidR::opt_filter(ctg) <- paste("-drop_z_above", filter_height)
            write(
                paste("\nSet max filter height to", filter_height, ".\n"),
                stdout()
            )
        }

        # set buffer to min possible value
        lidR::opt_chunk_buffer(ctg) <- opt$filter_gridsize
        lidR::opt_laz_compression(ctg) <- TRUE # compress output files

        # Set reference filter quantile
        OPT_FILTER_QUANTILE <- opt$filter_quantile
        # Ensure files are spatially indexed to greatly speed up computations
        ensure_lax_index(ctg, verbose = TRUE)
        # Perform noise filtering
        ctg <- filter_noise(ctg,
            sensitivity = opt$filter_sensitivity,
            grid_size = opt$filter_gridsize
        )
    }
} else {
    write("Skipping noise-filter.")
}

tmp_ <- timestamp()
tmp_ <- time_since_first_timestamp(write_output = TRUE)
write("Success.", stdout())