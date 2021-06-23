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
    make_option(c("--save_prefix"),
        type = "character",
        default = "",
        help = "Prefix to use when saving files",
        metavar = "character"
    ),
    make_option(c("--perform_check"),
        type = "logical",
        default = TRUE,
        help = "Whether to perform a deep check on the files",
        metavar = "bool"
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
    make_option(c("--filter_quantile"),
        type = "double",
        default = 0.95,
        help = "Quantile to use for as reference for height noise filtering.",
        metavar = "number"
    ),
    make_option(c("--filter_sensitivity"),
        type = "double",
        default = 1.1,
        help = "Points higher than `sensitivity`*`height at filter_quantile`
            will be filtered",
        metavar = "number"
    ),
    make_option(c("--filter_gridsize"),
        type = "integer",
        default = 10,
        help = "Grid size at which the filter quantile will be computed",
        metavar = "number"
    ),
    make_option(c("--classify_ground"),
        type = "logical",
        default = TRUE,
        help = "Whether to classify the ground. If FALSE, uses existing Classifiation.",
        metavar = "bool"
    ),
    make_option(c("--seed"),
        type = "integer",
        default = 32456,
        help = "Random seed for stochastic computations",
        metavar = "number"
    )
)

# Import custom scripts
r_script_path <- "/home/users/svm/Code/gedi_biomass_mapping/src/processing/"
timestamp_path <- paste0(r_script_path, "timestamp.R")
source(timestamp_path)
las_clean_path <- paste0(r_script_path, "las_clean.R")
source(las_clean_path)
las_indexing_path <- paste0(r_script_path, "las_indexing.R")
source(las_indexing_path)

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
lidR::opt_laz_compression(ctg) <- opt$compress_intermediates # Save output files in compressed format
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


# Step 1: Classify ground ===================================================
write(paste("\n", timestamp()$now, ": ... Classifying ground ...\n"), stdout())

if (opt$classify_ground) {
    if (opt$save_intermediates) {
        lidR::opt_output_files(ctg) <- paste0(
            opt$save_path,
            "/csf_ground/",
            opt$save_prefix,
            "{ID}_{XLEFT}_{YBOTTOM}"
        )
    } else {
        lidR::opt_output_files(ctg) <- paste0(
            tempdir(), "/{ID}_{XLEFT}_{YBOTTOM}"
        )
    }

    # Ensure files are spatially indexed to greatly speed up computations
    ensure_lax_index(ctg, verbose = TRUE)
    # Perform ground classification with cloth simulation function
    ctg <- lidR::classify_ground(ctg,
        algorithm = lidR::csf(),
        last_returns = TRUE
    )
} else {
    write("Using pre-classified ground.")
}



# Step 2: Perform height normalisation ======================================
write(paste("\n", timestamp()$now, ": ... Normalising heights ...\n"), stdout())

if (opt$save_intermediates) {
    lidR::opt_output_files(ctg) <- paste0(
        opt$save_path,
        "/normalised/",
        opt$save_prefix,
        "{ID}_{XLEFT}_{YBOTTOM}"
    )
} else {
    lidR::opt_output_files(ctg) <- paste0(
        tempdir(), "/{ID}_{XLEFT}_{YBOTTOM}"
    )
}

# Ensure files are spatially indexed to greatly speed up computations
ensure_lax_index(ctg, verbose = TRUE)
# Normalise heights
ctg <- lidR::normalize_height(ctg, algorithm = lidR::tin())


# Step 3: Filter noise ======================================================
write(paste("\n", timestamp()$now, ": ... Filtering noise ...\n"), stdout())

lidR::opt_output_files(ctg) <- paste0(
    opt$save_path,
    "/processed/",
    opt$save_prefix,
    "{ID}_{XLEFT}_{YBOTTOM}"
)
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

tmp_ <- timestamp()
tmp_ <- time_since_first_timestamp(write_output = TRUE)
write("Success.", stdout())