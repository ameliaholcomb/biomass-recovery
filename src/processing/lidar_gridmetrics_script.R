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
    make_option(c("--metric"),
        type = "character",
        default = NULL,
        help = "Metric to compute",
        metavar = "character"
    ),
    make_option(c("--gridsize"),
        type = "integer",
        default = 10,
        help = "Grid size at which the metric will be computed",
        metavar = "number"
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
    make_option(c("--buffer"),
        type = "integer",
        default = 10,
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
    make_option(c("--quantile"),
        type = "double",
        default = NULL,
        help = "Height quantile to compute. Is only used if `metric` is `quantile`,
            otherwise it is ignored. Must be set when using `metric=quantile`.",
        metavar = "number"
    ),
    make_option(c("--check_index"),
        type = "logical",
        default = TRUE,
        help = "Whether to check for .lax index files (create if not existent)",
        metavar = "bool"
    ),
    make_option(c("--overwrite"),
        type = "logical",
        default = FALSE,
        help = "Whether to overwrite a potentially existing file",
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
las_indexing_path <- paste0(r_script_path, "las_indexing.R")
source(las_indexing_path)
las_biomass_path <- paste0(r_script_path, "las_biomass.R")
source(las_biomass_path)

# Create first timestamp
tmp_ <- timestamp(write_output = FALSE)

# Parse arguments
opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)
ALLOWED_METRICS <- list(
    point_density = ~ length(Z) / opt$gridsize^2,
    pulse_density = ~ length(Z) / opt$gridsize^2, # NB: Sets an additional filter below
    ground_point_density = ~ length(Z) / opt$gridsize^2, # NB: Sets an additional filter below
    n_points = ~ length(Z),
    n_pulses = ~ length(Z), # NB: Sets an additional filter below
    n_ground_points = ~ length(Z), # NB: Sets an additional filter below
    max = ~ max(Z),
    standard_dev = ~ sd(Z),
    mask = ~ !is.na(max(Z)),
    mean = ~ mean(Z),
    kurtosis = ~ kurtosis(Z),
    interquartile_range = ~ stats::IQR(Z),
    quantile = ~ stats::quantile(Z, c(opt$quantile)),
    longo_biomass = ~ longo_formula(Z)
)

if (is.null(opt$lidar_path)) {
    print_help(opt_parser)
    stop("At least one argument must be supplied (lidar path).", call. = FALSE)
}

if (is.null(opt$save_path)) {
    print_help(opt_parser)
    stop("At least one argument must be supplied (save path).", call. = FALSE)
}

if (opt$metric == "quantile") {
    if (is.null(opt$quantile)) {
        print_help(opt_parser)
        stop("Must supply `quantile` when using `metric=quantile`.",
            call. = FALSE
        )
    }
} else {
    if (!is.null(opt$quantile)) {
        print_help(opt_parser)
        stop("Setting a non-null quantile is only allowed
        with `metric=quantile`",
            call. = FALSE
        )
    }
}

if (!(opt$metric %in% names(ALLOWED_METRICS))) {
    print_help(opt_parser)
    stop(paste(
        "Metric must be one of the allowed metrics. Must be one of",
        list(names(ALLOWED_METRICS)), "not", opt$metric
    ),
    call. = FALSE
    )
}

# Check if file exists already
save_name <- paste0(
    opt$save_path,
    "/grid_metrics/",
    opt$save_prefix,
    opt$metric, opt$quantile, "_", opt$gridsize, "m.tif"
)
if (file.exists(save_name)) {
    if (!opt$overwrite) {
        stop("File exists already.")
    } else {
        write("File exists. Overwriting.", stdout())
    }
}

# Read in data
write(paste(
    "\n", timestamp(write_output = FALSE)$now,
    ": ... Reading in catalog ...\n"
), stdout())
ctg <- lidR::readLAScatalog(opt$lidar_path)

if (opt$check_index) {
    # Ensure files are spatially indexed to greatly speed up computations
    #  This creates a .lax index if it does not yet exist
    ensure_lax_index(ctg, verbose = TRUE)
}

# Set catalog processing parameters
lidR::opt_select(ctg) <- "*" # Select all variables
lidR::opt_filter(ctg) <- "" # Do not filter any points
lidR::opt_chunk_size(ctg) <- opt$chunk_size # Process by original files
lidR::opt_chunk_buffer(ctg) <- max(opt$buffer, opt$gridsize) # Use a buffer
lidR::opt_progress(ctg) <- TRUE # Show progress
lidR::opt_stop_early(ctg) <- FALSE # Continue upon errors
lidR::opt_output_files(ctg) <- ""

# Summarise processing options and write to output
write(
    paste(
        "\nFiles:", length(ctg$filename),
        "\nChunk size:", opt$chunk_size,
        "\nBuffer:", max(opt$buffer, opt$gridsize),
        "\nSave path:", opt$save_path,
        "\nGrid size:", opt$gridsize,
        "\nSeed:", opt$seed,
        "\n\n"
    ),
    stdout()
)
summary(ctg)

# Apply function to catalog in gridded fashion
write(paste(
    "\n", timestamp()$now, ": ... Calculating", opt$metric, opt$quantile,
    "at", opt$gridsize, "m resolution ...\n"
), stdout())

# Set filter for certain metrics
if (opt$metric %in% c("pulse_density", "n_pulses")) {
    filter <- ~ ReturnNumber == 1L
} else if (opt$metric %in% c("ground_point_density", "n_ground_points")) {
    filter <- ~ Classification == lidR::LASGROUND
} else {
    filter <- NULL
}

# Compute grid metrics with specified parameters
out <- lidR::grid_metrics(
    las = ctg,
    func = ALLOWED_METRICS[[opt$metric]],
    res = opt$gridsize,
    filter = filter
)
raster::writeRaster(out[[1]], filename = save_name, overwrite = opt$overwrite)
warnings()

tmp_ <- timestamp()
tmp_ <- time_since_first_timestamp(write_output = TRUE)
write("Success.", stdout())