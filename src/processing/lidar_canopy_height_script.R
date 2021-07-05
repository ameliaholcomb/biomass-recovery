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
    make_option(c("--lidar_crs"),
        type = "character",
        default = NULL,
        help = "Proj4 string for the crs of the lidar data.",
        metavar = "character"
    ),
    make_option(c("--gridsize"),
        type = "double",
        default = 2,
        help = "Grid size at which the metric will be computed",
        metavar = "number"
    ),
    make_option(c("--subcircle"),
        type = "double",
        default = 0.15,
        help = "Radius of the Lidar beam to assume for a more realistic
         simulation of beam width for the canopy height model. The default
         value of 0.15m (=15cm) was selected because the EBA data was chosen
         to have a beam diameter of around 30 cm.",
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
    ),
    make_option(c("--n_workers"),
        type = "integer",
        default = 1L,
        help = "Number of workers to use",
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
r_script_path <- "/home/users/svm/Code/gedi_biomass_mapping/src/processing/"
source(paste0(r_script_path, "timestamp.R"))
source(paste0(r_script_path, "las_indexing.R"))
source(paste0(r_script_path, "las_utils.R"))

# Create first timestamp
tmp_ <- timestamp(write_output = FALSE)

# Parse arguments
opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

if (is.null(opt$lidar_path)) {
    print_help(opt_parser)
    stop("At least one argument must be supplied (lidar path).", call. = FALSE)
}

if (is.null(opt$save_path)) {
    print_help(opt_parser)
    stop("At least one argument must be supplied (save path).", call. = FALSE)
}

# Check if file exists already
save_name <- paste0(
    opt$save_path,
    "/grid_metrics/",
    opt$save_prefix,
    "canopy_height_model_", opt$gridsize, "m.tif"
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

# Set up multiprocessing pool if requesting more workers
if (opt$n_workers > 1L) {
    n_workers <- get_n_workers(opt$n_workers, length(ctg))
    future::plan(future::multisession(workers = (n_workers)))
} else {
    write("Running in non-parallelised mode.", stdout())
}

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
# NB: threshodlds:
#  Thresholds at which to create a triangulated surface model. Models will be
#  stacked to produce a pit-free canopy height model
thresholds <- c(0, 2, 5, 10, 15, 25, 35, 45)

# NB: max_edges:
#  first number: max edge length at for interpolation between all points
#   setting a too low value can result in lots of NaN values.
#  second number: max edge length between triangulated models at higher
#   heights
max_edges <- c(20, 2)

write(
    paste(
        "\nFiles:", length(ctg$filename),
        "\nChunk size:", opt$chunk_size,
        "\nBuffer:", max(opt$buffer, opt$gridsize),
        "\nSave path:", opt$save_path,
        "\nGrid size:", opt$gridsize,
        "\nThresholds:", list(thresholds),
        "\nSubcircle:", opt$subcircle,
        "\nMax edges:", list(max_edges),
        "\nSeed:", opt$seed,
        "\n\n"
    ),
    stdout()
)

# Apply function to catalog in gridded fashion
write(paste(
    "\n", timestamp()$now, ": ... Calculating canopy_height at",
    opt$gridsize, "m resolution ...\n"
), stdout())

# Activate height filter from height map
if (!is.null(opt$lidar_crs)) {
    lidar_crs <- opt$lidar_crs
} else if (!is.na(raster::crs(ctg))) {
    lidar_crs <- raster::crs(ctg)
} else {
    lidar_crs <- NULL
}

if (!is.null(lidar_crs)) {
    # Load height map
    height_map <- raster::raster(opt$height_map_path)
    # Get ALS raster
    extents <- raster::raster(raster::extent(ctg), crs = lidar_crs)
    # Transform extents to WGS84
    extents <- raster::projectExtent(
        extents,
        crs = raster::crs(height_map)
    )@extent
    # Crop tree height map values from tree height map
    tryCatch(
        {
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
        },
        error = function(cond) {
            write(paste("Could not set max height filter.", cond), stdout())
        }
    )
}

# Compute grid canopy height model with specified parameters
out <- lidR::grid_canopy(
    las = ctg,
    res = opt$gridsize,
    algorithm = lidR::pitfree(
        thresholds = thresholds,
        subcircle = opt$subcircle,
        max_edge = max_edges
    )
)
raster::writeRaster(out[[1]], filename = save_name, overwrite = opt$overwrite)
warnings()

tmp_ <- timestamp()
tmp_ <- time_since_first_timestamp(write_output = TRUE)
write("Success.", stdout())