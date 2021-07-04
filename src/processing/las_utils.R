unprocessed_files <- function(ctg, tolerance = 1) {

    # Initialise by activating processing for all files
    is_unprocessed <- rep(TRUE, length(ctg))

    # Extract spatial information of catalog
    out_dir <- dirname(ctg@output_options$output_files)
    out_ctg <- lidR::readLAScatalog(out_dir)
    processed_polygon <- lidR::as.spatial(out_ctg)

    # For each file in the catalog to process, check whether
    #  it is already contained in the processed output
    for (i in 1:length(ctg)) {

        # Extract the polygon shape of each file in ctg
        single_poly <- as(
            raster::extent(
                ctg@polygons[[i]]@Polygons[[1]]@coords
            ) - tolerance,
            "SpatialPolygons"
        )

        # Update the input ctg files that should be processed as only
        #  those which are not already contained in the processed polygon
        is_unprocessed[[i]] <- !rgeos::gContains(processed_polygon, single_poly)
    }
    return(is_unprocessed)
}

reload_ctg_output <- function(ctg) {
    out_dir <- dirname(ctg@output_options$output_files)
    reloaded_ctg <- lidR::readLAScatalog(out_dir)

    # Set processing options as for ctg
    lidR::opt_select(reloaded_ctg) <- lidR::opt_select(ctg)
    lidR::opt_filter(reloaded_ctg) <- lidR::opt_filter(ctg)
    lidR::opt_chunk_size(reloaded_ctg) <- lidR::opt_chunk_size(ctg)
    lidR::opt_chunk_buffer(reloaded_ctg) <- lidR::opt_chunk_buffer(ctg)
    lidR::opt_laz_compression(reloaded_ctg) <- lidR::opt_laz_compression(ctg)
    lidR::opt_progress(reloaded_ctg) <- lidR::opt_progress(ctg)
    lidR::opt_stop_early(reloaded_ctg) <- lidR::opt_stop_early(ctg)

    return(reloaded_ctg)
}