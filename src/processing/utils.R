N_CORES <- parallel::detectCores()
n_workers <- NULL

check_for_processed_files <- function(ctg, save_path, postfix) {
    # Find all existing files
    glob_pattern <- paste0("*", postfix)
    existing_files <- list.files(save_path, pattern = glob_pattern)
    # print(paste("Found", length(existing_files), "existing files."))

    # Extract filenames without extension
    regex_pattern <- stringr::regex(paste0("(.*)", postfix, "\\.[a-z]+$"))
    existing_filenames <- stringr::str_match(
        existing_files,
        regex_pattern
    )[, 2]

    # Extract catalog names
    catalog_filenames <- tools::file_path_sans_ext(basename(ctg$filename))

    # Note: All TRUE values will be processed
    to_process <- rep(TRUE, length(catalog_filenames))
    for (i in seq_len(length(catalog_filenames))) {
        to_process[i] <- !(catalog_filenames[[i]] %in% existing_filenames)
    }
    # print(to_process)
    # print(paste("Files to process:", sum(to_process)))
    return(to_process)
}

multi_process <- function(ctg,
                          las_func,
                          save_path,
                          postfix,
                          ...,
                          requested_workers = 10L) {
    # Determine the number of files to process
    ctg$processed <- check_for_processed_files(ctg, save_path, postfix)
    n_files_to_process <- sum(ctg$processed)
    print(paste("Files to process:", n_files_to_process))

    if (n_files_to_process > 0L) {
        # Determine number of workers between 1 and 10 (fewer if fewer files)
        #  Note: this needs to be a global variable bc of the way future works
        n_workers <<- max(
            min(
                requested_workers,
                n_files_to_process,
                N_CORES - 1L
            ),
            1L
        )
        print(paste0("Working with ", n_workers, " workers"))
        # Enable paralellisation with n_worker workers
        future::plan(future::multisession(workers = (n_workers)))
        # Process
        lidR::opt_output_files(ctg) <- paste0(
            save_path,
            "/{ORIGINALFILENAME}",
            postfix
        )
        out_ctg <- las_func(ctg, ...)
        return(out_ctg)
    } else {
        print("Already processed.")
        return(lidR::readLAScatalog(save_path))
    }
}

create_grid <- function(ctg,
                        func,
                        res,
                        save_path,
                        ...,
                        requested_workers = 18L,
                        verbose = TRUE) {
    if (!file.exists(save_path)) {
        # Determine number of workers between 1 and 10 (fewer if fewer files)
        #  Note: this needs to be a global variable bc of the way future works
        n_files_to_process <- length(ctg$filename)
        n_workers <<- max(
            min(
                requested_workers,
                n_files_to_process,
                N_CORES - 1L
            ),
            1L
        )
        if (verbose) print(paste("Working with", n_workers, "workers"))
        # Enable paralellisation with n_worker workers
        future::plan(future::multisession(workers = (n_workers)))

        # TODO: Distribute in LAScatalog and LAS options
        # Configure data to work in memory rather than saving output grids
        #  for each file in the LAS catalog
        lidR::opt_output_files(ctg) <- ""
        # Set buffer region size to 0 (no buffer needed for this operation)
        lidR::opt_chunk_buffer(ctg) <- 0

        if (verbose) {
            print(paste0(
                "Creating grid of resolution ",
                res,
                "m at ",
                save_path
            ))
        }

        # Apply function to catalog in gridded fashion
        out <- lidR::grid_metrics(
            las = ctg,
            func = func,
            res = res,
            ...
        )
        raster::writeRaster(out, filename = save_path)
        return(out)
    } else {
        if (verbose) print(paste("File", save_path, "already exists."))
    }
}

create_grids <- function(ctg,
                         func,
                         res_list,
                         save_dir,
                         save_name_pattern,
                         ...) {
    for (res in res_list) {
        save_name <- paste0(save_name_pattern, "_", res, "m.tif")
        save_path <- paste0(save_dir, save_name)

        create_grid(
            ctg = ctg,
            func = func,
            res = res,
            save_path = save_path,
            ...
        )
    }
}