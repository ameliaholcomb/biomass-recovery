
ensure_lax_index <- function(ctg, verbose = TRUE) {
    # Make sure only LAScatalog objects are passed
    if (!is(ctg, "LAScatalog")) {
        stop("Invalid input. `ctg` must be a LAScatalog")
    }

    # Check if index already exists
    if (!lidR::is.indexed(ctg)) {
        if (verbose) {
            write(
                "Indexing catalogue with spatial index and
                saving to .lax files.",
                stdout()
            )
        }
        lidR:::catalog_laxindex(ctg)
    } else {
        if (verbose) {
            write("Catalogue is already indexed.", stdout())
        }
    }
}