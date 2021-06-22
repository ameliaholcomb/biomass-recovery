timestamp <- function(write_output = TRUE) {
    if (!exists("TIMESTAMP")) {
        TIMESTAMP <<- list(start = Sys.time(), last = Sys.time())
    }

    # Create a new timepoint
    current <- Sys.time()
    # Measure distance to last timepoint
    diff <- current - TIMESTAMP$last
    # Update most recent timepoint
    TIMESTAMP$last <<- current

    # Optionally, write output
    if (write_output) {
        write(
            paste("\n Time taken:", as.numeric(diff, units = "secs"), "secs\n"),
            stdout()
        )
    }
    return(list(now = current, diff = diff))
}

time_since_first_timestamp <- function(write_output = TRUE) {
    if (!exists("TIMESTAMP")) {
        TIMESTAMP <<- list(start = Sys.time(), last = Sys.time())
        diff <- 0
    } else {
        # Get time difference to first timestamp
        diff <- Sys.time() - TIMESTAMP$start
    }

    # Optionally, write output
    if (write_output) {
        write(
            paste(
                "\n Total time elapsed:",
                as.numeric(diff, units = "secs"),
                "secs\n"
            ),
            stdout()
        )
    }
    return(diff)
}