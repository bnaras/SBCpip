#' Computing moving average of all past values including last value
#' using a window size
#' @param z the numeric vector
#' @param window_size the integer window size
#' @return a vector of the same length pre-padded with NAs
#' @export
ma <- function(z, window_size = 5L) {
    ## moving average of all z's INCLUDING the last
    sapply(seq_len(length(z)),
           function(i) if (i < window_size) NA else mean(z[seq.int(i - window_size + 1L, i)]))
}

#' Computing moving sum of all past values including last value
#' using a window size
#' @param z the numeric vector
#' @param window_size the integer window size
#' @return a vector of the same length pre-padded with NAs
#' @export
ms <- function(z, window_size = 3L) {
    ## moving average of all z's INCLUDING the last
    sapply(seq_len(length(z)),
           function(i) if (i < window_size) NA else sum(z[seq.int(i - window_size + 1L, i)]))
}

# Helper function to obtain a list of unique dates in the folder following each filename pattern
gather_dates_by_pattern <- function(data_folder, pattern) {
    files <- list.files(path = data_folder, pattern = pattern, full.names = TRUE)
    
    # Extract the date from each filename (assume date immediately follows pattern in YYYY-MM-DD format)
    dates <- sapply(files, function(x) {
        date <- substring(x, first = stringr::str_length(data_folder) + stringr::str_length(pattern) + 1L, 
                          last = stringr::str_length(data_folder) + stringr::str_length(pattern) + 10L)
    })
    unique(dates)
}


