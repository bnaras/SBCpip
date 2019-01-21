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


## Satisfy R package requirements
## utils::globalVariables( names = c("a", "b", "c")) etc.


#'
#' Check if model parameters have changed in two configurations.
#'
#' @description We only consider those parameters that can be changed
#'     by the webapp in detecting change. This is used to decide if
#'     model needs to be rebuilt
#' @param config1 first list
#' @param config2 second list
#' @return TRUE or FALSE
#'
model_config_changed <- function(config1, config2) {

    ## This can happen the first time.
    if (is.null(config1) || is.null(config2))
        return(TRUE)

    ## Any change to this function has to be matched with the webapp
    ## and vice-versa
    effective_components <- c("c0",
                              "min_inventory",
                              "history_window",
                              "penalty_factor",
                              "start",
                              "model_update_frequency")
    !identical(config1[effective_components], config2[effective_components])
}

