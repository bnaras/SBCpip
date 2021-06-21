.onLoad <- function(libname, pkgname) {
    today <- as.character(Sys.Date(), format = "%Y-%m-%d-%H:%M")
    ## invisible(
    ##     loggit::set_logfile(
    ##                 file.path(getwd(),
    ##                           sprintf("%s_%s.json", libname, today))
    ##               , confirm = FALSE
    ##             )
    ## )
    invisible(
        loggit::set_logfile(
                    file.path(getwd(),
                              sprintf(get_SBC_config()$log_filename_prefix, today))
                  , confirm = FALSE
                )
    )
}


