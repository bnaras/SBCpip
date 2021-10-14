#' Run the Stanford Blood Center dashboard app
#' @importFrom shiny runApp
#' @export
sbc_dashboard <- function(){
    shiny::runApp(system.file("webapps", "dashboard", package = "SBCpip"))
}

